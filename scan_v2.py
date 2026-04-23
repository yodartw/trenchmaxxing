"""
Enhanced /scan command — research view for historical winners.
- Fetches first 60min of buyers
- Scores each buyer's 14-day general history
- Ranks by combination of entry earliness (70%) + 14d P&L (30%)
- NO auto-save to DB — use /promote to curate
"""

import asyncio
import logging
from scoring import fetch_wallet_history, classify_wallet

logger = logging.getLogger(__name__)

SCORE_LIMIT_PER_SCAN = 50
WINDOW_SECONDS = 3600  # 60 minutes


def compute_research_score(wallet_idx, total_wallets, pnl_rank, total_scored):
    """
    Combined score: 70% entry earliness, 30% P&L rank.
    wallet_idx: 0 = earliest buyer, total_wallets-1 = latest
    pnl_rank: 0 = worst P&L, total_scored-1 = best
    """
    earliness = 100 * (1 - wallet_idx / max(1, total_wallets - 1))
    pnl_score = 100 * (pnl_rank / max(1, total_scored - 1)) if total_scored > 1 else 50
    return round(0.7 * earliness + 0.3 * pnl_score, 1)


async def score_buyers_bulk(buyers_ordered, max_to_score=50):
    """
    Score up to max_to_score buyers concurrently. Returns dict: addr -> {scored, history}
    """
    to_score = buyers_ordered[:max_to_score]
    scored_map = {}

    async def _score_one(addr):
        try:
            history = await fetch_wallet_history(addr, days=14)
            scored = classify_wallet(history)
            return addr, {"scored": scored, "history": history}
        except Exception as e:
            logger.warning(f"Score failed for {addr[:10]}: {e}")
            return addr, None

    tasks = [_score_one(addr) for addr in to_score]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    for addr, data in results:
        if data:
            scored_map[addr] = data
    return scored_map


async def scan_command_v2(update, context, supabase, fetch_early_buyers, detect_bundles):
    """Research-mode /scan: show ranked buyers, no DB writes."""
    if not context.args:
        await update.message.reply_text("Usage: /scan <mint_address>")
        return

    mint = context.args[0].strip()
    if len(mint) < 30 or len(mint) > 50:
        await update.message.reply_text("That doesn't look like a Solana mint address.")
        return

    await update.message.reply_text(
        f"Scanning {mint[:10]}... first 60min of buyers, scoring up to {SCORE_LIMIT_PER_SCAN}..."
    )

    buyers = await fetch_early_buyers(mint, window_seconds=WINDOW_SECONDS)
    if not buyers:
        await update.message.reply_text("No early buyers found or API error.")
        return

    bundles = detect_bundles(buyers)

    # Dedupe: earliest buy per wallet
    unique_buyers = {}
    for b in buyers:
        w = b["buyer"]
        if w not in unique_buyers or b["block_time"] < unique_buyers[w]["block_time"]:
            unique_buyers[w] = b

    wallet_to_bundle = {}
    for tx_sig, wallets in bundles.items():
        group_id = f"bundle:{mint[:8]}:{tx_sig[:8]}"
        for w in wallets:
            wallet_to_bundle[w] = group_id

    # Cross-reference DB for attribution + existing scores
    addresses = list(unique_buyers.keys())
    existing = (
        supabase.table("wallets")
        .select("address, classification, smart_money_score, insider_score, cabals(name), cabal_members(x_handle)")
        .in_("address", addresses)
        .execute()
    ).data or []
    existing_map = {w["address"]: w for w in existing}

    # Token symbol
    sym_result = supabase.table("tokens").select("symbol").eq("address", mint).eq("chain", "sol").execute()
    token_symbol = sym_result.data[0]["symbol"] if sym_result.data else mint[:6]

    # Order buyers by entry time (earliest first)
    ordered_buyers = sorted(unique_buyers.items(), key=lambda x: x[1]["block_time"])
    ordered_addrs = [addr for addr, _ in ordered_buyers]

    # Score only wallets NOT already in DB with recent score
    to_score = []
    for addr in ordered_addrs:
        if addr in existing_map and existing_map[addr].get("smart_money_score") is not None:
            continue
        to_score.append(addr)

    await update.message.reply_text(
        f"Found {len(unique_buyers)} buyers. Scoring {min(len(to_score), SCORE_LIMIT_PER_SCAN)} new wallets (14d history)..."
    )
    scored_new = await score_buyers_bulk(to_score, SCORE_LIMIT_PER_SCAN)

    # Build unified list with P&L and classification for each wallet
    entries = []  # list of dicts, one per wallet
    for idx, (addr, buyer_info) in enumerate(ordered_buyers):
        existing_rec = existing_map.get(addr)
        score_data = scored_new.get(addr)

        if score_data:
            history = score_data["history"]
            scored = score_data["scored"]
            pnl = history["net_sol_pnl"]
            trades = history["trade_count"]
            unique_coins = history["unique_coins"]
            winners = history["winners"]
            classification = scored["classification"]
            sm_score = scored["smart_money_score"]
            tier = scored.get("tier")
        elif existing_rec:
            pnl = None
            trades = None
            unique_coins = None
            winners = None
            classification = existing_rec.get("classification") or "unscored"
            sm_score = existing_rec.get("smart_money_score") or 0
            tier = existing_rec.get("tier")
        else:
            # Neither scored nor in DB (exceeded scan limit)
            pnl = None
            trades = None
            unique_coins = None
            winners = None
            classification = "unscored"
            sm_score = 0
            tier = None

        entries.append({
            "addr": addr,
            "buyer_info": buyer_info,
            "entry_idx": idx,
            "pnl": pnl,
            "trades": trades,
            "unique_coins": unique_coins,
            "winners": winners,
            "classification": classification,
            "sm_score": sm_score,
            "tier": tier,
            "existing": existing_rec,
            "bundle_id": wallet_to_bundle.get(addr),
        })

    # Rank entries that have P&L data
    entries_with_pnl = [e for e in entries if e["pnl"] is not None]
    # Sort by P&L asc to assign pnl_rank (index = rank, higher index = better P&L)
    entries_with_pnl_sorted = sorted(entries_with_pnl, key=lambda e: e["pnl"])
    pnl_rank_map = {e["addr"]: idx for idx, e in enumerate(entries_with_pnl_sorted)}

    # Compute research score for every entry
    total_buyers = len(entries)
    total_with_pnl = len(entries_with_pnl)
    for e in entries:
        if e["pnl"] is None:
            # Unscored: give 0 on P&L dimension, keep earliness
            earliness = 100 * (1 - e["entry_idx"] / max(1, total_buyers - 1))
            e["research_score"] = round(0.7 * earliness + 0.3 * 0, 1)
        else:
            pnl_rank = pnl_rank_map[e["addr"]]
            e["research_score"] = compute_research_score(
                e["entry_idx"], total_buyers, pnl_rank, total_with_pnl
            )

    # Sort by research score desc
    entries.sort(key=lambda e: e["research_score"], reverse=True)

    # Hard filter: only smart_money (tiered) and insider classifications
    qualifying = [
        e for e in entries
        if e["classification"] in ("smart_money", "insider")
    ]

    # Track filtered counts by category
    category_counts = {"smart_money": 0, "insider": 0, "volume_scalper": 0, "noise": 0, "dormant": 0, "unscored": 0}
    for e in entries:
        cls = e.get("classification") or "unscored"
        category_counts[cls] = category_counts.get(cls, 0) + 1

    # Debug: hit rate distribution
    debug_stats = {
        "wallets_with_activity": 0,
        "wallets_30plus_hit": 0,
        "wallets_20_30_hit": 0,
        "wallets_10_20_hit": 0,
        "wallets_under_10_hit": 0,
        "wallets_profitable": 0,
        "wallets_losing": 0,
        "highest_hit_rate": 0,
        "highest_pnl": 0,
    }
    for e in entries:
        if e["trades"] and e["trades"] >= 5 and e["unique_coins"] and e["unique_coins"] >= 3:
            debug_stats["wallets_with_activity"] += 1
            hit_rate = (e["winners"] / e["unique_coins"]) if e["unique_coins"] else 0
            if hit_rate >= 0.3:
                debug_stats["wallets_30plus_hit"] += 1
            elif hit_rate >= 0.2:
                debug_stats["wallets_20_30_hit"] += 1
            elif hit_rate >= 0.1:
                debug_stats["wallets_10_20_hit"] += 1
            else:
                debug_stats["wallets_under_10_hit"] += 1

            if e["pnl"] and e["pnl"] > 0:
                debug_stats["wallets_profitable"] += 1
            elif e["pnl"] and e["pnl"] < 0:
                debug_stats["wallets_losing"] += 1

            if hit_rate > debug_stats["highest_hit_rate"]:
                debug_stats["highest_hit_rate"] = hit_rate
            if e["pnl"] and e["pnl"] > debug_stats["highest_pnl"]:
                debug_stats["highest_pnl"] = e["pnl"]

    # Build output
    lines = [
        f"🔍 <b>/scan ${token_symbol}</b>",
        f"<code>{mint}</code>",
        f"{len(unique_buyers)} buyers · 60min window · {len(bundles)} bundle groups",
        f"Scored {len(scored_new)} new · {len(existing_map)} already in DB",
        "",
    ]

    # Always show debug stats, even when nothing qualifies
    lines.append(f"<b>🔬 Debug (hit rate distribution):</b>")
    lines.append(f"  Wallets with 5+ trades: {debug_stats['wallets_with_activity']}")
    lines.append(f"  30%+ hit rate: {debug_stats['wallets_30plus_hit']}")
    lines.append(f"  20-30% hit rate: {debug_stats['wallets_20_30_hit']}")
    lines.append(f"  10-20% hit rate: {debug_stats['wallets_10_20_hit']}")
    lines.append(f"  &lt;10% hit rate: {debug_stats['wallets_under_10_hit']}")
    lines.append(f"  Profitable: {debug_stats['wallets_profitable']} · Losing: {debug_stats['wallets_losing']}")
    lines.append(f"  Max hit rate: {debug_stats['highest_hit_rate']*100:.0f}%")
    lines.append(f"  Max P&L: +{debug_stats['highest_pnl']:.1f} SOL")
    lines.append("")

    # Show top 20 entries
    if not qualifying:
        lines.append("<b>⚠️ No qualifying buyers found.</b>")
        lines.append("")
        lines.append("None of the early buyers had:")
        lines.append("  • 5+ trades in last 14d")
        lines.append("  • 3+ unique coins traded")
        lines.append("  • Positive P&L")
        lines.append("  • 20%+ hit rate")
        lines.append("")
        lines.append("This coin may be pre-viral or not cabal-driven.")
        await update.message.reply_text("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)
        return

    lines.append(f"<b>Ranked by research score (entry 70% + 14d P&L 30%)</b>")
    lines.append("")

    display_count = min(20, len(qualifying))
    for i, e in enumerate(qualifying[:display_count]):
        addr = e["addr"]
        entry_min = (e["buyer_info"]["block_time"] - ordered_buyers[0][1]["block_time"]) / 60

        # Tags
        tags = []
        if e["existing"] and e["existing"].get("cabals"):
            cabal = e["existing"]["cabals"]["name"]
            handle = e["existing"].get("cabal_members", {})
            handle_str = handle.get("x_handle", "") if handle else ""
            tags.append(f"⚡ {cabal}{' ' + handle_str if handle_str else ''}")
        elif e["classification"] == "smart_money":
            tier = e.get("tier")
            tier_str = f" [{tier}]" if tier else ""
            tags.append(f"🎯 smart_money{tier_str} · sm:{e['sm_score']}")
        elif e["classification"] == "insider":
            tags.append(f"🔒 insider")
        elif e["classification"] == "volume_scalper":
            tags.append(f"⚡ volume_scalper")
        elif e["classification"] == "noise":
            tags.append(f"🤖 noise")
        elif e["classification"] == "dormant":
            tags.append(f"💤 dormant")
        elif e["classification"] == "unscored":
            tags.append(f"❓ unscored")

        if e["bundle_id"]:
            tags.append("🔗")
        tag_str = " · ".join(tags)

        sol_in = e["buyer_info"]["sol_in"]

        # Header line: rank + score + tag
        lines.append(f"<b>#{i+1} · {e['research_score']}</b> · {tag_str}")

        # Entry
        lines.append(f"    Entry: {sol_in:.2f} SOL @ {entry_min:.1f}min")

        # Stats
        if e["pnl"] is not None:
            hit_rate = (e["winners"] / e["unique_coins"] * 100) if e["unique_coins"] else 0
            lines.append(
                f"    14d: {e['pnl']:+.1f} SOL · {e['trades']}t · {e['unique_coins']}c · {hit_rate:.0f}% hit"
            )

        # Full copyable address
        lines.append(f"    <code>{addr}</code>")

        # Solscan + Axiom links
        solscan = f"https://solscan.io/account/{addr}"
        axiom = f"https://axiom.trade/@{addr}"
        lines.append(f'    <a href="{solscan}">solscan</a> · <a href="{axiom}">axiom</a>')

        lines.append("")

    lines.append("")
    lines.append(f"<b>Filtered:</b>")
    if category_counts.get("volume_scalper", 0) > 0:
        lines.append(f"  ⚡ {category_counts['volume_scalper']} volume_scalpers (profitable but too active)")
    if category_counts.get("noise", 0) > 0:
        lines.append(f"  🤖 {category_counts['noise']} noise (bots / losing high-vol)")
    if category_counts.get("dormant", 0) > 0:
        lines.append(f"  💤 {category_counts['dormant']} dormant / low activity")
    if category_counts.get("unscored", 0) > 0:
        lines.append(f"  ❓ {category_counts['unscored']} unscored (over scan limit)")
    lines.append("")

    lines.append(f"<b>To save a wallet:</b> /promote <code>&lt;address&gt;</code>")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)
