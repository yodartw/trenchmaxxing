"""
Enhanced /scan command — research view for historical winners.
- Fetches first 30min of buyers
- Scores each buyer's 14-day general history
- Ranks by combination of entry earliness (70%) + 14d P&L (30%)
- NO auto-save to DB — use /promote to curate
"""

import asyncio
import logging
from scoring import fetch_wallet_history, classify_wallet

logger = logging.getLogger(__name__)

SCORE_LIMIT_PER_SCAN = 50
WINDOW_SECONDS = 1800  # 30 minutes


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
        f"Scanning {mint[:10]}... first 30min of buyers, scoring up to {SCORE_LIMIT_PER_SCAN}..."
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
        elif existing_rec:
            pnl = None
            trades = None
            unique_coins = None
            winners = None
            classification = existing_rec.get("classification") or "unscored"
            sm_score = existing_rec.get("smart_money_score") or 0
        else:
            # Neither scored nor in DB (exceeded scan limit)
            pnl = None
            trades = None
            unique_coins = None
            winners = None
            classification = "unscored"
            sm_score = 0

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

    # Hard filter: must have real history AND positive P&L
    qualifying = [
        e for e in entries
        if e["trades"] is not None
        and e["trades"] >= 5
        and e["unique_coins"] is not None
        and e["unique_coins"] >= 3
        and e["pnl"] is not None
        and e["pnl"] > 0
    ]

    # Build output
    lines = [
        f"🔍 <b>/scan ${token_symbol}</b>",
        f"<code>{mint}</code>",
        f"{len(unique_buyers)} buyers · 30min window · {len(bundles)} bundle groups",
        f"Scored {len(scored_new)} new · {len(existing_map)} already in DB",
        "",
    ]

    # Show top 20 entries
    if not qualifying:
        lines.append("<b>⚠️ No qualifying buyers found.</b>")
        lines.append("")
        lines.append("None of the early buyers had:")
        lines.append("  • 5+ trades in last 14d")
        lines.append("  • 3+ unique coins traded")
        lines.append("  • Positive P&L")
        lines.append("")
        lines.append("This coin may be pre-viral or not cabal-driven.")
        await update.message.reply_text("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)
        return

    lines.append(f"<b>Ranked by research score (entry 70% + 14d P&L 30%)</b>")
    lines.append("")

    display_count = min(20, len(qualifying))
    for i, e in enumerate(qualifying[:display_count]):
        addr_short = e["addr"][:8]
        entry_min = (e["buyer_info"]["block_time"] - ordered_buyers[0][1]["block_time"]) / 60

        # Tags
        tags = []
        if e["existing"] and e["existing"].get("cabals"):
            cabal = e["existing"]["cabals"]["name"]
            handle = e["existing"].get("cabal_members", {})
            handle_str = handle.get("x_handle", "") if handle else ""
            tags.append(f"⚡ {cabal}{' ' + handle_str if handle_str else ''}")
        elif e["classification"] == "smart_money":
            tags.append(f"🎯 sm:{e['sm_score']}")
        elif e["classification"] == "insider":
            tags.append(f"🔒 insider")
        elif e["classification"] == "noise":
            tags.append(f"🤖 noise")
        elif e["classification"] == "dormant":
            tags.append(f"💤 dormant")
        elif e["classification"] == "unscored":
            tags.append(f"❓ unscored")

        if e["bundle_id"]:
            tags.append("🔗")

        tag_str = " · ".join(tags)

        # Core line
        sol_in = e["buyer_info"]["sol_in"]
        lines.append(
            f"<b>#{i+1} · {e['research_score']}</b> · <code>{addr_short}...</code> · {tag_str}"
        )
        lines.append(f"    Entry: {sol_in:.2f} SOL @ {entry_min:.1f}min")

        # Stats line (only if scored)
        if e["pnl"] is not None:
            hit_rate = (e["winners"] / e["unique_coins"] * 100) if e["unique_coins"] else 0
            lines.append(
                f"    14d: {e['pnl']:+.1f} SOL · {e['trades']}t · {e['unique_coins']}c · {hit_rate:.0f}% hit"
            )
        lines.append("")

    filtered_out = len(entries) - len(qualifying)
    if filtered_out > 0:
        lines.append(f"<i>{filtered_out} buyers filtered (dormant / no activity / negative P&L)</i>")
        lines.append("")

    lines.append(f"<b>To save a wallet:</b> /promote <code>&lt;address&gt;</code>")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)
