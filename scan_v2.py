"""
Enhanced /scan command.
Fetches early buyers, scores them via scoring.py, filters noise,
displays only signal wallets (attributed / smart_money / insider).
"""

import asyncio
import logging
import datetime
from scoring import fetch_wallet_history, classify_wallet

logger = logging.getLogger(__name__)

# Tuning knobs
SCORE_LIMIT_PER_SCAN = 50      # Max wallets to score per /scan (Helius budget)
MIN_DISPLAY_SCORE = 50          # Smart money score threshold for display


async def score_buyers_bulk(buyers_dict, existing_map, max_to_score=50):
    """
    Score up to max_to_score unknown buyers concurrently.
    Returns: {wallet_address: {scored_dict, history}}
    """
    # Only score buyers NOT already in DB (attributed or known-unknown)
    # Prioritize by entry size (larger SOL in = more likely smart money)
    unscored = [
        (addr, info) for addr, info in buyers_dict.items()
        if addr not in existing_map
    ]
    unscored.sort(key=lambda x: x[1].get("sol_in", 0), reverse=True)
    unscored = unscored[:max_to_score]

    scored_map = {}

    async def _score_one(addr):
        try:
            history = await fetch_wallet_history(addr, days=14)
            scored = classify_wallet(history)
            return addr, {"scored": scored, "history": history}
        except Exception as e:
            logger.warning(f"Score failed for {addr[:10]}: {e}")
            return addr, None

    tasks = [_score_one(addr) for addr, _ in unscored]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    for addr, data in results:
        if data:
            scored_map[addr] = data

    return scored_map


async def scan_command_v2(update, context, supabase, fetch_early_buyers, detect_bundles):
    """Enhanced /scan with scoring integration."""
    if not context.args:
        await update.message.reply_text("Usage: /scan <mint_address>")
        return

    mint = context.args[0].strip()
    if len(mint) < 30 or len(mint) > 50:
        await update.message.reply_text("That doesn't look like a Solana mint address.")
        return

    await update.message.reply_text(f"Scanning {mint[:10]}... pulling buyers + scoring...")

    buyers = await fetch_early_buyers(mint, window_seconds=300)
    if not buyers:
        await update.message.reply_text("No early buyers found or API error.")
        return

    bundles = detect_bundles(buyers)

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

    # Cross-reference against DB
    addresses = list(unique_buyers.keys())
    existing = (
        supabase.table("wallets")
        .select("address, quality_tier, classification, smart_money_score, cabals(name), cabal_members(x_handle)")
        .in_("address", addresses)
        .execute()
    ).data or []
    existing_map = {w["address"]: w for w in existing}

    # Resolve token symbol
    sym_result = supabase.table("tokens").select("symbol").eq("address", mint).eq("chain", "sol").execute()
    token_symbol = sym_result.data[0]["symbol"] if sym_result.data else mint[:6]

    # Score new buyers (up to limit)
    await update.message.reply_text(
        f"Found {len(unique_buyers)} buyers. Scoring up to {SCORE_LIMIT_PER_SCAN} new wallets..."
    )
    scored_new = await score_buyers_bulk(unique_buyers, existing_map, SCORE_LIMIT_PER_SCAN)

    # Categorize all buyers
    attributed = []
    smart_money = []
    insiders = []
    filtered_out = 0
    saved_count = 0

    for addr, buyer_info in unique_buyers.items():
        bundle_group = wallet_to_bundle.get(addr)

        if addr in existing_map:
            w = existing_map[addr]
            if w.get("cabals"):
                attributed.append((addr, buyer_info, w))
            elif w.get("classification") == "smart_money":
                smart_money.append((addr, buyer_info, w, None))
            elif w.get("classification") == "insider" or bundle_group:
                insiders.append((addr, buyer_info, w, None))
            # else: already in DB but not promising, skip
            continue

        # New wallet — check score
        score_data = scored_new.get(addr)
        if not score_data:
            filtered_out += 1
            continue

        scored = score_data["scored"]
        history = score_data["history"]
        classification = scored["classification"]

        if classification == "smart_money":
            smart_money.append((addr, buyer_info, None, score_data))
        elif classification == "insider" or (bundle_group and scored["insider_score"] >= 30):
            insiders.append((addr, buyer_info, None, score_data))
        else:
            # Noise / dormant — don't save, don't display
            filtered_out += 1
            continue

        # Save the ones we kept
        try:
            label = f"scan:{token_symbol} {buyer_info['sol_in']:.1f}SOL sm:{scored['smart_money_score']}"
            if bundle_group:
                label = f"{bundle_group} {label}"

            supabase.table("wallets").insert({
                "chain": "sol",
                "address": addr,
                "wallet_type": "unknown",
                "confidence": "suspected",
                "label": label,
                "discovered_via": f"scan:{token_symbol}",
                "quality_tier": "raw",
                "bundle_group_id": bundle_group,
                "smart_money_score": scored["smart_money_score"],
                "insider_score": scored["insider_score"],
                "classification": classification,
                "last_scored_at": datetime.datetime.utcnow().isoformat(),
                "trade_count_30d": history["trade_count"],
                "winners_2x": history["winners"],
            }).execute()
            saved_count += 1
        except Exception as e:
            if "duplicate" not in str(e).lower():
                logger.warning(f"Scan insert failed {addr[:10]}: {e}")

    # Unscored = buyers beyond SCORE_LIMIT_PER_SCAN; count them but don't display
    unscored_count = max(0, len(unique_buyers) - len(existing_map) - len(scored_new))

    # Build output
    lines = [
        f"🔍 <b>/scan ${token_symbol}</b>",
        f"<code>{mint}</code>",
        f"{len(unique_buyers)} buyers · {len(bundles)} bundles · scored {len(scored_new)}",
        "",
    ]

    if attributed:
        lines.append(f"<b>⚡ Attributed ({len(attributed)}):</b>")
        for addr, info, w in sorted(attributed, key=lambda x: x[1]["block_time"])[:10]:
            cabal = w["cabals"]["name"] if w.get("cabals") else "?"
            handle = w["cabal_members"]["x_handle"] if w.get("cabal_members") else ""
            bundle_str = " 🔗" if wallet_to_bundle.get(addr) else ""
            lines.append(f"  {cabal} {handle}{bundle_str} · {info['sol_in']:.2f} SOL · <code>{addr[:8]}...</code>")
        lines.append("")

    if smart_money:
        lines.append(f"<b>🎯 Smart Money ({len(smart_money)}):</b>")
        # Sort by smart_money_score desc
        def sm_score(entry):
            _, _, db_w, score_data = entry
            if score_data:
                return score_data["scored"]["smart_money_score"]
            return db_w.get("smart_money_score") or 0
        for addr, info, db_w, score_data in sorted(smart_money, key=sm_score, reverse=True)[:10]:
            bundle_str = " 🔗" if wallet_to_bundle.get(addr) else ""
            if score_data:
                s = score_data["scored"]["smart_money_score"]
                pnl = score_data["history"]["net_sol_pnl"]
                trades = score_data["history"]["trade_count"]
                stats = f"sm:{s} · {trades}t · {pnl:+.1f}SOL 14d"
            else:
                s = db_w.get("smart_money_score") or 0
                stats = f"sm:{s}"
            lines.append(f"  {info['sol_in']:.2f} SOL · <code>{addr[:8]}...</code>{bundle_str} · {stats}")
        lines.append("")

    if insiders:
        lines.append(f"<b>🔒 Insider / Bundle ({len(insiders)}):</b>")
        for addr, info, db_w, score_data in sorted(insiders, key=lambda x: x[1]["block_time"])[:8]:
            bundle_str = " 🔗" if wallet_to_bundle.get(addr) else ""
            if score_data:
                ins = score_data["scored"]["insider_score"]
                stats = f"ins:{ins}"
            else:
                ins = db_w.get("insider_score") if db_w else 0
                stats = f"ins:{ins or '?'}"
            lines.append(f"  {info['sol_in']:.2f} SOL · <code>{addr[:8]}...</code>{bundle_str} · {stats}")
        lines.append("")

    # Summary footer
    lines.append(f"<b>📊 Summary:</b>")
    lines.append(f"  Saved to DB: {saved_count}")
    lines.append(f"  Filtered (noise/dormant): {filtered_out}")
    if unscored_count > 0:
        lines.append(f"  Unscored (over limit): {unscored_count}")
    if bundles:
        lines.append(f"  Bundle groups: {len(bundles)}")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)
