"""
Wallet scoring system for Trencher.
Pulls 14-day history from Helius and classifies wallets as
smart_money / insider / noise / dormant.
"""

import os
import time
import httpx
import logging
from dotenv import load_dotenv

load_dotenv()
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
HELIUS_BASE = "https://api.helius.xyz/v0"

SKIP_MINTS = {
    "So11111111111111111111111111111111111111112",
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
}

logger = logging.getLogger(__name__)


async def fetch_wallet_history(wallet_address, days=14):
    """Pull swap activity for a wallet over last N days."""
    cutoff = int(time.time()) - (days * 86400)

    url = f"{HELIUS_BASE}/addresses/{wallet_address}/transactions"
    params_base = {"api-key": HELIUS_API_KEY, "limit": 100}

    all_txs = []
    before = None
    for _ in range(10):
        params = dict(params_base)
        if before:
            params["before"] = before
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                r = await client.get(url, params=params)
                r.raise_for_status()
                chunk = r.json()
        except Exception as e:
            logger.warning(f"History fetch failed for {wallet_address[:10]}: {e}")
            break
        if not chunk:
            break
        oldest_in_chunk = chunk[-1].get("timestamp", 0)
        all_txs.extend(chunk)
        before = chunk[-1].get("signature")
        if oldest_in_chunk < cutoff or len(chunk) < 100:
            break

    if not all_txs:
        return _empty_history()

    coins = {}
    in_window_count = 0
    earliest_ts = None

    for tx in all_txs:
        ts = tx.get("timestamp", 0)
        if ts < cutoff:
            continue

        token_transfers = tx.get("tokenTransfers") or []
        account_data = tx.get("accountData") or []

        # Only count tx if it involves a non-stable token transfer for this wallet
        is_swap_like = False
        for tt in token_transfers:
            mint = tt.get("mint")
            if mint in SKIP_MINTS:
                continue
            if tt.get("toUserAccount") == wallet_address or tt.get("fromUserAccount") == wallet_address:
                is_swap_like = True
                break
        if not is_swap_like:
            continue

        in_window_count += 1
        if earliest_ts is None or ts < earliest_ts:
            earliest_ts = ts

        sol_delta = 0
        for ad in account_data:
            if ad.get("account") == wallet_address:
                sol_delta = int(ad.get("nativeBalanceChange", 0)) / 1e9
                break

        for tt in token_transfers:
            mint = tt.get("mint")
            if mint in SKIP_MINTS:
                continue
            to_user = tt.get("toUserAccount")
            from_user = tt.get("fromUserAccount")

            if mint not in coins:
                coins[mint] = {"buys_sol": 0, "sells_sol": 0, "first_buy_ts": ts}

            if to_user == wallet_address and sol_delta < 0:
                coins[mint]["buys_sol"] += abs(sol_delta)
                if ts < coins[mint]["first_buy_ts"]:
                    coins[mint]["first_buy_ts"] = ts
            elif from_user == wallet_address and sol_delta > 0:
                coins[mint]["sells_sol"] += sol_delta

    winners = 0
    losers = 0
    open_positions = 0
    total_buy = 0
    total_sell = 0
    for mint, stats in coins.items():
        buys = stats["buys_sol"]
        sells = stats["sells_sol"]
        if buys <= 0:
            continue
        total_buy += buys
        total_sell += sells
        ratio = sells / buys if buys > 0 else 0
        if sells == 0:
            open_positions += 1
        elif ratio >= 2.0:
            winners += 1
        elif ratio < 0.5:
            losers += 1

    first_trade_days_ago = 0
    if earliest_ts:
        first_trade_days_ago = int((time.time() - earliest_ts) / 86400)

    return {
        "trade_count": in_window_count,
        "unique_coins": len(coins),
        "net_sol_pnl": round(total_sell - total_buy, 3),
        "winners": winners,
        "losers": losers,
        "open_positions": open_positions,
        "first_trade_days_ago": first_trade_days_ago,
    }


def _empty_history():
    return {
        "trade_count": 0,
        "unique_coins": 0,
        "net_sol_pnl": 0.0,
        "winners": 0,
        "losers": 0,
        "open_positions": 0,
        "first_trade_days_ago": 0,
    }


def classify_wallet(history):
    """Score + classify. v4 — tiered, profit-gated, bot-flagging."""
    trade_count = history["trade_count"]
    unique_coins = history["unique_coins"]
    winners = history["winners"]
    losers = history["losers"]
    net_pnl = history["net_sol_pnl"]
    open_positions = history.get("open_positions", 0)

    win_rate = winners / unique_coins if unique_coins > 0 else 0

    smart_money_score = 0
    insider_score = 0
    noise_score = 0

    # ---- Layer 1: Hard bot filters ----
    is_bot = False
    bot_reasons = []

    if trade_count > 500:
        is_bot = True
        bot_reasons.append("extreme_volume")
    if trade_count > 100 and win_rate < 0.05:
        is_bot = True
        bot_reasons.append("spray_low_winrate")
    if unique_coins > 50 and win_rate < 0.15:
        is_bot = True
        bot_reasons.append("shotgun_pattern")
    if trade_count == 0:
        # dormant, not bot
        pass

    # ---- Layer 2: Compute scores ----

    # Activity
    if 5 <= trade_count <= 50:
        smart_money_score += 40
    elif 51 <= trade_count <= 150:
        smart_money_score += 30
    elif 151 <= trade_count <= 300 and net_pnl > 20:
        smart_money_score += 25  # high-volume but profitable

    # Focused scalper
    if 3 <= unique_coins <= 15 and trade_count >= 10:
        smart_money_score += 20

    # Win rate (30% floor for smart money)
    if win_rate >= 0.3:
        smart_money_score += 25
    if win_rate >= 0.4:
        smart_money_score += 15
    if win_rate >= 0.5:
        smart_money_score += 10

    # P&L
    if net_pnl > 50:
        smart_money_score += 35
    elif net_pnl > 20:
        smart_money_score += 25
    elif net_pnl > 10:
        smart_money_score += 18
    elif net_pnl > 5:
        smart_money_score += 12
    elif net_pnl > 2:
        smart_money_score += 8
    elif net_pnl < -10:
        smart_money_score -= 20
        noise_score += 15

    # Insider pattern
    if unique_coins <= 4 and winners >= 2 and trade_count < 15:
        insider_score += 50

    # ---- Layer 3: Classification flow ----
    if trade_count == 0:
        classification = "dormant"
        tier = None
    elif is_bot:
        classification = "noise"
        tier = None
    elif insider_score >= 50:
        classification = "insider"
        tier = None
    elif net_pnl > 30 and trade_count > 150:
        # Volume scalper: profitable but too active to copy-trade
        classification = "volume_scalper"
        tier = None
    elif win_rate >= 0.4 and net_pnl > 10 and 10 <= trade_count <= 100:
        classification = "smart_money"
        tier = "A"
    elif win_rate >= 0.3 and net_pnl > 5 and 5 <= trade_count <= 150:
        classification = "smart_money"
        tier = "B"
    elif win_rate >= 0.3 and net_pnl > 2 and 5 <= trade_count <= 200:
        classification = "smart_money"
        tier = "C"
    elif trade_count < 5:
        classification = "dormant"
        tier = None
    else:
        # Active but doesn't meet smart_money criteria
        classification = "dormant"
        tier = None

    return {
        "smart_money_score": max(0, min(100, smart_money_score)),
        "insider_score": max(0, min(100, insider_score)),
        "noise_score": max(0, min(100, noise_score)),
        "classification": classification,
        "tier": tier,
        "bot_reasons": bot_reasons,
    }