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
    params_base = {"api-key": HELIUS_API_KEY, "limit": 100, "type": "SWAP"}

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
        in_window_count += 1
        if earliest_ts is None or ts < earliest_ts:
            earliest_ts = ts

        token_transfers = tx.get("tokenTransfers") or []
        account_data = tx.get("accountData") or []

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
    """Score + classify based on 14d history. v2."""
    trade_count = history["trade_count"]
    unique_coins = history["unique_coins"]
    winners = history["winners"]
    losers = history["losers"]
    net_pnl = history["net_sol_pnl"]

    smart_money_score = 0
    insider_score = 0
    noise_score = 0

    # Activity tier
    if 5 <= trade_count <= 50:
        smart_money_score += 40
    elif 51 <= trade_count <= 150:
        smart_money_score += 25
    elif trade_count > 200:
        noise_score += 60
    elif trade_count < 2:
        noise_score += 40

    # Focused scalper signal
    if 3 <= unique_coins <= 15 and trade_count >= 10:
        smart_money_score += 30

    # Win rate (requires at least some coin sample)
    win_rate = winners / unique_coins if unique_coins > 0 else 0
    if unique_coins >= 3:
        if win_rate >= 0.3:
            smart_money_score += 20
        if win_rate >= 0.5:
            smart_money_score += 10

    # P&L is king
    if net_pnl > 5:
        smart_money_score += 25
    elif net_pnl > 2:
        smart_money_score += 15
    elif net_pnl < -10:
        smart_money_score -= 20

    # Shotgun loser pattern
    if unique_coins > 10 and win_rate < 0.1:
        noise_score += 30

    # Insider pattern: surgical, small, profitable
    if unique_coins <= 4 and winners >= 2 and trade_count < 15:
        insider_score += 50

    # Classification flow
    if trade_count == 0:
        classification = "dormant"
    elif noise_score > 50 or trade_count > 200:
        classification = "noise"
    elif insider_score >= 50:
        classification = "insider"
    elif smart_money_score >= 60 and noise_score < 30:
        classification = "smart_money"
    else:
        classification = "dormant"

    return {
        "smart_money_score": max(0, min(100, smart_money_score)),
        "insider_score": max(0, min(100, insider_score)),
        "noise_score": max(0, min(100, noise_score)),
        "classification": classification,
    }