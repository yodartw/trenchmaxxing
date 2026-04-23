import os
import logging
import httpx
from dotenv import load_dotenv
from supabase import create_client, Client
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from scoring import fetch_wallet_history, classify_wallet

load_dotenv()
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
logger.info("Supabase client created")


VALID_ROLES = {"dev", "lead_kol", "kol", "shiller", "insider", "bundler"}
VALID_CONFIDENCE = {"confirmed", "strong", "suspected"}
VALID_CHAINS = {"sol", "base", "eth", "bsc"}
VALID_INVOLVEMENT = {"launched", "insider", "promoted", "rotated_into", "collaborator"}
VALID_OUTCOMES = {"open", "runner", "mid", "rug", "slow_bleed", "dead"}

OUTCOME_PRIORITY = {
    "open": 0,
    "runner": 1,
    "mid": 2,
    "slow_bleed": 3,
    "dead": 4,
    "rug": 5,
}

DEXSCREENER_BASE = "https://api.dexscreener.com"
DEX_CHAIN_MAP = {
    "sol": "solana",
    "base": "base",
    "eth": "ethereum",
    "bsc": "bsc",
}


# ---------- helpers ----------

def find_cabal_by_name(name):
    r = supabase.table("cabals").select("*").ilike("name", name).execute()
    return r.data[0] if r.data else None


def find_token_by_symbol(symbol, chain=None):
    q = supabase.table("tokens").select("*").ilike("symbol", symbol)
    if chain:
        q = q.eq("chain", chain)
    r = q.execute()
    return r.data[0] if r.data else None


def handle_link(handle):
    u = handle.lstrip("@")
    return f'<a href="https://x.com/{u}">{handle}</a>'


def _safe_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def format_number(n):
    if n is None:
        return "—"
    if n >= 1_000_000_000:
        return f"${n / 1_000_000_000:.2f}B"
    if n >= 1_000_000:
        return f"${n / 1_000_000:.2f}M"
    if n >= 1_000:
        return f"${n / 1_000:.1f}K"
    return f"${n:.2f}"


def format_change(c):
    if c is None:
        return ""
    sign = "📈" if c >= 0 else "📉"
    return f"{sign} {c:+.1f}%"


# ---------- DexScreener ----------

async def fetch_dexscreener(chain, address):
    dex_chain = DEX_CHAIN_MAP.get(chain)
    if not dex_chain:
        return None
    url = f"{DEXSCREENER_BASE}/tokens/v1/{dex_chain}/{address}"
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        logger.warning(f"DexScreener fail {chain}:{address} - {e}")
        return None
    if not data:
        return None
    pair = max(data, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
    return {
        "price_usd": _safe_float(pair.get("priceUsd")),
        "mcap_usd": _safe_float(pair.get("marketCap")),
        "fdv_usd": _safe_float(pair.get("fdv")),
        "liquidity_usd": _safe_float(pair.get("liquidity", {}).get("usd")),
        "volume_24h_usd": _safe_float(pair.get("volume", {}).get("h24")),
        "price_change_24h": _safe_float(pair.get("priceChange", {}).get("h24")),
        "price_change_1h": _safe_float(pair.get("priceChange", {}).get("h1")),
        "raw": pair,
    }


async def save_snapshot(token_id, data):
    if not data:
        return
    try:
        supabase.table("token_snapshots").insert({
            "token_id": token_id,
            "price_usd": data.get("price_usd"),
            "mcap_usd": data.get("mcap_usd"),
            "fdv_usd": data.get("fdv_usd"),
            "liquidity_usd": data.get("liquidity_usd"),
            "volume_24h_usd": data.get("volume_24h_usd"),
            "price_change_24h": data.get("price_change_24h"),
            "price_change_1h": data.get("price_change_1h"),
            "raw": data.get("raw"),
        }).execute()
    except Exception as e:
        logger.warning(f"Snapshot save failed: {e}")


def get_token_peak_mcap(token_id):
    try:
        r = (
            supabase.table("token_snapshots")
            .select("mcap_usd")
            .eq("token_id", token_id)
            .order("mcap_usd", desc=True)
            .limit(1)
            .execute()
        )
        if r.data and r.data[0].get("mcap_usd"):
            return r.data[0]["mcap_usd"]
    except Exception:
        pass
    return None


# ---------- commands ----------

async def start_command(update, context):
    u = update.effective_user
    logger.info(f"/start from {u.username}")
    await update.message.reply_text(
        f"Trencher online. Hello {u.first_name}.\n\n"
        "Cabals: /addcabal /setchain /listcabals /cabal\n"
        "Members: /addmember /members\n"
        "Tokens: /addtoken /linktoken /setoutcome /tokens\n\n"
        "Roles: dev, lead_kol, kol, shiller, insider, bundler\n"
        "Chains: sol, base, eth, bsc\n"
        "Involvement: launched, insider, promoted, rotated_into, collaborator\n"
        "Outcomes: open, runner, mid, rug, slow_bleed, dead"
    )


async def addcabal_command(update, context):
    if not context.args:
        await update.message.reply_text("Usage: /addcabal <name>")
        return
    name = " ".join(context.args).strip()
    if "/" in name:
        await update.message.reply_text("Name cannot contain /.")
        return
    try:
        r = supabase.table("cabals").insert({"name": name}).execute()
        if r.data:
            c = r.data[0]
            await update.message.reply_text(f"Added cabal: {c['name']} (status: {c['status']})")
    except Exception as e:
        if "duplicate key" in str(e).lower():
            await update.message.reply_text(f"Cabal '{name}' already exists.")
        else:
            await update.message.reply_text(f"Error: {e}")


async def listcabals_command(update, context):
    try:
        r = supabase.table("cabals").select("*").order("created_at").execute()
        if not r.data:
            await update.message.reply_text("No cabals yet.")
            return
        lines = ["Tracked cabals:\n"]
        for c in r.data:
            chain = c.get("chain") or "-"
            lines.append(f"• {c['name']} ({chain}) [{c['status']}]")
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")


async def setchain_command(update, context):
    if len(context.args) < 2:
        await update.message.reply_text(f"Usage: /setchain <cabal> <chain>\nChains: {', '.join(sorted(VALID_CHAINS))}")
        return
    chain = context.args[-1].lower()
    cabal_name = " ".join(context.args[:-1])
    if chain not in VALID_CHAINS:
        await update.message.reply_text(f"Invalid chain. Use: {', '.join(sorted(VALID_CHAINS))}")
        return
    cabal = find_cabal_by_name(cabal_name)
    if not cabal:
        await update.message.reply_text(f"Cabal '{cabal_name}' not found.")
        return
    try:
        r = supabase.table("cabals").update({"chain": chain}).eq("id", cabal["id"]).execute()
        if r.data:
            await update.message.reply_text(f"Set {cabal['name']} chain = {chain}")
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")


async def cabal_command(update, context):
    if not context.args:
        await update.message.reply_text("Usage: /cabal <name>")
        return
    cabal_name = " ".join(context.args)
    cabal = find_cabal_by_name(cabal_name)
    if not cabal:
        await update.message.reply_text(f"Cabal '{cabal_name}' not found.")
        return
    try:
        members = (
            supabase.table("cabal_members")
            .select("x_handle, role, confidence")
            .eq("cabal_id", cabal["id"]).eq("active", True).order("role").execute()
        ).data or []

        coins = (
            supabase.table("cabal_coin_links")
            .select("is_primary, involvement, outcome, tokens(id, symbol, chain, address)")
            .eq("cabal_id", cabal["id"]).execute()
        ).data or []

        total = len(coins)
        primary = [c for c in coins if c["is_primary"]]
        runners = [c for c in coins if c["outcome"] == "runner"]
        rugs = [c for c in coins if c["outcome"] == "rug"]
        opens = [c for c in coins if c["outcome"] == "open"]
        resolved = [c for c in coins if c["outcome"] != "open"]
        rate = f"{len(runners) / len(resolved) * 100:.0f}%" if resolved else "n/a"
        chain = cabal.get("chain") or "-"

        lines = [
            f"🎭 <b>{cabal['name']}</b>",
            f"Chain: {chain} · Status: {cabal['status']}",
            "",
            f"Coins: {total} total · {len(primary)} primary · {len(opens)} open",
            f"Runners: {len(runners)} · Rugs: {len(rugs)} · Runner rate: {rate}",
            "",
        ]

        if members:
            lines.append(f"<b>Members ({len(members)}):</b>")
            for m in members[:10]:
                lines.append(f"• {handle_link(m['x_handle'])} — {m['role']} ({m['confidence']})")
            if len(members) > 10:
                lines.append(f"  ...+{len(members) - 10} more")
            lines.append("")
        else:
            lines.append("<i>No members tracked yet.</i>\n")

        if coins:
            lines.append(f"<b>Coins ({total}):</b>")
            sc = sorted(coins, key=lambda x: (not x["is_primary"], OUTCOME_PRIORITY.get(x["outcome"], 99), x["tokens"]["symbol"]))
            for c in sc:
                t = c["tokens"]
                marker = "★" if c["is_primary"] else "•"
                market = await fetch_dexscreener(t["chain"], t["address"])
                if market:
                    await save_snapshot(t["id"], market)
                peak = get_token_peak_mcap(t["id"])
                peak_str = f" · peak {format_number(peak)}" if peak else ""
                lines.append(f"{marker} {t['symbol']} ({t['chain']}) — {c['involvement']} [{c['outcome']}]")
                if market:
                    mcap = format_number(market["mcap_usd"])
                    vol = format_number(market["volume_24h_usd"])
                    change = format_change(market["price_change_24h"])
                    lines.append(f"    MC {mcap}{peak_str}")
                else:
                    lines.append(f"    <i>no market data</i>")
        else:
            lines.append("<i>No coins linked yet.</i>")

        await update.message.reply_text("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")
        logger.exception("cabal failed")


async def addmember_command(update, context):
    if len(context.args) < 3:
        await update.message.reply_text("Usage: /addmember <cabal> <@handle> <role> [confidence]")
        return
    args = list(context.args)
    confidence = "suspected"
    if args[-1].lower() in VALID_CONFIDENCE:
        confidence = args[-1].lower()
        args = args[:-1]
    role = args[-1].lower()
    if role not in VALID_ROLES:
        await update.message.reply_text(f"Invalid role. Use: {', '.join(sorted(VALID_ROLES))}")
        return
    args = args[:-1]
    handle = args[-1]
    if not handle.startswith("@"):
        handle = "@" + handle
    args = args[:-1]
    cabal_name = " ".join(args)
    if not cabal_name:
        await update.message.reply_text("Missing cabal name.")
        return
    cabal = find_cabal_by_name(cabal_name)
    if not cabal:
        await update.message.reply_text(f"Cabal '{cabal_name}' not found.")
        return
    try:
        r = supabase.table("cabal_members").insert({
            "cabal_id": cabal["id"], "x_handle": handle, "role": role, "confidence": confidence,
        }).execute()
        if r.data:
            await update.message.reply_text(f"Added {handle} to {cabal['name']} as {role} ({confidence})")
    except Exception as e:
        if "duplicate key" in str(e).lower():
            await update.message.reply_text(f"{handle} already in {cabal['name']}.")
        else:
            await update.message.reply_text(f"Error: {e}")


async def members_command(update, context):
    if not context.args:
        await update.message.reply_text("Usage: /members <cabal>")
        return
    cabal_name = " ".join(context.args)
    cabal = find_cabal_by_name(cabal_name)
    if not cabal:
        await update.message.reply_text(f"Cabal '{cabal_name}' not found.")
        return
    try:
        r = (
            supabase.table("cabal_members").select("*")
            .eq("cabal_id", cabal["id"]).eq("active", True).order("role").execute()
        )
        if not r.data:
            await update.message.reply_text(f"No members for {cabal['name']} yet.")
            return
        lines = [f"<b>Members of {cabal['name']}:</b>", ""]
        for m in r.data:
            lines.append(f"• {handle_link(m['x_handle'])} — {m['role']} ({m['confidence']})")
        await update.message.reply_text("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")


async def addtoken_command(update, context):
    if len(context.args) < 3:
        await update.message.reply_text("Usage: /addtoken <chain> <address> <symbol>")
        return
    chain = context.args[0].lower()
    address = context.args[1]
    symbol = " ".join(context.args[2:]).upper()
    if chain not in VALID_CHAINS:
        await update.message.reply_text(f"Invalid chain. Use: {', '.join(sorted(VALID_CHAINS))}")
        return
    try:
        r = supabase.table("tokens").insert({"chain": chain, "address": address, "symbol": symbol}).execute()
        if r.data:
            t = r.data[0]
            await update.message.reply_text(f"Added {t['symbol']} on {t['chain']}\n<code>{t['address']}</code>", parse_mode="HTML")
    except Exception as e:
        if "duplicate key" in str(e).lower():
            await update.message.reply_text(f"Token already exists on {chain}.")
        else:
            await update.message.reply_text(f"Error: {e}")


async def linktoken_command(update, context):
    if len(context.args) < 3:
        await update.message.reply_text("Usage: /linktoken <cabal> <symbol> <involvement> [primary]")
        return
    args = list(context.args)
    is_primary = False
    if args[-1].lower() == "primary":
        is_primary = True
        args = args[:-1]
    involvement = args[-1].lower()
    if involvement not in VALID_INVOLVEMENT:
        await update.message.reply_text(f"Invalid involvement. Use: {', '.join(sorted(VALID_INVOLVEMENT))}")
        return
    args = args[:-1]
    symbol = args[-1].upper()
    args = args[:-1]
    cabal_name = " ".join(args)
    if not cabal_name:
        await update.message.reply_text("Missing cabal name.")
        return
    cabal = find_cabal_by_name(cabal_name)
    if not cabal:
        await update.message.reply_text(f"Cabal '{cabal_name}' not found.")
        return
    token = find_token_by_symbol(symbol)
    if not token:
        await update.message.reply_text(f"Token '{symbol}' not found.")
        return
    try:
        r = supabase.table("cabal_coin_links").insert({
            "cabal_id": cabal["id"], "token_id": token["id"],
            "is_primary": is_primary, "involvement": involvement,
        }).execute()
        if r.data:
            lt = "PRIMARY" if is_primary else "collaborator"
            await update.message.reply_text(f"Linked {cabal['name']} ↔ {token['symbol']} as {lt} ({involvement})")
    except Exception as e:
        err = str(e).lower()
        if "duplicate key" in err and "cabal_id_token_id" in err:
            await update.message.reply_text(f"{cabal['name']} already linked to {symbol}.")
        elif "idx_one_primary_cabal_per_token" in err:
            await update.message.reply_text(f"{symbol} already has a primary cabal.")
        else:
            await update.message.reply_text(f"Error: {e}")


async def tokens_command(update, context):
    if not context.args:
        await update.message.reply_text("Usage: /tokens <cabal>")
        return
    cabal_name = " ".join(context.args)
    cabal = find_cabal_by_name(cabal_name)
    if not cabal:
        await update.message.reply_text(f"Cabal '{cabal_name}' not found.")
        return
    try:
        r = (
            supabase.table("cabal_coin_links")
            .select("is_primary, involvement, outcome, tokens(id, symbol, chain, address)")
            .eq("cabal_id", cabal["id"]).execute()
        )
        if not r.data:
            await update.message.reply_text(f"No tokens for {cabal['name']}.")
            return
        sl = sorted(r.data, key=lambda x: (not x["is_primary"], OUTCOME_PRIORITY.get(x["outcome"], 99), x["tokens"]["symbol"]))
        lines = [f"Tokens linked to {cabal['name']}:", ""]
        for link in sl:
            t = link["tokens"]
            marker = "★" if link["is_primary"] else "•"
            market = await fetch_dexscreener(t["chain"], t["address"])
            if market:
                await save_snapshot(t["id"], market)
                peak = get_token_peak_mcap(t["id"])
            peak_str = f" · peak {format_number(peak)}" if peak else ""
            lines.append(f"{marker} {t['symbol']} ({t['chain']}) — {link['involvement']} [{link['outcome']}]")
            if market:
                mcap = format_number(market["mcap_usd"])
                vol = format_number(market["volume_24h_usd"])
                change = format_change(market["price_change_24h"])
                lines.append(f"    MC {mcap}{peak_str}")
            else:
                lines.append(f"    <i>no market data</i>")
        await update.message.reply_text("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")


async def setoutcome_command(update, context):
    if len(context.args) < 2:
        await update.message.reply_text(f"Usage: /setoutcome <symbol> <outcome>\nOutcomes: {', '.join(sorted(VALID_OUTCOMES))}")
        return
    symbol = context.args[0].upper()
    outcome = context.args[1].lower()
    if outcome not in VALID_OUTCOMES:
        await update.message.reply_text(f"Invalid outcome. Use: {', '.join(sorted(VALID_OUTCOMES))}")
        return
    token = find_token_by_symbol(symbol)
    if not token:
        await update.message.reply_text(f"Token '{symbol}' not found.")
        return
    try:
        r = supabase.table("cabal_coin_links").update({"outcome": outcome}).eq("token_id", token["id"]).execute()
        count = len(r.data) if r.data else 0
        if count > 0:
            s = "s" if count != 1 else ""
            await update.message.reply_text(f"Updated {symbol}: outcome = {outcome} ({count} link{s})")
        else:
            await update.message.reply_text(f"{symbol} has no links yet.")
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")


# ---------- wallets ----------

VALID_WALLET_TYPES = {"dev", "insider", "team_fund", "bundler", "sniper", "kol_personal", "unknown"}


def find_member_by_handle(handle):
    if not handle.startswith("@"):
        handle = "@" + handle
    r = supabase.table("cabal_members").select("*, cabals(id, name)").eq("x_handle", handle).execute()
    return r.data[0] if r.data else None


async def addwallet_command(update, context):
    if len(context.args) < 4:
        types = ", ".join(sorted(VALID_WALLET_TYPES))
        await update.message.reply_text(
            f"Usage: /addwallet <chain> <address> <cabal|@member> <type> [confidence]\n"
            f"Types: {types}\n"
            f"Confidence: confirmed, strong, suspected"
        )
        return

    args = list(context.args)
    confidence = "suspected"
    if args[-1].lower() in VALID_CONFIDENCE:
        confidence = args[-1].lower()
        args = args[:-1]

    wallet_type = args[-1].lower()
    if wallet_type not in VALID_WALLET_TYPES:
        await update.message.reply_text(f"Invalid type. Use: {', '.join(sorted(VALID_WALLET_TYPES))}")
        return
    args = args[:-1]

    owner = args[-1]
    args = args[:-1]

    if len(args) < 2:
        await update.message.reply_text("Need chain + address.")
        return

    chain = args[0].lower()
    address = args[1]

    if chain not in VALID_CHAINS:
        await update.message.reply_text(f"Invalid chain. Use: {', '.join(sorted(VALID_CHAINS))}")
        return

    cabal_id = None
    member_id = None
    label_prefix = ""

    if owner.startswith("@"):
        member = find_member_by_handle(owner)
        if not member:
            await update.message.reply_text(f"Member {owner} not found.")
            return
        member_id = member["id"]
        cabal_id = member["cabal_id"]
        label_prefix = owner
    else:
        cabal = find_cabal_by_name(owner)
        if not cabal:
            await update.message.reply_text(f"Cabal '{owner}' not found.")
            return
        cabal_id = cabal["id"]
        label_prefix = cabal["name"]

    label = f"{label_prefix} {wallet_type}"

    try:
        r = supabase.table("wallets").insert({
            "chain": chain,
            "address": address,
            "cabal_id": cabal_id,
            "member_id": member_id,
            "wallet_type": wallet_type,
            "confidence": confidence,
            "label": label,
            "discovered_via": "manual",
        }).execute()
        if r.data:
            await update.message.reply_text(
                f"Added wallet: {label} ({wallet_type}, {confidence})\n<code>{address}</code>",
                parse_mode="HTML",
            )
    except Exception as e:
        if "duplicate key" in str(e).lower():
            await update.message.reply_text(f"Wallet already exists on {chain}.")
        else:
            await update.message.reply_text(f"Error: {e}")


async def wallet_command(update, context):
    if not context.args:
        await update.message.reply_text("Usage: /wallet <address>")
        return
    address = context.args[0]

    r = (
        supabase.table("wallets")
        .select("*, cabals(name), cabal_members(x_handle)")
        .eq("address", address)
        .execute()
    )
    if not r.data:
        await update.message.reply_text(f"Wallet not found: {address}")
        return

    w = r.data[0]
    cabal_name = w["cabals"]["name"] if w.get("cabals") else "-"
    member_handle = w["cabal_members"]["x_handle"] if w.get("cabal_members") else None

    lines = [
        f"<b>Wallet</b> ({w['chain']})",
        f"<code>{w['address']}</code>",
        "",
        f"Type: {w['wallet_type']}",
        f"Confidence: {w['confidence']}",
        f"Cabal: {cabal_name}",
    ]
    if member_handle:
        lines.append(f"Member: {handle_link(member_handle)}")
    if w.get("label"):
        lines.append(f"Label: {w['label']}")
    if w.get("discovered_via"):
        lines.append(f"Found via: {w['discovered_via']}")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)


async def wallets_command(update, context):
    if not context.args:
        await update.message.reply_text("Usage: /wallets <cabal>")
        return
    cabal_name = " ".join(context.args)
    cabal = find_cabal_by_name(cabal_name)
    if not cabal:
        await update.message.reply_text(f"Cabal '{cabal_name}' not found.")
        return

    r = (
        supabase.table("wallets")
        .select("*, cabal_members(x_handle)")
        .eq("cabal_id", cabal["id"])
        .order("wallet_type")
        .execute()
    )
    if not r.data:
        await update.message.reply_text(f"No wallets tracked for {cabal['name']} yet.")
        return

    lines = [f"<b>Wallets for {cabal['name']}:</b>", ""]
    for w in r.data:
        owner = ""
        if w.get("cabal_members"):
            owner = f" ({handle_link(w['cabal_members']['x_handle'])})"
        short_addr = f"{w['address'][:6]}...{w['address'][-4:]}"
        lines.append(
            f"• <code>{short_addr}</code> ({w['chain']}) — "
            f"{w['wallet_type']} [{w['confidence']}]{owner}"
        )
    await update.message.reply_text("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)
# ---------- wallet activity (Helius) ----------

HELIUS_BASE = "https://api.helius.xyz/v0"

# Cache token mcap lookups to avoid hammering DexScreener
_mcap_cache = {}


async def fetch_token_mcap(chain, address):
    """Get mcap for a token, cached for 60s."""
    import time
    cache_key = f"{chain}:{address}"
    now = time.time()
    cached = _mcap_cache.get(cache_key)
    if cached and now - cached["at"] < 60:
        return cached["mcap"]
    market = await fetch_dexscreener(chain, address)
    mcap = market["mcap_usd"] if market else None
    _mcap_cache[cache_key] = {"mcap": mcap, "at": now}
    return mcap


async def fetch_helius_swaps(address, limit=25):
    """Fetch recent parsed transactions for a wallet. Returns list of swap events."""
    url = f"{HELIUS_BASE}/addresses/{address}/transactions"
    params = {"api-key": HELIUS_API_KEY, "limit": limit, "type": "SWAP"}
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(url, params=params)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        logger.warning(f"Helius fetch failed for {address}: {e}")
        return []

    swaps = []
    for tx in data:
        try:
            events = tx.get("events", {})
            swap = events.get("swap")
            if not swap:
                continue

            sig = tx.get("signature")
            timestamp = tx.get("timestamp")
            block_time = (
                __import__("datetime").datetime.fromtimestamp(timestamp).isoformat()
                if timestamp else None
            )

            # Helius SWAP event: we need to identify which side was the meme/alt
            # vs which was SOL/USDC. Native SOL swap, tokenInput/tokenOutput arrays.
            native_input = swap.get("nativeInput") or {}
            native_output = swap.get("nativeOutput") or {}
            token_inputs = swap.get("tokenInputs") or []
            token_outputs = swap.get("tokenOutputs") or []

            direction = None
            token_mint = None
            token_amount = None
            sol_amount = None

            # Buy: wallet spent SOL (or wSOL) and received a token
            if native_input.get("amount") and token_outputs:
                direction = "buy"
                tok = token_outputs[0]
                token_mint = tok.get("mint")
                token_amount = float(tok.get("rawTokenAmount", {}).get("tokenAmount", 0)) / (
                    10 ** int(tok.get("rawTokenAmount", {}).get("decimals", 0))
                )
                sol_amount = float(native_input.get("amount", 0)) / 1e9

            # Sell: wallet sent a token and received SOL
            elif native_output.get("amount") and token_inputs:
                direction = "sell"
                tok = token_inputs[0]
                token_mint = tok.get("mint")
                token_amount = float(tok.get("rawTokenAmount", {}).get("tokenAmount", 0)) / (
                    10 ** int(tok.get("rawTokenAmount", {}).get("decimals", 0))
                )
                sol_amount = float(native_output.get("amount", 0)) / 1e9

            if not direction or not token_mint:
                continue

            swaps.append({
                "signature": sig,
                "direction": direction,
                "token_mint": token_mint,
                "token_amount": token_amount,
                "sol_amount": sol_amount,
                "block_time": block_time,
                "raw": tx,
            })
        except Exception as e:
            logger.warning(f"Parse swap failed: {e}")
            continue

    return swaps


async def save_wallet_activity(wallet_id, swap):
    """Persist a swap to DB. Idempotent on (wallet_id, tx_signature)."""
    try:
        # Get token mcap at tx time (actually current — we don't have historical)
        mcap = await fetch_token_mcap("sol", swap["token_mint"])

        # Try to find symbol from our tokens table if we know this token
        sym_result = supabase.table("tokens").select("symbol").eq("address", swap["token_mint"]).eq("chain", "sol").execute()
        symbol = sym_result.data[0]["symbol"] if sym_result.data else None

        supabase.table("wallet_activity").insert({
            "wallet_id": wallet_id,
            "tx_signature": swap["signature"],
            "direction": swap["direction"],
            "token_address": swap["token_mint"],
            "token_symbol": symbol,
            "amount_token": swap["token_amount"],
            "amount_sol": swap["sol_amount"],
            "mcap_usd_at_tx": mcap,
            "block_time": swap["block_time"],
            "raw": swap["raw"],
        }).execute()
    except Exception as e:
        if "duplicate" not in str(e).lower():
            logger.warning(f"Activity save failed: {e}")


def format_sol(amount):
    if amount is None:
        return "-"
    if amount >= 1000:
        return f"{amount:.0f} SOL"
    if amount >= 10:
        return f"{amount:.1f} SOL"
    return f"{amount:.2f} SOL"


async def activity_command(update, context):
    """Usage: /activity <address> [limit]"""
    if not context.args:
        await update.message.reply_text("Usage: /activity <address> [limit]")
        return

    address = context.args[0]
    limit = 25
    if len(context.args) >= 2:
        try:
            limit = int(context.args[1])
            limit = min(limit, 100)
        except ValueError:
            pass

    # Find the wallet in our DB
    w = supabase.table("wallets").select("id, chain, label, cabals(name), cabal_members(x_handle)").eq("address", address).execute()
    if not w.data:
        await update.message.reply_text(f"Wallet not tracked: {address[:10]}...")
        return
    wallet = w.data[0]
    if wallet["chain"] != "sol":
        await update.message.reply_text(f"Activity only supported for Solana wallets right now.")
        return

    await update.message.reply_text(f"Fetching last {limit} swaps...")

    swaps = await fetch_helius_swaps(address, limit=limit)
    if not swaps:
        await update.message.reply_text("No swaps found or Helius error.")
        return

    # Save each swap
    for swap in swaps:
        await save_wallet_activity(wallet["id"], swap)

    # Build display: filter to <$50M mcap
    owner = wallet["cabals"]["name"] if wallet.get("cabals") else "?"
    if wallet.get("cabal_members"):
        owner += f" / {wallet['cabal_members']['x_handle']}"

    lines = [
        f"<b>Activity</b> for {owner}",
        f"<code>{address[:12]}...{address[-6:]}</code>",
        "",
    ]

    shown = 0
    for swap in swaps:
        mcap = await fetch_token_mcap("sol", swap["token_mint"])
        # Filter: skip if mcap > $50M (established coins)
        if mcap and mcap > 50_000_000:
            continue
        # Skip SOL/wSOL/USDC noise
        if swap["token_mint"] in ("So11111111111111111111111111111111111111112", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"):
            continue

        sym_result = supabase.table("tokens").select("symbol").eq("address", swap["token_mint"]).eq("chain", "sol").execute()
        symbol = sym_result.data[0]["symbol"] if sym_result.data else swap["token_mint"][:6]

        direction_emoji = "🟢" if swap["direction"] == "buy" else "🔴"
        mcap_str = format_number(mcap) if mcap else "?"
        sol_str = format_sol(swap["sol_amount"])

        lines.append(f"{direction_emoji} {swap['direction'].upper()} {symbol} · {sol_str} · mcap {mcap_str}")
        shown += 1

    if shown == 0:
        lines.append("<i>No trades under $50M mcap in recent swaps.</i>")

    lines.append("")
    lines.append(f"<i>Saved {len(swaps)} swaps to DB.</i>")

    await update.message.reply_text(
        "\n".join(lines), parse_mode="HTML", disable_web_page_preview=True
    )


import asyncio


async def _fetch_and_save_for_wallet(wallet):
    """Helper: fetch + persist swaps for a single wallet. Used by concurrent gather."""
    try:
        swaps = await fetch_helius_swaps(wallet["address"], limit=25)
        for swap in swaps:
            await save_wallet_activity(wallet["id"], swap)
        return len(swaps)
    except Exception as e:
        logger.warning(f"Refresh failed for {wallet['address'][:10]}...: {e}")
        return 0


async def _get_cabal_sol_wallets(cabal_id=None, include_unknown=False):
    """Return list of sol wallets. By default excludes the 'unknown' tier (raw/Axiom imports).
    Pass include_unknown=True to include the full set."""
    q = supabase.table("wallets").select(
        "id, address, cabal_id, cabals(name), cabal_members(x_handle)"
    ).eq("chain", "sol")

    if not include_unknown:
        q = q.eq("quality_tier", "attributed")

    if cabal_id:
        q = q.eq("cabal_id", cabal_id)
    return q.execute().data or []


async def recent_command(update, context):
    """Usage: /recent <cabal> [hours]"""
    if not context.args:
        await update.message.reply_text("Usage: /recent <cabal> [hours]")
        return

    args = list(context.args)
    hours = 24
    try:
        hours = int(args[-1])
        args = args[:-1]
    except ValueError:
        pass

    cabal_name = " ".join(args)
    cabal = find_cabal_by_name(cabal_name)
    if not cabal:
        await update.message.reply_text(f"Cabal '{cabal_name}' not found.")
        return

    wallets = await _get_cabal_sol_wallets(cabal_id=cabal["id"])
    if not wallets:
        await update.message.reply_text(f"No Solana wallets tracked for {cabal['name']}.")
        return

    await update.message.reply_text(
        f"Fetching {hours}h activity across {len(wallets)} wallets (parallel)..."
    )

    # Parallel fetch — much faster than sequential
    await asyncio.gather(*[_fetch_and_save_for_wallet(w) for w in wallets])

    from datetime import datetime, timedelta, timezone
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    wallet_ids = [w["id"] for w in wallets]
    result = (
        supabase.table("wallet_activity")
        .select("*, wallets(address, cabal_members(x_handle))")
        .in_("wallet_id", wallet_ids)
        .gte("block_time", cutoff)
        .order("block_time", desc=True)
        .execute()
    )

    if not result.data:
        await update.message.reply_text(f"No activity in last {hours}h.")
        return

    SKIP_MINTS = {
        "So11111111111111111111111111111111111111112",
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    }

    by_token = {}
    for act in result.data:
        mint = act["token_address"]
        if mint in SKIP_MINTS:
            continue
        if act.get("mcap_usd_at_tx") and act["mcap_usd_at_tx"] > 50_000_000:
            continue

        entry = by_token.setdefault(mint, {
            "symbol": act.get("token_symbol"),
            "mcap": act.get("mcap_usd_at_tx"),
            "buys": 0,
            "sells": 0,
            "buy_sol": 0.0,
            "sell_sol": 0.0,
            "handles": set(),
        })

        sol = float(act.get("amount_sol") or 0)
        if act["direction"] == "buy":
            entry["buys"] += 1
            entry["buy_sol"] += sol
        else:
            entry["sells"] += 1
            entry["sell_sol"] += sol

        handle = (
            act["wallets"]["cabal_members"]["x_handle"]
            if act["wallets"].get("cabal_members")
            else f"{act['wallets']['address'][:6]}..."
        )
        entry["handles"].add(handle)

        if act.get("mcap_usd_at_tx") and not entry.get("mcap"):
            entry["mcap"] = act["mcap_usd_at_tx"]

    if not by_token:
        await update.message.reply_text(f"No trades under $50M mcap in last {hours}h.")
        return

    sorted_tokens = sorted(
        by_token.items(),
        key=lambda kv: kv[1]["buy_sol"] + kv[1]["sell_sol"],
        reverse=True,
    )

    lines = [
        f"<b>Recent: {cabal['name']} (last {hours}h)</b>",
        f"<i>{len(sorted_tokens)} tokens across {len(wallets)} wallets</i>",
        "",
    ]

    for mint, e in sorted_tokens[:15]:
        sym = e["symbol"] or mint[:6]
        mcap_str = format_number(e["mcap"]) if e["mcap"] else "?"
        net_sol = e["buy_sol"] - e["sell_sol"]
        net_sign = "+" if net_sol >= 0 else "-"

        lines.append(f"<b>${sym}</b> · {mcap_str}")
        parts = []
        if e["buys"]:
            parts.append(f"🟢 {e['buys']} ({format_sol(e['buy_sol'])})")
        if e["sells"]:
            parts.append(f"🔴 {e['sells']} ({format_sol(e['sell_sol'])})")
        lines.append("  " + " · ".join(parts) + f" · net {net_sign}{format_sol(abs(net_sol))}")
        lines.append(f"  {', '.join(sorted(e['handles']))}")
        lines.append("")

    if len(sorted_tokens) > 15:
        lines.append(f"<i>...+{len(sorted_tokens) - 15} more</i>")

    await update.message.reply_text(
        "\n".join(lines), parse_mode="HTML", disable_web_page_preview=True
    )


async def confluence_command(update, context):
    """
    Usage:
      /confluence                    - all wallets, 24h, 3+ unique wallets
      /confluence 12                 - all wallets, 12h, 3+ wallets
      /confluence 24 2               - all wallets, 24h, 2+ wallets
      /confluence JAMIE              - JAMIE only, 24h, 3+ wallets
      /confluence JAMIE 12 2         - JAMIE only, 12h, 2+ wallets
    """
    args = list(context.args)
    hours = 24
    min_wallets = 3
    cabal = None
    include_unknown = False

    # Check for 'all' flag — includes unknown tier
    if args and args[0].lower() == "all":
        include_unknown = True
        args = args[1:]

    # Parse from the tail: last arg might be min_wallets, then hours, then cabal name
    if args and args[-1].isdigit():
        candidate = int(args[-1])
        # If there's another number before it, this is min_wallets; else it's hours
        if len(args) >= 2 and args[-2].isdigit():
            min_wallets = candidate
            hours = int(args[-2])
            args = args[:-2]
        else:
            hours = candidate
            args = args[:-1]

    if args:
        cabal_name = " ".join(args)
        cabal = find_cabal_by_name(cabal_name)
        if not cabal:
            await update.message.reply_text(f"Cabal '{cabal_name}' not found.")
            return

    scope_label = cabal["name"] if cabal else ("ALL wallets (incl unknown)" if include_unknown else "attributed wallets")
    wallets = await _get_cabal_sol_wallets(
        cabal_id=(cabal["id"] if cabal else None),
        include_unknown=include_unknown,
    )
    if not wallets:
        await update.message.reply_text(f"No Solana wallets tracked for {scope_label}.")
        return

    await update.message.reply_text(
        f"Scanning {scope_label}, last {hours}h, threshold {min_wallets}+ wallets...\n"
        f"Parallel fetch across {len(wallets)} wallets..."
    )

    await asyncio.gather(*[_fetch_and_save_for_wallet(w) for w in wallets])

    from datetime import datetime, timedelta, timezone
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    wallet_ids = [w["id"] for w in wallets]
    result = (
        supabase.table("wallet_activity")
        .select("*, wallets(id, address, cabal_id, cabals(name), cabal_members(x_handle))")
        .in_("wallet_id", wallet_ids)
        .gte("block_time", cutoff)
        .order("block_time", desc=True)
        .execute()
    )

    if not result.data:
        await update.message.reply_text(f"No activity in last {hours}h across {scope_label}.")
        return

    SKIP_MINTS = {
        "So11111111111111111111111111111111111111112",
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    }

    # Aggregate by token, tracking unique wallets AND cabals
    by_token = {}
    for act in result.data:
        mint = act["token_address"]
        if mint in SKIP_MINTS:
            continue
        if act.get("mcap_usd_at_tx") and act["mcap_usd_at_tx"] > 50_000_000:
            continue

        entry = by_token.setdefault(mint, {
            "symbol": act.get("token_symbol"),
            "mcap": act.get("mcap_usd_at_tx"),
            "wallet_ids": set(),
            "cabals": set(),
            "buys": 0,
            "sells": 0,
            "buy_sol": 0.0,
            "sell_sol": 0.0,
            "trades": [],
        })

        wallet_id = act["wallets"]["id"]
        entry["wallet_ids"].add(wallet_id)
        if act["wallets"].get("cabals"):
            entry["cabals"].add(act["wallets"]["cabals"]["name"])

        sol = float(act.get("amount_sol") or 0)
        if act["direction"] == "buy":
            entry["buys"] += 1
            entry["buy_sol"] += sol
        else:
            entry["sells"] += 1
            entry["sell_sol"] += sol

        handle = (
            act["wallets"]["cabal_members"]["x_handle"]
            if act["wallets"].get("cabal_members")
            else f"{act['wallets']['address'][:6]}..."
        )
        cabal_tag = (
            act["wallets"]["cabals"]["name"]
            if act["wallets"].get("cabals") else "?"
        )
        entry["trades"].append({
            "handle": handle,
            "cabal": cabal_tag,
            "direction": act["direction"],
            "sol": sol,
            "mcap": act.get("mcap_usd_at_tx"),
            "block_time": act["block_time"],
        })

        if act.get("mcap_usd_at_tx") and not entry.get("mcap"):
            entry["mcap"] = act["mcap_usd_at_tx"]

    # CONFLUENCE FILTER: only keep tokens hit by min_wallets+ unique wallets
    confluent = {
        mint: e for mint, e in by_token.items()
        if len(e["wallet_ids"]) >= min_wallets
    }

    if not confluent:
        await update.message.reply_text(
            f"No confluence detected.\n"
            f"Scope: {scope_label} · {hours}h · threshold {min_wallets}+ wallets.\n"
            f"Tokens scanned: {len(by_token)}"
        )
        return

    # Sort: strongest signal first. Rank by # unique wallets, then # cabals, then net SOL flow.
    sorted_tokens = sorted(
        confluent.items(),
        key=lambda kv: (
            len(kv[1]["wallet_ids"]),
            len(kv[1]["cabals"]),
            kv[1]["buy_sol"] - kv[1]["sell_sol"],
        ),
        reverse=True,
    )

    lines = [
        f"🎯 <b>Confluence: {scope_label}</b>",
        f"<i>last {hours}h · {min_wallets}+ wallets · {len(sorted_tokens)} hits</i>",
        "",
    ]

    for mint, e in sorted_tokens[:10]:
        sym = e["symbol"] or mint[:6]
        mcap_str = format_number(e["mcap"]) if e["mcap"] else "?"
        n_wallets = len(e["wallet_ids"])
        n_cabals = len(e["cabals"])
        net_sol = e["buy_sol"] - e["sell_sol"]
        net_sign = "+" if net_sol >= 0 else "-"

        # Pressure icon based on strength
        if n_wallets >= 5:
            icon = "💥"
        elif n_wallets >= 4:
            icon = "🔥"
        else:
            icon = "🎯"

        cabal_badge = ""
        if n_cabals > 1:
            cabal_badge = f" · ⚡ <i>{n_cabals} cabals</i>"

        lines.append(f"{icon} <b>${sym}</b> · {mcap_str}{cabal_badge}")
        lines.append(
            f"  {n_wallets} wallets · 🟢 {e['buys']} ({format_sol(e['buy_sol'])}) · "
            f"🔴 {e['sells']} ({format_sol(e['sell_sol'])}) · net {net_sign}{format_sol(abs(net_sol))}"
        )

        # Show up to 6 individual trades sorted by mcap ascending (earliest entries first)
        trades_sorted = sorted(
            e["trades"],
            key=lambda t: t["mcap"] if t["mcap"] else float("inf"),
        )
        for t in trades_sorted[:6]:
            d_emoji = "🟢" if t["direction"] == "buy" else "🔴"
            mcap = format_number(t["mcap"]) if t["mcap"] else "?"
            cabal_suffix = f" ({t['cabal']})" if t["cabal"] != "?" else ""
            lines.append(
                f"    {d_emoji} {t['handle']}{cabal_suffix} · {format_sol(t['sol'])} at {mcap}"
            )
        if len(trades_sorted) > 6:
            lines.append(f"    <i>+ {len(trades_sorted) - 6} more trades</i>")

        lines.append("")

    if len(sorted_tokens) > 10:
        lines.append(f"<i>...+{len(sorted_tokens) - 10} more tokens truncated</i>")

    await update.message.reply_text(
        "\n".join(lines), parse_mode="HTML", disable_web_page_preview=True
    )


# ---------- /scan — early buyer discovery ----------

async def fetch_early_buyers(mint_address, window_seconds=300, max_pages=5):
    url = f"{HELIUS_BASE}/addresses/{mint_address}/transactions"
    params_base = {"api-key": HELIUS_API_KEY, "limit": 100}

    all_txs = []
    before = None
    for _ in range(max_pages):
        params = dict(params_base)
        if before:
            params["before"] = before
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                r = await client.get(url, params=params)
                r.raise_for_status()
                chunk = r.json()
        except Exception as e:
            logger.warning(f"Pagination failed: {e}")
            break
        if not chunk:
            break
        all_txs.extend(chunk)
        before = chunk[-1].get("signature")
        if len(chunk) < 100:
            break

    if not all_txs:
        return []

    timestamps = [t.get("timestamp") for t in all_txs if t.get("timestamp")]
    if not timestamps:
        return []
    earliest = min(timestamps)
    window_end = earliest + window_seconds
    in_window = [t for t in all_txs if t.get("timestamp") and earliest <= t["timestamp"] <= window_end]

    buyers = []
    for tx in in_window:
        try:
            token_transfers = tx.get("tokenTransfers") or []
            account_data = tx.get("accountData") or []
            sig = tx.get("signature")
            timestamp = tx.get("timestamp")

            for tt in token_transfers:
                if tt.get("mint") != mint_address:
                    continue
                to_user = tt.get("toUserAccount")
                from_user = tt.get("fromUserAccount")
                if not to_user or to_user == from_user:
                    continue

                sol_delta = 0
                for ad in account_data:
                    if ad.get("account") == to_user:
                        sol_delta = int(ad.get("nativeBalanceChange", 0))
                        break
                sol_spent = abs(sol_delta) / 1e9 if sol_delta < 0 else 0
                if sol_spent <= 0:
                    continue

                buyers.append({
                    "buyer": to_user,
                    "tx_signature": sig,
                    "block_time": timestamp,
                    "sol_in": sol_spent,
                })
        except Exception as e:
            logger.warning(f"Parse buyer failed: {e}")
            continue
    return buyers


def detect_bundles(buyers):
    by_tx = {}
    for b in buyers:
        by_tx.setdefault(b["tx_signature"], []).append(b["buyer"])
    return {sig: addrs for sig, addrs in by_tx.items() if len(addrs) > 1}


async def scan_command(update, context):
    from scan_v2 import scan_command_v2
    await scan_command_v2(update, context, supabase, fetch_early_buyers, detect_bundles)


async def promote_command(update, context):
    """Usage: /promote <wallet_address> [optional note]"""
    if not context.args:
        await update.message.reply_text("Usage: /promote <wallet_address> [note]")
        return

    wallet = context.args[0].strip()
    note = " ".join(context.args[1:]) if len(context.args) > 1 else None

    if len(wallet) < 30 or len(wallet) > 50:
        await update.message.reply_text("That doesn't look like a Solana wallet address.")
        return

    # Check if already in DB
    existing = supabase.table("wallets").select("id, classification, label").eq("address", wallet).execute()
    if existing.data:
        existing_rec = existing.data[0]
        if note:
            new_label = f"{existing_rec.get('label') or ''} | promoted: {note}".strip(" |")
            supabase.table("wallets").update({"label": new_label}).eq("address", wallet).execute()
            await update.message.reply_text(
                f"✅ Wallet already in DB. Updated label.\n"
                f"<code>{wallet}</code>\n"
                f"Classification: {existing_rec.get('classification') or '?'}",
                parse_mode="HTML"
            )
        else:
            await update.message.reply_text(
                f"ℹ️ Already in DB.\n"
                f"<code>{wallet}</code>\n"
                f"Classification: {existing_rec.get('classification') or '?'}",
                parse_mode="HTML"
            )
        return

    # Score the wallet fresh
    await update.message.reply_text(f"Scoring {wallet[:10]}... (14d)")
    from scoring import fetch_wallet_history, classify_wallet
    history = await fetch_wallet_history(wallet, days=14)
    scored = classify_wallet(history)

    # Insert
    import datetime
    label = f"promoted"
    if note:
        label = f"promoted: {note}"

    try:
        supabase.table("wallets").insert({
            "chain": "sol",
            "address": wallet,
            "wallet_type": "unknown",
            "confidence": "suspected",
            "label": label,
            "discovered_via": "manual_promote",
            "quality_tier": "raw",
            "smart_money_score": scored["smart_money_score"],
            "insider_score": scored["insider_score"],
            "classification": scored["classification"],
            "last_scored_at": datetime.datetime.utcnow().isoformat(),
            "trade_count_30d": history["trade_count"],
            "winners_2x": history["winners"],
        }).execute()

        emoji = {
            "smart_money": "🎯",
            "insider": "🔒",
            "noise": "🤖",
            "dormant": "💤",
        }.get(scored["classification"], "❓")

        await update.message.reply_text(
            f"✅ <b>Promoted</b>\n"
            f"<code>{wallet}</code>\n"
            f"{emoji} {scored['classification'].upper()}\n"
            f"Smart money: {scored['smart_money_score']}/100 · Insider: {scored['insider_score']}/100\n"
            f"14d: {history['trade_count']} trades, {history['unique_coins']} coins, "
            f"{history['net_sol_pnl']:+.1f} SOL P&L",
            parse_mode="HTML"
        )
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")


# ---------- /score — wallet classification ----------

async def score_command(update, context):
    """Usage: /score <wallet_address>"""
    if not context.args:
        await update.message.reply_text("Usage: /score <wallet_address>")
        return

    wallet = context.args[0].strip()
    if len(wallet) < 30 or len(wallet) > 50:
        await update.message.reply_text("That doesn't look like a Solana wallet address.")
        return

    await update.message.reply_text(f"Scoring {wallet[:10]}... (14d history)")

    history = await fetch_wallet_history(wallet, days=14)
    scored = classify_wallet(history)

    try:
        existing = supabase.table("wallets").select("id").eq("address", wallet).execute()
        if existing.data:
            import datetime
            supabase.table("wallets").update({
                "smart_money_score": scored["smart_money_score"],
                "insider_score": scored["insider_score"],
                "classification": scored["classification"],
                "last_scored_at": datetime.datetime.utcnow().isoformat(),
                "trade_count_30d": history["trade_count"],
                "winners_2x": history["winners"],
            }).eq("address", wallet).execute()
    except Exception as e:
        logger.warning(f"Score save failed: {e}")

    emoji = {
        "smart_money": "🎯",
        "insider": "🔒",
        "noise": "🤖",
        "dormant": "💤",
    }.get(scored["classification"], "❓")

    tier_display = f" [{scored.get('tier')}]" if scored.get("tier") else ""
    bot_flags = ""
    if scored.get("bot_reasons"):
        bot_flags = f"\n⚠️ Bot flags: {', '.join(scored['bot_reasons'])}"

    msg = (
        f"{emoji} <b>{scored['classification'].upper()}{tier_display}</b>{bot_flags}\n"
        f"<code>{wallet}</code>\n\n"
        f"<b>14-day history:</b>\n"
        f"  Total swaps: {history['trade_count']}\n"
        f"  Unique coins: {history['unique_coins']}\n"
        f"  Realized P&L: {history['net_sol_pnl']:+.2f} SOL\n"
        f"  Closed winners (≥2x): {history['winners']}\n"
        f"  Closed losers (≤0.5x): {history['losers']}\n"
        f"  Open/partial positions: {history.get('open_positions', 0)}\n"
        f"  First trade: {history['first_trade_days_ago']}d ago\n\n"
        f"<b>Scores:</b>\n"
        f"  Smart money: {scored['smart_money_score']}/100\n"
        f"  Insider: {scored['insider_score']}/100\n"
        f"  Noise: {scored['noise_score']}/100"
    )

    await update.message.reply_text(msg, parse_mode="HTML")


def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("addcabal", addcabal_command))
    app.add_handler(CommandHandler("listcabals", listcabals_command))
    app.add_handler(CommandHandler("setchain", setchain_command))
    app.add_handler(CommandHandler("cabal", cabal_command))
    app.add_handler(CommandHandler("addmember", addmember_command))
    app.add_handler(CommandHandler("members", members_command))
    app.add_handler(CommandHandler("addtoken", addtoken_command))
    app.add_handler(CommandHandler("linktoken", linktoken_command))
    app.add_handler(CommandHandler("tokens", tokens_command))
    app.add_handler(CommandHandler("setoutcome", setoutcome_command))
    app.add_handler(CommandHandler("addwallet", addwallet_command))
    app.add_handler(CommandHandler("wallet", wallet_command))
    app.add_handler(CommandHandler("wallets", wallets_command))
    app.add_handler(CommandHandler("activity", activity_command))
    app.add_handler(CommandHandler("recent", recent_command))
    app.add_handler(CommandHandler("confluence", confluence_command))
    app.add_handler(CommandHandler("scan", scan_command))
    app.add_handler(CommandHandler("score", score_command))
    app.add_handler(CommandHandler("promote", promote_command))
    logger.info("Bot starting polling...")
    app.run_polling()


if __name__ == "__main__":
    main()