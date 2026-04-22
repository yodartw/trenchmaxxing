"""
Bulk CSV importer for Trencher.
Reads cabals.csv, members.csv, tokens.csv, wallets.csv from ./import/.
Idempotent: running twice is safe, duplicates are skipped.
"""

import os
import csv
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_ANON_KEY"))

IMPORT_DIR = Path(__file__).parent / "import"


# ---------- helpers ----------

def get_cabal_id_by_name(name):
    r = supabase.table("cabals").select("id").ilike("name", name).execute()
    return r.data[0]["id"] if r.data else None


def get_token_id(chain, address):
    r = (
        supabase.table("tokens")
        .select("id")
        .eq("chain", chain)
        .eq("address", address)
        .execute()
    )
    return r.data[0]["id"] if r.data else None


def get_member_by_handle(handle):
    if not handle.startswith("@"):
        handle = "@" + handle
    r = supabase.table("cabal_members").select("id, cabal_id").eq("x_handle", handle).execute()
    return r.data[0] if r.data else None


# ---------- imports ----------

def import_cabals():
    path = IMPORT_DIR / "cabals.csv"
    if not path.exists():
        print(f"Skipping {path.name} (not found)")
        return 0, 0
    added, skipped = 0, 0
    with open(path) as f:
        for row in csv.DictReader(f):
            name = row["name"].strip()
            try:
                supabase.table("cabals").insert({
                    "name": name,
                    "chain": row.get("chain", "").strip() or None,
                    "status": row.get("status", "active").strip() or "active",
                }).execute()
                print(f"  +  cabal: {name}")
                added += 1
            except Exception as e:
                if "duplicate" in str(e).lower():
                    supabase.table("cabals").update({
                        "chain": row.get("chain", "").strip() or None,
                        "status": row.get("status", "active").strip() or "active",
                    }).ilike("name", name).execute()
                    print(f"  ~  cabal: {name} (updated)")
                    skipped += 1
                else:
                    print(f"  !  cabal: {name} -- {e}")
    return added, skipped


def import_members():
    path = IMPORT_DIR / "members.csv"
    if not path.exists():
        print(f"Skipping {path.name} (not found)")
        return 0, 0
    added, skipped = 0, 0
    with open(path) as f:
        for row in csv.DictReader(f):
            cabal_name = row["cabal_name"].strip()
            handle = row["x_handle"].strip()
            if not handle.startswith("@"):
                handle = "@" + handle
            cabal_id = get_cabal_id_by_name(cabal_name)
            if not cabal_id:
                print(f"  !  member: {handle} -- cabal '{cabal_name}' not found")
                continue
            try:
                supabase.table("cabal_members").insert({
                    "cabal_id": cabal_id,
                    "x_handle": handle,
                    "role": row["role"].strip().lower(),
                    "confidence": row.get("confidence", "suspected").strip().lower(),
                }).execute()
                print(f"  +  member: {handle} -> {cabal_name}")
                added += 1
            except Exception as e:
                if "duplicate" in str(e).lower():
                    print(f"  ~  member: {handle} -> {cabal_name} (exists)")
                    skipped += 1
                else:
                    print(f"  !  member: {handle} -- {e}")
    return added, skipped


def import_tokens():
    path = IMPORT_DIR / "tokens.csv"
    if not path.exists():
        print(f"Skipping {path.name} (not found)")
        return 0, 0, 0
    added_tokens, added_links, skipped = 0, 0, 0
    with open(path) as f:
        for row in csv.DictReader(f):
            chain = row["chain"].strip().lower()
            address = row["address"].strip()
            symbol = row["symbol"].strip().upper()
            cabal_name = row["cabal_name"].strip()
            involvement = row["involvement"].strip().lower()
            is_primary = row.get("is_primary", "false").strip().lower() in ("true", "1", "yes")
            outcome = row.get("outcome", "open").strip().lower() or "open"

            try:
                supabase.table("tokens").insert({
                    "chain": chain,
                    "address": address,
                    "symbol": symbol,
                }).execute()
                print(f"  +  token: {symbol} on {chain}")
                added_tokens += 1
            except Exception as e:
                if "duplicate" not in str(e).lower():
                    print(f"  !  token: {symbol} -- {e}")
                    continue

            cabal_id = get_cabal_id_by_name(cabal_name)
            token_id = get_token_id(chain, address)
            if not cabal_id or not token_id:
                print(f"  !  link: {symbol} <-> {cabal_name} -- lookup failed")
                continue

            try:
                supabase.table("cabal_coin_links").insert({
                    "cabal_id": cabal_id,
                    "token_id": token_id,
                    "involvement": involvement,
                    "is_primary": is_primary,
                    "outcome": outcome,
                }).execute()
                print(f"  +  link: {cabal_name} <-> {symbol} [{outcome}]")
                added_links += 1
            except Exception as e:
                if "duplicate" in str(e).lower():
                    print(f"  ~  link: {cabal_name} <-> {symbol} (exists)")
                    skipped += 1
                else:
                    print(f"  !  link: {cabal_name} <-> {symbol} -- {e}")
    return added_tokens, added_links, skipped


def import_wallets():
    path = IMPORT_DIR / "wallets.csv"
    if not path.exists():
        print(f"Skipping {path.name} (not found)")
        return 0, 0
    added, skipped = 0, 0
    with open(path) as f:
        for row in csv.DictReader(f):
            chain = row["chain"].strip().lower()
            address = row["address"].strip()
            owner = row["owner"].strip()
            wallet_type = row["wallet_type"].strip().lower()
            confidence = row.get("confidence", "suspected").strip().lower() or "suspected"

            cabal_id = None
            member_id = None
            label_prefix = ""

            if owner.startswith("@"):
                m = get_member_by_handle(owner)
                if not m:
                    print(f"  !  wallet: {address[:10]} -- member {owner} not found")
                    continue
                member_id = m["id"]
                cabal_id = m["cabal_id"]
                label_prefix = owner
            else:
                cid = get_cabal_id_by_name(owner)
                if not cid:
                    print(f"  !  wallet: {address[:10]} -- cabal {owner} not found")
                    continue
                cabal_id = cid
                label_prefix = owner

            label = f"{label_prefix} {wallet_type}"

            try:
                supabase.table("wallets").insert({
                    "chain": chain,
                    "address": address,
                    "cabal_id": cabal_id,
                    "member_id": member_id,
                    "wallet_type": wallet_type,
                    "confidence": confidence,
                    "label": label,
                    "discovered_via": "csv_import",
                }).execute()
                print(f"  +  wallet: {label} ({address[:8]}...)")
                added += 1
            except Exception as e:
                if "duplicate" in str(e).lower():
                    print(f"  ~  wallet: {label} ({address[:8]}...) exists")
                    skipped += 1
                else:
                    print(f"  !  wallet: {label} -- {e}")
    return added, skipped


def import_wallets_raw():
    path = IMPORT_DIR / "wallets_raw.csv"
    if not path.exists():
        print(f"Skipping {path.name} (not found)")
        return 0, 0
    added, skipped = 0, 0
    with open(path) as f:
        for row in csv.DictReader(f):
            chain = row["chain"].strip().lower()
            address = row["address"].strip()
            name = (row.get("name") or "").strip() or None

            try:
                supabase.table("wallets").insert({
                    "chain": chain,
                    "address": address,
                    "cabal_id": None,
                    "member_id": None,
                    "wallet_type": "unknown",
                    "confidence": "suspected",
                    "label": name,
                    "discovered_via": "axiom_raw",
                    "quality_tier": "unknown",
                }).execute()
                print(f"  +  raw wallet: {address[:10]}... ({name or 'unnamed'})")
                added += 1
            except Exception as e:
                if "duplicate" in str(e).lower():
                    print(f"  ~  raw wallet: {address[:10]}... exists")
                    skipped += 1
                else:
                    print(f"  !  raw wallet: {address[:10]} -- {e}")
    return added, skipped


# ---------- main ----------

if __name__ == "__main__":
    print(f"Importing from {IMPORT_DIR}\n")

    print("=== Cabals ===")
    ca, cs = import_cabals()

    print("\n=== Members ===")
    ma, ms = import_members()

    print("\n=== Tokens + Links ===")
    ta, la, tls = import_tokens()

    print("\n=== Wallets (attributed) ===")
    wa, ws = import_wallets()

    print("\n=== Wallets (raw / unattributed) ===")
    wra, wrs = import_wallets_raw()

    print(
        f"\nDone. "
        f"Cabals: +{ca} / ~{cs}. "
        f"Members: +{ma} / ~{ms}. "
        f"Tokens: +{ta}. "
        f"Links: +{la} / ~{tls}. "
        f"Wallets: +{wa} / ~{ws}. "
        f"Raw wallets: +{wra} / ~{wrs}."
    )