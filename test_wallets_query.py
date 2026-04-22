from dotenv import load_dotenv
import os
from supabase import create_client

load_dotenv()
s = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_ANON_KEY'))

# Replicate find_cabal_by_name exactly
cabal_name = "JAMIE"
r = s.table("cabals").select("*").ilike("name", cabal_name).execute()
print(f"ilike match for '{cabal_name}': {len(r.data)} rows")
if r.data:
    jamie = r.data[0]
    print(f"  id: {jamie['id']}")
    print(f"  name: '{jamie['name']}'")

    # Now run the wallets query
    w = (
        s.table("wallets")
        .select("*, cabal_members(x_handle)")
        .eq("cabal_id", jamie["id"])
        .order("wallet_type")
        .execute()
    )
    print(f"\nWallets matching cabal_id: {len(w.data)}")
