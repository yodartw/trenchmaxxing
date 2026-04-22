from dotenv import load_dotenv
import os
from supabase import create_client

load_dotenv()
s = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_ANON_KEY'))

r = s.table('wallets').select('*, cabals(name), cabal_members(x_handle)').execute()
print(f"Total wallets in DB: {len(r.data)}\n")

for w in r.data:
    cabal = w['cabals']['name'] if w.get('cabals') else 'NO CABAL'
    member = w['cabal_members']['x_handle'] if w.get('cabal_members') else 'no member'
    print(f"  {w['chain']} {w['address'][:12]}... -> cabal={cabal} member={member} type={w['wallet_type']}")
