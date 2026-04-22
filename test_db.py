import os
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")

print(f"URL loaded: {bool(SUPABASE_URL)}")
print(f"Key loaded: {bool(SUPABASE_ANON_KEY)}")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

print("Inserting Fapital...")
insert_result = supabase.table("cabals").insert({
    "name": "Fapital",
    "chain": "sol",
    "notes": "First cabal added to the system",
}).execute()
print(f"Inserted: {insert_result.data}")

print("\nReading all cabals...")
read_result = supabase.table("cabals").select("*").execute()
for cabal in read_result.data:
    print(f"- {cabal['name']} ({cabal['chain']}) — {cabal['status']}")
