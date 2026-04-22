import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_ANON_KEY")

print(f"Project URL: {url}")
print(f"Key prefix:  {key[:20]}...")

supabase = create_client(url, key)

# Try a raw read first
print("\n--- SELECT * FROM cabals ---")
result = supabase.table("cabals").select("*").execute()
print(f"Row count: {len(result.data)}")
for row in result.data:
    print(row)

# Try an insert and see exactly what comes back
print("\n--- INSERT test cabal ---")
try:
    insert = supabase.table("cabals").insert({"name": "diagnostic_test"}).execute()
    print(f"Insert result.data: {insert.data}")
    print(f"Insert result type: {type(insert.data)}")
except Exception as e:
    print(f"Insert error: {e}")
