import os
import json
import urllib.request
from dotenv import load_dotenv

load_dotenv()
token = os.getenv("TELEGRAM_BOT_TOKEN")

# Test 1 — who is this bot?
print("=== Test 1: getMe ===")
r = urllib.request.urlopen(f"https://api.telegram.org/bot{token}/getMe")
data = json.loads(r.read())
print(json.dumps(data, indent=2))

# Test 2 — any messages queued for me?
print("\n=== Test 2: getUpdates ===")
r = urllib.request.urlopen(f"https://api.telegram.org/bot{token}/getUpdates")
data = json.loads(r.read())
print(json.dumps(data, indent=2))