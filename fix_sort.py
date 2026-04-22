with open("bot.py") as f:
    code = f.read()

old_sort = '            for c in sorted(coins, key=lambda x: (not x["is_primary"], x["outcome"])):'
new_sort = '''            for c in sorted(
                coins,
                key=lambda x: (
                    not x["is_primary"],
                    OUTCOME_PRIORITY.get(x["outcome"], 99),
                    x["tokens"]["symbol"],
                ),
            ):'''

if old_sort in code:
    code = code.replace(old_sort, new_sort)
    print("Replaced sort line")
else:
    print("OLD SORT NOT FOUND")

marker = "        coins = coins_result.data or []"
dict_block = marker + '''

        OUTCOME_PRIORITY = {
            "open": 0,
            "runner": 1,
            "mid": 2,
            "slow_bleed": 3,
            "dead": 4,
            "rug": 5,
        }'''

if "OUTCOME_PRIORITY" in code:
    print("OUTCOME_PRIORITY already present")
elif marker in code:
    code = code.replace(marker, dict_block, 1)
    print("Inserted OUTCOME_PRIORITY dict")
else:
    print("MARKER NOT FOUND")

with open("bot.py", "w") as f:
    f.write(code)

print("Done. Saved bot.py")
