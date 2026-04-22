cat > README.md << 'EOF'
# Trencher

A personal intelligence bot for Solana/EVM memecoin cabal tracking. Maps known crews, their wallets, and their launches — then surfaces onchain confluence signals when multiple tracked wallets converge on a new token.

Built as a solo-founder tool, not a SaaS product.

## Core Thesis

Onchain wallet activity is a leading indicator. Social signal (X posts, Telegram calls) is a trailing indicator — by the time a KOL tweets a ticker, their wallet bought 15 minutes earlier. If we can track the wallets directly, we see the move before the market does.

The system has two layers:

1. **Attribution layer** — manually-curated cabal graph. Who belongs to which crew, what they've launched, what they're known for. This is the defensible edge — no public product has this.
2. **Signal layer** — onchain activity monitoring. When N attributed wallets (or auto-discovered "smart money" wallets) buy the same new token in a short window, that's confluence worth acting on.

The combination is stronger than either alone. Attribution without performance data is opinion. Performance data without attribution is noise.

## Stack

- **Python 3.12** + [python-telegram-bot](https://github.com/python-telegram-bot/python-telegram-bot) — Telegram interface
- **Supabase** (Postgres + RLS) — cabal graph, wallet store, activity history
- **DexScreener API** — market data across Solana, Base, Ethereum, BSC
- **Helius API** — Solana wallet activity (paid tier, $49/mo)
- **Alchemy / Etherscan** — EVM wallet activity (planned)

## Setup

Prereqs: Python 3.12+, Supabase project, Telegram bot (via [@BotFather](https://t.me/BotFather)), Helius API key.

```bash
git clone https://github.com/yodartw/trenchmaxxing.git
cd trenchmaxxing

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Fill in .env with actual values — request from Lah privately
```

Run the schema migrations against Supabase (see `/schema` folder or ping Lah for the SQL).

```bash
python3 bot.py
```

## Commands (current)

### Cabal management
- `/addcabal <name>` — create a cabal
- `/setchain <cabal> <chain>` — set primary chain
- `/listcabals` — all cabals
- `/cabal <name>` — full dossier with members, coins, stats, live market data

### Members
- `/addmember <cabal> <@handle> <role> [confidence]` — add member
- `/members <cabal>` — list members

### Tokens
- `/addtoken <chain> <address> <symbol>` — register token
- `/linktoken <cabal> <symbol> <involvement> [primary]` — attribute token to cabal
- `/setoutcome <symbol> <outcome>` — mark outcome (open/runner/mid/rug/slow_bleed/dead)
- `/tokens <cabal>` — list cabal's tokens with live mcap

### Wallets
- `/addwallet <chain> <address> <owner> <type> [confidence]` — add wallet (owner can be cabal or @member)
- `/wallet <address>` — wallet dossier
- `/wallets <cabal>` — wallets for a cabal

### Activity & Signal
- `/activity <address> [limit]` — last N swaps for a wallet, filters established coins
- `/recent <cabal> [hours]` — recent activity across cabal's wallets, aggregated by token
- `/confluence [cabal] [hours] [min_wallets]` — multi-wallet convergence within threshold
- `/confluence all [hours] [min_wallets]` — scan the full pool (attributed + unknown tier)

## Data Model

| Table | Purpose |
|---|---|
| `cabals` | Crews. One row per. Tracks name, primary chain, status. |
| `cabal_members` | X handles associated with a cabal. Role + confidence tier. |
| `tokens` + `cabal_coin_links` | Tokens and their cabal attributions. Primary cabal marked. |
| `token_snapshots` | Periodic DexScreener snapshots per token for historical mcap. |
| `wallets` | Addresses. Tied to cabal and/or member. `quality_tier` = attributed/unknown/raw. |
| `wallet_activity` | Persistent swap history fetched from Helius. Buys + sells. |

`quality_tier` is the critical filter:
- **attributed** — manually verified. Tied to a real cabal member. Highest signal.
- **unknown** — imported from external sources (Axiom, Cielo, etc). Known-good by reputation but unattributed.
- **raw** — auto-discovered via /scan. Lowest trust until validated by performance.

## Roadmap

### Phase 1 — Foundation (complete)
Manual cabal graph, member/token/wallet CRUD, DexScreener integration, `token_snapshots` history.

### Phase 2a — Solana activity tracking (complete)
Helius integration, wallet activity ingestion, `/activity` and `/recent` commands, confluence detection across attributed + unknown wallets with mcap filter.

### Phase 2b — Auto-discovery (`/scan`)
Input a CA → pull first 50 buyers from Helius → filter MEV/bundlers/copy-traders → cross-reference against existing DB → add survivors as `quality_tier='raw'` with provenance tag. Scales the wallet pool without sacrificing attribution integrity.

### Phase 2c — Wallet performance scoring
Per-wallet rolling metrics: win rate, avg multiple on winners, hit rate on sub-$50k entries. Auto-demote `raw` wallets that underperform. Auto-surface `unknown` wallets that consistently hit. Self-cleaning DB, inspired by the "confluence bot" design Lah originally scoped.

### Phase 2d — Real-time alerts (requires Railway)
Background job polling tracked wallets every N minutes (or Helius webhooks for lower latency). When threshold fires, DM to Telegram. Needs 24/7 hosting — current laptop setup isn't sufficient.

### Phase 3 — Cross-chain expansion
ETH/Base/BSC wallet activity via Alchemy or Etherscan. Matters because JAMIE is mostly ETH and the cabal graph should work uniformly.

### Phase 4 — X/Telegram scraping layer
Correlate social posts with onchain events. If @cryptojamie7 tweets a ticker 8 minutes after his wallet bought, that's a higher-conviction signal than the buy alone.

## Development Workflow

This is a private repo with a small team. Rules to prevent chaos:

1. **Never push to `main` directly.** Work on branches, open PRs, review before merge.
```bash
   git checkout -b feature/scan-command
   # ... work ...
   git push -u origin feature/scan-command
```
2. **Pull before you work.**
```bash
   git checkout main && git pull
```
3. **Secrets stay out of git.** `.env` is gitignored. Share values privately (Signal/TG/iMessage). Never paste keys in code, commits, or issues.
4. **Every new feature includes a schema migration SQL if tables change.** Store in `/schema/NNN_description.sql`.

## Trading Philosophy

The bot surfaces signals. It does not auto-trade. That's intentional.

- Small-size validation period (2-4 weeks, $500-1k trades per signal) before scaling.
- Track every trade + outcome. Feed outcomes back into wallet scoring.
- Pre-defined exits (3x profit target or -50% stop). No revenge trades.
- Signal quality compounds with discipline, not speed.

This is a co-pilot, not an autopilot.

## License & Ownership

Private. No license. Not for distribution.
EOF
