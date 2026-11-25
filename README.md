# SPIDER BOT

Solana market scanner and paper-trading bot with Telegram controls and a protected web dashboard.

## Safety and operating mode

- Paper/simulated trading only. The bot does not sign transactions, connect a wallet, or submit real orders.
- Virtual starting balance: €1,000.
- Daily session stop: -€40. Maximum open positions: 20.
- Total portfolio exposure is capped at 50% of estimated equity.
- The €40 daily stop is a software guardrail, not a guarantee against losses in a real market; this project is paper-only.
- Keep API keys and Telegram credentials in the hosting provider's environment settings. Never commit secrets.

## Features

- Solana market discovery from public DexScreener and GeckoTerminal feeds.
- Separate small/base-cap and large-cap screening lanes.
- Momentum score (0–100), market regime estimate, RugCheck hard gates, and supplementary risk score.
- Risk-adjusted simulated position sizing, scalp/runner exits, profit protection, and stop-loss cooldown.
- Optional manually configured token blacklist via `TOKEN_BLACKLIST` (comma-separated mint addresses).
- Wallet discovery and swap signals when Birdeye and Helius API keys are configured. Wallet signals can only create paper trades.
- Telegram alerts for major paper trades, exits, session halts, and scanner errors.
- Strategy results, 15m/60m forward signal outcomes, trade journal, and CSV export.
- Forward signal outcomes are not a historical backtest. Historical replay requires a separate candle dataset and is not inferred from live observations.

## Required environment variables

- `TELEGRAM_BOT_TOKEN`: Telegram bot token.
- `TELEGRAM_ALLOWED_USER_ID`: your numeric Telegram user ID; only this user can control the bot.
- `DASHBOARD_PASSWORD`: password for the dashboard. Dashboard username is `spider`.
- `STATE_FILE`: persistent state path. Render config uses `/var/data/state.json`.

## Optional environment variables

- `BIRDEYE_API_KEY` and `HELIUS_API_KEY`: enable wallet discovery and wallet-activity monitoring.
- `TOKEN_BLACKLIST`: comma-separated Solana token mint addresses to block.
- `TELEGRAM_ALERTS_ENABLED`: `true` by default; set `false` to disable event alerts.
- `JUPITER_API_KEY`: reserved for quote-route diagnostics; it does not enable live trading.

## Telegram commands

- `/start`, `/status`, `/portfolio`, `/trades`, `/scan`
- `/wallets`
- `/pause` stops new simulated buys while continuing to monitor and manage open paper positions.
- `/resume` resumes paper scanning and entries.

## Deploy

Deploy as the Docker web service configured in `render.yaml`. Attach persistent storage at `/var/data` so the virtual portfolio and signal history survive restarts. Configure the required environment variables in Render before starting the service.

## Validation

GitHub Actions checks Python compilation, dashboard JavaScript syntax, and the paper trading rule unit tests on changes to the bot, dashboard, requirements, or test suite.
