# SPIDER BOT

Telegram command bot starter for SPIDER BOT.

## Current scope
- Telegram polling with /start, /status, /portfolio, /trades, /pause and /resume
- Demo-only status with €50 virtual starting balance
- No wallet connection, private keys, signing, or real trades
- Market scanner and paper-trading engine are not yet connected

## Deploy
Deploy as a background worker using the included Dockerfile or render.yaml.
Set these environment variables in the hosting dashboard:
- TELEGRAM_BOT_TOKEN
- TELEGRAM_ALLOWED_USER_ID

Never commit your real Telegram token. To obtain your numeric Telegram user ID, deploy once with TELEGRAM_ALLOWED_USER_ID empty and send /start; the bot replies with your ID. Then set the variable and redeploy.

Free hosting may sleep or restrict background workers. An always-on worker may require a paid plan.
