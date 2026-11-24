import os
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("spider-bot")
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
ALLOWED_USER_ID = os.environ.get("TELEGRAM_ALLOWED_USER_ID", "").strip()
paused = False

def authorized(update: Update) -> bool:
    return bool(ALLOWED_USER_ID) and str(update.effective_user.id) == ALLOWED_USER_ID

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not ALLOWED_USER_ID:
        await update.message.reply_text(
            f"SPIDER BOT is online. Sinu Telegrami kasutaja ID: {uid}\n"
            "Admin peab lisama selle serveri muutujasse TELEGRAM_ALLOWED_USER_ID."
        )
    elif authorized(update):
        await update.message.reply_text(
            "🕷️ SPIDER BOT ühendus töötab!\nPAPER TRADING DEMO · virtuaalkapital €50.00\n"
            "Käsud: /status /portfolio /trades /pause /resume"
        )
    else:
        await update.message.reply_text(
            f"⛔ Ligipääs puudub. Sinu Telegrami kasutaja ID: {uid}\\n"
            "Lisa see täpselt Renderi TELEGRAM_ALLOWED_USER_ID väärtuseks."
        )

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        await update.message.reply_text("Ligipääs puudub või kasutaja pole seadistatud.")
        return
    await update.message.reply_text(
        "🕷️ SPIDER BOT STATUS\nTelegram: CONNECTED\nEngine: NOT STARTED\n"
        f"Mode: {'PAUSED' if paused else 'PAPER DEMO'}\n"
        "Virtual balance: €50.00\nMarket scanner: not connected\nReal trades: DISABLED"
    )

async def portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        await update.message.reply_text("Ligipääs puudub.")
        return
    await update.message.reply_text("📊 DEMO PORTFOLIO\nAlgkapital €50.00\nAvatud positsioone 0\nPnL €0.00\nTuruandmed pole veel ühendatud.")

async def trades(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        await update.message.reply_text("Ligipääs puudub.")
        return
    await update.message.reply_text("📈 Tehinguid pole veel. Paper-trading mootor pole käivitatud.")

async def pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global paused
    if not authorized(update):
        await update.message.reply_text("Ligipääs puudub.")
        return
    paused = True
    await update.message.reply_text("⏸️ Demo juhtimine peatatud.")

async def resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global paused
    if not authorized(update):
        await update.message.reply_text("Ligipääs puudub.")
        return
    paused = False
    await update.message.reply_text("▶️ Demo juhtimine lubatud. Turu skanner pole veel ühendatud.")

def main():
    if not TOKEN:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")
    app = Application.builder().token(TOKEN).build()
    for command, handler in [
        ("start", start), ("status", status), ("portfolio", portfolio),
        ("trades", trades), ("pause", pause), ("resume", resume)
    ]:
        app.add_handler(CommandHandler(command, handler))
    log.info("SPIDER BOT Telegram polling started")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
