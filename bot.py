import os, json, time, logging, asyncio
from pathlib import Path
from datetime import datetime, timezone
import requests
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("spider-bot")
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
ALLOWED_USER_ID = os.environ.get("TELEGRAM_ALLOWED_USER_ID", "").strip()
STATE_FILE = Path(os.environ.get("STATE_FILE", "data/state.json"))
STARTING_BALANCE = 50.0
TRADE_SIZE = 5.0
MAX_POSITIONS = 4
MAX_TRADES_PER_DAY = 20
TAKE_PROFIT = 0.20
STOP_LOSS = -0.10
MIN_LIQUIDITY_USD = 30000
MIN_M5_VOLUME_USD = 5000
MIN_M5_CHANGE = 6.0
MAX_AGE_HOURS = 72
SCAN_SECONDS = 60
DEX = "https://api.dexscreener.com"
paused = False
last_scan = "not yet"
last_error = ""
state = {"cash": STARTING_BALANCE, "positions": {}, "trades": [], "day": "", "daily_count": 0}

def load_state():
    global state
    try:
        if STATE_FILE.exists():
            data = json.loads(STATE_FILE.read_text())
            if isinstance(data, dict) and "cash" in data:
                state.update(data)
    except Exception as e:
        log.exception("Could not load state: %s", e)

def save_state():
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(STATE_FILE)

def authorized(update: Update) -> bool:
    return bool(ALLOWED_USER_ID) and str(update.effective_user.id) == ALLOWED_USER_ID

async def reply_denied(update):
    uid = update.effective_user.id
    await update.message.reply_text(f"⛔ Ligipääs puudub. Sinu Telegrami ID: {uid}")

def get_json(url):
    r = requests.get(url, timeout=15, headers={"User-Agent":"SPIDER-BOT-paper/1.0"})
    r.raise_for_status()
    return r.json()

def discover_candidates():
    profiles = get_json(f"{DEX}/token-profiles/latest/v1")
    addresses = list(dict.fromkeys(x.get("tokenAddress") for x in profiles
                      if x.get("chainId") == "solana" and x.get("tokenAddress")))
    candidates = []
    # DexScreener token endpoint supports comma-separated addresses in small batches.
    for i in range(0, min(len(addresses), 60), 30):
        batch = addresses[i:i+30]
        data = get_json(f"{DEX}/tokens/v1/solana/{','.join(batch)}")
        if isinstance(data, list):
            candidates.extend(data)
    return candidates

def pair_age_hours(pair):
    created = pair.get("pairCreatedAt")
    if not created:
        return 999999
    return max(0, (time.time()*1000 - float(created))/3600000)

def scan_once():
    global last_scan, last_error
    last_error = ""
    pairs = discover_candidates()
    eligible = []
    for p in pairs:
        try:
            if p.get("chainId") != "solana":
                continue
            liq = float((p.get("liquidity") or {}).get("usd") or 0)
            vol = float((p.get("volume") or {}).get("m5") or 0)
            change = float((p.get("priceChange") or {}).get("m5") or 0)
            age = pair_age_hours(p)
            price = float(p.get("priceUsd") or 0)
            address = (p.get("baseToken") or {}).get("address")
            if not address or price <= 0:
                continue
            if liq >= MIN_LIQUIDITY_USD and vol >= MIN_M5_VOLUME_USD and change >= MIN_M5_CHANGE and age <= MAX_AGE_HOURS:
                eligible.append(p)
        except (TypeError, ValueError):
            continue
    eligible.sort(key=lambda p: float((p.get("volume") or {}).get("m5") or 0), reverse=True)
    # Update current prices and close paper positions on TP/SL.
    by_address = {((p.get("baseToken") or {}).get("address")):p for p in pairs if p.get("baseToken")}
    for address, pos in list(state["positions"].items()):
        p = by_address.get(address)
        if not p:
            continue
        price = float(p.get("priceUsd") or 0)
        if price <= 0:
            continue
        pos["last_price"] = price
        pnl_pct = price / pos["entry_price"] - 1
        if pnl_pct >= TAKE_PROFIT or pnl_pct <= STOP_LOSS:
            proceeds = pos["quantity"] * price
            pnl = proceeds - pos["cost"]
            state["cash"] += proceeds
            state["trades"].append({"symbol":pos["symbol"],"address":address,"entry":pos["entry_price"],
                "exit":price,"pnl":pnl,"pnl_pct":pnl_pct,"closed_at":datetime.now(timezone.utc).isoformat()})
            del state["positions"][address]
    today = datetime.now(timezone.utc).date().isoformat()
    if state.get("day") != today:
        state["day"], state["daily_count"] = today, 0
    if not paused and state["daily_count"] < MAX_TRADES_PER_DAY:
        for p in eligible:
            address = p["baseToken"]["address"]
            if address in state["positions"] or len(state["positions"]) >= MAX_POSITIONS or state["cash"] < TRADE_SIZE:
                continue
            price = float(p["priceUsd"])
            state["cash"] -= TRADE_SIZE
            state["positions"][address] = {"symbol":p["baseToken"].get("symbol","?"),"entry_price":price,
                "last_price":price,"quantity":TRADE_SIZE/price,"cost":TRADE_SIZE,
                "opened_at":datetime.now(timezone.utc).isoformat(),"liquidity_usd":float((p.get("liquidity") or {}).get("usd") or 0)}
            state["daily_count"] += 1
            if len(state["positions"]) >= MAX_POSITIONS:
                break
    save_state()
    last_scan = datetime.now(timezone.utc).isoformat()
    return len(pairs), len(eligible)

async def scanner_loop(app):
    while True:
        try:
            total, found = await asyncio.get_running_loop().run_in_executor(app.bot_data["executor"], scan_once)
            log.info("Scan complete: %s Solana pairs, %s momentum candidates", total, found)
        except Exception as e:
            global last_error
            last_error = str(e)
            log.exception("Scanner failed")
        await __import__("asyncio").sleep(SCAN_SECONDS)

async def post_init(app):
    import asyncio
    app.bot_data["executor"] = __import__("concurrent.futures").futures.ThreadPoolExecutor(max_workers=1)
    app.create_task(scanner_loop(app))

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ALLOWED_USER_ID:
        await update.message.reply_text(f"SPIDER BOT online. Telegram ID: {update.effective_user.id}. Lisa see Renderi TELEGRAM_ALLOWED_USER_ID muutujasse.")
    elif authorized(update):
        await update.message.reply_text("🕷️ SPIDER BOT aktiivne! PAPER DEMO €50.\nKäsud: /status /portfolio /trades /scan /pause /resume")
    else:
        await reply_denied(update)

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update): return await reply_denied(update)
    value = sum(x["quantity"]*x.get("last_price",x["entry_price"]) for x in state["positions"].values())
    await update.message.reply_text(
        f"🕷️ SPIDER BOT STATUS\nTelegram: CONNECTED\nEngine: {'PAUSED' if paused else 'RUNNING'}\n"
        f"Mode: PAPER DEMO\nCash: €{state['cash']:.2f}\nPositions value: €{value:.2f}\n"
        f"Open positions: {len(state['positions'])}\nScanner last run: {last_scan}\n"
        f"Last scanner error: {last_error or 'none'}\nReal trades: DISABLED"
    )

async def portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update): return await reply_denied(update)
    value = sum(x["quantity"]*x.get("last_price",x["entry_price"]) for x in state["positions"].values())
    await update.message.reply_text(f"📊 PAPER PORTFOLIO\nCash: €{state['cash']:.2f}\nOpen positions: €{value:.2f}\nTotal: €{state['cash']+value:.2f}\nRealized trades: {len(state['trades'])}")

async def trades(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update): return await reply_denied(update)
    rows = state["trades"][-8:]
    if not rows: return await update.message.reply_text("Tehinguid pole veel suletud. Vaata avatud positsioone käsuga /portfolio.")
    lines = [f"{x['symbol']}: {x['pnl_pct']*100:+.1f}% ({x['pnl']:+.2f} €)" for x in rows]
    await update.message.reply_text("📈 VIIMASED DEMOTEHINGUD\n" + "\n".join(lines))

async def scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update): return await reply_denied(update)
    await update.message.reply_text(f"Viimane skann: {last_scan}\nViga: {last_error or 'puudub'}")

async def pause_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global paused
    if not authorized(update): return await reply_denied(update)
    paused=True
    await update.message.reply_text("⏸️ Paper-mootor peatatud. Avatud positsioone jälgitakse edasi.")

async def resume_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global paused
    if not authorized(update): return await reply_denied(update)
    paused=False
    await update.message.reply_text("▶️ Paper-mootor jätkab.")

def main():
    if not TOKEN: raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")
    load_state()
    app = Application.builder().token(TOKEN).post_init(post_init).build()
    for cmd, fn in [("start",start),("status",status),("portfolio",portfolio),("trades",trades),("scan",scan),("pause",pause_cmd),("resume",resume_cmd)]:
        app.add_handler(CommandHandler(cmd,fn))
    log.info("SPIDER BOT Telegram + paper scanner starting")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
