import os, json, time, logging, asyncio, threading
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import requests
from flask import Flask, jsonify, request, Response
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("spider-bot")
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
ALLOWED_USER_ID = os.environ.get("TELEGRAM_ALLOWED_USER_ID", "").strip()
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "")
STATE_FILE = Path(os.environ.get("STATE_FILE", "/var/data/state.json"))
STARTING_BALANCE = 1000.0
TRADE_SIZE = 20.0
MAX_POSITIONS = 8
MAX_TRADES_PER_DAY = 30
TAKE_PROFIT = 0.50
STOP_LOSS = -0.10
MIN_LIQUIDITY_USD = 3000
MIN_M5_VOLUME_USD = 3000
MIN_M5_CHANGE = 5.0
MAX_AGE_HOURS = 24
SCAN_SECONDS = 10
MAX_MARKET_CAP_USD = 300000
MIN_BUY_SELL_RATIO = 1.2
RUGCHECK_BASE = "https://api.rugcheck.xyz"
RUGCHECK_CACHE_TTL = 900
RUGCHECK_FAIL_TTL = 60
RUGCHECK_MAX_SCORE = 40
rugcheck_cache = {}
rugcheck_last_request = 0.0
rugcheck_stats = {"checked": 0, "passed": 0, "blocked": 0, "errors": 0, "status": "starting"}
DEX = "https://api.dexscreener.com"
paused = False
last_scan = "not yet"
last_error = ""
last_pair_count = 0
last_candidate_count = 0
last_candidates = []
events = []
state_lock = threading.RLock()
state = {"cash": STARTING_BALANCE, "starting_balance": STARTING_BALANCE, "positions": {}, "trades": [], "day": "", "daily_count": 0, "price_history": {}}

def add_event(message):
    events.append({"time": datetime.now(timezone.utc).isoformat(), "message": message})
    del events[:-40]
    log.info(message)

def load_state():
    global state
    try:
        if STATE_FILE.exists():
            data = json.loads(STATE_FILE.read_text())
            if isinstance(data, dict) and "cash" in data:
                state.update(data)
                previous_start = float(state.get("starting_balance", 50.0))
                if previous_start < STARTING_BALANCE:
                    state["cash"] = float(state.get("cash", previous_start)) + (STARTING_BALANCE - previous_start)
                    state["starting_balance"] = STARTING_BALANCE
                    save_state()
                    add_event("virtuaalne algkapital suurendatud €1000-ni")
    except Exception as e:
        log.exception("Could not load state: %s", e)

def save_state():
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(STATE_FILE)

def authorized(update):
    return bool(ALLOWED_USER_ID) and str(update.effective_user.id) == ALLOWED_USER_ID

async def send_text(update, message):
    await update.message.reply_text(str(message).lower())

async def reply_denied(update):
    await send_text(update, f"⛔ ligipääs puudub. sinu telegrami id: {update.effective_user.id}")

def get_json(url):
    r = requests.get(url, timeout=15, headers={"User-Agent":"SPIDER-BOT-paper/1.0"})
    r.raise_for_status()
    return r.json()

def rugcheck_token(address):
    """Return (passed, report summary). Fail closed when RugCheck is unavailable."""
    global rugcheck_last_request
    now = time.time()
    cached = rugcheck_cache.get(address)
    if cached and now - cached["time"] < cached["ttl"]:
        return cached["passed"], cached["data"]
    # Stay below the public API's documented free-tier request rate.
    delay = 0.42 - (time.monotonic() - rugcheck_last_request)
    if delay > 0:
        time.sleep(delay)
    try:
        response = requests.get(
            f"{RUGCHECK_BASE}/v1/tokens/{address}/report/summary",
            timeout=8,
            headers={"Accept": "application/json", "User-Agent": "SPIDER-BOT-paper/1.0"},
        )
        rugcheck_last_request = time.monotonic()
        response.raise_for_status()
        report = response.json()
        if not isinstance(report, dict) or report.get("error") or report.get("score_normalised") is None:
            raise ValueError("RugCheck returned an incomplete report")
        score = float(report["score_normalised"])
        risks = report.get("risks") or []
        danger = [r for r in risks if str(r.get("level", "")).lower() in ("danger", "critical", "high")]
        passed = score < RUGCHECK_MAX_SCORE and not danger
        data = {
            "score": score,
            "raw_score": report.get("score"),
            "lp_locked_pct": report.get("lpLockedPct"),
            "risk_count": len(risks),
            "danger_count": len(danger),
            "risks": [{"name": r.get("name", ""), "level": r.get("level", ""), "description": r.get("description", "")} for r in risks[:5]],
            "status": "passed" if passed else "blocked",
            "reason": "risk score too high or danger finding" if not passed else "risk screen passed",
        }
        rugcheck_cache[address] = {"time": now, "ttl": RUGCHECK_CACHE_TTL, "passed": passed, "data": data}
        rugcheck_stats["checked"] += 1
        rugcheck_stats["passed" if passed else "blocked"] += 1
        return passed, data
    except Exception as exc:
        data = {"status": "unavailable", "reason": str(exc)[:160], "score": None, "risk_count": 0, "danger_count": 0, "risks": []}
        rugcheck_cache[address] = {"time": now, "ttl": RUGCHECK_FAIL_TTL, "passed": False, "data": data}
        rugcheck_stats["errors"] += 1
        return False, data

def discover_candidates():
    profiles = get_json(f"{DEX}/token-profiles/latest/v1")
    addresses = list(dict.fromkeys(x.get("tokenAddress") for x in profiles
                      if x.get("chainId") == "solana" and x.get("tokenAddress")))
    candidates = []
    for i in range(0, min(len(addresses), 90), 30):
        data = get_json(f"{DEX}/tokens/v1/solana/{','.join(addresses[i:i+30])}")
        if isinstance(data, list):
            candidates.extend(data)
    return candidates

def pair_age_hours(pair):
    created = pair.get("pairCreatedAt")
    if not created: return 999999
    return max(0, (time.time()*1000 - float(created))/3600000)

def scan_once():
    global last_scan, last_error, last_pair_count, last_candidate_count, last_candidates
    pairs = discover_candidates()
    eligible = []
    for p in pairs:
        try:
            if p.get("chainId") != "solana": continue
            liq = float((p.get("liquidity") or {}).get("usd") or 0)
            market_cap = float(p.get("marketCap") or p.get("fdv") or 0)
            vol = float((p.get("volume") or {}).get("m5") or 0)
            change = float((p.get("priceChange") or {}).get("m5") or 0)
            age = pair_age_hours(p)
            price = float(p.get("priceUsd") or 0)
            address = (p.get("baseToken") or {}).get("address")
            tx5 = (p.get("txns") or {}).get("m5") or {}
            buys = int(tx5.get("buys") or 0)
            sells = int(tx5.get("sells") or 0)
            buy_sell_ratio = buys / max(sells, 1)
            if not address or price <= 0: continue
            if (10000 <= market_cap <= MAX_MARKET_CAP_USD and liq >= MIN_LIQUIDITY_USD
                and vol >= MIN_M5_VOLUME_USD and change >= MIN_M5_CHANGE
                and buys >= 5 and buy_sell_ratio >= MIN_BUY_SELL_RATIO and age <= MAX_AGE_HOURS):
                eligible.append(p)
        except (TypeError, ValueError):
            continue
    eligible.sort(key=lambda p: float((p.get("volume") or {}).get("m5") or 0), reverse=True)
    rug_eligible = []
    for candidate in eligible[:20]:
        token_address = (candidate.get("baseToken") or {}).get("address")
        passed, report = rugcheck_token(token_address)
        candidate["_rugcheck"] = report
        if passed:
            rug_eligible.append(candidate)
        else:
            add_event(f"rugcheck blokeeris {candidate.get('baseToken', {}).get('symbol', '?')}: {report.get('status')} | {report.get('reason', 'risk')}")
    eligible = rug_eligible
    rugcheck_stats["status"] = "connected" if rugcheck_stats["checked"] else "checking"
    with state_lock:
        history_map = state.setdefault("price_history", {})
        for candidate in eligible[:20]:
            token_address = (candidate.get("baseToken") or {}).get("address")
            token_price = float(candidate.get("priceUsd") or 0)
            if token_address and token_price > 0:
                points = history_map.setdefault(token_address, [])
                points.append({"t": int(time.time()), "p": token_price})
                del points[:-80]
        by_address = {(p.get("baseToken") or {}).get("address"):p for p in pairs if p.get("baseToken")}
        for address, pos in list(state["positions"].items()):
            p = by_address.get(address)
            if not p: continue
            price = float(p.get("priceUsd") or 0)
            if price <= 0: continue
            pos["last_price"] = price
            hist = state.setdefault("price_history", {}).setdefault(address, [])
            hist.append({"t": int(time.time()), "p": price})
            del hist[:-80]
            pnl_pct = price / pos["entry_price"] - 1
            if pnl_pct >= TAKE_PROFIT or pnl_pct <= STOP_LOSS:
                proceeds = pos["quantity"] * price
                pnl = proceeds - pos["cost"]
                state["cash"] += proceeds
                state["trades"].append({"symbol":pos["symbol"],"address":address,"entry":pos["entry_price"],
                    "exit":price,"pnl":pnl,"pnl_pct":pnl_pct,"closed_at":datetime.now(timezone.utc).isoformat()})
                add_event(f"müük {pos['symbol']} | {pnl_pct*100:+.1f}% | {pnl:+.2f} €")
                del state["positions"][address]
        today = datetime.now(timezone.utc).date().isoformat()
        if state.get("day") != today:
            state["day"], state["daily_count"] = today, 0
        if not paused and state["daily_count"] < MAX_TRADES_PER_DAY:
            for p in eligible:
                address = p["baseToken"]["address"]
                if address in state["positions"] or len(state["positions"]) >= MAX_POSITIONS or state["cash"] < TRADE_SIZE: continue
                price = float(p["priceUsd"])
                symbol = p["baseToken"].get("symbol","?")
                state["cash"] -= TRADE_SIZE
                state["positions"][address] = {"symbol":symbol,"entry_price":price,"last_price":price,
                    "quantity":TRADE_SIZE/price,"cost":TRADE_SIZE,"opened_at":datetime.now(timezone.utc).isoformat(),
                    "liquidity_usd":float((p.get("liquidity") or {}).get("usd") or 0), "market_cap":float(p.get("marketCap") or p.get("fdv") or 0)}
                state["daily_count"] += 1
                add_event(f"ost {symbol} | €{TRADE_SIZE:.2f} | momentum +{float((p.get('priceChange') or {}).get('m5') or 0):.1f}%")
                if len(state["positions"]) >= MAX_POSITIONS: break
        save_state()
        last_candidates = [{
            "symbol": (p.get("baseToken") or {}).get("symbol","?"),
            "name": (p.get("baseToken") or {}).get("name",""),
            "price": p.get("priceUsd"),
            "change5m": (p.get("priceChange") or {}).get("m5",0),
            "volume5m": (p.get("volume") or {}).get("m5",0),
            "buys5m": ((p.get("txns") or {}).get("m5") or {}).get("buys",0),
            "sells5m": ((p.get("txns") or {}).get("m5") or {}).get("sells",0),
            "buys5m": ((p.get("txns") or {}).get("m5") or {}).get("buys",0),
            "sells5m": ((p.get("txns") or {}).get("m5") or {}).get("sells",0),
            "liquidity": (p.get("liquidity") or {}).get("usd",0),
            "marketCap": p.get("marketCap") or p.get("fdv") or 0,
            "history": state.setdefault("price_history", {}).setdefault((p.get("baseToken") or {}).get("address",""), [])[-60:],
            "address": (p.get("baseToken") or {}).get("address",""),
            "url": p.get("url",""),
            "rugcheck": p.get("_rugcheck", {"status":"not_checked"})
        } for p in eligible[:20]]
        last_pair_count, last_candidate_count = len(pairs), len(eligible)
        last_scan = datetime.now(timezone.utc).isoformat()
        last_error = ""
    return len(pairs), len(eligible)

async def scanner_loop(app):
    while True:
        try:
            total, found = await asyncio.get_running_loop().run_in_executor(app.bot_data["executor"], scan_once)
            add_event(f"skann valmis: {total} paari, {found} signaali")
        except Exception as e:
            global last_error
            last_error = str(e)
            add_event(f"skanni viga: {e}")
            log.exception("Scanner failed")
        await asyncio.sleep(SCAN_SECONDS)

async def post_init(app):
    app.bot_data["executor"] = ThreadPoolExecutor(max_workers=1)
    app.create_task(scanner_loop(app))

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ALLOWED_USER_ID:
        await send_text(update, f"SPIDER BOT online. Telegram ID: {update.effective_user.id}.")
    elif authorized(update):
        await send_text(update, "🕷️ spider bot aktiivne! paper demo €50.\nkäsud: /status /portfolio /trades /scan /pause /resume")
    else: await reply_denied(update)

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update): return await reply_denied(update)
    with state_lock:
        value = sum(x["quantity"]*x.get("last_price",x["entry_price"]) for x in state["positions"].values())
        msg = (f"🕷️ spider bot status\ntelegram: connected\nmootor: {'pausil' if paused else 'töötab'}\n"
        f"režiim: paper demo\nvaba raha: €{state['cash']:.2f}\npositsioonide väärtus: €{value:.2f}\n"
        f"avatud positsioonid: {len(state['positions'])}\nstop loss: -5% | skann: 15s | min turuväärtus: $10k\npaarid: {last_pair_count} | signaalid: {last_candidate_count}\n"
        f"viimane skann: {last_scan}\nskanneri viga: {last_error or 'puudub'}\npäris tehingud: väljas")
    await send_text(update, msg)

async def portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update): return await reply_denied(update)
    with state_lock:
        value = sum(x["quantity"]*x.get("last_price",x["entry_price"]) for x in state["positions"].values())
        await send_text(update, f"📊 paper portfell\nCash: €{state['cash']:.2f}\nOpen positions: €{value:.2f}\nTotal: €{state['cash']+value:.2f}\nsuletud tehinguid: {len(state['trades'])}")

async def trades(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update): return await reply_denied(update)
    with state_lock: rows = state["trades"][-8:]
    if not rows: return await send_text(update, "suletud demotehinguid veel pole.")
    await send_text(update, "📈 viimased demotehingud\n" + "\n".join(f"{x['symbol']}: {x['pnl_pct']*100:+.1f}% ({x['pnl']:+.2f} €)" for x in rows))

async def scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update): return await reply_denied(update)
    await send_text(update, f"Viimane skann: {last_scan}\nPaare: {last_pair_count}\nSignaale: {last_candidate_count}\nViga: {last_error or 'puudub'}")

async def pause_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global paused
    if not authorized(update): return await reply_denied(update)
    paused=True
    await send_text(update, "⏸️ uued paper-ostud peatatud. avatud positsioone jälgitakse edasi.")

async def resume_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global paused
    if not authorized(update): return await reply_denied(update)
    paused=False
    await send_text(update, "▶️ paper-mootor jätkab.")

web = Flask(__name__)
def check_auth(auth):
    return auth and auth.username == "spider" and DASHBOARD_PASSWORD and auth.password == DASHBOARD_PASSWORD
def auth_required():
    return Response("Autentimine vajalik", 401, {"WWW-Authenticate": 'Basic realm="SPIDER BOT"'})
@web.get("/health")
def health():
    return jsonify({"ok": True, "service": "spiderbot"})
@web.get("/")
def dashboard():
    if not check_auth(request.authorization): return auth_required()
    return Response(Path("dashboard.html").read_text(), mimetype="text/html")
@web.get("/api/live")
def live_data():
    if not check_auth(request.authorization): return auth_required()
    with state_lock:
        positions = []
        for address, p in state["positions"].items():
            price = p.get("last_price",p["entry_price"])
            positions.append({"symbol":p["symbol"],"address":address,"entry":p["entry_price"],"price":price,
                "pnl":p["quantity"]*price-p["cost"],"pnl_pct":(price/p["entry_price"]-1)*100,"history":state.get("price_history",{}).get(address,[])})
        pos_value = sum(p["quantity"]*p.get("last_price",p["entry_price"]) for p in state["positions"].values())
        payload = {"cash":state["cash"],"positions_value":pos_value,"total":state["cash"]+pos_value,
            "realized_pnl":sum(t["pnl"] for t in state["trades"]),"positions":positions,
            "trades":state["trades"][-10:],"candidates":last_candidates,"events":events[-20:],
            "last_scan":last_scan,"last_error":last_error,"pair_count":last_pair_count,
            "signal_count":last_candidate_count,"rugcheck":dict(rugcheck_stats),"paused":paused,"mode":"PAPER DEMO","settings":{"min_market_cap":10000,"max_market_cap":MAX_MARKET_CAP_USD,"min_liquidity":MIN_LIQUIDITY_USD,"min_m5_volume":MIN_M5_VOLUME_USD,"min_m5_change":MIN_M5_CHANGE,"min_buy_sell_ratio":MIN_BUY_SELL_RATIO,"stop_loss_pct":STOP_LOSS*100,"scan_seconds":SCAN_SECONDS,"trade_size":TRADE_SIZE}}
    return jsonify(payload)

def run_web():
    web.run(host="0.0.0.0", port=int(os.environ.get("PORT","10000")), use_reloader=False)

def main():
    if not TOKEN: raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")
    load_state()
    threading.Thread(target=run_web, daemon=True).start()
    app = Application.builder().token(TOKEN).post_init(post_init).build()
    for cmd, fn in [("start",start),("status",status),("portfolio",portfolio),("trades",trades),("scan",scan),("pause",pause_cmd),("resume",resume_cmd)]:
        app.add_handler(CommandHandler(cmd,fn))
    add_event("spider bot käivitus; paper demo")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
