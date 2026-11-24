import os, json, time, logging, asyncio, threading, copy, random, xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo
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
POSITION_RISK_PCT = 0.075
MAX_POSITIONS = 3
MAX_TRADES_PER_DAY = 200
SCALP_TAKE_PROFIT = 0.30
SCALP_STOP_LOSS = -0.15
TIME_STOP_SECONDS = 240
FLAT_BAND_PCT = 0.03
RUNNER_EVERY_NTH_ENTRY = 3
RUNNER_HALF_TP = 0.50
RUNNER_TRAIL_GIVEBACK = 0.25
DAILY_STOP_LOSS_EUR = -200.0
REDUCE_SIZE_AT_PROFIT_EUR = 500.0
DAILY_TARGET_EUR = 2000.0
SIM_SLIPPAGE_PCT = 0.035
SIM_LATENCY_MS = 300
SIM_FAILED_TX_RATE = 0.12
SIM_TX_FEE_EUR = 0.10
SIM_FAILED_TX_FEE_EUR = 0.05
MIN_LIQUIDITY_USD = 25000.0
MIN_MARKET_CAP_USD = 100000.0
MAX_MARKET_CAP_USD = 2000000.0
MIN_POOL_AGE_MINUTES = 5.0
MAX_POOL_AGE_MINUTES = 60.0
MIN_VOLUME_SPIKE_MULTIPLE = 3.0
MIN_BUYER_SELLER_RATIO = 1.5
MAX_TOP10_HOLDERS_PCT = 30.0
MAX_DEV_HOLDER_PCT = 5.0
MIN_LP_LOCKED_PCT = 95.0
MAX_BUNDLE_CLUSTER_PCT = 20.0
MAX_SINGLE_WALLET_VOLUME_PCT = 20.0
MIN_HOLDER_GROWTH_PCT = 5.0
MAX_10M_PRICE_GAIN_PCT = 100.0
MAX_SELL_PRICE_IMPACT_PCT = 1.0
SCAN_SECONDS = 10
JUPITER_API_KEY = os.environ.get("JUPITER_API_KEY", "").strip()
JUPITER_BASE = "https://api.jup.ag/swap/v2"
SOL_MINT = "So11111111111111111111111111111111111111112"
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
AGENT_DEFS = {
    "uljas": {"name":"ULJAS","role":"Võimaluste otsija","goal":"Leiab uusi Solana tokenipaare ja filtreerib turuaktiivsuse järgi.","color":"#ffd23f"},
    "teataja": {"name":"TEATAJA","role":"Uudiste ja katalüsaatorite uurija","goal":"Uurib DEX-i projektilinke ja avalikke kogukonnasignaale; X-i tasuline API ei ole kasutusel.","color":"#ff5964"},
    "taju": {"name":"TAJU","role":"Turu meeleolu analüütik","goal":"Hindab ostu- ja müügisurvet päris tehinguandmete põhjal.","color":"#c66bff"},
    "teras": {"name":"TERAS","role":"Graafikute analüütik","goal":"Hindab hinnamomenti, volüümi ja võimalikku sisenemiskohta.","color":"#3298ff"},
    "valvur": {"name":"VALVUR","role":"Riskikaitsja","goal":"Kontrollib RugChecki riske ja blokeerib ohtlikud või kontrollimata tokenid.","color":"#31d98b"},
    "jano": {"name":"JANO","role":"Peajuht ja otsustaja","goal":"Koondab tõendid ning lubab ainult reeglitele vastava paper-tehingu.","color":"#ff9b35"},
}
agents = {key: {"id":key, **meta, "status":"ootel", "task":"Ootan esimest skanni", "result":"Andmeid pole veel kogutud", "updated":None, "checks":0, "last_symbol":None, "decision":"WAIT"} for key,meta in AGENT_DEFS.items()}
agent_history = []
def update_agent(key, status, task, result, symbol=None, checks=None, decision=None):
    row = agents[key]
    row.update({"status":status, "task":task, "result":str(result)[:260],
                "updated":datetime.now(timezone.utc).isoformat(), "last_symbol":symbol})
    if decision is not None: row["decision"] = decision
    if checks is not None: row["checks"] = int(checks)
    agent_history.append({"agent":row["name"], "status":status, "message":f"{task}: {result}", "time":row["updated"], "symbol":symbol})
    del agent_history[:-60]
state_lock = threading.RLock()
state = {"cash": STARTING_BALANCE, "starting_balance": STARTING_BALANCE, "positions": {}, "trades": [], "day": "", "daily_count": 0, "price_history": {}, "volume_history": {}, "holder_history": {}, "stopped_tokens": {}, "entry_count": 0, "day_start_equity": STARTING_BALANCE, "session_halted": False, "session_halt_reason": ""}
persistence_ok = False
persistence_error = "state not loaded"

def add_event(message):
    events.append({"time": datetime.now(timezone.utc).isoformat(), "message": message})
    del events[:-40]
    log.info(message)

def load_state():
    global state, persistence_ok, persistence_error
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        if STATE_FILE.exists():
            data = json.loads(STATE_FILE.read_text())
            if not isinstance(data, dict) or "cash" not in data or "positions" not in data or "trades" not in data:
                raise ValueError("state file is incomplete; refusing to reset portfolio")
            state.update(data)
            previous_start = float(state.get("starting_balance", 50.0))
            if previous_start < STARTING_BALANCE:
                state["cash"] = float(state.get("cash", previous_start)) + (STARTING_BALANCE - previous_start)
                state["starting_balance"] = STARTING_BALANCE
                save_state()
                add_event("virtuaalne algkapital suurendatud €1000-ni")
            else:
                persistence_ok = True
                persistence_error = ""
                add_event(f"portfell taastatud püsikettalt: {len(state['positions'])} avatud positsiooni")
        else:
            save_state()
            add_event("uus paper-portfell loodud €1000 algkapitaliga")
    except Exception as e:
        persistence_ok = False
        persistence_error = str(e)[:240]
        log.exception("Could not load persistent portfolio: %s", e)
        raise RuntimeError(f"Portfolio persistence unavailable: {e}") from e

def save_state():
    global persistence_ok, persistence_error
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE.with_suffix(".tmp")
        with tmp.open("w") as f:
            json.dump(state, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        tmp.replace(STATE_FILE)
        persistence_ok = True
        persistence_error = ""
    except Exception as e:
        persistence_ok = False
        persistence_error = str(e)[:240]
        log.exception("Could not save persistent portfolio: %s", e)
        raise

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

DISCOVERY_TTL = 60
discovery_cache = {"time": 0, "addresses": [], "sources": {}}
discovery_stats = {"addresses": 0, "dex_pairs": 0, "sources": {}, "updated": "not yet", "errors": []}

def _solana_addresses(rows):
    out = []
    for row in rows if isinstance(rows, list) else []:
        if row.get("chainId") == "solana" and row.get("tokenAddress"):
            out.append(row["tokenAddress"])
    return out

def _gecko_solana_addresses(payload):
    out = []
    if not isinstance(payload, dict):
        return out
    for item in payload.get("data", []):
        rel = ((item.get("relationships") or {}).get("base_token") or {}).get("data") or {}
        token_id = rel.get("id", "")
        if token_id.startswith("solana_"):
            out.append(token_id.split("_", 1)[1])
    for item in payload.get("included", []):
        if item.get("type") == "token":
            attrs = item.get("attributes") or {}
            address = attrs.get("address")
            if address:
                out.append(address)
    return out

def discover_candidates():
    now = time.time()
    if now - discovery_cache["time"] >= DISCOVERY_TTL:
        sources = {}
        errors = []
        def pull(name, url, parser):
            try:
                sources[name] = parser(get_json(url))
            except Exception as exc:
                errors.append(f"{name}: {str(exc)[:100]}")
                sources[name] = []
        # DexScreener discovery feeds: newest profiles, new boosts, top boosts, community takeovers.
        pull("profiles", f"{DEX}/token-profiles/latest/v1", _solana_addresses)
        pull("boosts_latest", f"{DEX}/token-boosts/latest/v1", _solana_addresses)
        pull("boosts_top", f"{DEX}/token-boosts/top/v1", _solana_addresses)
        pull("community_takeovers", f"{DEX}/community-takeovers/latest/v1", _solana_addresses)
        # GeckoTerminal supplies actual new Solana pools and a separate trending-pool feed.
        pull("new_pools", "https://api.geckoterminal.com/api/v2/networks/solana/new_pools?include=base_token", _gecko_solana_addresses)
        pull("trending_pools", "https://api.geckoterminal.com/api/v2/networks/solana/trending_pools?duration=5m&include=base_token", _gecko_solana_addresses)
        merged = {}
        for source, items in sources.items():
            for address in items:
                if address:
                    merged.setdefault(address, set()).add(source)
        discovery_cache["addresses"] = list(merged.keys())[:240]
        discovery_cache["sources"] = {address: sorted(list(tags)) for address, tags in merged.items()}
        discovery_cache["time"] = now
        discovery_stats.update({
            "addresses": len(discovery_cache["addresses"]),
            "sources": {name: len(items) for name, items in sources.items()},
            "updated": datetime.now(timezone.utc).isoformat(),
            "errors": errors
        })
        if errors:
            add_event("discovery source warnings: " + "; ".join(errors))
    addresses = discovery_cache["addresses"]
    candidates = []
    # DexScreener supports up to 30 token addresses per batch; stay within its 300 rpm limit.
    for i in range(0, len(addresses), 30):
        batch = addresses[i:i+30]
        try:
            data = get_json(f"{DEX}/tokens/v1/solana/{','.join(batch)}")
            if isinstance(data, list):
                for pair in data:
                    token_address = (pair.get("baseToken") or {}).get("address")
                    pair["_discovery_sources"] = discovery_cache["sources"].get(token_address, [])
                candidates.extend(data)
        except Exception as exc:
            add_event(f"dex batch error: {str(exc)[:120]}")
    discovery_stats["dex_pairs"] = len(candidates)
    return candidates

def pair_age_hours(pair):
    created = pair.get("pairCreatedAt")
    if not created: return 999999
    return max(0, (time.time()*1000 - float(created))/3600000)

def score_candidate(pair):
    """Heuristic short-term score; 0-100, not a prediction."""
    price = float(pair.get("priceUsd") or 0)
    liq = float((pair.get("liquidity") or {}).get("usd") or 0)
    v5 = float((pair.get("volume") or {}).get("m5") or 0)
    v1 = float((pair.get("volume") or {}).get("h1") or 0)
    ch = pair.get("priceChange") or {}
    m5, h1, h6 = float(ch.get("m5") or 0), float(ch.get("h1") or 0), float(ch.get("h6") or 0)
    tx = (pair.get("txns") or {}).get("m5") or {}
    buys, sells = int(tx.get("buys") or 0), int(tx.get("sells") or 0)
    ratio, total = buys / max(sells, 1), buys + sells
    if price <= 0 or liq < MIN_LIQUIDITY_USD or v5 < MIN_M5_VOLUME_USD or total < 4:
        return 0, "low activity", ratio
    score = min(20, (liq / 3000) ** 0.5 * 5) + min(20, (v5 / 1000) ** 0.5 * 4)
    score += min(15, ratio * 5) + min(10, total * 1.2)
    if h1 > 0 and h6 > -5: score += min(15, h1 * 1.2)
    if 1 <= m5 <= 12 and h1 > 0:
        score += 15
        setup = "momentum"
    elif -8 <= m5 < 1 and h1 >= 2 and h6 > -8 and ratio >= 1:
        score += 18
        setup = "dip-buy"
    elif m5 > 12:
        score -= 12
        setup = "overheated"
    else:
        setup = "mixed"
    if v1 > 0 and v5 / max(v1, 1) > 0.8: score += 5
    return min(100, max(0, score)), setup, ratio

def dynamic_trade_size(score, cash):
    wanted = 50 if score >= 82 else 40 if score >= 72 else 25 if score >= 64 else 10
    return round(min(MAX_TRADE_SIZE, wanted, cash), 2)
def scan_once():
    global last_scan, last_error, last_pair_count, last_candidate_count, last_candidates
    update_agent("uljas","working","Otsib uusi Solana paare","Laeb DEX Screeneri ja GeckoTerminali vooge")
    pairs = discover_candidates()
    update_agent("uljas","active","Skaneeris turgu",f"{len(pairs)} paari; {len(discovery_cache.get('addresses',[]))} tokeniaadressi",checks=len(pairs))
    eligible = []
    for p in pairs:
        try:
            if p.get("chainId") != "solana": continue
            address = (p.get("baseToken") or {}).get("address")
            price = float(p.get("priceUsd") or 0)
            market_cap = float(p.get("marketCap") or p.get("fdv") or 0)
            if not address or price <= 0 or market_cap < 10000: continue
            score, setup, ratio = score_candidate(p)
            if score >= MIN_SCORE:
                p["_strategy_score"] = round(score, 1)
                p["_setup"] = setup
                p["_buy_sell_ratio"] = round(ratio, 2)
                eligible.append(p)
        except (TypeError, ValueError):
            continue
    eligible.sort(key=lambda p: float(p.get("_strategy_score") or 0), reverse=True)
    buy_total = sum(int(((p.get("txns") or {}).get("m5") or {}).get("buys") or 0) for p in eligible)
    sell_total = sum(int(((p.get("txns") or {}).get("m5") or {}).get("sells") or 0) for p in eligible)
    pressure = buy_total / max(sell_total, 1)
    update_agent("taju","active","Hindab ostu- ja müügisurvet",f"{len(eligible)} kandidaati; {buy_total} ostu / {sell_total} müüki 5 minutiga; suhe {pressure:.2f}",checks=len(pairs))
    best = eligible[0] if eligible else None
    if best:
        best_symbol = (best.get("baseToken") or {}).get("symbol","?")
        best_change = float((best.get("priceChange") or {}).get("m5") or 0)
        best_volume = float((best.get("volume") or {}).get("m5") or 0)
        tech_result = f"{best_symbol}: 5m muutus {best_change:+.1f}%; 5m maht USD {best_volume:,.0f}"
    else:
        best_symbol, tech_result = None, "Ükski token ei ületanud praeguseid tehnilisi filtreid"
    update_agent("teras","active","Analüüsib hinnamomenti ja volüümi",tech_result,symbol=best_symbol,checks=len(pairs))
    # Match headlines conservatively to candidate token names/symbols.
    with news_lock:
        available_news = list(news_cache.get("items") or [])
    matched_news_total = 0
    for candidate in eligible[:20]:
        token = candidate.get("baseToken") or {}
        symbol = str(token.get("symbol") or "").strip()
        name = str(token.get("name") or "").strip()
        terms = []
        if len(name) >= 4:
            terms.append(name)
        if len(symbol) >= 5:
            terms.append(symbol)
        matches = []
        for article in available_news:
            haystack = (str(article.get("title") or "") + " " + str(article.get("description") or "")).lower()
            if any(__import__("re").search(r"(?<![a-z0-9])" + __import__("re").escape(term.lower()) + r"(?![a-z0-9])", haystack) for term in terms):
                matches.append({"title": article.get("title",""), "url": article.get("url",""),
                                "source": article.get("source",""), "published": article.get("published","")})
        candidate["_news_matches"] = matches[:3]
        matched_news_total += len(matches)
    linked = 0
    x_links = 0
    tg_links = 0
    reddit_links = 0
    for candidate in eligible:
        info = candidate.get("info") or {}
        websites = info.get("websites") or []
        socials = info.get("socials") or []
        urls = [str(x.get("url") or "") for x in websites + socials if isinstance(x, dict)]
        if urls:
            linked += 1
        if any("x.com/" in u.lower() or "twitter.com/" in u.lower() for u in urls):
            x_links += 1
        if any("t.me/" in u.lower() or "telegram.me/" in u.lower() for u in urls):
            tg_links += 1
        if any("reddit.com/" in u.lower() for u in urls):
            reddit_links += 1
    matched_tokens = sum(1 for p in eligible[:20] if p.get("_news_matches"))
    teataja_status = "active" if linked or available_news else "limited"
    teataja_result = (f"{linked}/{len(eligible)} kandidaadil projektilingid; "
        f"X-profiile {x_links}, Telegrami linke {tg_links}, Redditi linke {reddit_links}; "
        f"uudisvoost {len(available_news)} pealkirja, tokeniga vasteid {matched_tokens}. "
        "Vaste on nime/tickeri tekstiline kokkulangevus, mitte kinnitatud seos.")
    update_agent("teataja",teataja_status,"Seob avalikud uudispealkirjad tokenikandidaatidega",teataja_result,checks=len(pairs))
    rug_eligible = []
    rug_blocked = 0
    rug_checked = 0
    for candidate in eligible[:20]:
        rug_checked += 1
        token_address = (candidate.get("baseToken") or {}).get("address")
        passed, report = rugcheck_token(token_address)
        candidate["_rugcheck"] = report
        if passed:
            rug_eligible.append(candidate)
        else:
            rug_blocked += 1
            add_event(f"rugcheck blokeeris {candidate.get('baseToken', {}).get('symbol', '?')}: {report.get('status')} | {report.get('reason', 'risk')}")
    eligible = rug_eligible
    risk_status = "active" if rug_checked else "waiting"
    update_agent("valvur",risk_status,"Kontrollib tokenite riskiraporteid",f"Kontrollitud {rug_checked}; läbis {len(eligible)}; blokeeritud {rug_blocked}; puuduv raport = blokeering",checks=rug_checked)
    pending_buys = [p for p in eligible if (p.get("baseToken") or {}).get("address") not in state.get("positions", {})]
    if paused:
        jano_status, verdict, decision_text = "waiting", "WAIT", "Ostud on kasutaja pausile pannud"
    elif not pending_buys:
        jano_status, verdict, decision_text = "waiting", "WAIT", "Kõik filtrid läbinud uut ostukandidaati praegu pole"
    elif len(state.get("positions", {})) >= MAX_POSITIONS or state.get("cash", 0) < MIN_TRADE_SIZE or state.get("daily_count", 0) >= MAX_TRADES_PER_DAY:
        jano_status, verdict, decision_text = "blocked", "SKIP", "Portfelli või päevase kauplemise piirang ei luba uut positsiooni"
    else:
        news_hits = sum(1 for p in pending_buys if p.get("_news_matches"))
        jano_status, verdict, decision_text = "active", "BUY", f"{len(pending_buys)} kandidaati läbis tehnilised ja RugCheck filtrid; uudisevaste {news_hits}; demotehingu lubamine"
    decision_symbol = (pending_buys[0].get("baseToken") or {}).get("symbol") if pending_buys else None
    update_agent("jano",jano_status,"Koondab agentide tulemused",f"{verdict}: {decision_text}; uudisevaste on lisakontekst, mitte ostufilter",symbol=decision_symbol,checks=len(pairs),decision=verdict)
    rugcheck_stats["status"] = "connected" if rugcheck_stats["checked"] else ("unavailable" if rugcheck_stats["errors"] else "checking")
    with state_lock:
        if not persistence_ok:
            raise RuntimeError(f"paper trading paused: persistent storage unavailable ({persistence_error})")
        state_before_scan = copy.deepcopy(state)
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
            pos["peak_price"] = max(float(pos.get("peak_price") or pos["entry_price"]), price)
            peak_gain = pos["peak_price"] / pos["entry_price"] - 1
            trailing_exit = peak_gain >= TRAIL_ACTIVATE and price / pos["peak_price"] - 1 <= -TRAIL_GIVEBACK
            if pnl_pct >= TAKE_PROFIT or pnl_pct <= STOP_LOSS or trailing_exit:
                proceeds = pos["quantity"] * price
                pnl = proceeds - pos["cost"]
                state["cash"] += proceeds
                state["trades"].append({"symbol":pos["symbol"],"address":address,"entry":pos["entry_price"],
                    "exit":price,"pnl":pnl,"pnl_pct":pnl_pct,"closed_at":datetime.now(timezone.utc).isoformat()})
                reason = "trailing stop" if trailing_exit else ("take profit" if pnl_pct >= TAKE_PROFIT else "stop loss")
                add_event(f"müük {pos['symbol']} | {reason} | {pnl_pct*100:+.1f}% | {pnl:+.2f} €")
                del state["positions"][address]
        today = datetime.now(timezone.utc).date().isoformat()
        if state.get("day") != today:
            state["day"], state["daily_count"] = today, 0
        if not paused and state["daily_count"] < MAX_TRADES_PER_DAY:
            for p in eligible:
                address = p["baseToken"]["address"]
                if address in state["positions"] or len(state["positions"]) >= MAX_POSITIONS: continue
                size = dynamic_trade_size(float(p.get("_strategy_score") or 0), float(state["cash"]))
                if size < MIN_TRADE_SIZE: continue
                price = float(p["priceUsd"])
                symbol = p["baseToken"].get("symbol","?")
                state["cash"] -= size
                state["positions"][address] = {"symbol":symbol,"entry_price":price,"last_price":price,
                    "peak_price":price,"quantity":size/price,"cost":size,"opened_at":datetime.now(timezone.utc).isoformat(),
                    "strategy":p.get("_setup","momentum"),"score":p.get("_strategy_score",0),
                    "liquidity_usd":float((p.get("liquidity") or {}).get("usd") or 0), "market_cap":float(p.get("marketCap") or p.get("fdv") or 0)}
                state["daily_count"] += 1
                add_event(f"ost {symbol} | €{size:.2f} | {p.get('_setup')} | skoor {p.get('_strategy_score')}")
                if len(state["positions"]) >= MAX_POSITIONS or state["daily_count"] >= MAX_TRADES_PER_DAY: break
        try:
            save_state()
        except Exception:
            state.clear()
            state.update(state_before_scan)
            raise
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
            "rugcheck": p.get("_rugcheck", {"status":"not_checked"}),
            "strategy_score": p.get("_strategy_score", 0),
            "setup": p.get("_setup", ""),
            "buy_sell_ratio": p.get("_buy_sell_ratio", 0),
            "sources": p.get("_discovery_sources", []),
            "news": p.get("_news_matches", [])
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
        await send_text(update, f"janobot online. Telegram ID: {update.effective_user.id}.")
    elif authorized(update):
        await send_text(update, "🕷️ janobot aktiivne! Paper-konto €1000. Kuus analüüsiagenti töötavad turuandmetega.\nKäsud: /status /portfolio /trades /scan /pause /resume")
    else: await reply_denied(update)

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update): return await reply_denied(update)
    with state_lock:
        value = sum(x["quantity"]*x.get("last_price",x["entry_price"]) for x in state["positions"].values())
        msg = (f"🕷️ janobot status\ntelegram: connected\nmootor: {'pausil' if paused else 'töötab'}\n"
        f"režiim: paper demo\nvaba raha: €{state['cash']:.2f}\npositsioonide väärtus: €{value:.2f}\n"
        f"avatud positsioonid: {len(state['positions'])}\nstop loss: {STOP_LOSS*100:.0f}% | kasumisiht: +{TAKE_PROFIT*100:.0f}% | skann: {SCAN_SECONDS}s | min turuväärtus: $10k\npaarid: {last_pair_count} | signaalid: {last_candidate_count}\n"
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


# Free public RSS news feed; no paid API keys or scraping.
NEWS_FEEDS = [
    ("CoinDesk", "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("Cointelegraph", "https://cointelegraph.com/rss"),
]
news_lock = threading.RLock()
news_cache = {"items": [], "updated": None, "status": "waiting", "error": ""}

def refresh_news():
    items = []
    errors = []
    for source, url in NEWS_FEEDS:
        try:
            response = requests.get(url, timeout=12, headers={"User-Agent": "janobot-news/1.0"})
            response.raise_for_status()
            root = ET.fromstring(response.content)
            for item in root.iter():
                tag = item.tag.rsplit("}", 1)[-1].lower()
                if tag != "item":
                    continue
                fields = {}
                for child in list(item):
                    key = child.tag.rsplit("}", 1)[-1].lower()
                    if key in ("title", "link", "pubdate", "published", "description", "summary"):
                        fields[key] = "".join(child.itertext()).strip()
                title = fields.get("title", "").strip()
                link = fields.get("link", "").strip()
                if title and link.startswith("https://"):
                    items.append({"title": title[:220], "description": fields.get("description") or fields.get("summary") or "",
                                  "url": link, "source": source,
                                  "published": fields.get("pubdate") or fields.get("published") or ""})
        except Exception as exc:
            errors.append(f"{source}: {str(exc)[:100]}")
    # Deduplicate by canonical URL and keep newest feed entries in source order.
    seen = set()
    unique = []
    for item in items:
        if item["url"] not in seen:
            seen.add(item["url"])
            unique.append(item)
    with news_lock:
        news_cache["items"] = unique[:30]
        news_cache["updated"] = datetime.now(timezone.utc).isoformat()
        news_cache["status"] = "connected" if unique else "unavailable"
        news_cache["error"] = "; ".join(errors)
    if unique:
        log.info("news feed refreshed: %s headlines", len(unique))
    else:
        log.warning("news feed unavailable: %s", news_cache["error"] or "no RSS items")

def news_loop():
    while True:
        refresh_news()
        time.sleep(300)

web = Flask(__name__)
def check_auth(auth):
    return auth and auth.username == "spider" and DASHBOARD_PASSWORD and auth.password == DASHBOARD_PASSWORD
def auth_required():
    return Response("Autentimine vajalik", 401, {"WWW-Authenticate": 'Basic realm="janobot"'})
@web.get("/health")
def health():
    return jsonify({"ok": True, "service": "spiderbot"})
@web.get("/")
def dashboard():
    if not check_auth(request.authorization): return auth_required()
    return Response(Path("dashboard.html").read_text(), mimetype="text/html")
@web.get("/api/news")
def news_data():
    if not check_auth(request.authorization): return auth_required()
    with news_lock:
        return jsonify(dict(news_cache))

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
            "trades":state["trades"][-10:],"candidates":last_candidates,"events":events[-20:],"agents":[dict(a) for a in agents.values()],"agent_history":agent_history[-24:],
            "last_scan":last_scan,"last_error":last_error,"pair_count":last_pair_count,
            "signal_count":last_candidate_count,"discovery":dict(discovery_stats),"rugcheck":dict(rugcheck_stats),"paused":paused,"mode":"PAPER DEMO","persistence":{"ok":persistence_ok,"file":str(STATE_FILE),"error":persistence_error},"settings":{"min_market_cap":10000,"max_market_cap":None,"min_liquidity":MIN_LIQUIDITY_USD,"min_m5_volume":MIN_M5_VOLUME_USD,"min_score":MIN_SCORE,"stop_loss_pct":STOP_LOSS*100,"take_profit_pct":TAKE_PROFIT*100,"trail_activate_pct":TRAIL_ACTIVATE*100,"trail_giveback_pct":TRAIL_GIVEBACK*100,"scan_seconds":SCAN_SECONDS,"max_trade_size":MAX_TRADE_SIZE,"min_trade_size":MIN_TRADE_SIZE}}
    return jsonify(payload)

def run_web():
    web.run(host="0.0.0.0", port=int(os.environ.get("PORT","10000")), use_reloader=False)

def main():
    if not TOKEN: raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")
    load_state()
    threading.Thread(target=news_loop, daemon=True).start()
    threading.Thread(target=run_web, daemon=True).start()
    app = Application.builder().token(TOKEN).post_init(post_init).build()
    for cmd, fn in [("start",start),("status",status),("portfolio",portfolio),("trades",trades),("scan",scan),("pause",pause_cmd),("resume",resume_cmd)]:
        app.add_handler(CommandHandler(cmd,fn))
    add_event("janobot käivitus; paper demo")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
