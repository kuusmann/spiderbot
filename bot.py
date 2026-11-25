import os, json, time, logging, asyncio, threading, copy, random, re, csv, io, xml.etree.ElementTree as ET
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
POSITION_RISK_PCT = 0.06
MAX_POSITIONS = 20
MAX_PORTFOLIO_EXPOSURE_PCT = 0.50
MAX_SMALL_CAP_MCAP = 5000000.0
LARGE_CAP_MIN_MCAP = 5000000.0
LARGE_CAP_MIN_LIQUIDITY = 250000.0
LARGE_CAP_MIN_VOLUME24 = 1000000.0
LARGE_CAP_MIN_BUY_RATIO = 1.35
MAX_BUYS_PER_SCAN = 2
MAX_TRADES_PER_DAY = 100
SCALP_TAKE_PROFIT = 0.25
SCALP_STOP_LOSS = -0.12
TIME_STOP_SECONDS = 3600
FLAT_BAND_PCT = 0.03
RUNNER_EVERY_NTH_ENTRY = 2
RUNNER_HALF_TP = 0.40
RUNNER_TRAIL_GIVEBACK = 0.20
DAILY_STOP_LOSS_EUR = -40.0
REDUCE_SIZE_AT_PROFIT_EUR = 500.0
DAILY_TARGET_EUR = 2000.0
SIM_SLIPPAGE_PCT = 0.035
SIM_LATENCY_MS = 300
SIM_FAILED_TX_RATE = 0.12
SIM_TX_FEE_EUR = 0.10
SIM_FAILED_TX_FEE_EUR = 0.05
MIN_LIQUIDITY_USD = 8000.0
MIN_MARKET_CAP_USD = 50000.0
MAX_MARKET_CAP_USD = 2000000000.0
MIN_POOL_AGE_MINUTES = 2.0
MAX_POOL_AGE_MINUTES = 1440.0
MIN_VOLUME_SPIKE_MULTIPLE = 1.0
MIN_BUYER_SELLER_RATIO = 1.0
MIN_VOLUME_24H_USD = 25000.0
MIN_VOLUME_5M_USD = 250.0
MIN_BUYS_5M = 1
MIN_DIP_PCT = 1.0
MAX_DIP_PCT = 8.0
MIN_BOUNCE_PCT = 0.35
MAX_TOP10_HOLDERS_PCT = 30.0
MAX_DEV_HOLDER_PCT = 5.0
MIN_LP_LOCKED_PCT = 80.0
MAX_BUNDLE_CLUSTER_PCT = 25.0
MAX_SINGLE_WALLET_VOLUME_PCT = 25.0
MIN_HOLDER_GROWTH_PCT = 3.0
MAX_10M_PRICE_GAIN_PCT = 100.0
MAX_SELL_PRICE_IMPACT_PCT = 2.0
SCAN_SECONDS = 10
JUPITER_API_KEY = os.environ.get("JUPITER_API_KEY", "").strip()
BIRDEYE_API_KEY = os.environ.get("BIRDEYE_API_KEY", "").strip()
HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", "").strip()
WALLET_DISCOVERY_SECONDS = 900
WALLET_POLL_SECONDS = 60
MAX_TRACKED_WALLETS = 25
TOKEN_COOLDOWN_HOURS = 12
TOKEN_COOLDOWN_SECONDS = TOKEN_COOLDOWN_HOURS * 3600
BLACKLISTED_TOKEN_ADDRESSES = {x.strip() for x in os.environ.get('TOKEN_BLACKLIST', '').split(',') if x.strip()}
TELEGRAM_ALERTS_ENABLED = os.environ.get('TELEGRAM_ALERTS_ENABLED', 'true').strip().lower() in ('1', 'true', 'yes', 'on')
ALERT_EVENT_PREFIXES = ('paper buy ', 'PAPER COPY BUY ', 'PAPER COPY SELL ', 'sell ', 'runner partial sell ', 'session exit ', 'SESSION HALTED:', 'skanni viga:', 'paper sell failed:')
pending_alerts = []
alert_lock = threading.RLock()
alert_recent = {}
wallet_tracker = {"status":"waiting","updated":None,"wallets":[],"signals":[],"errors":[],"discovery_errors":[],"poll_errors":[],"last_discovery":None}
wallet_tracker_lock = threading.RLock()
wallet_seen_signatures = {}; wallet_poll_cursor = 0

JUPITER_BASE = "https://api.jup.ag/swap/v2"
SOL_MINT = "So11111111111111111111111111111111111111112"
RUGCHECK_BASE = "https://api.rugcheck.xyz"
RUGCHECK_CACHE_TTL = 900
RUGCHECK_FAIL_TTL = 60
RUGCHECK_MAX_SCORE = 40
MAX_ENTRY_RISK_SCORE = 60.0
rugcheck_cache = {}
rugcheck_last_request = 0.0
rugcheck_stats = {"checked": 0, "passed": 0, "blocked": 0, "errors": 0, "status": "starting"}
chart_cache = {}
chart_cache_lock = threading.RLock()
CHART_CACHE_SECONDS = 180
chart_request_lock = threading.RLock()
chart_last_request = 0.0
chart_rate_limit_until = 0.0
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
state = {"cash": STARTING_BALANCE, "starting_balance": STARTING_BALANCE, "positions": {}, "trades": [], "day": "", "daily_count": 0, "price_history": {}, "volume_history": {}, "holder_history": {}, "stopped_tokens": {}, "entry_count": 0, "day_start_equity": STARTING_BALANCE, "session_halted": False, "session_halt_reason": "", "signal_log": [], "wallet_copy_consumed": []}
persistence_ok = False
persistence_error = "state not loaded"

def add_event(message):
    stamp = datetime.now(timezone.utc).isoformat()
    events.append({"time": stamp, "message": message})
    del events[:-40]
    text_message = str(message)
    if TELEGRAM_ALERTS_ENABLED and ALLOWED_USER_ID and text_message.startswith(ALERT_EVENT_PREFIXES):
        now = time.time()
        with alert_lock:
            if now - alert_recent.get(text_message, 0) >= 300:
                pending_alerts.append({"time": stamp, "message": text_message[:3500]})
                del pending_alerts[:-30]
                alert_recent[text_message] = now
            for key, last in list(alert_recent.items()):
                if now - last > 3600:
                    alert_recent.pop(key, None)
    log.info(message)

def _portfolio_equity_unlocked():
    open_value = sum(
        float(p.get("quantity", 0)) * float(p.get("last_price", p.get("entry_price", 0))) * (1 - SIM_SLIPPAGE_PCT)
        for p in state.get("positions", {}).values()
    )
    return float(state.get("cash", 0)) + open_value - SIM_TX_FEE_EUR * len(state.get("positions", {}))

def _tallinn_day():
    return datetime.now(ZoneInfo("Europe/Tallinn")).date().isoformat()

def load_state():
    global state, persistence_ok, persistence_error
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        existed = STATE_FILE.exists()
        has_daily_baseline = False
        if existed:
            data = json.loads(STATE_FILE.read_text())
            if not isinstance(data, dict) or "cash" not in data or "positions" not in data or "trades" not in data:
                raise ValueError("state file is incomplete; refusing to reset portfolio")
            has_daily_baseline = "day_start_equity" in data
            state.update(data)
            previous_start = float(state.get("starting_balance", 50.0))
            if previous_start < STARTING_BALANCE:
                state["cash"] = float(state.get("cash", previous_start)) + (STARTING_BALANCE - previous_start)
                state["starting_balance"] = STARTING_BALANCE
                add_event("virtuaalne algkapital korrigeeritud €1000-ni")
        else:
            state.update({"cash": STARTING_BALANCE, "starting_balance": STARTING_BALANCE, "positions": {}, "trades": []})

        for key, default in {
            "price_history": {}, "volume_history": {}, "holder_history": {},
            "stopped_tokens": {}, "entry_count": 0, "daily_count": 0,
            "day": "", "day_start_equity": STARTING_BALANCE,
            "session_halted": False, "session_halt_reason": "", "signal_log": []
        }.items():
            state.setdefault(key, default)

        today = _tallinn_day()
        if state.get("day") != today or not has_daily_baseline:
            state["day"] = today
            state["daily_count"] = 0
            # Keep stop-loss cooldowns until their own expiry, across day rollover.
            state["session_halted"] = False
            state["session_halt_reason"] = ""
            state["day_start_equity"] = _portfolio_equity_unlocked()
            add_event("uus Tallinna kauplemissessioon: päevased piirid lähtestatud")
        save_state()
        if existed:
            add_event(f"portfell taastatud püsikettalt: {len(state['positions'])} avatud positsiooni")
        else:
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

def _number_from_sources(sources, names):
    for source in sources:
        if isinstance(source, dict):
            for name in names:
                value = source.get(name)
                if value is not None:
                    try:
                        return float(value)
                    except (TypeError, ValueError):
                        pass
    return None

def daily_pnl_unlocked():
    open_value = sum(float(p.get("quantity", 0)) * float(p.get("last_price", p.get("entry_price", 0))) * (1 - SIM_SLIPPAGE_PCT) for p in state.get("positions", {}).values())
    equity = float(state.get("cash", 0)) + open_value - SIM_TX_FEE_EUR * len(state.get("positions", {}))
    return equity - float(state.get("day_start_equity", STARTING_BALANCE))

def update_market_snapshots(pairs):
    now = time.time()
    features = {}
    with state_lock:
        prices = state.setdefault("price_history", {})
        volumes = state.setdefault("volume_history", {})
        for pair in pairs:
            address = (pair.get("baseToken") or {}).get("address")
            try:
                price = float(pair.get("priceUsd") or 0)
                volume5 = float((pair.get("volume") or {}).get("m5") or 0)
            except (TypeError, ValueError):
                continue
            if not address or price <= 0:
                continue
            ph = prices.setdefault(address, [])
            prior = [x for x in ph if now - float(x.get("t", 0)) <= 3600]
            old_5m = [float(x["p"]) for x in prior if now - float(x["t"]) <= 300]
            old_2m = [float(x["p"]) for x in prior if now - float(x["t"]) <= 120]
            old_10m = [x for x in prior if 570 <= now - float(x["t"]) <= 720]
            old_prices = [float(x["p"]) for x in prior]
            observed_high = max(old_prices + [price])
            high_5m = max(old_5m + [price])
            low_2m = min(old_2m + [price])
            drawdown_5m = (price / high_5m - 1) * 100 if high_5m > 0 else 0.0
            bounce_2m = (price / low_2m - 1) * 100 if low_2m > 0 else 0.0
            gain_10m = None
            if old_10m:
                oldest = min(old_10m, key=lambda x: float(x["t"]))
                if float(oldest.get("p") or 0) > 0:
                    gain_10m = (price / float(oldest["p"]) - 1) * 100
            vh = volumes.setdefault(address, [])
            baseline = [float(x["v"]) for x in vh if 300 <= now - float(x.get("t", 0)) <= 1200]
            oldest_age = max([now - float(x.get("t", 0)) for x in vh] or [0])
            multiple = volume5 / (sum(baseline) / len(baseline)) if len(baseline) >= 20 and oldest_age >= 1140 and sum(baseline) > 0 else None
            features[address] = {
                "breakout": len(old_5m) >= 3 and price > max(old_5m),
                "observed_high": observed_high,
                "above_60pct_high": price >= observed_high * 0.60,
                "gain_10m_pct": gain_10m,
                "drawdown_5m_pct": drawdown_5m,
                "bounce_2m_pct": bounce_2m,
                "volume_multiple": multiple
            }
            ph.append({"t": now, "p": price})
            vh.append({"t": now, "v": volume5})
            del ph[:-420]
            del vh[:-140]
    return features

def risk_assessment(pair, report):
    """Estimate risk from known report fields; never overrides hard RugCheck gates."""
    risk = min(100.0, max(0.0, float(report.get("score") or 0)))
    factors = []
    lp_locked = _number_from_sources([report], ["lpLockedPct", "lp_locked_pct"])
    if lp_locked is not None and lp_locked < 90:
        risk += min(18.0, (90.0 - lp_locked) * 0.6)
        factors.append(f"LP lock {lp_locked:.1f}%")
    holders = report.get("topHolders")
    if isinstance(holders, list) and holders:
        shares = [_number_from_sources([h], ["pct", "percentage"]) for h in holders
                  if isinstance(h, dict) and not h.get("isPool") and not h.get("isBondingCurve")]
        shares = [x for x in shares if x is not None]
        if len(shares) >= 5:
            top_share = sum(sorted(shares, reverse=True)[:10])
            if top_share > 15:
                risk += min(22.0, (top_share - 15.0) * 0.8)
                factors.append(f"top holder concentration {top_share:.1f}%")
    creator_pct = _number_from_sources([report], ["creatorPct", "creator_pct"])
    if creator_pct is not None and creator_pct > 1:
        risk += min(18.0, creator_pct * 2.0)
        factors.append(f"creator holds {creator_pct:.1f}%")
    networks = report.get("insiderNetworks")
    clusters = networks.get("clusters", networks.get("networks")) if isinstance(networks, dict) else networks
    if isinstance(clusters, list):
        shares = [_number_from_sources([x], ["pct", "percentage", "combinedPct"])
                  for x in clusters if isinstance(x, dict)]
        shares = [x for x in shares if x is not None]
        if shares and max(shares) > 10:
            risk += min(20.0, (max(shares) - 10.0) * 0.8)
            factors.append(f"insider cluster {max(shares):.1f}%")
    liquidity = float((pair.get("liquidity") or {}).get("usd") or 0)
    if 0 < liquidity < 20000:
        risk += 8.0
        factors.append("low absolute liquidity")
    risk = round(min(100.0, risk), 1)
    return {"score": risk, "level": "HIGH" if risk >= 60 else ("MEDIUM" if risk >= 30 else "LOW"),
            "factors": factors}

def risk_entry_allowed(risk):
    """Fail closed if supplementary risk data is missing, malformed, or outside 0–100."""
    if not isinstance(risk, dict) or risk.get("score") is None:
        return False
    try:
        score = float(risk["score"])
    except (TypeError, ValueError, OverflowError):
        return False
    if not 0 <= score <= 100:
        return False
    return score < MAX_ENTRY_RISK_SCORE

def strict_safety_check(pair, report):
    """Paper-entry safety: reject confirmed hazards, but do not require unavailable analytics."""
    reasons = []
    token_info = report.get("token") if isinstance(report.get("token"), dict) else {}
    for field, label in (("mintAuthority", "mint authority"), ("freezeAuthority", "freeze authority")):
        if field not in token_info or token_info.get(field) is not None:
            reasons.append(f"{label} not confirmed revoked")

    lp_locked = _number_from_sources([report], ["lpLockedPct", "lp_locked_pct"])
    if lp_locked is not None and lp_locked < MIN_LP_LOCKED_PCT:
        reasons.append(f"LP lock below {MIN_LP_LOCKED_PCT:.0f}%")

    holders = report.get("topHolders")
    if isinstance(holders, list) and holders:
        percentages = [_number_from_sources([h], ["pct", "percentage"]) for h in holders if isinstance(h, dict) and not h.get("isPool") and not h.get("isBondingCurve")]
        percentages = [x for x in percentages if x is not None]
        if len(percentages) >= 10 and sum(sorted(percentages, reverse=True)[:10]) >= MAX_TOP10_HOLDERS_PCT:
            reasons.append("top-10 holder concentration too high")

    creator_pct = _number_from_sources([report], ["creatorPct", "creator_pct"])
    if creator_pct is not None and creator_pct >= MAX_DEV_HOLDER_PCT:
        reasons.append("developer wallet concentration too high")

    networks = report.get("insiderNetworks")
    if networks is not None:
        clusters = networks.get("clusters", networks.get("networks")) if isinstance(networks, dict) else networks
        if isinstance(clusters, list):
            for cluster in clusters:
                share = _number_from_sources([cluster], ["pct", "percentage", "combinedPct"])
                if share is not None and share > MAX_BUNDLE_CLUSTER_PCT:
                    reasons.append("insider wallet cluster too large")
                    break

    activity = report.get("creatorActivity") if isinstance(report.get("creatorActivity"), dict) else {}
    dev_sells = _number_from_sources([report, activity], ["creatorSells15m", "creator_sells_15m", "devSells15m", "sells15m"])
    sold_flag = report.get("creatorSold15m", report.get("creator_sold_15m", activity.get("sold15m")))
    if (dev_sells is not None and dev_sells > 0) or sold_flag is True:
        reasons.append("developer sold recently")
    return not reasons, reasons

def _holder_growth_ok(address, report, observed_at):
    count = _number_from_sources([report, report.get("token") or {}], ["totalHolders", "holderCount", "holdersCount", "total_holders"])
    if count is None or count <= 0:
        return False, "holder count missing"
    with state_lock:
        history = state.setdefault("holder_history", {}).setdefault(address, [])
        if not any(abs(float(x.get("t", 0)) - observed_at) < 1 for x in history):
            history.append({"t": observed_at, "count": count})
            del history[:-12]
        prior = [x for x in history if 840 <= observed_at - float(x.get("t", 0)) <= 1800]
    if not prior:
        return False, "15m holder baseline not collected yet"
    base = min(prior, key=lambda x: abs((observed_at - float(x["t"])) - 900))
    growth = (count / max(float(base["count"]), 1) - 1) * 100
    return growth >= MIN_HOLDER_GROWTH_PCT, f"holder growth {growth:+.1f}%"

def jupiter_sell_simulation(token_address, price_usd, notional_eur, report):
    if not JUPITER_API_KEY:
        return False, "JUPITER_API_KEY missing"
    token_info = report.get("token") if isinstance(report.get("token"), dict) else {}
    try:
        decimals = int(token_info["decimals"])
        amount = int((float(notional_eur) / max(float(price_usd), 1e-18)) * (10 ** decimals))
    except (KeyError, TypeError, ValueError, OverflowError):
        return False, "token decimals/amount unavailable"
    try:
        response = requests.get(
            f"{JUPITER_BASE}/order",
            params={"inputMint": token_address, "outputMint": SOL_MINT, "amount": str(amount)},
            headers={"x-api-key": JUPITER_API_KEY, "Accept": "application/json"},
            timeout=10
        )
        response.raise_for_status()
        quote = response.json()
        out_amount = int(quote.get("outAmount") or 0)
        route = quote.get("router") or quote.get("routePlan")
        impact_ratio = quote.get("priceImpactPct")
        impact_points = quote.get("priceImpact")
        if out_amount <= 0 or not route or (impact_ratio is None and impact_points is None):
            return False, "sell quote incomplete (route/output/impact missing)"
        # V2 priceImpactPct is a ratio; priceImpact is already percentage points.
        impact_pct = abs(float(impact_ratio)) * 100 if impact_ratio is not None else abs(float(impact_points))
        if impact_pct > MAX_SELL_PRICE_IMPACT_PCT:
            return False, f"sell quote impact {impact_pct:.2f}%"
        return True, f"Jupiter sell route ok; impact {impact_pct:.2f}%"
    except Exception as exc:
        return False, f"sell quote unavailable: {str(exc)[:120]}"


def discover_smart_wallets():
    if not BIRDEYE_API_KEY:
        with wallet_tracker_lock:
            wallet_tracker.update({"status":"needs BIRDEYE_API_KEY","updated":datetime.now(timezone.utc).isoformat()})
        return
    found, errors = {}, []
    for sort_by in ("realized_pnl","volume_usd"):
        try:
            params={"sort_by":sort_by,"interval":"24h","limit":20}
            response=requests.get("https://public-api.birdeye.so/wallet/v2/leaderboard",
                params=params,
                headers={"X-API-KEY":BIRDEYE_API_KEY,"x-chain":"solana"},timeout=15)
            if response.status_code == 429:
                retry_after=response.headers.get("Retry-After","1")
                try: wait_seconds=max(1,min(10,float(retry_after)))
                except (TypeError,ValueError): wait_seconds=1
                time.sleep(wait_seconds)
                response=requests.get("https://public-api.birdeye.so/wallet/v2/leaderboard",
                    params=params,
                    headers={"X-API-KEY":BIRDEYE_API_KEY,"x-chain":"solana"},timeout=15)
            if response.status_code == 429:
                retry_after=response.headers.get("Retry-After","unknown")
                errors.append("Birdeye "+sort_by+": rate limited (429); Retry-After="+retry_after)
                continue
            if not response.ok:
                detail=(response.text or "")[:240].replace("\\n"," ")
                errors.append("Birdeye "+sort_by+": HTTP "+str(response.status_code)+" "+detail)
                continue
            payload=response.json()
            rows=(payload.get("data") or {}).get("items",[]) if isinstance(payload.get("data"),dict) else payload.get("data",[])
            for row in rows if isinstance(rows,list) else []:
                address=row.get("wallet") or row.get("address") or row.get("owner")
                if not address: continue
                item=found.setdefault(address,{"address":address,"label":row.get("label") or row.get("name") or address[:7]+"…"+address[-5:],"sources":[],"pnl":row.get("pnl"),"volume":row.get("volume"),"score":0})
                item["sources"].append("Birdeye "+sort_by); item["score"]+=1
        except Exception as exc:
            errors.append("Birdeye "+sort_by+": "+str(exc)[:180])
    with wallet_tracker_lock:
        old={w["address"]:w for w in wallet_tracker.get("wallets",[])}
        merged=[]
        for item in sorted(found.values(),key=lambda x:(x["score"],float(x.get("pnl") or 0)),reverse=True):
            prev=old.get(item["address"],{})
            item.update({"added_at":prev.get("added_at") or datetime.now(timezone.utc).isoformat(),"last_seen":prev.get("last_seen"),"last_action":prev.get("last_action"),"status":"tracking"})
            merged.append(item)
        merged += [w for a,w in old.items() if a not in found and w.get("manual")]
        wallet_tracker.update({"wallets":merged[:MAX_TRACKED_WALLETS],"last_discovery":datetime.now(timezone.utc).isoformat(),"updated":datetime.now(timezone.utc).isoformat(),"discovery_errors":errors,"errors":errors+wallet_tracker.get("poll_errors",[]),"status":"live" if merged and not errors else ("source error" if errors else ("live" if merged else "no wallets found"))})
    for error in errors: log.warning("Wallet discovery error: %s", error)

def classify_wallet_swap(wallet, tx):
    transfers=tx.get("tokenTransfers") or []
    received=[x for x in transfers if x.get("toUserAccount")==wallet and x.get("mint") and float(x.get("tokenAmount") or 0)>0]
    sent=[x for x in transfers if x.get("fromUserAccount")==wallet and x.get("mint") and float(x.get("tokenAmount") or 0)>0]
    sol_mints={SOL_MINT,"So11111111111111111111111111111111111111111"}
    buy=next((x for x in received if x["mint"] not in sol_mints),None)
    spent_sol=any(x["mint"] in sol_mints for x in sent)
    sell=next((x for x in sent if x["mint"] not in sol_mints),None)
    got_sol=any(x["mint"] in sol_mints for x in received)
    if buy and spent_sol: return "BUY",buy["mint"],float(buy.get("tokenAmount") or 0)
    if sell and got_sol: return "SELL",sell["mint"],float(sell.get("tokenAmount") or 0)
    return None

def poll_tracked_wallets():
    global wallet_poll_cursor
    if not HELIUS_API_KEY:
        with wallet_tracker_lock:
            wallet_tracker.update({"status":"needs HELIUS_API_KEY","updated":datetime.now(timezone.utc).isoformat()})
        return
    with wallet_tracker_lock:
        wallets=list(wallet_tracker.get("wallets",[]))
    if not wallets:
        return
    # Stay under Helius rate limits: poll at most five wallets per minute, rotating through the list.
    start=wallet_poll_cursor % len(wallets)
    batch=[wallets[(start+i)%len(wallets)] for i in range(min(5,len(wallets)))]
    wallet_poll_cursor=(start+len(batch)) % max(1,len(wallets))
    signals,errors=[],[]
    for wallet in batch:
        address=wallet.get("address")
        if not address: continue
        try:
            response=requests.get(f"https://api-mainnet.helius-rpc.com/v0/addresses/{address}/transactions",
                params={"api-key":HELIUS_API_KEY,"limit":10},timeout=15)
            if response.status_code==429:
                retry_after=response.headers.get("Retry-After","60")
                errors.append(address[:8]+": Helius rate limited (429), retry-after="+retry_after)
                continue
            response.raise_for_status()
            rows=response.json()
            if not isinstance(rows,list): continue
            seen=wallet_seen_signatures.get(address)
            if seen is None:
                # Establish a baseline on first contact; do not copy old historical swaps.
                wallet_seen_signatures[address]={x.get("signature") for x in rows if x.get("signature")}
                wallet["status"]="tracking"
                continue
            for tx in reversed(rows):
                sig=tx.get("signature")
                if not sig or sig in seen: continue
                seen.add(sig)
                if len(seen)>500: wallet_seen_signatures[address]=set(list(seen)[-250:])
                if tx.get("type")!="SWAP": continue
                action=classify_wallet_swap(address,tx)
                if not action: continue
                side,mint,amount=action
                signals.append({"wallet":address,"label":wallet.get("label",address[:8]),"side":side,"mint":mint,"amount":amount,"signature":sig,"time":datetime.fromtimestamp(float(tx.get("timestamp") or time.time()),timezone.utc).isoformat()})
                wallet["last_seen"]=signals[-1]["time"]; wallet["last_action"]=side+" "+mint[:8]
            wallet["status"]="tracking"
        except Exception as exc:
            errors.append(address[:8]+": "+str(exc)[:140])
        time.sleep(0.25)
    if signals:
        with wallet_tracker_lock:
            wallet_tracker["signals"]=(signals+wallet_tracker.get("signals",[]))[:100]
        for x in signals: add_event(f"WALLET COPY SIGNAL {x['side']} {x['mint'][:8]} · {x['label']}")
    with wallet_tracker_lock:
        discovery_errors=wallet_tracker.get("discovery_errors",[])
        wallet_tracker.update({"updated":datetime.now(timezone.utc).isoformat(),"poll_errors":errors,"errors":discovery_errors+errors})
        for error in errors: log.warning("Wallet activity poll error: %s", error)
        if discovery_errors: wallet_tracker["status"]="source error"
        elif errors and not wallet_tracker.get("wallets"): wallet_tracker["status"]="source error"
        elif wallet_tracker.get("wallets"): wallet_tracker["status"]="live"
        else: wallet_tracker["status"]="no wallets found"

def wallet_tracker_loop():
    last_discovery=0
    while True:
        try:
            if time.time()-last_discovery>=WALLET_DISCOVERY_SECONDS:
                discover_smart_wallets(); last_discovery=time.time()
            poll_tracked_wallets()
        except Exception as exc:
            with wallet_tracker_lock: wallet_tracker.update({"status":"error","errors":[str(exc)[:160]]})
            log.exception("Wallet tracker failed")
        time.sleep(WALLET_POLL_SECONDS)

def simulated_fill(side, market_price):
    time.sleep(SIM_LATENCY_MS / 1000.0)
    if random.random() < SIM_FAILED_TX_RATE:
        return None
    slippage = SIM_SLIPPAGE_PCT if side == "buy" else -SIM_SLIPPAGE_PCT
    return float(market_price) * (1 + slippage)

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
    """Fetch a full RugCheck report. Unknown/missing security fields fail closed later."""
    global rugcheck_last_request
    now = time.time()
    cached = rugcheck_cache.get(address)
    if cached and now - cached["time"] < cached["ttl"]:
        return cached["passed"], cached["data"]
    delay = 0.42 - (time.monotonic() - rugcheck_last_request)
    if delay > 0:
        time.sleep(delay)
    try:
        response = requests.get(
            f"{RUGCHECK_BASE}/v1/tokens/{address}/report",
            timeout=10,
            headers={"Accept": "application/json", "User-Agent": "janobot-paper/1.0"},
        )
        rugcheck_last_request = time.monotonic()
        response.raise_for_status()
        report = response.json()
        if not isinstance(report, dict) or not isinstance(report.get("token"), dict):
            raise ValueError("RugCheck full report missing token details")
        score = report.get("score_normalised", report.get("score_normalized"))
        if score is None:
            raise ValueError("RugCheck normalized score missing")
        score = float(score)
        risks = report.get("risks") or []
        danger = [x for x in risks if str(x.get("level", "")).lower() in ("danger", "critical", "high")]
        data = dict(report)
        data.update({
            "score": score,
            "risk_count": len(risks),
            "danger_count": len(danger),
            "status": "passed" if score < RUGCHECK_MAX_SCORE and not danger else "blocked",
            "reason": "risk score/danger finding" if score >= RUGCHECK_MAX_SCORE or danger else "base risk score passed",
        })
        passed = score < RUGCHECK_MAX_SCORE and not danger
        rugcheck_cache[address] = {"time": now, "ttl": RUGCHECK_CACHE_TTL, "passed": passed, "data": data}
        rugcheck_stats["checked"] += 1
        rugcheck_stats["passed" if passed else "blocked"] += 1
        return passed, data
    except Exception as exc:
        data = {"status": "unavailable", "reason": str(exc)[:160], "score": None, "risk_count": 0, "danger_count": 0}
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
        pull("top_volume_pools", "https://api.geckoterminal.com/api/v2/networks/solana/pools?sort=h24_volume_usd_desc&page=1&include=base_token", _gecko_solana_addresses)
        merged = {}
        # Put the highest-volume established Solana pools first so they survive the scan cap.
        source_order = ("top_volume_pools", "trending_pools", "new_pools", "profiles",
                        "boosts_top", "boosts_latest", "community_takeovers")
        for source in source_order:
            for address in sources.get(source, []):
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
    best_pairs = {}
    for pair in candidates:
        address = (pair.get("baseToken") or {}).get("address")
        if not address:
            continue
        def pair_quality(item):
            liquidity = float((item.get("liquidity") or {}).get("usd") or 0)
            volume = float((item.get("volume") or {}).get("h24") or 0)
            return liquidity, volume
        if address not in best_pairs or pair_quality(pair) > pair_quality(best_pairs[address]):
            best_pairs[address] = pair
    candidates = list(best_pairs.values())
    discovery_stats["dex_pairs"] = len(candidates)
    discovery_stats["unique_tokens"] = len(candidates)
    return candidates

def pair_age_hours(pair):
    created = pair.get("pairCreatedAt")
    if not created: return 999999
    return max(0, (time.time()*1000 - float(created))/3600000)


# === Enhanced paper-only analytics ===
market_regime = {"label":"UNKNOWN","score":50,"updated":None,"details":{}}
def market_regime_from_pairs(pairs):
    valid=[]
    for p in pairs:
        try:
            liq=float((p.get("liquidity") or {}).get("usd") or 0); vol=float((p.get("volume") or {}).get("h24") or 0); ch=float((p.get("priceChange") or {}).get("h1") or 0)
            if liq>=10000 and vol>=50000: valid.append(ch)
        except (TypeError,ValueError): pass
    if not valid: return {"label":"UNKNOWN","score":50,"breadth":0,"median_change":0,"samples":0}
    breadth=sum(x>0 for x in valid)/len(valid); med=sorted(valid)[len(valid)//2]
    score=max(0,min(100,round(50+35*(breadth-.5)+2*max(-10,min(10,med)))))
    return {"label":"BULL" if score>=65 else ("BEAR" if score<=35 else "NEUTRAL"),"score":score,"breadth":round(breadth,3),"median_change":round(med,2),"samples":len(valid)}
def momentum_score(pair):
    tx=(pair.get("txns") or {}).get("m5") or {}; buys=int(tx.get("buys") or 0); sells=int(tx.get("sells") or 0); ratio=buys/max(sells,1)
    vol5=float((pair.get("volume") or {}).get("m5") or 0); liq=float((pair.get("liquidity") or {}).get("usd") or 0); ch5=float((pair.get("priceChange") or {}).get("m5") or 0); ch1=float((pair.get("priceChange") or {}).get("h1") or 0)
    features=pair.get("_momentum") or {}; bounce=float(features.get("bounce_2m_pct") or 0); age=float(features.get("pool_age_minutes") or 0)
    score=min(25,max(0,(ratio-1)*12))+min(20,vol5/2500)+min(15,liq/25000)+min(15,max(0,bounce)*3)+min(10,max(0,ch5)*1.5)+(10 if 5<=age<=240 else 5 if age<=720 else 0)+(5 if ch1>=0 else 0)
    return round(min(100,max(0,score)),1)
def evaluate_signal_forward(signal, history, horizon_seconds, now=None):
    """Evaluate a signal only when an actual later market observation is available."""
    try:
        started = float(signal.get("signal_ts") or 0)
        entry = float(signal.get("price_usd") or 0)
    except (TypeError, ValueError, OverflowError):
        return None
    if started <= 0 or entry <= 0:
        return None
    now = time.time() if now is None else float(now)
    target_time = started + float(horizon_seconds)
    if now < target_time:
        return None
    future = [row for row in history if float(row.get("t", 0)) >= target_time and float(row.get("p", 0)) > 0]
    if not future:
        return None
    observation = min(future, key=lambda row: float(row.get("t", 0)))
    return round((float(observation["p"]) / entry - 1) * 100, 3)

def update_forward_signal_outcomes():
    with state_lock:
        signals = state.setdefault("signal_log", [])
        histories = state.setdefault("price_history", {})
        for signal in signals:
            history = histories.get(signal.get("address"), [])
            for horizon, field in ((900, "return_15m_pct"), (3600, "return_60m_pct")):
                if field not in signal:
                    result = evaluate_signal_forward(signal, history, horizon)
                    if result is not None:
                        signal[field] = result

def forward_signal_performance():
    with state_lock:
        signals = list(state.get("signal_log", []))
    summary = {}
    for field in ("return_15m_pct", "return_60m_pct"):
        values = [float(row[field]) for row in signals if row.get(field) is not None]
        summary[field] = {
            "samples": len(values),
            "win_rate_pct": round(100 * sum(value > 0 for value in values) / len(values), 1) if values else None,
            "average_return_pct": round(sum(values) / len(values), 3) if values else None,
        }
    summary["method"] = "forward outcomes from recorded scanner signals; not a historical backtest"
    return summary

def closed_trade_performance():
    with state_lock:
        trades = list(state.get("trades", []))
    pnls = [float(row.get("pnl") or 0) for row in trades]
    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value <= 0]
    gross_loss = abs(sum(losses))
    return {
        "sample_size": len(pnls),
        "win_rate_pct": round(100 * len(wins) / len(pnls), 1) if pnls else None,
        "net_pnl_eur": round(sum(pnls), 2),
        "profit_factor": round(sum(wins) / gross_loss, 2) if gross_loss else ("no_losses" if wins else None),
        "average_win_eur": round(sum(wins) / len(wins), 2) if wins else None,
        "average_loss_eur": round(sum(losses) / len(losses), 2) if losses else None,
    }

def token_entry_blocked(address):
    if not address:
        return True, "missing token address"
    if address in BLACKLISTED_TOKEN_ADDRESSES:
        return True, "manual blacklist"
    with state_lock:
        expiry = state.setdefault("stopped_tokens", {}).get(address)
        if expiry is None:
            return False, ""
        if isinstance(expiry, (int, float)):
            blocked = float(expiry) > time.time()
        else:
            # Legacy state stored a Tallinn date instead of a timestamp.
            blocked = str(expiry) == _tallinn_day()
        if not blocked:
            state["stopped_tokens"].pop(address, None)
        return blocked, "temporary stop-loss cooldown" if blocked else ""

def score_candidate(pair):
    tx = (pair.get("txns") or {}).get("m5") or {}
    buys = int(tx.get("buys") or 0)
    sells = int(tx.get("sells") or 0)
    return buys / max(sells, 1), "momentum", buys / max(sells, 1)

def position_exit_decision(pos, pair, price, age_seconds):
    """Deterministic exit policy; returns (reason, partial) for paper trades."""
    entry = float(pos.get("entry_price") or 0)
    if entry <= 0 or price <= 0:
        return None, False
    pnl_pct = price / entry - 1.0
    runner = bool(pos.get("runner"))
    half_sold = bool(pos.get("runner_half_sold"))
    peak = max(float(pos.get("peak_price") or entry), price)
    # Tiny epsilon avoids missing exact configured thresholds due to binary float rounding.
    epsilon = 1e-12
    if pnl_pct <= SCALP_STOP_LOSS + epsilon:
        return f"stop loss {SCALP_STOP_LOSS * 100:.0f}%", False
    if runner and not half_sold and pnl_pct + epsilon >= RUNNER_HALF_TP:
        return f"runner partial TP +{RUNNER_HALF_TP * 100:.0f}%", True
    if runner and half_sold and price <= float(pos.get("runner_peak_price") or peak) * (1 - RUNNER_TRAIL_GIVEBACK):
        return f"runner trailing giveback {RUNNER_TRAIL_GIVEBACK * 100:.0f}%", False
    if not runner and pnl_pct >= SCALP_TAKE_PROFIT:
        return f"scalp TP +{SCALP_TAKE_PROFIT * 100:.0f}%", False
    if not runner and pnl_pct >= 0.12 and price <= peak * 0.93:
        return "adaptive profit-protection trail", False
    if pnl_pct > 0.05:
        change5 = float((pair.get("priceChange") or {}).get("m5") or 0)
        current_liquidity = float((pair.get("liquidity") or {}).get("usd") or 0)
        entry_liquidity = float(pos.get("liquidity_usd") or 0)
        if change5 <= -8.0:
            return "adaptive momentum deterioration", False
        if entry_liquidity > 0 and current_liquidity > 0 and current_liquidity <= entry_liquidity * 0.45:
            return "adaptive liquidity contraction", False
    if not runner and age_seconds >= TIME_STOP_SECONDS and abs(pnl_pct) <= FLAT_BAND_PCT:
        return f"time stop {TIME_STOP_SECONDS // 60}m flat", False
    return None, False

def dynamic_trade_size(equity, daily_pnl):
    """Compound measured paper position sizes with a strict total-exposure ceiling."""
    fraction = POSITION_RISK_PCT * (0.5 if daily_pnl >= REDUCE_SIZE_AT_PROFIT_EUR else 1.0)
    fraction *= 0.75 if market_regime.get("label") == "BEAR" else 1.0
    invested = sum(float(p.get("cost", 0)) for p in state.get("positions", {}).values())
    exposure_room = max(0.0, float(equity) * MAX_PORTFOLIO_EXPOSURE_PCT - invested)
    return round(max(0.0, min(float(equity) * fraction,
                              float(state.get("cash", 0)) - SIM_TX_FEE_EUR, exposure_room)), 2)

def strategy_performance():
    """Closed paper-trade performance grouped by entry strategy."""
    with state_lock:
        trades = list(state.get("trades", []))
    groups = {}
    for trade in trades:
        key = trade.get("strategy") or "unknown"
        row = groups.setdefault(key, {"strategy":key,"trades":0,"wins":0,"pnl_eur":0.0})
        row["trades"] += 1
        row["wins"] += int(float(trade.get("pnl") or 0) > 0)
        row["pnl_eur"] += float(trade.get("pnl") or 0)
    result = []
    for row in groups.values():
        row["win_rate_pct"] = round(100*row["wins"]/row["trades"],1) if row["trades"] else 0
        row["pnl_eur"] = round(row["pnl_eur"],2)
        result.append(row)
    return sorted(result,key=lambda x:x["pnl_eur"],reverse=True)

def wallet_performance():
    """Performance from this bot's closed paper-copy trades; never treats Birdeye PnL as verified."""
    with state_lock:
        trades = [t for t in state.get("trades", []) if t.get("strategy") == "wallet-copy"]
        positions = list(state.get("positions", {}).values())
    stats = {}
    for trade in trades:
        metrics = trade.get("entry_metrics") or {}
        wallet = metrics.get("wallet")
        if not wallet: continue
        row = stats.setdefault(wallet, {"wallet":wallet,"label":metrics.get("wallet_label") or wallet[:8]+"…","trades":0,"wins":0,"pnl_eur":0.0})
        row["trades"] += 1
        row["wins"] += int(float(trade.get("pnl") or 0) > 0)
        row["pnl_eur"] += float(trade.get("pnl") or 0)
    for pos in positions:
        if pos.get("strategy") != "wallet-copy": continue
        wallet = pos.get("copy_wallet")
        if not wallet: continue
        row = stats.setdefault(wallet, {"wallet":wallet,"label":pos.get("copy_wallet_label") or wallet[:8]+"…","trades":0,"wins":0,"pnl_eur":0.0})
        row["open_positions"] = row.get("open_positions",0) + 1
    result = []
    for row in stats.values():
        row["win_rate_pct"] = round(100 * row["wins"] / row["trades"], 1) if row["trades"] else None
        row["score"] = round(max(0,min(100, 50 + row["win_rate_pct"]*0.35 + max(-20,min(20,row["pnl_eur"]))*0.75)),1) if row["trades"] else None
        row["open_positions"] = row.get("open_positions",0)
        result.append(row)
    return sorted(result,key=lambda x:(x.get("score") if x.get("score") is not None else -1,x["pnl_eur"]),reverse=True)

def apply_wallet_copy_signals(safe_candidates, pairs):
    """Paper-copy only fresh swaps from tracked wallets, gated by existing token safety checks."""
    now=time.time()
    candidates={(p.get("baseToken") or {}).get("address"):p for p in safe_candidates}
    pair_prices={(p.get("baseToken") or {}).get("address"):float(p.get("priceUsd") or 0) for p in pairs}
    with wallet_tracker_lock:
        pending=list(wallet_tracker.get("signals",[]))
    consumed=set(state.setdefault("wallet_copy_consumed",[]))
    changed=False
    for signal in reversed(pending):
        sig=signal.get("signature")
        if not sig or sig in consumed: continue
        try:
            age=now-datetime.fromisoformat(signal.get("time","").replace("Z","+00:00")).timestamp()
        except Exception:
            age=999999
        if age>180:
            consumed.add(sig); changed=True; continue
        mint=signal.get("mint")
        side=signal.get("side")
        wallet=signal.get("wallet")
        if side=="BUY":
            candidate=candidates.get(mint)
            if not candidate:
                continue
            # Require either independent wallet consensus or a proven wallet track record.
            recent = []
            for other in pending:
                if other.get("side") != "BUY" or other.get("mint") != mint: continue
                try:
                    if now-datetime.fromisoformat(other.get("time","").replace("Z","+00:00")).timestamp() <= 180:
                        recent.append(other.get("wallet"))
                except Exception:
                    pass
            consensus = len({w for w in recent if w}) >= 2
            performance = next((w for w in wallet_performance() if w.get("wallet") == wallet), None)
            proven = bool(performance and performance.get("trades",0) >= 2 and performance.get("win_rate_pct",0) >= 55 and performance.get("pnl_eur",0) > 0)
            if not (consensus or proven):
                continue
            if paused or state.get("session_halted") or mint in state["positions"] or len(state["positions"])>=MAX_POSITIONS or state["daily_count"]>=MAX_TRADES_PER_DAY:
                consumed.add(sig); changed=True; continue
            price=float(candidate.get("priceUsd") or 0)
            if price<=0: continue
            equity=float(state["cash"])+sum(float(p.get("quantity",0))*float(p.get("last_price",p.get("entry_price",0))) for p in state["positions"].values())
            size=round(dynamic_trade_size(equity, daily_pnl_unlocked()) * 0.5, 2)
            size=round(size * max(0.5, 1.0 - min(80.0, float(candidate.get("_risk_score") or 0)) / 160.0), 2)
            if size<=1: continue
            execution=simulated_fill("buy",price)
            if execution is None: continue
            symbol=(candidate.get("baseToken") or {}).get("symbol","?")
            state["cash"]-=size+SIM_TX_FEE_EUR
            state["entry_count"]=int(state.get("entry_count",0))+1
            state["daily_count"]=int(state.get("daily_count",0))+1
            state["positions"][mint]={"symbol":symbol,"entry_price":execution,"last_price":price,"peak_price":price,
                "quantity":size/execution,"cost":size+SIM_TX_FEE_EUR,"opened_at":datetime.now(timezone.utc).isoformat(),
                "opened_ts":now,"strategy":"wallet-copy","runner":False,"runner_half_sold":False,
                "runner_peak_price":price,"copy_wallet":wallet,"copy_wallet_label":signal.get("label","tracked wallet"),
                "sim_slippage_pct":SIM_SLIPPAGE_PCT*100,"liquidity_usd":float((candidate.get("liquidity") or {}).get("usd") or 0),
                "risk_score":candidate.get("_risk_score",0),"momentum_score":candidate.get("_momentum_score",0),
                "market_cap":float(candidate.get("marketCap") or candidate.get("fdv") or 0),
                "entry_metrics":{"source":"wallet-copy","wallet":wallet,"wallet_label":signal.get("label"),"wallet_signal":sig,"risk_score":candidate.get("_risk_score"),"momentum_score":candidate.get("_momentum_score")}}
            add_event(f"PAPER COPY BUY {symbol} from {signal.get('label')} | EUR {size:.2f}")
            consumed.add(sig); changed=True
        elif side=="SELL":
            pos=state["positions"].get(mint)
            if not pos or pos.get("strategy")!="wallet-copy" or pos.get("copy_wallet")!=wallet:
                consumed.add(sig); changed=True; continue
            price=pair_prices.get(mint,0)
            if price<=0:
                try:
                    extra=get_json(f"{DEX}/token-pairs/v1/solana/{mint}")
                    matches=[x for x in extra if (x.get("baseToken") or {}).get("address")==mint and float(x.get("priceUsd") or 0)>0] if isinstance(extra,list) else []
                    if matches: price=float(max(matches,key=lambda x:float((x.get("liquidity") or {}).get("usd") or 0))["priceUsd"])
                except Exception: price=0
            if price<=0: continue
            execution=simulated_fill("sell",price)
            if execution is None: continue
            proceeds=float(pos["quantity"])*execution
            realized=proceeds-float(pos["cost"])-SIM_TX_FEE_EUR
            state["cash"]+=proceeds-SIM_TX_FEE_EUR
            state["trades"].append({"symbol":pos["symbol"],"address":mint,"entry":pos["entry_price"],"exit":execution,
                "pnl":realized,"pnl_pct":price/float(pos["entry_price"])-1,"closed_at":datetime.now(timezone.utc).isoformat(),
                "reason":"tracked wallet sell","partial":False,"strategy":"wallet-copy","runner":False,
                "entry_metrics":pos.get("entry_metrics",{}),"risk_score":pos.get("risk_score"),
                "momentum_score":pos.get("momentum_score"),"simulated_slippage_pct":pos.get("sim_slippage_pct")})
            del state["positions"][mint]
            add_event(f"PAPER COPY SELL {pos['symbol']} on {signal.get('label')} exit | {realized:+.2f} EUR")
            consumed.add(sig); changed=True
    state["wallet_copy_consumed"]=list(consumed)[-1000:]
    if changed: save_state()

def scan_once():
    global last_scan, last_error, last_pair_count, last_candidate_count, last_candidates
    update_agent("uljas","working","Otsib Solana migrated/trending pools","Laeb uusi ja trending basseine")
    pairs = discover_candidates()
    now = time.time()
    market_features = update_market_snapshots(pairs)
    market_regime.update(market_regime_from_pairs(pairs))
    market_regime["updated"] = datetime.now(timezone.utc).isoformat()
    update_agent("uljas","active","Skaneeris turgu",f"{len(pairs)} paari; {len(discovery_cache.get('addresses',[]))} tokeniaadressi",checks=len(pairs))

    local_candidates = []
    for pair in pairs:
        try:
            if pair.get("chainId") != "solana":
                continue
            token = pair.get("baseToken") or {}
            address = token.get("address")
            price = float(pair.get("priceUsd") or 0)
            market_cap = float(pair.get("marketCap") or pair.get("fdv") or 0)
            liquidity = float((pair.get("liquidity") or {}).get("usd") or 0)
            created = float(pair.get("pairCreatedAt") or 0) / 1000
            pool_age = (now - created) / 60 if created else None
            features = market_features.get(address, {})
            volume_multiple = features.get("volume_multiple")
            gain_10m = features.get("gain_10m_pct")
            tx = (pair.get("txns") or {}).get("m5") or {}
            buys, sells = int(tx.get("buys") or 0), int(tx.get("sells") or 0)
            volume5 = float((pair.get("volume") or {}).get("m5") or 0)
            price_changes = pair.get("priceChange") or {}
            change5 = float(price_changes.get("m5") or 0)
            change1h = float(price_changes.get("h1") or 0)
            change24h = float(price_changes.get("h24") or 0)
            if not address or price <= 0:
                continue
            # Prefer mature, liquid Solana markets with substantial 24h activity.
            if pool_age is None or not (MIN_POOL_AGE_MINUTES <= pool_age <= MAX_POOL_AGE_MINUTES):
                continue
            if not (MIN_MARKET_CAP_USD <= market_cap <= MAX_MARKET_CAP_USD):
                continue
            volume24 = float((pair.get("volume") or {}).get("h24") or 0)
            large_lane = market_cap >= LARGE_CAP_MIN_MCAP
            if large_lane:
                if liquidity < LARGE_CAP_MIN_LIQUIDITY or volume24 < LARGE_CAP_MIN_VOLUME24 or buys / max(sells,1) < LARGE_CAP_MIN_BUY_RATIO:
                    continue
            elif liquidity < MIN_LIQUIDITY_USD:
                continue
            if change24h <= 0 or change1h < -5.0:
                continue
            if volume24 < MIN_VOLUME_24H_USD or volume5 < MIN_VOLUME_5M_USD or buys < MIN_BUYS_5M:
                continue
            buy_sell_ratio = buys / max(sells, 1)
            if buy_sell_ratio < MIN_BUYER_SELLER_RATIO:
                continue
            drawdown = float(features.get("drawdown_5m_pct") or 0)
            bounce = float(features.get("bounce_2m_pct") or 0)
            dip_rebound = (-MAX_DIP_PCT <= drawdown <= -MIN_DIP_PCT and bounce >= MIN_BOUNCE_PCT)
            if not dip_rebound:
                continue
            if gain_10m is not None and gain_10m > MAX_10M_PRICE_GAIN_PCT:
                continue
            is_blocked, _ = token_entry_blocked(address)
            if is_blocked:
                continue
            volume_multiple = float(volume_multiple) if volume_multiple is not None else 1.0
            pair["_momentum"] = {
                "volume_multiple": round(volume_multiple, 2),
                "pool_age_minutes": round(pool_age, 1),
                "observed_high": features.get("observed_high"),
                "drawdown_5m_pct": round(drawdown, 2),
                "bounce_2m_pct": round(bounce, 2),
                "price_gain_10m_pct": round(float(gain_10m), 1) if gain_10m is not None else None
            }
            score, setup, ratio = score_candidate(pair)
            pair["_score"] = score
            pair["_setup"] = "dip-rebound" if dip_rebound else "momentum-continuation"
            pair["_buy_sell_ratio"] = ratio
            pair["_entry_volume24"] = volume24
            pair["_entry_change1h"] = change1h
            pair["_entry_change24h"] = change24h
            pair["_market_lane"] = "large-cap" if market_cap >= LARGE_CAP_MIN_MCAP else "base-cap"
            local_candidates.append(pair)
        except (TypeError, ValueError):
            continue

    with news_lock:
        available_news = list(news_cache.get("items") or [])
    for candidate in local_candidates[:20]:
        token = candidate.get("baseToken") or {}
        terms = [str(token.get(k) or "").strip() for k in ("name","symbol")]
        matches = []
        for article in available_news:
            haystack = (str(article.get("title") or "") + " " + str(article.get("description") or "")).lower()
            if any(len(term) >= 4 and term.lower() in haystack for term in terms):
                matches.append({"title":article.get("title",""),"url":article.get("url",""),"source":article.get("source",""),"published":article.get("published","")})
        candidate["_news_matches"] = matches[:3]

    safe_candidates = []
    blocked = 0
    for candidate in local_candidates[:20]:
        address = (candidate.get("baseToken") or {}).get("address")
        passed, report = rugcheck_token(address)
        candidate["_rugcheck"] = {
            "status": report.get("status","unavailable"),
            "score": report.get("score"),
            "risk_count": report.get("risk_count",0),
            "danger_count": report.get("danger_count",0),
            "lp_locked_pct": report.get("lpLockedPct")
        }
        if not passed:
            blocked += 1
            candidate["_entry_blockers"] = [str(report.get("reason","RugCheck blocked"))]
            add_event(f"RugCheck SKIP {candidate.get('baseToken',{}).get('symbol','?')}: {report.get('reason','risk')}")
            continue
        basic_ok, reasons = strict_safety_check(candidate, report)
        if not basic_ok:
            blocked += 1
            candidate["_entry_blockers"] = reasons
            add_event(f"hard filter SKIP {candidate.get('baseToken',{}).get('symbol','?')}: {', '.join(reasons[:3])}")
            continue
        metrics = candidate.get("marketMetrics") or candidate.get("onchainMetrics") or candidate.get("metrics") or {}
        buyers = _number_from_sources([candidate, metrics], ["uniqueBuyers5m", "unique_buyers_5m", "buyersUnique5m"])
        sellers = _number_from_sources([candidate, metrics], ["uniqueSellers5m", "unique_sellers_5m", "sellersUnique5m"])
        candidate["_unique_buyers_5m"] = buyers
        candidate["_unique_sellers_5m"] = sellers
        candidate["_unique_buyer_seller_ratio"] = buyers / max(sellers, 1) if buyers is not None and sellers is not None else 0
        tx5 = (candidate.get("txns") or {}).get("m5") or {}
        trade_ratio = int(tx5.get("buys") or 0) / max(int(tx5.get("sells") or 0), 1)
        volume_factor = min(5.0, float((candidate.get("volume") or {}).get("m5") or 0) / 10000.0)
        recovery_factor = max(0.0, float((candidate.get("_momentum") or {}).get("bounce_2m_pct") or 0))
        candidate["_rank_score"] = round(trade_ratio * (1.0 + volume_factor) * (1.0 + min(recovery_factor, 5.0) / 10.0), 4)
        candidate["_momentum_score"] = momentum_score(candidate)
        candidate["_risk"] = risk_assessment(candidate, report)
        candidate["_risk_score"] = candidate["_risk"]["score"]
        candidate["_rugcheck_report"] = report
        if not risk_entry_allowed(candidate["_risk"]):
            blocked += 1
            candidate["_entry_blockers"] = [f"supplementary risk score {candidate['_risk_score']:.1f} >= {MAX_ENTRY_RISK_SCORE:.0f}"]
            add_event(f"risk score SKIP {candidate.get('baseToken',{}).get('symbol','?')}: {candidate['_entry_blockers'][0]}")
            continue
        safe_candidates.append(candidate)

    safe_candidates.sort(key=lambda p: (float(p.get("_momentum_score") or 0),float(p.get("_rank_score") or 0)), reverse=True)
    safe_addresses = {(p.get("baseToken") or {}).get("address") for p in safe_candidates}
    checked_addresses = {(p.get("baseToken") or {}).get("address") for p in local_candidates[:20]}
    signal_rows = []
    for candidate in local_candidates:
        address = (candidate.get("baseToken") or {}).get("address")
        tx = (candidate.get("txns") or {}).get("m5") or {}
        metrics = candidate.get("marketMetrics") or candidate.get("onchainMetrics") or candidate.get("metrics") or {}
        if address not in checked_addresses:
            blockers = ["not risk-checked: outside top-20 scan limit"]
        else:
            blockers = candidate.get("_entry_blockers") or ([] if address in safe_addresses else ["not qualified"])
        signal_rows.append({
            "time": datetime.now(timezone.utc).isoformat(),
            "signal_ts": now,
            "address": address,
            "symbol": (candidate.get("baseToken") or {}).get("symbol","?"),
            "price_usd": float(candidate.get("priceUsd") or 0),
            "market_cap_usd": float(candidate.get("marketCap") or candidate.get("fdv") or 0),
            "liquidity_usd": float((candidate.get("liquidity") or {}).get("usd") or 0),
            "pool_age_minutes": (candidate.get("_momentum") or {}).get("pool_age_minutes"),
            "volume_5m_usd": float((candidate.get("volume") or {}).get("m5") or 0),
            "volume_multiple": (candidate.get("_momentum") or {}).get("volume_multiple"),
            "buys_5m": int(tx.get("buys") or 0),
            "sells_5m": int(tx.get("sells") or 0),
            "unique_buyers_5m": _number_from_sources([candidate,metrics],["uniqueBuyers5m","unique_buyers_5m","buyersUnique5m"]),
            "unique_sellers_5m": _number_from_sources([candidate,metrics],["uniqueSellers5m","unique_sellers_5m","sellersUnique5m"]),
            "single_wallet_volume_pct": _number_from_sources([candidate,metrics],["largestWalletVolumePct5m","largest_wallet_volume_pct_5m","topWalletVolumeShare5m"]),
            "price_gain_10m_pct": (candidate.get("_momentum") or {}).get("price_gain_10m_pct"),
            "breakout": True,
            "rank_score": candidate.get("_rank_score"),
            "momentum_score": candidate.get("_momentum_score"),
            "risk_score": candidate.get("_risk_score"),
            "market_lane": candidate.get("_market_lane"),
            "qualified": address in safe_addresses,
            "blockers": blockers,
            "outcome": "already-open" if address in state.get("positions",{}) else ("qualified" if address in safe_addresses else "blocked")
        })
    with state_lock:
        state.setdefault("signal_log", []).extend(signal_rows)
        del state["signal_log"][:-1500]
    update_forward_signal_outcomes()
    buy_total = sum(int(((p.get("txns") or {}).get("m5") or {}).get("buys") or 0) for p in local_candidates)
    sell_total = sum(int(((p.get("txns") or {}).get("m5") or {}).get("sells") or 0) for p in local_candidates)
    update_agent("taju","active" if local_candidates else "waiting","Hindab ostu- ja müügisurvet",
                 f"{len(local_candidates)} momentum kandidaati; {buy_total} buys / {sell_total} sells (tehinguloendus, mitte unikaalsed rahakotid)",
                 checks=len(pairs))
    top = local_candidates[0] if local_candidates else None
    update_agent("teras","active" if top else "waiting","Kontrollib breakouti ja volüümi",
                 f"{(top.get('baseToken') or {}).get('symbol','?')}: {top.get('_momentum',{}).get('volume_multiple')}x volüüm" if top else "Ootan 15m volüümi ajalugu ja 5m hinnamustrit",
                 symbol=(top.get("baseToken") or {}).get("symbol") if top else None,checks=len(pairs))
    update_agent("teataja","active" if available_news else "limited","Seob avalikud uudispealkirjad tokenitega",
                 f"{len(available_news)} uudispealkirja; tekstiline vaste on ainult lisakontekst",checks=len(pairs))
    update_agent("valvur","active" if local_candidates else "waiting","Kontrollib hard safety filtreid",
                 f"{len(local_candidates)} kontrollitud; {len(safe_candidates)} läbis; {blocked} blokeeritud; teadmata väli = SKIP",
                 checks=len(local_candidates))

    with state_lock:
        if not persistence_ok:
            raise RuntimeError(f"paper trading paused: persistent storage unavailable ({persistence_error})")
        state_before_scan = copy.deepcopy(state)
        today = _tallinn_day()
        if state.get("day") != today:
            state["day"] = today
            state["daily_count"] = 0
            # Keep stop-loss cooldowns until their own expiry, across day rollover.
            state["session_halted"] = False
            state["session_halt_reason"] = ""
            state["day_start_equity"] = float(state["cash"]) + sum(
                float(p.get("quantity",0))*float(p.get("last_price",p.get("entry_price",0)))
                for p in state["positions"].values()
            )
            add_event("uus Tallinna kauplemissessioon: päevased piirid lähtestatud")

        # Update open-position prices even after a token leaves the trending feed.
        by_address = {(p.get("baseToken") or {}).get("address"):p for p in pairs if p.get("baseToken")}
        for address in list(state["positions"].keys()):
            if address not in by_address:
                try:
                    extra = get_json(f"{DEX}/token-pairs/v1/solana/{address}")
                    if isinstance(extra, list):
                        matches = [x for x in extra if (x.get("baseToken") or {}).get("address") == address and float(x.get("priceUsd") or 0) > 0]
                        if matches:
                            by_address[address] = max(matches, key=lambda x: float((x.get("liquidity") or {}).get("usd") or 0))
                except Exception as exc:
                    add_event(f"position refresh failed {address[:8]}: {str(exc)[:80]}")

        for address, pos in list(state["positions"].items()):
            pair = by_address.get(address)
            if not pair:
                continue
            price = float(pair.get("priceUsd") or 0)
            if price <= 0:
                continue
            pos["last_price"] = price
            pos["peak_price"] = max(float(pos.get("peak_price") or pos["entry_price"]), price)
            state.setdefault("price_history", {}).setdefault(address, []).append({"t":now,"p":price})
            state["price_history"][address] = state["price_history"][address][-420:]
            if not pos.get("opened_ts"):
                try:
                    pos["opened_ts"] = datetime.fromisoformat(pos.get("opened_at", "").replace("Z", "+00:00")).timestamp()
                except Exception:
                    pos["opened_ts"] = now
            age_seconds = now - float(pos.get("opened_ts") or now)
            pnl_pct = price / float(pos["entry_price"]) - 1
            runner = bool(pos.get("runner"))
            half_sold = bool(pos.get("runner_half_sold"))
            reason, partial = position_exit_decision(pos, pair, price, age_seconds)

            if reason:
                quantity = float(pos["quantity"]) / 2 if partial else float(pos["quantity"])
                part = quantity / max(float(pos["quantity"]), 1e-18)
                execution_price = simulated_fill("sell", price)
                if execution_price is None:
                    state["cash"] = max(0.0, float(state["cash"]) - SIM_FAILED_TX_FEE_EUR)
                    add_event(f"paper sell failed: {pos['symbol']} · retrying next scan")
                    continue
                proceeds = quantity * execution_price
                sold_cost = float(pos["cost"]) * part
                realized = proceeds - sold_cost - SIM_TX_FEE_EUR
                state["cash"] += proceeds - SIM_TX_FEE_EUR
                state["trades"].append({
                    "symbol":pos["symbol"],"address":address,"entry":pos["entry_price"],"exit":execution_price,
                    "pnl":realized,"pnl_pct":pnl_pct,"closed_at":datetime.now(timezone.utc).isoformat(),
                    "reason":reason,"partial":partial,"strategy":pos.get("strategy"),
                    "runner":bool(pos.get("runner")),"entry_metrics":pos.get("entry_metrics",{}),
                    "risk_score":pos.get("risk_score"),"momentum_score":pos.get("momentum_score"),
                    "simulated_slippage_pct":pos.get("sim_slippage_pct")
                })
                if partial:
                    pos["quantity"] -= quantity
                    pos["cost"] -= sold_cost
                    pos["runner_half_sold"] = True
                    pos["runner_peak_price"] = price
                    add_event(f"runner partial sell {pos['symbol']} | 50% | {pnl_pct*100:+.1f}%")
                else:
                    if reason.startswith("stop loss"):
                        state.setdefault("stopped_tokens", {})[address] = time.time() + TOKEN_COOLDOWN_SECONDS
                    del state["positions"][address]
                    add_event(f"sell {pos['symbol']} | {reason} | {pnl_pct*100:+.1f}% | {realized:+.2f} EUR")
            if address in state["positions"] and state["positions"][address].get("runner_half_sold"):
                state["positions"][address]["runner_peak_price"] = max(float(state["positions"][address].get("runner_peak_price") or price), price)

        day_pnl = daily_pnl_unlocked()
        if not state.get("session_halted") and (day_pnl <= DAILY_STOP_LOSS_EUR or day_pnl >= DAILY_TARGET_EUR):
            state["session_halted"] = True
            state["session_halt_reason"] = "daily loss cap" if day_pnl <= DAILY_STOP_LOSS_EUR else "daily profit target"
            add_event(f"SESSION HALTED: {state['session_halt_reason']} | daily PnL {day_pnl:+.2f} EUR")

        if state.get("session_halted"):
            for address, pos in list(state["positions"].items()):
                pair = by_address.get(address)
                if not pair:
                    continue
                price = float(pair.get("priceUsd") or 0)
                execution_price = simulated_fill("sell", price) if price > 0 else None
                if execution_price is None:
                    continue
                proceeds = float(pos["quantity"]) * execution_price
                realized = proceeds - float(pos["cost"]) - SIM_TX_FEE_EUR
                state["cash"] += proceeds - SIM_TX_FEE_EUR
                state["trades"].append({"symbol":pos["symbol"],"address":address,"entry":pos["entry_price"],"exit":execution_price,
                    "pnl":realized,"pnl_pct":price/float(pos["entry_price"])-1,"closed_at":datetime.now(timezone.utc).isoformat(),
                    "reason":state.get("session_halt_reason"),"partial":False,
                    "strategy":pos.get("strategy"),"runner":bool(pos.get("runner")),
                    "entry_metrics":pos.get("entry_metrics",{}),"risk_score":pos.get("risk_score"),
                    "momentum_score":pos.get("momentum_score"),"simulated_slippage_pct":pos.get("sim_slippage_pct")})
                del state["positions"][address]
                add_event(f"session exit {pos['symbol']} | {realized:+.2f} EUR")

        apply_wallet_copy_signals(safe_candidates, pairs)

        bought_symbols = []
        if not paused and not state.get("session_halted") and len(state["positions"]) < MAX_POSITIONS and state["daily_count"] < MAX_TRADES_PER_DAY:
            for candidate in safe_candidates:
                address = (candidate.get("baseToken") or {}).get("address")
                is_blocked, _ = token_entry_blocked(address)
                if not address or address in state["positions"] or is_blocked:
                    continue
                equity = float(state["cash"]) + sum(float(p.get("quantity",0))*float(p.get("last_price",p.get("entry_price",0))) for p in state["positions"].values())
                day_pnl = daily_pnl_unlocked()
                size = dynamic_trade_size(equity, day_pnl)
                score_scale = max(0.50, min(1.25, float(candidate.get("_momentum_score") or 0) / 80.0))
                risk_scale = max(0.50, 1.0 - min(80.0, float(candidate.get("_risk_score") or 0)) / 160.0)
                regime_scale = 0.75 if market_regime.get("label") == "BEAR" else 1.0
                size = round(size * score_scale * risk_scale * regime_scale, 2)
                if size <= 1 or size + SIM_TX_FEE_EUR > float(state["cash"]):
                    continue
                # Paper-only execution uses the observed DEX price plus simulated slippage;
                # a live Jupiter route is not required because no on-chain order is submitted.
                execution_price = simulated_fill("buy", float(candidate["priceUsd"]))
                if execution_price is None:
                    state["cash"] = max(0.0, float(state["cash"]) - SIM_FAILED_TX_FEE_EUR)
                    add_event(f"paper buy failed: {candidate.get('baseToken',{}).get('symbol','?')}")
                    continue
                state["cash"] -= size + SIM_TX_FEE_EUR
                state["entry_count"] = int(state.get("entry_count", 0)) + 1
                runner = state["entry_count"] % RUNNER_EVERY_NTH_ENTRY == 0
                symbol = (candidate.get("baseToken") or {}).get("symbol","?")
                state["positions"][address] = {
                    "symbol":symbol,"entry_price":execution_price,"last_price":float(candidate["priceUsd"]),
                    "peak_price":float(candidate["priceUsd"]),"quantity":size/execution_price,"cost":size+SIM_TX_FEE_EUR,
                    "opened_at":datetime.now(timezone.utc).isoformat(),"opened_ts":time.time(),
                    "strategy":"runner" if runner else "scalp","runner":runner,"runner_half_sold":False,
                    "runner_peak_price":float(candidate["priceUsd"]),"score":candidate.get("_rank_score",0),"momentum_score":candidate.get("_momentum_score",0),
                    "risk_score":candidate.get("_risk_score",0),
                    "liquidity_usd":float((candidate.get("liquidity") or {}).get("usd") or 0),
                    "market_cap":float(candidate.get("marketCap") or candidate.get("fdv") or 0),
                    "sim_slippage_pct":SIM_SLIPPAGE_PCT*100,
                    "entry_metrics":{
                        "rank_score":candidate.get("_rank_score"),
                        "momentum_score":candidate.get("_momentum_score"),
                        "volume_multiple":(candidate.get("_momentum") or {}).get("volume_multiple"),
                        "unique_buyers_5m":candidate.get("_unique_buyers_5m"),
                        "unique_sellers_5m":candidate.get("_unique_sellers_5m"),
                        "unique_buyer_seller_ratio":candidate.get("_unique_buyer_seller_ratio"),
                        "pool_age_minutes":(candidate.get("_momentum") or {}).get("pool_age_minutes"),
                        "price_gain_10m_pct":(candidate.get("_momentum") or {}).get("price_gain_10m_pct"),
                        "liquidity_usd":float((candidate.get("liquidity") or {}).get("usd") or 0),
                        "market_cap_usd":float(candidate.get("marketCap") or candidate.get("fdv") or 0)
                    }
                }
                state["daily_count"] += 1
                bought_symbols.append(symbol)
                for signal in reversed(state.get("signal_log", [])):
                    if signal.get("address") == address and signal.get("outcome") == "qualified":
                        signal["outcome"] = "entered"
                        signal["entry_strategy"] = "RUNNER" if runner else "SCALP"
                        signal["entry_size_eur"] = size
                        break
                add_event(f"paper buy {symbol} | EUR {size:.2f} | {candidate.get('_setup')} | {'RUNNER' if runner else 'SCALP'} | rank {candidate.get('_rank_score')}")
                if len(bought_symbols) >= MAX_BUYS_PER_SCAN or len(state["positions"]) >= MAX_POSITIONS or state["daily_count"] >= MAX_TRADES_PER_DAY:
                    break

        try:
            save_state()
        except Exception:
            state.clear()
            state.update(state_before_scan)
            raise

        shown = safe_candidates or local_candidates
        # Keep live price history for every displayed market, not only open paper positions.
        # This powers the dashboard's real-time candlestick charts.
        for market in shown[:20]:
            market_address = (market.get("baseToken") or {}).get("address")
            market_price = float(market.get("priceUsd") or 0)
            if market_address and market_price > 0:
                history = state.setdefault("price_history", {}).setdefault(market_address, [])
                if not history or float(history[-1].get("t") or 0) < now:
                    history.append({"t": now, "p": market_price})
                state["price_history"][market_address] = history[-420:]

        last_candidates = [{
            "symbol":(p.get("baseToken") or {}).get("symbol","?"),
            "name":(p.get("baseToken") or {}).get("name",""),
            "price":p.get("priceUsd"),
            "change5m":(p.get("priceChange") or {}).get("m5",0),
            "volume5m":(p.get("volume") or {}).get("m5",0),
            "buys5m":((p.get("txns") or {}).get("m5") or {}).get("buys",0),
            "sells5m":((p.get("txns") or {}).get("m5") or {}).get("sells",0),
            "liquidity":(p.get("liquidity") or {}).get("usd",0),
            "marketCap":p.get("marketCap") or p.get("fdv") or 0,
            "history":state.setdefault("price_history",{}).setdefault((p.get("baseToken") or {}).get("address",""),[])[-60:],
            "address":(p.get("baseToken") or {}).get("address",""),
            "pair_address":p.get("pairAddress",""),
            "url":p.get("url",""),
            "entry_setup":p.get("_setup",""),
            "rugcheck":p.get("_rugcheck",{"status":"not_checked"}),
            "strategy_score":p.get("_score",0),
            "setup":p.get("_setup","momentum"),
            "buy_sell_ratio":p.get("_buy_sell_ratio",0),
            "volume_multiple":(p.get("_momentum") or {}).get("volume_multiple"),
            "rank_score":p.get("_rank_score"),
            "momentum_score":p.get("_momentum_score"),
            "risk_score":p.get("_risk_score"),
            "risk_level":(p.get("_risk") or {}).get("level"),
            "risk_factors":(p.get("_risk") or {}).get("factors",[]),
            "sources":p.get("_discovery_sources",[]),
            "news":p.get("_news_matches",[])
        } for p in shown[:20]]
        last_pair_count, last_candidate_count = len(pairs), len(safe_candidates)
        last_scan = datetime.now(timezone.utc).isoformat()
        last_error = ""
        bought_label = ", ".join(bought_symbols)
        jano_verdict = "BUY" if bought_symbols else ("WAIT" if not state.get("session_halted") else "HALT")
        update_agent("jano","active" if bought_symbols else "waiting","JANO controls the paper session",
                     f"{jano_verdict}: {bought_label or state.get('session_halt_reason') or 'no safe dip/recovery entry'}; open {len(state['positions'])}/{MAX_POSITIONS}; day PnL {daily_pnl_unlocked():+.2f} EUR",
                     symbol=bought_label or None,checks=len(pairs),decision=jano_verdict)
    rugcheck_stats["status"] = "connected" if rugcheck_stats["checked"] else ("unavailable" if rugcheck_stats["errors"] else "checking")
    return len(pairs), len(safe_candidates)

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
        if TELEGRAM_ALERTS_ENABLED and ALLOWED_USER_ID:
            with alert_lock:
                queued = list(pending_alerts)
                pending_alerts.clear()
            for alert in queued:
                try:
                    await app.bot.send_message(chat_id=int(ALLOWED_USER_ID), text=alert["message"])
                except Exception as alert_error:
                    log.warning("Telegram alert delivery failed: %s", str(alert_error)[:160])
                    with alert_lock:
                        pending_alerts.insert(0, alert)
                        del pending_alerts[30:]
                    break
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
        msg = (f"janobot status\ntelegram: connected\nmootor: {'pausil' if paused else 'töötab'}\n"
        f"režiim: paper demo\nvaba raha: €{state['cash']:.2f}\npositsioonide väärtus: €{value:.2f}\n"
        f"avatud positsioonid: {len(state['positions'])}/{MAX_POSITIONS}\n"
        f"scalp TP +{SCALP_TAKE_PROFIT*100:.0f}% / SL {SCALP_STOP_LOSS*100:.0f}% / time stop {TIME_STOP_SECONDS//60}m\n"
        f"runner: iga {RUNNER_EVERY_NTH_ENTRY}. sisenemine, pool +{RUNNER_HALF_TP*100:.0f}%, trail {RUNNER_TRAIL_GIVEBACK*100:.0f}%\n"
        f"päeva PnL: {daily_pnl_unlocked():+.2f} € | sessioon: {'PEATATUD: '+state.get('session_halt_reason','') if state.get('session_halted') else 'aktiivne'}\n"
        f"suurus: {POSITION_RISK_PCT*100:.1f}% equityst | päevane kaotuspiir: -€{abs(DAILY_STOP_LOSS_EUR):.0f} | kasumisiht: +€{DAILY_TARGET_EUR:.0f}\n"
        f"simulatsioon: {SIM_SLIPPAGE_PCT*100:.1f}% slippage/külg, {SIM_LATENCY_MS}ms, {SIM_FAILED_TX_RATE*100:.0f}% fail\n"
        f"paarid: {last_pair_count} | signaalid: {last_candidate_count}\n"
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


async def wallets_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update): return await reply_denied(update)
    with wallet_tracker_lock:
        rows=list(wallet_tracker.get("wallets",[])); signals=list(wallet_tracker.get("signals",[]))[:5]; status_text=wallet_tracker.get("status","waiting")
    lines=[f"wallet tracker: {status_text}",f"jälgitavaid: {len(rows)}"]
    lines += [f"{w.get('label')} · pnl {w.get('pnl') or 'n/a'} · {w.get('last_action') or 'ootab'}" for w in rows[:10]]
    lines += ["viimased signaalid:"] + [f"{x['side']} {x['mint'][:8]} · {x['label']}" for x in signals]
    await send_text(update,"\\n".join(lines))

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
    stale = False
    if last_scan != "not yet":
        try:
            stale = time.time() - datetime.fromisoformat(last_scan.replace("Z", "+00:00")).timestamp() > 180
        except Exception:
            stale = True
    healthy = bool(persistence_ok and not stale and not last_error)
    # Keep the hosting health-check response HTTP 200; expose degraded readiness in JSON.
    return jsonify({"ok": healthy, "service": "spiderbot", "paper_only": True,
                    "persistence_ok": persistence_ok, "scanner_stale": stale,
                    "last_scan": last_scan, "last_error": last_error}), 200
@web.get("/")
def dashboard():
    if not check_auth(request.authorization): return auth_required()
    return Response(Path("dashboard.html").read_text(), mimetype="text/html")
@web.get("/api/news")
def news_data():
    if not check_auth(request.authorization): return auth_required()
    with news_lock:
        return jsonify(dict(news_cache))

@web.get("/api/chart/<pool_address>")
def chart_data(pool_address):
    """Serve cached real GeckoTerminal candles with shared rate-limit protection."""
    if not check_auth(request.authorization): return auth_required()
    if not re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{32,44}", pool_address):
        return jsonify({"error":"invalid Solana pool address","candles":[]}), 400
    now = time.time()
    with chart_cache_lock:
        cached = chart_cache.get(pool_address)
        if cached and now - cached["time"] < CHART_CACHE_SECONDS:
            return jsonify({"candles":cached["candles"],"source":"GeckoTerminal","cached":True,
                            "updated":cached["updated"]})
    global chart_last_request, chart_rate_limit_until
    with chart_request_lock:
        now = time.time()
        if now < chart_rate_limit_until:
            with chart_cache_lock:
                stale = chart_cache.get(pool_address)
            if stale:
                return jsonify({"candles":stale["candles"],"source":"GeckoTerminal","cached":True,
                                "stale":True,"warning":"GeckoTerminal rate-limit cooldown","updated":stale["updated"]})
            return jsonify({"error":"GeckoTerminal rate-limit cooldown; retry shortly","candles":[]}), 503
        # Serialize chart fetches and add spacing to avoid bursts from several cards.
        wait = 1.5 - (now - chart_last_request)
        if wait > 0:
            time.sleep(wait)
        chart_last_request = time.time()
        try:
            url = f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool_address}/ohlcv/minute"
            response = requests.get(url, params={"aggregate":"1","limit":"180","currency":"usd","token":"base"},
                                   headers={"Accept":"application/json;version=20230203",
                                            "User-Agent":"janobot-dashboard/1.0"}, timeout=12)
            if response.status_code == 429:
                chart_rate_limit_until = time.time() + 120
                with chart_cache_lock:
                    stale = chart_cache.get(pool_address)
                if stale:
                    return jsonify({"candles":stale["candles"],"source":"GeckoTerminal","cached":True,
                                    "stale":True,"warning":"GeckoTerminal rate limited; using cached candles","updated":stale["updated"]})
                return jsonify({"error":"GeckoTerminal rate-limited requests; retry in about 2 minutes","candles":[]}), 503
            response.raise_for_status()
            payload = response.json()
            raw = (((payload.get("data") or {}).get("attributes") or {}).get("ohlcv_list") or [])
            candles = []
            seen = set()
            for row in raw:
                if not isinstance(row, list) or len(row) < 6:
                    continue
                try:
                    stamp = int(row[0])
                    candle = {"time":stamp,"open":float(row[1]),"high":float(row[2]),
                              "low":float(row[3]),"close":float(row[4]),"volume":float(row[5])}
                    if stamp > 0 and all(candle[k] > 0 for k in ("open","high","low","close")) and stamp not in seen:
                        seen.add(stamp)
                        candles.append(candle)
                except (TypeError,ValueError,OverflowError):
                    continue
            candles.sort(key=lambda x:x["time"])
            if not candles:
                raise ValueError("GeckoTerminal returned no OHLCV candles for this pool")
            updated = datetime.now(timezone.utc).isoformat()
            with chart_cache_lock:
                chart_cache[pool_address] = {"time":time.time(),"candles":candles,"updated":updated}
            return jsonify({"candles":candles,"source":"GeckoTerminal","cached":False,"updated":updated})
        except Exception as exc:
            with chart_cache_lock:
                stale = chart_cache.get(pool_address)
            if stale:
                return jsonify({"candles":stale["candles"],"source":"GeckoTerminal","cached":True,
                                "stale":True,"warning":str(exc)[:140],"updated":stale["updated"]})
            return jsonify({"error":str(exc)[:180],"candles":[],"source":"GeckoTerminal"}), 502

@web.get("/api/trades.csv")
def trades_csv():
    if not check_auth(request.authorization):
        return auth_required()
    with state_lock:
        rows = list(state.get("trades", []))
    output = io.StringIO()
    fields = ("closed_at", "symbol", "address", "entry", "exit", "pnl", "pnl_pct",
              "reason", "strategy", "partial", "runner", "risk_score", "momentum_score",
              "simulated_slippage_pct")
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return Response(output.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=spiderbot-paper-trades.csv"})

@web.get("/api/live")
def live_data():
    if not check_auth(request.authorization): return auth_required()
    with wallet_tracker_lock:
        wallet_payload = copy.deepcopy(wallet_tracker)
    with state_lock:
        positions = []
        for address, p in state["positions"].items():
            price = p.get("last_price",p["entry_price"])
            positions.append({"symbol":p["symbol"],"address":address,"entry":p["entry_price"],"price":price,
                "pnl":p["quantity"]*price-p["cost"],"pnl_pct":(price/p["entry_price"]-1)*100,"history":state.get("price_history",{}).get(address,[])})
        pos_value = sum(p["quantity"]*p.get("last_price",p["entry_price"]) for p in state["positions"].values())
        payload = {"cash":state["cash"],"positions_value":pos_value,"total":state["cash"]+pos_value,
            "realized_pnl":sum(t["pnl"] for t in state["trades"]),"positions":positions,
            "trades":state["trades"][-10:],"signals":state.get("signal_log",[])[-100:],"wallet_performance":wallet_performance(),"strategy_performance":strategy_performance(),
            "candidates":last_candidates,"market_regime":dict(market_regime),"closed_trade_performance":closed_trade_performance(),"forward_signal_performance":forward_signal_performance(),"wallet_tracker":wallet_payload,"events":events[-20:],"agents":[dict(a) for a in agents.values()],"agent_history":agent_history[-24:],
            "last_scan":last_scan,"last_error":last_error,"pair_count":last_pair_count,
            "signal_count":last_candidate_count,"discovery":dict(discovery_stats),"rugcheck":dict(rugcheck_stats),"paused":paused,"mode":"PAPER DEMO","persistence":{"ok":persistence_ok,"file":str(STATE_FILE),"error":persistence_error},"settings":{"strategy":"FULL SEND PAPER","starting_balance":STARTING_BALANCE,"position_risk_pct":POSITION_RISK_PCT*100,"max_positions":MAX_POSITIONS,"max_entry_risk_score":MAX_ENTRY_RISK_SCORE,"max_portfolio_exposure_pct":MAX_PORTFOLIO_EXPOSURE_PCT*100,"max_buys_per_scan":MAX_BUYS_PER_SCAN,"scalp_take_profit_pct":SCALP_TAKE_PROFIT*100,"scalp_stop_loss_pct":SCALP_STOP_LOSS*100,"time_stop_seconds":TIME_STOP_SECONDS,"runner_every_nth_entry":RUNNER_EVERY_NTH_ENTRY,"runner_half_take_profit_pct":RUNNER_HALF_TP*100,"runner_trailing_pct":RUNNER_TRAIL_GIVEBACK*100,"daily_loss_limit_eur":DAILY_STOP_LOSS_EUR,"reduce_size_at_profit_eur":REDUCE_SIZE_AT_PROFIT_EUR,"daily_profit_target_eur":DAILY_TARGET_EUR,"daily_pnl_eur":daily_pnl_unlocked(),"session_halted":state.get("session_halted",False),"session_halt_reason":state.get("session_halt_reason",""),"simulated_slippage_pct_per_side":SIM_SLIPPAGE_PCT*100,"simulated_latency_ms":SIM_LATENCY_MS,"simulated_failed_tx_rate_pct":SIM_FAILED_TX_RATE*100,"min_market_cap_usd":MIN_MARKET_CAP_USD,"max_market_cap_usd":MAX_MARKET_CAP_USD,"min_liquidity_usd":MIN_LIQUIDITY_USD,"min_volume_24h_usd":MIN_VOLUME_24H_USD,"min_pool_age_minutes":MIN_POOL_AGE_MINUTES,"large_cap_min_mcap":LARGE_CAP_MIN_MCAP,"large_cap_min_liquidity":LARGE_CAP_MIN_LIQUIDITY,"large_cap_min_volume24":LARGE_CAP_MIN_VOLUME24,"min_volume_5m_usd":MIN_VOLUME_5M_USD,"min_dip_pct":MIN_DIP_PCT,"max_dip_pct":MAX_DIP_PCT,"min_bounce_pct":MIN_BOUNCE_PCT,"min_pool_age_minutes":MIN_POOL_AGE_MINUTES,"max_pool_age_minutes":MAX_POOL_AGE_MINUTES,"min_volume_multiple":MIN_VOLUME_SPIKE_MULTIPLE,"min_unique_buyer_seller_ratio":MIN_BUYER_SELLER_RATIO,"min_lp_locked_pct":MIN_LP_LOCKED_PCT,"max_top10_holder_pct":MAX_TOP10_HOLDERS_PCT,"max_dev_holder_pct":MAX_DEV_HOLDER_PCT,"max_bundle_cluster_pct":MAX_BUNDLE_CLUSTER_PCT,"max_single_wallet_volume_pct":MAX_SINGLE_WALLET_VOLUME_PCT,"scan_seconds":SCAN_SECONDS,"jupiter_api_configured":bool(JUPITER_API_KEY)}}
    return jsonify(payload)

def run_web():
    web.run(host="0.0.0.0", port=int(os.environ.get("PORT","10000")), use_reloader=False)

def main():
    if not TOKEN: raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")
    load_state()
    threading.Thread(target=news_loop, daemon=True).start()
    threading.Thread(target=wallet_tracker_loop, daemon=True).start()
    threading.Thread(target=run_web, daemon=True).start()
    app = Application.builder().token(TOKEN).post_init(post_init).build()
    for cmd, fn in [("start",start),("status",status),("portfolio",portfolio),("trades",trades),("scan",scan),("wallets",wallets_cmd),("pause",pause_cmd),("resume",resume_cmd)]:
        app.add_handler(CommandHandler(cmd,fn))
    add_event("janobot käivitus; paper demo")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
