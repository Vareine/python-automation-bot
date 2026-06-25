import asyncio
import aiohttp
import socket
import pandas as pd
import numpy as np
import time as pytime
import logging
from datetime import datetime, timezone, timedelta
from datetime import time as dt_time
from collections import deque
from aiohttp.resolver import AsyncResolver

# ==========================================
# 1. CONFIGURATION (XAUUSD)
# ==========================================
TELEGRAM_TOKEN = "YOUR TOKEN ID"
CHAT_ID = "YOUR CHAT ID"
FINNHUB_API_KEY = "YOUR FINNHUB API KEY"

# QUOTA FINNHUB
MAX_REQUESTS_PER_MINUTE = 25
MAX_REQUESTS_PER_DAY = 1500

ACCOUNT_BALANCE = 20.0
RISK_PER_TRADE = 0.04           # 4%
DAILY_LOSS_LIMIT = 0.23 * ACCOUNT_BALANCE
DAILY_PROFIT_TARGET = 0.30 * ACCOUNT_BALANCE
MIN_SIGNAL_SCORE = 80
STALE_CACHE_EXTENSION = 1.5
LOW_QUOTA_CACHE_EXTENSION = 2.0

# 🎯 KILLZONE INSTITUTIONNEL
SESSION_ENABLED = True
LONDON_KILLZONE_START = dt_time(7, 0)
LONDON_KILLZONE_END = dt_time(10, 0)
NY_KILLZONE_START = dt_time(12, 0)
NY_KILLZONE_END = dt_time(15, 0)

# ⏰ Blackout News (CPI, NFP, FOMC)
NEWS_BLACKOUT_TIMES = [
    (13, 30, 14, 30),
    (15, 0, 16, 0),
    (18, 0, 19, 0)
]

request_tracker = {
    'minute_requests': deque(),
    'day_requests': 0,
    'day_start': datetime.now(timezone.utc).date(),
    'cache_hits': 0,
    'api_calls': 0
}

DATA_CACHE = {}
CACHE_TTL = {"5": 120, "15": 300, "60": 600}

trade_state = {
    'today_loss': 0.0,
    'today_profit': 0.0,
    'last_reset': datetime.now(timezone.utc).date(),
    'scan_count': 0,
    'last_update_id': 0
}
last_signal_time = {}

log_filename = f"smc_v4_xau_{datetime.now().strftime('%Y%m%d')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_filename, encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ==========================================
# 2. UTILITIES & QUOTA
# ==========================================
def can_make_request():
    now = datetime.now(timezone.utc)
    if now.date() != request_tracker['day_start']:
        request_tracker['day_requests'] = 0
        request_tracker['day_start'] = now.date()
    while request_tracker['minute_requests'] and (now - request_tracker['minute_requests'][0]).seconds >= 60:
        request_tracker['minute_requests'].popleft()
    if len(request_tracker['minute_requests']) >= MAX_REQUESTS_PER_MINUTE: return False
    if request_tracker['day_requests'] >= MAX_REQUESTS_PER_DAY: return False
    return True

def record_request():
    request_tracker['minute_requests'].append(datetime.now(timezone.utc))
    request_tracker['day_requests'] += 1
    request_tracker['api_calls'] += 1

def get_quota_status():
    return {'minute_remaining': MAX_REQUESTS_PER_MINUTE - len(request_tracker['minute_requests']),
            'day_remaining': MAX_REQUESTS_PER_DAY - request_tracker['day_requests'],
            'cache_hits': request_tracker['cache_hits']}

async def send_telegram_message(session, message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        async with session.post(url, json={'chat_id': CHAT_ID, 'text': message, 'parse_mode': 'HTML'}, timeout=5) as resp:
            if resp.status != 200: logger.error(f"Telegram error: {resp.status}")
    except Exception as e: logger.error(f"Telegram exception: {e}")

async def check_telegram_commands(session):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
    params = {'offset': trade_state['last_update_id'] + 1, 'timeout': 5}
    try:
        async with session.get(url, params=params, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                for result in data.get('result', []):
                    trade_state['last_update_id'] = result['update_id']
                    if 'message' in result and 'text' in result['message']:
                        text = result['message']['text'].lower().strip()
                        if text.startswith('/loss'):
                            try:
                                amount = float(text.split(' ')[1])
                                trade_state['today_loss'] += amount
                                await send_telegram_message(session, f"📉 Loss: ${amount:.2f}. Anio: ${trade_state['today_loss']:.2f}")
                            except: pass
                        elif text == '/status':
                            await send_telegram_message(session, f"💰 Balance: ${ACCOUNT_BALANCE:.2f}\n✅ Profit: ${trade_state['today_profit']:.2f}\n⛔ Loss: ${trade_state['today_loss']:.2f}")
    except Exception as e: logger.error(f"Telegram command error: {e}")

def check_daily_limits():
    now = datetime.now(timezone.utc).date()
    if now != trade_state['last_reset']:
        trade_state['today_loss'] = 0.0
        trade_state['today_profit'] = 0.0
        trade_state['last_reset'] = now
    return trade_state['today_loss'] < DAILY_LOSS_LIMIT and trade_state['today_profit'] < DAILY_PROFIT_TARGET

def is_blackout_time():
    now = datetime.now(timezone.utc).time()
    for sh, sm, eh, em in NEWS_BLACKOUT_TIMES:
        if dt_time(sh, sm) <= now <= dt_time(eh, em): return True
    return False

def is_good_session():
    if not SESSION_ENABLED: return True
    now = datetime.now(timezone.utc).time()
    return (LONDON_KILLZONE_START <= now <= LONDON_KILLZONE_END) or (NY_KILLZONE_START <= now <= NY_KILLZONE_END)

# ==========================================
# 3. INDICATEUR & SCORING SMC KILASY 3
# ==========================================
def calculate_atr(df, period=14):
    try:
        h, l, c = df['high'].astype(float), df['low'].astype(float), df['close'].astype(float)
        tr = pd.concat([h-l, abs(h-c.shift()), abs(l-c.shift())], axis=1).max(axis=1)
        return tr.rolling(period).mean().iloc[-1]
    except: return 0.001

def get_swing_points(df):
    if df is None or len(df) < 10: return [], []
    h, l = df['high'].astype(float).values, df['low'].astype(float).values
    pivots_h, pivots_l = [], []
    for i in range(3, len(h)-3):
        if h[i] == max(h[i-3:i+4]): pivots_h.append((i, h[i]))
        if l[i] == min(l[i-3:i+4]): pivots_l.append((i, l[i]))
    return pivots_h, pivots_l

def detect_swing_direction(df, min_pivots=2):
    pivots_h, pivots_l = get_swing_points(df)
    if len(pivots_h) < min_pivots or len(pivots_l) < min_pivots: return 'Neutral'
    lh, ll = pivots_h[-1][1], pivots_l[-1][1]
    ph, pl = pivots_h[-2][1], pivots_l[-2][1]
    if lh > ph and ll > pl: return 'Bullish'
    elif lh < ph and ll < pl: return 'Bearish'
    return 'Neutral'

def detect_liquidity_sweep(df, swing_high, swing_low, tolerance=0.002):
    if df is None or len(df) < 5: return None
    close = df['close'].astype(float)
    high = df['high'].astype(float)
    low = df['low'].astype(float)
    last_close = close.iloc[-1]
    last_high = high.iloc[-1]
    last_low = low.iloc[-1]
    if swing_high and last_high > swing_high * (1 + tolerance) and last_close < swing_high: return 'Bearish Sweep'
    if swing_low and last_low < swing_low * (1 - tolerance) and last_close > swing_low: return 'Bullish Sweep'
    return None

def detect_cho_ch(df_m5, bias):
    if df_m5 is None or len(df_m5) < 10: return None
    pivots_h, pivots_l = get_swing_points(df_m5.tail(20))
    if len(pivots_h) < 2 or len(pivots_l) < 2: return None
    lh, ll = pivots_h[-1][1], pivots_l[-1][1]
    ph, pl = pivots_h[-2][1], pivots_l[-2][1]
    close = df_m5['close'].astype(float).iloc[-1]
    if bias == 'Bullish' and close < ll and close < pl: return 'CHOCH Bearish'
    if bias == 'Bearish' and close > lh and close > ph: return 'CHOCH Bullish'
    return None

def detect_bos(df_m5, choch_signal):
    if df_m5 is None or len(df_m5) < 10 or not choch_signal: return None
    pivots_h, pivots_l = get_swing_points(df_m5.tail(20))
    if len(pivots_h) < 2 or len(pivots_l) < 2: return None
    lh, ll = pivots_h[-1][1], pivots_l[-1][1]
    close = df_m5['close'].astype(float).iloc[-1]
    if 'CHOCH Bullish' in choch_signal and close > lh: return 'BOS Bullish'
    if 'CHOCH Bearish' in choch_signal and close < ll: return 'BOS Bearish'
    return None

def detect_order_block(df_h1, bias, price, atr):
    o, h, l, c = df_h1['open'], df_h1['high'], df_h1['low'], df_h1['close']
    for i in range(len(df_h1)-3, 2, -1):
        if bias == 'Bullish':
            if c.iloc[i] < o.iloc[i] and c.iloc[i+1] > o.iloc[i+1]: # Red then Green impulse
                ob_low = l.iloc[i]
                if ob_low - atr*0.1 <= price <= h.iloc[i] + atr*0.1:
                    return True, ob_low
        if bias == 'Bearish':
            if c.iloc[i] > o.iloc[i] and c.iloc[i+1] < o.iloc[i+1]: # Green then Red impulse
                ob_high = h.iloc[i]
                if l.iloc[i] - atr*0.1 <= price <= ob_high + atr*0.1:
                    return True, ob_high
    return False, 0.0

def detect_fvg(df_h1, bias, price, atr):
    o, h, l, c = df_h1['open'], df_h1['high'], df_h1['low'], df_h1['close']
    for i in range(len(df_h1)-2, 2, -1):
        if bias == 'Bullish':
            if l.iloc[i] > h.iloc[i-1]:
                if h.iloc[i-1] - atr*0.1 <= price <= l.iloc[i] + atr*0.1:
                    return True, h.iloc[i-1] # SL Bottom FVG
        if bias == 'Bearish':
            if h.iloc[i] < l.iloc[i-1]:
                if h.iloc[i] - atr*0.1 <= price <= l.iloc[i-1] + atr*0.1:
                    return True, l.iloc[i-1] # SL Top FVG
    return False, 0.0

def detect_premium_discount(df_1h, price):
    if df_1h is None or len(df_1h) < 24: return 'Neutral', 0
    range_high = df_1h['high'].tail(24).max()
    range_low = df_1h['low'].tail(24).min()
    eq = (range_high + range_low) / 2
    if price <= eq: return 'Discount', 5
    elif price >= eq: return 'Premium', 5
    return 'Neutral', 0

def detect_equal_highs_lows(df_m5, atr):
    h, l = df_m5['high'].astype(float).tail(20), df_m5['low'].astype(float).tail(20)
    for i in range(1, len(h)):
        if abs(h.iloc[i] - h.iloc[i-1]) < atr * 0.1: return True, 10
        if abs(l.iloc[i] - l.iloc[i-1]) < atr * 0.1: return True, 10
    return False, 0

def detect_volume_spike_proxy(df_m5):
    if len(df_m5) < 20: return False, 0
    ranges = df_m5['high'].astype(float) - df_m5['low'].astype(float)
    avg_range = ranges.rolling(20).mean().iloc[-2] # Exclude current to avoid forward bias
    current_range = ranges.iloc[-1]
    if current_range > avg_range * 1.8:
        return True, 10
    return False, 0

async def fetch_finnhub(session, symbol, interval, ttl_multiplier=None):
    finnhub_symbol = f"OANDA:{symbol.replace('/', '_')}"
    resolution = {'1h':'60', '15min':'15', '5min':'5'}.get(interval, '15')
    cache_key = f"{symbol}_{resolution}"
    now = pytime.time()
    base_ttl = CACHE_TTL.get(resolution, 300)
    multiplier = ttl_multiplier or STALE_CACHE_EXTENSION
    if cache_key in DATA_CACHE and (now - DATA_CACHE[cache_key]['time']) < (base_ttl * multiplier):
        request_tracker['cache_hits'] += 1
        return DATA_CACHE[cache_key]['df'], "cache"
    if not can_make_request(): return None, "no_quota"
    to_timestamp = int(pytime.time())
    from_timestamp = to_timestamp - (10 * 86400 if resolution == '60' else 3 * 86400)
    url = "https://finnhub.io/api/v1/forex/candle"
    params = {'symbol': finnhub_symbol, 'resolution': resolution, 'from': from_timestamp, 'to': to_timestamp, 'token': FINNHUB_API_KEY}
    async with session.get(url, params=params, timeout=10) as resp:
        record_request()
        if resp.status == 200:
            data = await resp.json()
            if data.get('s') == 'ok' and len(data.get('c', [])) > 0:
                df = pd.DataFrame({'open': data['o'], 'high': data['h'], 'low': data['l'], 'close': data['c'], 'datetime': pd.to_datetime(data['t'], unit='s')}).set_index('datetime').sort_index()
                if len(df) >= 20:
                    DATA_CACHE[cache_key] = {'df': df, 'time': now}
                    return df, "fresh"
    return None, "error"

# ==========================================
# 4. SCAN CYCLE SMC VERSION 4 (No early return 600)
# ==========================================
async def smart_scan_cycle(session):
    # 🛑 FANITSANA LEHIBE: Tsy misy intsony ny "if not is_good_session" !
    if not check_daily_limits(): return []
    if is_blackout_time(): return []
    
    quota = get_quota_status()
    ttl_mult = LOW_QUOTA_CACHE_EXTENSION if quota['day_remaining'] < 30 else None
    
    df_h1, _ = await fetch_finnhub(session, 'XAU/USD', '1h', ttl_multiplier=ttl_mult)
    if df_h1 is None or len(df_h1) < 70: return []
    
    df_4h = df_h1.resample('4h').agg({'open':'first','high':'max','low':'min','close':'last'}).dropna()
    if len(df_4h) < 10: return []
    
    h4_dir = detect_swing_direction(df_4h)
    h1_dir = detect_swing_direction(df_h1.tail(30))
    if h1_dir == 'Neutral': return []
    
    df_m5, _ = await fetch_finnhub(session, 'XAU/USD', '5min', ttl_multiplier=ttl_mult)
    if df_m5 is None or len(df_m5) < 20: return []
    
    atr = calculate_atr(df_h1)
    current_price = df_m5['close'].astype(float).iloc[-1]
    pivots_h_h1, pivots_l_h1 = get_swing_points(df_h1.tail(60))
    last_swing_high = pivots_h_h1[-1][1] if pivots_h_h1 else 0
    last_swing_low = pivots_l_h1[-1][1] if pivots_l_h1 else 0
    
    score = 0
    confluences = []
    signal_type = "BUY" if h1_dir == 'Bullish' else "SELL"
    
    if h4_dir == h1_dir:
        score += 20
        confluences.append(f"H4:{h4_dir} (+20)")
    score += 10
    confluences.append(f"H1:{h1_dir} (+10)")
    
    pd_zone, pd_score = detect_premium_discount(df_h1, current_price)
    if pd_score > 0 and ((signal_type == "BUY" and pd_zone == 'Discount') or (signal_type == "SELL" and pd_zone == 'Premium')):
        score += pd_score
        confluences.append(f"Zone:{pd_zone} (+{pd_score})")
    
    has_ob, ob_sl = detect_order_block(df_h1, h1_dir, current_price, atr)
    if has_ob:
        score += 15
        confluences.append(f"OB (+15)")
    
    has_fvg, fvg_sl = detect_fvg(df_h1, h1_dir, current_price, atr)
    if has_fvg:
        score += 15
        confluences.append(f"FVG (+15)")
    
    sweep = detect_liquidity_sweep(df_m5, last_swing_high, last_swing_low)
    if not sweep: return []
    score += 15
    confluences.append(f"Sweep:{sweep} (+15)")
    
    choch = detect_cho_ch(df_m5, h1_dir)
    if not choch: return []
    score += 10
    confluences.append(f"{choch} (+10)")
    
    bos = detect_bos(df_m5, choch)
    if not bos: return []
    score += 10
    confluences.append(f"{bos} (+10)")
    
    has_eq, eq_score = detect_equal_highs_lows(df_m5, atr)
    if has_eq:
        score += eq_score
        confluences.append(f"EQH/EQL (+{eq_score})")
    
    has_vol, vol_score = detect_volume_spike_proxy(df_m5)
    if has_vol:
        score += vol_score
        confluences.append(f"Vol Spike (+{vol_score})")
    
    if score < MIN_SIGNAL_SCORE:
        logger.info(f"⏳ Signal ignored (Score: {score}/100 < 80)")
        return []
    
    sl = 0.0
    if has_ob: sl = ob_sl
    elif has_fvg: sl = fvg_sl
    elif signal_type == "BUY": sl = last_swing_low - (atr * 0.25) if last_swing_low > 0 else current_price - (atr * 1.5)
    else: sl = last_swing_high + (atr * 0.25) if last_swing_high > 0 else current_price + (atr * 1.5)
    
    risk_dist = abs(current_price - sl)
    tp1 = current_price + risk_dist * 2 if signal_type == "BUY" else current_price - risk_dist * 2
    tp2 = current_price + risk_dist * 4 if signal_type == "BUY" else current_price - risk_dist * 4
    tp3 = (pivots_h_h1[-2][1] if len(pivots_h_h1) >= 2 else current_price + risk_dist * 6) if signal_type == "BUY" else (pivots_l_h1[-2][1] if len(pivots_l_h1) >= 2 else current_price - risk_dist * 6)
    
    risk_dollars = (risk_dist / current_price) * ACCOUNT_BALANCE
    
    signal = {
        'type': signal_type,
        'entry': round(current_price, 5),
        'stop_loss': round(sl, 5),
        'tp1': round(tp1, 5),
        'tp2': round(tp2, 5),
        'tp3': round(tp3, 5),
        'rr_tp1': round(abs(tp1 - current_price) / risk_dist, 2),
        'rr_tp2': round(abs(tp2 - current_price) / risk_dist, 2),
        'score': score,
        'confluences': confluences,
        'atr': round(atr, 5),
        'risk_percent': round(risk_dollars / ACCOUNT_BALANCE * 100, 2),
        'risk_dollars': round(risk_dollars, 2)
    }
    
    logger.info(f"🚀 SMC V4 SIGNAL | {signal['type']} | Score: {signal['score']}/100 | RR: {signal['rr_tp1']}")
    return [signal]

# ==========================================
# 5. MAIN LOOP (VERSION 4 - DYNAMIC INTERVAL)
# ==========================================
async def scan_market(session):
    await check_telegram_commands(session)
    signals = await smart_scan_cycle(session)
    
    if signals:
        for sig in signals:
            utc_time = datetime.now(timezone.utc).strftime("%H:%M")
            clean_conf = " | ".join([c.split(" (+")[0] for c in sig['confluences']])
            
            msg = (f"🎯 <b>SMART MONEY SNIPER</b> (Score: {sig['score']}/100)\n"
                   f"🕒 Time: {utc_time} UTC | 📉 {sig['type']} Setup\n"
                   f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                   f"💰 <b>Balance:</b> ${ACCOUNT_BALANCE:.2f} | <b>Risk:</b> {sig['risk_percent']}% (${sig['risk_dollars']:.2f})\n"
                   f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                   f"📈 <b>Entry:</b> {sig['entry']:.5f}\n"
                   f"🛑 <b>Stop Loss:</b> {sig['stop_loss']:.5f}\n"
                   f"✅ <b>TP1</b> (1:{sig['rr_tp1']}): {sig['tp1']:.5f}\n"
                   f"✅ <b>TP2</b> (1:{sig['rr_tp2']}): {sig['tp2']:.5f}\n"
                   f"🎯 <b>TP3</b> (Liquidity): {sig['tp3']:.5f}\n"
                   f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                   f"💡 <b>Confluences:</b> {clean_conf}\n"
                   f"📊 <b>ATR:</b> {sig['atr']:.5f}")
            
            await send_telegram_message(session, msg)
            
    trade_state['scan_count'] += 1
    
    # 🔄 VAOVAO LEHIBE : Dynamic interval
    if is_good_session():
        sleep_time = 180     # 3 minitra rehefa ao Killzone (London/NY)
    else:
        sleep_time = 900     # 15 minitra rehefa ivelany
    return sleep_time

async def main():
    logger.info("🔥 SMC BOT VERSION 4 - 20$ to 52.000$ Challenge (24h / Dynamic)")
    connector = aiohttp.TCPConnector(family=socket.AF_INET, resolver=AsyncResolver(nameservers=['8.8.8.8', '8.8.4.4']))
    async with aiohttp.ClientSession(connector=connector) as session:
        await send_telegram_message(session, "🤖 SMC V4 (Score 80+) XAUUSD 24h ! Killzone = 3min, Outside = 15min")
        while True:
            try:
                sleep_time = await scan_market(session)
                await asyncio.sleep(sleep_time)
            except KeyboardInterrupt:
                logger.info("Arrêt manuel.")
                break
            except Exception as e:
                logger.error(f"Erreur boucle: {e}")
                await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
