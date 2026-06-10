"""
=============================================================
T = THREAD (proses paralel). Ada 3 thread:
  T1 = Screener + Open Long (tiap 3 menit)
  T2 = Add Funds            (tiap 15 detik)
  T3 = Close Long           (tiap 15 detik)

UROTAN EKSEKUSI T1 (tiap 3 menit):
─────────────────────────────────────────────────────────
  LAPIS 1   Anti Market Dump
            BTC candle 4H yang sedang berjalan ≤ −3% dari harga open candle itu
            → T1 dibatalkan total (reset tiap candle baru)

  FASE 1    Kumpulkan Kandidat Pool (sepanjang candle)
            Scan 400+ pair, cek 8 syarat:
            1. Chg 24H antara −10% s.d. 0%
            2. Volume 24H ≥ $5.000.000
            3. Supertrend downtrend (SUPERTd=−1)
            4. Elapsed ≥ 10%
            5. RSI naik dari candle sebelumnya
            6. Stoch K > D
            7. Volume buy naik dari candle sebelumnya
            8. Harga dalam zona 0% s.d. −3% di bawah EMA50 4H
            + Skip pair yang sudah ada di active_deals

  FASE 2    Seleksi AI + Open Long (3 window per candle)
  ├─ WINDOW 1  menit 50–59   (elapsed ~20–25%)
  ├─ WINDOW 2  menit 110–119 (elapsed ~46–50%)
  └─ WINDOW 3  menit 170–209 (elapsed ~70–87%)

            Setiap window cek urutan gerbang:
            G1. Pool tidak kosong
            G2. Window belum dieksekusi
            G3. AI pilih 1 pemenang (fallback: RSI terendah)
            G4. Slot deal tersedia (max 1)
            G5. LAPIS 2 — BTC filter:
                BTC ≥ EMA20×0.98 DAN c-1 bullish DAN c-2 bullish
                → gagal = bypass, pool dipertahankan
            G6. Elapsed ≤ 75%    → gagal = bypass
            G7. DistST ≥ −7.5%  → gagal = bypass
            G8. EstCandle ≤ 8   → gagal = bypass
            → Semua lolos = OPEN LONG dieksekusi via 3Commas
─────────────────────────────────────────────────────────
=============================================================
  INSTALL:
    py -3.12 -m pip install pandas pandas-ta requests
  JALANKAN:
    py -3.12 D:\tradingview\binance_screener.py
=============================================================
"""

import requests
import pandas as pd
import pandas_ta as ta
import time
import sys
import json
import threading
import os
from datetime import datetime, timedelta

# ============================================================
#  ⚙️  KONFIGURASI
# ============================================================

TELEGRAM_TOKEN    = "8813981840:AAFv-eDq72btyc5MHiW3aZebJi3bLtbeymM"
TELEGRAM_CHAT_ID  = "192390919"
ANTHROPIC_API_KEY = "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"

COMMAS_BOT_ID      = 16380123
COMMAS_EMAIL_TOKEN = "f97400b9-e9a4-4058-913e-35eb8372f920"
COMMAS_DELAY_SEC   = 0

# Thread 1 — Screener + Open Long (intrabar)

# ── SETTINGAN KONDISI v1 — ARSIP (tidak aktif) ──────────────
# POOL — Kondisi masuk kandidat (check_screener):
#   1. Chg 24H < -5%
#   2. Price < BB Lower 4H
#   3. Stoch K > D
#   4. Stoch D < 23
#   5. Stoch D sekarang > Stoch D sebelumnya
#   6. RSI 25 < RSI < 32 (oversold tapi bukan free fall)
#   7. RSI sekarang > RSI sebelumnya
#   8. Bullish running candle (close > open) + elapsed >= 10%
#   9. Volume buy (tbbav) candle ini > candle sebelumnya
#   10. BB Width sekarang < rata-rata BB Width 5 candle terakhir (menyempit)
#   11. Volume sell sekarang < rata-rata volume sell 3 candle sebelumnya (tekanan jual melemah)
# ─────────────────────────────────────────────────────────────

# ── SETTINGAN KONDISI v2 — AKTIF (30 Mei 2026) ──────────────
# POOL — Supertrend Near-Switch Uptrend:
#   1. Chg 24H -10% hingga 0%
#   2. Volume 24H >= $5M
#   3. Supertrend masih downtrend (SUPERTd = -1)
#   4. Elapsed >= 10%
#   5. RSI sekarang > RSI sebelumnya
#   6. Stoch K > D
#   7. Volume buy (tbbav) candle ini > candle sebelumnya
#   (jarak ke ST line tidak dibatasi — AI yang nilai)
#
# NOTIF POOL:
#   Fase 1: menit 15, 30, 45
#   Fase 2: menit 75, 90, 105
#   Fase 3: menit 135, 150, 165
#
# FASE 2 — Open Long:
#   Window 1: menit 50-59 (elapsed 20.83%-24.58%)
#   Window 2: menit 110-119 (elapsed 45.83%-49.58%)
#   Window 3: menit 170-209 (elapsed 70.83%-87.08%) — diperlebar
#   AI pilih 1 terbaik dari pool (kriteria: dist_to_st, est_candles, momentum)
#
# ADD FUNDS (T2) — kondisi v4 tetap
# CLOSE LONG (T3) — kondisi v3 tetap
# ─────────────────────────────────────────────────────────────
CHG_24H_MIN              = -10.0  # v2: Chg 24H >= -10% (tidak terlalu crash)
CHG_24H_MAX              =  0.0   # v2: Chg 24H <= 0% (masih turun/flat)
MIN_VOLUME_USD_V2        = 5_000_000  # v2: min volume 24H $5M
ST_LENGTH                = 10     # Supertrend ATR length
ST_MULTIPLIER            = 3.0    # Supertrend multiplier
SCAN_INTERVAL           = 3       # menit
TF_ELAPSED_OPEN         = 30.0   # % elapsed untuk open long
TF_POOL_MIN             = 0.0    # % elapsed minimum untuk masuk pool Fase 1 (0% = sejak awal candle)
TF_PHASE2_MIN           = 20.0   # % elapsed minimum untuk Fase 2 — seleksi AI
TF_PHASE2_MAX           = 20.8   # % elapsed maksimum untuk Fase 2
OPEN_LONG_ELAPSED_MAX   = 75.0   # % elapsed maksimum boleh OPEN LONG (cegah entry di ujung candle)
COMMAS_MAX_ACTIVE_DEALS = 1      # samakan dengan "Max active trades" di 3Commas

# Filter Anti Market Dump
BTC_CHG_4H_MAX  = -3.0
MIN_VOLUME_USD  = 500_000
MAX_CANDIDATES  = 5

# Exclude pair stablecoin/fiat vs USDT (base asset = mata uang juga).
# Emas (PAXG, XAUT) SENGAJA TIDAK di-exclude — tetap boleh masuk pool.
EXCLUDED_BASE_ASSETS = {
    # Stablecoin USD
    'USDC', 'USDE', 'FDUSD', 'TUSD', 'DAI', 'USDP', 'BUSD', 'UST', 'USTC', 'USD1', 'U',
    'USDD', 'PYUSD', 'FRAX', 'GUSD', 'LUSD', 'USDJ', 'USDN', 'USD0', 'USDY',
    'USDS', 'SUSD', 'CRVUSD', 'GHO', 'USDX', 'USDL', 'RLUSD', 'XUSD',
    # Stablecoin Euro
    'EUR', 'EURI', 'EURS', 'AEUR', 'EURT', 'CEUR', 'EURC', 'EURQ',
    # Stablecoin fiat lain
    'GBP', 'GBPT', 'CHF', 'TRY', 'TRYB', 'BRL', 'BRZ', 'ARS', 'ZAR',
    'IDRT', 'JPY', 'JPYC', 'AUD', 'MXN', 'NGN', 'COP', 'UAH',
}

# Thread 2 — Add Funds (ex Thread 3)
ADDFUNDS_INTERVAL   = 15
TF_ELAPSED_ADDFUNDS = 13.0
ADD_FUNDS_VOLUME    = 56    # ← ubah nilai USDT add funds di sini
BASE_ORDER_VOLUME   = 6     # ← Base Order 3Commas dalam USDT (BO)

# Thread 3 — Close Long (ex Thread 4)
CLOSE_INTERVAL    = 15
TF_ELAPSED_CLOSE  = 30.0
RVOL_THRESHOLD    = 1.2
RVOL_PERIOD       = 20
PROFIT_THRESHOLD  = 12.0
PROFIT_MIN_CLOSE  = 2.0   # Minimum profit untuk Kondisi A & C (cover fee + slippage)

CANDLE_INTERVAL = '4h'
BASE_URL        = "https://data-api.binance.vision"

# Filter Fase 2: bypass kalau DistST terlalu jauh atau EstCandle terlalu banyak
DIST_ST_MAX_PCT  = -7.5   # DistST lebih negatif dari ini = bypass (terlalu jauh dari ST)
EST_CANDLE_MAX   = 15     # EstCandle lebih dari ini = bypass (butuh terlalu lama)

# File persistensi
ACTIVE_DEALS_FILE = r"D:\tradingview\active_deals.json"
POOL_LOG_FILE     = r"D:\tradingview\pool_log.json"
# ============================================================

# Shared state
active_deals_lock = threading.Lock()
active_deals      = {}   # { symbol: { 'add_funds_sent': bool, ... } }

confirmed_alerted = {}
already_alerted   = {}
scan_running      = False

# Pool kandidat Fase 1
pool_lock         = threading.Lock()
pool_candidates   = {}
pool_candle_ts    = 0
pool_executed     = False
pool_executed_windows = set()  # Set window index yang sudah dieksekusi {0, 1}
pool_notif_sent   = set()  # Set interval yang sudah dinotif
POOL_NOTIF_MAX      = 9         # 9 notif per candle (3 Fase1 + 3 Fase2 + 3 Fase3)
# Jadwal notif (menit): 15,30,45 (F1) + 75,90,105 (F2) + 135,150,165 (F3)
POOL_NOTIF_MINUTES  = [15, 30, 45, 75, 90, 105, 135, 150, 165]
# Fase 2 terjadi 3x per candle:
#   Window 1: menit 50-59 (elapsed 20.83%-24.58%)
#   Window 2: menit 110-119 (elapsed 45.83%-49.58%)
#   Window 3: menit 170-209 (elapsed 70.83%-87.08%) — diperlebar
TF_PHASE2_WINDOWS   = [(50/240*100, 59/240*100), (110/240*100, 119/240*100), (170/240*100, 209/240*100)]

session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0'})


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


# ── active_deals.json ────────────────────────────────────────

def load_active_deals():
    """Baca active_deals.json saat startup."""
    global active_deals
    if not os.path.exists(ACTIVE_DEALS_FILE):
        log("   📂 active_deals.json tidak ditemukan, mulai kosong.")
        return
    try:
        with open(ACTIVE_DEALS_FILE, 'r') as f:
            data = json.load(f)
        with active_deals_lock:
            active_deals = data
        log(f"   📂 Loaded active_deals: {list(active_deals.keys())}")
    except Exception as e:
        log(f"⚠️  Gagal baca active_deals.json: {e}")


def save_active_deals():
    """Tulis active_deals ke JSON."""
    import numpy as np
    def convert(obj):
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        raise TypeError(f"Not serializable: {type(obj)}")
    try:
        with active_deals_lock:
            data = dict(active_deals)
        with open(ACTIVE_DEALS_FILE, 'w') as f:
            json.dump(data, f, indent=2, default=convert)
    except Exception as e:
        log(f"⚠️  Gagal simpan active_deals.json: {e}")


def add_to_active_deals(symbol: str, data: dict):
    with active_deals_lock:
        active_deals[symbol] = {
            **data,
            'add_funds_count'     : 0,     # berapa kali add funds sudah dikirim
            'add_funds_skip_until': 0,     # timestamp, skip add funds sampai waktu ini
            'opened_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    save_active_deals()
    log(f"   💾 {symbol} ditambah ke active_deals.json")


def load_pool_from_log():
    """
    Saat startup, cek pool_log.json — kalau candle_ts masih sama dengan
    candle yang sedang berjalan, load kandidat lama ke pool_candidates.
    Mencegah kehilangan kandidat saat restart di tengah candle.
    """
    global pool_candidates, pool_candle_ts, pool_notif_sent, pool_executed_windows

    if not os.path.exists(POOL_LOG_FILE):
        return

    try:
        with open(POOL_LOG_FILE, 'r') as f:
            data = json.load(f)

        saved_candle_ts = data.get('candle_ts', 0)
        if not saved_candle_ts:
            return

        # Cek apakah candle_ts di file sama dengan candle 4H yang sedang berjalan
        import datetime as dt
        now_utc = dt.datetime.now(dt.timezone.utc)
        seconds_in_day = now_utc.hour * 3600 + now_utc.minute * 60 + now_utc.second
        seconds_into_candle = seconds_in_day % (4 * 3600)
        current_candle_ts = int((now_utc.timestamp() - seconds_into_candle) * 1000)

        if saved_candle_ts != current_candle_ts:
            log(f"   📂 Pool log: candle berbeda, tidak load kandidat lama")
            return

        saved_candidates = data.get('candidates', {})
        if not saved_candidates:
            return

        with pool_lock:
            pool_candidates = saved_candidates
            pool_candle_ts  = saved_candle_ts

        # Load notif yang sudah terkirim
        saved_notifs = data.get('notif_sent', [])
        pool_notif_sent = set(saved_notifs)

        # Load window yang sudah dieksekusi
        saved_windows = data.get('executed_windows', [])
        pool_executed_windows = set(saved_windows)

        log(f"   📂 Pool log: loaded {len(saved_candidates)} kandidat dari candle yang sama ✅")

    except Exception as e:
        log(f"   ⚠️ Gagal load pool_log.json: {e}")


def save_pool_log(candidates: dict = None, winner: dict = None):
    """
    Tulis pool_log.json.
    candidates : dict pool_candidates saat ini (opsional)
    winner     : dict {'symbol', 'picked_at', 'reason'} (opsional)
    """
    import numpy as np
    def convert(obj):
        if isinstance(obj, (np.integer,)): return int(obj)
        if isinstance(obj, (np.floating,)): return float(obj)
        raise TypeError(f"Not serializable: {type(obj)}")
    try:
        existing = {}
        if os.path.exists(POOL_LOG_FILE):
            try:
                with open(POOL_LOG_FILE, 'r') as f:
                    existing = json.load(f)
            except Exception:
                existing = {}

        existing['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Simpan candle_ts supaya bisa divalidasi saat restart
        with pool_lock:
            existing['candle_ts']        = pool_candle_ts
            existing['notif_sent']       = list(pool_notif_sent)
            existing['executed_windows'] = list(pool_executed_windows)

        if candidates is not None:
            existing['candidates'] = {
                sym: {
                    'rsi'        : round(float(v.get('rsi', 0)), 2),
                    'stoch_k'    : round(float(v.get('stoch_k', 0)), 2),
                    'stoch_d'    : round(float(v.get('stoch_d', 0)), 2),
                    'chg_24h'    : round(float(v.get('chg_24h', 0)), 2),
                    'elapsed'    : round(float(v.get('elapsed', 0)), 1),
                    'bb_lower'   : round(float(v.get('bb_lower', 0)), 8),
                    'price'      : round(float(v.get('price', 0)), 8),
                    'vol_usd'    : round(float(v.get('vol_usd', 0)), 2),
                    'dist_to_st' : round(float(v.get('dist_to_st', 0)), 2) if v.get('dist_to_st') is not None else None,
                    'est_candles': v.get('est_candles'),
                    'first_seen' : datetime.fromtimestamp(
                        v.get('first_seen', time.time())
                    ).strftime('%Y-%m-%d %H:%M:%S'),
                }
                for sym, v in candidates.items()
            }

        if winner is not None:
            existing['last_winner'] = winner

        with open(POOL_LOG_FILE, 'w') as f:
            json.dump(existing, f, indent=2, default=convert)

    except Exception as e:
        log(f"⚠️  Gagal simpan pool_log.json: {e}")


def remove_from_active_deals(symbol: str):
    with active_deals_lock:
        active_deals.pop(symbol, None)
    save_active_deals()
    log(f"   💾 {symbol} dihapus dari active_deals.json")


def mark_add_funds_sent(symbol: str, price: float = 0):
    with active_deals_lock:
        if symbol in active_deals:
            active_deals[symbol]['add_funds_count'] = active_deals[symbol].get('add_funds_count', 0) + 1
            history = active_deals[symbol].get('add_funds_history', [])
            history.append({
                'time'  : datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'amount': ADD_FUNDS_VOLUME,
                'price' : price,
            })
            active_deals[symbol]['add_funds_history'] = history
    save_active_deals()


def calc_avg_price(symbol: str) -> float:
    """
    Hitung average price berdasarkan base order + add funds history.
    Average price = total USDT spent / total coin received.
    Kalau tidak ada add funds → return entry price (base price).
    """
    with active_deals_lock:
        data = active_deals.get(symbol, {})

    entry_price = data.get('price', 0)
    if entry_price <= 0:
        return entry_price

    history = data.get('add_funds_history', [])
    if not history:
        return entry_price

    # Base order
    total_usdt  = float(BASE_ORDER_VOLUME)
    total_coins = total_usdt / entry_price

    # Add funds
    for h in history:
        af_price  = float(h.get('price', 0))
        af_amount = float(h.get('amount', ADD_FUNDS_VOLUME))
        if af_price > 0:
            total_usdt  += af_amount
            total_coins += af_amount / af_price

    if total_coins <= 0:
        return entry_price

    return total_usdt / total_coins

def mark_add_funds_skip(symbol: str, minutes: int = 30):
    """Tandai skip add funds untuk X menit ke depan (kalau AI bilang jangan)."""
    with active_deals_lock:
        if symbol in active_deals:
            active_deals[symbol]['add_funds_skip_until'] = time.time() + (minutes * 60)
    save_active_deals()


# ── Telegram ─────────────────────────────────────────────────

def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    silent = "kosong" in message.lower()
    try:
        resp = session.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "disable_notification": silent
        }, timeout=10)
        if resp.status_code != 200:
            log(f"⚠️  Telegram error: {resp.text}")
    except Exception as e:
        log(f"⚠️  Gagal kirim Telegram: {e}")


# ── Format ────────────────────────────────────────────────────

def to_commas_pair(symbol: str) -> str:
    return f"USDT_{symbol.replace('USDT', '')}"

def to_display_pair(symbol: str) -> str:
    return f"{symbol.replace('USDT', '')}/USDT"


# ── 3Commas Webhooks ─────────────────────────────────────────

def send_3commas(payload: dict, label: str) -> bool:
    try:
        resp = session.post(
            "https://3commas.io/trade_signal/trading_view",
            json=payload, timeout=10
        )
        pair = payload.get('pair', '')

        # Cek HTTP status
        if resp.status_code != 200:
            log(f"⚠️  [3C] {label} HTTP error [{resp.status_code}]: {resp.text}")
            return False

        # Cek response body — 3Commas kadang return 200 tapi dengan error di body
        try:
            body = resp.json()
            # 3Commas return {"deal": {...}} kalau sukses
            # Return {"error": "..."} atau {"errors": [...]} kalau gagal
            if isinstance(body, dict):
                if 'error' in body or 'errors' in body:
                    err = body.get('error') or body.get('errors')
                    log(f"⚠️  [3C] {label} ditolak 3Commas: {err} | pair: {pair}")
                    return False
                if 'deal' in body or body.get('success') is True:
                    log(f"✅ [3C] {label} terkirim & dikonfirmasi: {pair}")
                    return True
                # Response tidak dikenali tapi status 200 — anggap sukses
                log(f"✅ [3C] {label} terkirim (status 200): {pair}")
                return True
            else:
                log(f"✅ [3C] {label} terkirim (status 200): {pair}")
                return True
        except Exception:
            # Tidak bisa parse JSON — anggap sukses kalau status 200
            log(f"✅ [3C] {label} terkirim (status 200): {pair}")
            return True

    except Exception as e:
        log(f"⚠️  [3C] Gagal {label}: {e}")
        return False

def send_open_long(symbol: str) -> bool:
    return send_3commas({
        "message_type": "bot",
        "bot_id": COMMAS_BOT_ID,
        "email_token": COMMAS_EMAIL_TOKEN,
        "delay_seconds": COMMAS_DELAY_SEC,
        "pair": to_commas_pair(symbol)
    }, "open_long")

def send_add_funds(symbol: str) -> bool:
    return send_3commas({
        "action": "add_funds_in_quote",
        "message_type": "bot",
        "bot_id": COMMAS_BOT_ID,
        "email_token": COMMAS_EMAIL_TOKEN,
        "delay_seconds": COMMAS_DELAY_SEC,
        "pair": to_commas_pair(symbol),
        "volume": ADD_FUNDS_VOLUME
    }, "add_funds")

def send_close_long(symbol: str) -> bool:
    return send_3commas({
        "action": "close_at_market_price",
        "message_type": "bot",
        "bot_id": COMMAS_BOT_ID,
        "email_token": COMMAS_EMAIL_TOKEN,
        "delay_seconds": COMMAS_DELAY_SEC,
        "pair": to_commas_pair(symbol)
    }, "close_long")


# ── Claude AI Pick ───────────────────────────────────────────

AI_TOP_N = 3  # Jumlah pair terbaik yang masuk antrian Thread 2

def ai_pick_top3(cands: list) -> dict:
    """
    AI ranking semua pair, return top 3 untuk masuk antrian Thread 2.
    Return: {'top3': [...symbols...], 'best': symbol, 'reason': str, 'ranking': [...]}
    """
    if not cands:
        return None

    # Kalau pair <= AI_TOP_N, semua langsung masuk
    if len(cands) <= AI_TOP_N:
        return {
            'top3'   : [c['symbol'] for c in cands],
            'best'   : cands[0]['symbol'],
            'reason' : f'Hanya {len(cands)} pair lolos, semua masuk antrian.',
            'ranking': [c['symbol'] for c in cands]
        }

    lines = [
        f"- {to_display_pair(c['symbol'])}: RSI={c['rsi']:.1f}, "
        f"Stoch K={c['stoch_k']:.1f} D={c['stoch_d']:.1f}, "
        f"BB%={((c['price']-c['bb_lower'])/c['bb_lower']*100):.2f}%, "
        f"Chg24H={c['chg_24h']:.2f}%"
        for c in cands
    ]

    prompt = (
        "Kamu adalah analis trading crypto. "
        f"Dari daftar berikut, ranking dan pilih TOP {AI_TOP_N} pair terbaik untuk open long DCA. "
        "Semua sudah lolos: Price < BB Lower 4H, Stoch K cross up D, Stoch D < 20, RSI < 35, Chg24H < -5%.\n"
        "Kriteria ranking: RSI paling oversold, Stoch paling kuat, "
        "harga paling jauh di bawah BB Lower, penurunan 24H terbesar.\n\n"
        "Data:\n" + "\n".join(lines) +
        f'\n\nJawab JSON persis (tanpa backtick):\n'
        f'{{"best":"SIMBOLUSDT","reason":"alasan 1-2 kalimat",'
        f'"top3":["SIMBOL1USDT","SIMBOL2USDT","SIMBOL3USDT"],'
        f'"ranking":["SIMBOL1USDT","SIMBOL2USDT","SIMBOL3USDT","dst..."]}}'
    )

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 500,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=30
        )
        resp.raise_for_status()
        text = resp.json()['content'][0]['text'].strip()
        text = text.replace("```json","").replace("```","").strip()
        data = json.loads(text)

        top3    = data.get('top3', [c['symbol'] for c in cands[:AI_TOP_N]])
        best    = data.get('best', top3[0] if top3 else cands[0]['symbol'])
        reason  = data.get('reason', '-')
        ranking = data.get('ranking', top3)

        # Validasi — pastikan symbol ada di cands
        valid_symbols = {c['symbol'] for c in cands}
        top3    = [s for s in top3 if s in valid_symbols][:AI_TOP_N]
        ranking = [s for s in ranking if s in valid_symbols]

        # Fallback kalau top3 kosong
        if not top3:
            top3 = [c['symbol'] for c in sorted(cands, key=lambda x: x['rsi'])[:AI_TOP_N]]

        return {
            'top3'   : top3,
            'best'   : best if best in valid_symbols else top3[0],
            'reason' : reason,
            'ranking': ranking
        }

    except Exception as e:
        log(f"⚠️  AI pick gagal: {e} — fallback RSI terendah")
        sorted_cands = sorted(cands, key=lambda x: x['rsi'])
        top3 = [c['symbol'] for c in sorted_cands[:AI_TOP_N]]
        return {
            'top3'   : top3,
            'best'   : top3[0],
            'reason' : f"Fallback: top {AI_TOP_N} RSI terendah",
            'ranking': [c['symbol'] for c in sorted_cands]
        }


def fetch_performance(symbol: str) -> dict:
    """
    Fetch performance 1W dan 1M untuk satu symbol dari Binance klines daily.
    Return: {'perf_1w': float, 'perf_1m': float} dalam persen, atau None jika gagal.
    """
    try:
        url    = f"{BASE_URL}/api/v3/klines"
        params = {"symbol": symbol, "interval": "1d", "limit": 32}
        resp   = session.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data   = resp.json()
        if len(data) < 8:
            return None
        closes     = [float(k[4]) for k in data]
        price_now  = closes[-1]
        price_7d   = closes[-8]   if len(closes) >= 8  else None
        price_30d  = closes[-31]  if len(closes) >= 31 else None
        perf_1w    = ((price_now - price_7d)  / price_7d  * 100) if price_7d  else None
        perf_1m    = ((price_now - price_30d) / price_30d * 100) if price_30d else None
        return {'perf_1w': perf_1w, 'perf_1m': perf_1m}
    except Exception as e:
        log(f"   [Perf] Gagal fetch {symbol}: {e}")
        return None


def ai_pick_best_from_pool(pool: dict) -> dict:
    """
    Fase 2: AI pilih 1 pair terbaik dari semua kandidat di pool.
    Kriteria: teknikal + performance 1W/1M + market cap tier + lama di pool.
    Return: {'best': symbol, 'reason': str, 'ranking': [...]}
    Selalu ada 1 pemenang (fallback RSI terendah kalau AI gagal).
    """
    if not pool:
        return None

    cands = list(pool.values())

    # Fetch performance 1W/1M untuk semua kandidat sebelum kirim ke AI
    log(f"   [AI-P2] Fetch performance 1W/1M untuk {len(cands)} kandidat...")
    for c in cands:
        perf = fetch_performance(c['symbol'])
        c['perf_1w'] = perf['perf_1w'] if perf and perf['perf_1w'] is not None else None
        c['perf_1m'] = perf['perf_1m'] if perf and perf['perf_1m'] is not None else None

    now_ts = time.time()
    lines = []
    for c in cands:
        bb_dist    = ((c['price'] - c['bb_lower']) / c['bb_lower'] * 100) if c['bb_lower'] > 0 else 0
        lama_menit = int((now_ts - c.get('first_seen', now_ts)) / 60)
        vol_m      = c.get('vol_usd', 0) / 1_000_000
        perf_1w_str = f"{c['perf_1w']:.1f}%" if c.get('perf_1w') is not None else "N/A"
        perf_1m_str = f"{c['perf_1m']:.1f}%" if c.get('perf_1m') is not None else "N/A"
        chg_4h_val  = c.get('chg_4h', 0)
        bb_pb_val   = c.get('bb_pb', 0)
        support_val = c.get('support', None)
        dist_sup    = c.get('dist_to_support', None)
        if support_val is not None and dist_sup is not None:
            support_str = f"{support_val:.6g} (jarak {dist_sup:.2f}%)"
        else:
            support_str = "N/A"
        dist_st_str = f"{c['dist_to_st']:.2f}%" if c.get('dist_to_st') is not None else 'N/A'
        est_str     = str(c.get('est_candles', '?'))
        lines.append(
            f"- {to_display_pair(c['symbol'])}: "
            f"RSI={c['rsi']:.1f}, "
            f"Stoch K={c['stoch_k']:.1f} D={c['stoch_d']:.1f}, "
            f"DistST={dist_st_str}, "
            f"EstCandle={est_str}, "
            f"BB_dist={bb_dist:.2f}%, "
            f"BB_%b={bb_pb_val:.3f}, "
            f"Chg24H={c['chg_24h']:.2f}%, "
            f"Chg4H={chg_4h_val:.2f}%, "
            f"Perf1W={perf_1w_str}, "
            f"Perf1M={perf_1m_str}, "
            f"Support={support_str}, "
            f"Vol=${vol_m:.1f}M, "
            f"Di pool={lama_menit} menit"
        )

    single = len(cands) == 1
    prompt = (
        "Kamu adalah analis trading crypto. "
        "Ini adalah Fase 2 — candle 4H sudah berjalan ~50%. "
        f"{'Hanya ada 1 kandidat di pool. ' if single else 'Dari pool kandidat berikut, '}"
        f"{'Berikan analisis lengkap apakah pair ini layak untuk open long DCA.' if single else 'Pilih SATU pair terbaik untuk open long DCA.'}\n\n"
        "Kandidat sudah lolos: Supertrend masih downtrend, RSI naik, Stoch K > D, "
        "volume buy naik, elapsed ≥ 10%, chg 24H -10% hingga 0%, volume ≥ $5M.\n\n"
        "Kriteria penilaian (holistik):\n"
        "1. DistST mendekati 0% — makin dekat ke switch uptrend\n"
        "2. EstCandle kecil — estimasi sedikit candle untuk switch\n"
        "3. RSI oversold tapi naik — momentum reversal\n"
        "4. Stoch K > D dengan selisih besar — momentum beli kuat\n"
        "5. Chg24H tidak terlalu dalam — hindari free fall\n"
        "6. Perf1W dan 1M — konteks tren jangka menengah\n"
        "7. Support proximity — harga dekat support\n"
        "8. Volume tinggi — likuiditas baik\n"
        "9. Coin tier — prefer major/mid cap\n"
        "10. Lama di pool — konsistensi sinyal\n\n"
        "Data kandidat:\n" + "\n".join(lines) +
        '\n\nJawab JSON persis (tanpa backtick):\n'
        '{"best":"SIMBOLUSDT","reason":"alasan 2-3 kalimat holistik berdasarkan data di atas","ranking":["SIM1USDT"]}'
    )

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 400,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=30
        )
        resp.raise_for_status()
        text = resp.json()['content'][0]['text'].strip()
        text = text.replace("```json","").replace("```","").strip()
        data = json.loads(text)

        valid_symbols = {c['symbol'] for c in cands}
        best    = data.get('best', '')
        reason  = data.get('reason', '-')
        ranking = [s for s in data.get('ranking', []) if s in valid_symbols]

        if best not in valid_symbols:
            raise ValueError(f"AI return symbol tidak valid: {best}")

        if not ranking:
            ranking = [c['symbol'] for c in sorted(cands, key=lambda x: x['rsi'])]

        log(f"   [AI-P2] Pilihan: {best} — {reason}")
        return {'best': best, 'reason': reason, 'ranking': ranking}

    except Exception as e:
        log(f"⚠️  [AI-P2] Gagal: {e} — fallback RSI terendah")
        sorted_cands = sorted(cands, key=lambda x: x['rsi'])
        return {
            'best'   : sorted_cands[0]['symbol'],
            'reason' : f"Fallback otomatis: RSI terendah ({sorted_cands[0]['rsi']:.1f})",
            'ranking': [c['symbol'] for c in sorted_cands]
        }


def ai_check_add_funds(symbol: str, entry_price: float, df) -> tuple:
    """
    AI evaluasi apakah kondisi pair layak untuk add funds sekarang.
    Return: (approve: bool, reason: str)
    """
    if df is None or len(df) < 10:
        return True, "Data tidak cukup — default approve"
    try:
        last     = df.iloc[-1]
        ema9     = ta.ema(df['close'], length=9)
        ema26    = ta.ema(df['close'], length=26)
        avg_vol  = df['volume'].iloc[-21:-1].mean() if len(df) >= 21 else 0
        rvol     = last['volume'] / avg_vol if avg_vol > 0 else 0
        profit   = ((last['close'] - entry_price) / entry_price * 100) if entry_price > 0 else 0

        avg_price = calc_avg_price(symbol)
        profit    = ((last['close'] - avg_price) / avg_price * 100) if avg_price > 0 else 0
        resistance = get_resistance_level(df, strength=2)
        dist_to_res = ((last['close'] - resistance) / resistance * 100) if resistance is not None else None
        res_str = f"{resistance:.8g} (jarak {dist_to_res:.2f}%)" if resistance is not None else "N/A"

        ema9_val  = float(ema9.iloc[-1]) if ema9 is not None and not ema9.empty and not ema9.iloc[-1] != ema9.iloc[-1] else 0
        ema26_val = float(ema26.iloc[-1]) if ema26 is not None and not ema26.empty and not ema26.iloc[-1] != ema26.iloc[-1] else 0

        prompt = (
            f"Kamu adalah AI trading assistant. Evaluasi apakah sebaiknya add funds sekarang untuk pair {symbol}.\n\n"
            f"Data saat ini:\n"
            f"- Entry price    : {entry_price:.8g}\n"
            f"- Average price  : {avg_price:.8g} (setelah add funds sebelumnya)\n"
            f"- Current price  : {float(last['close']):.8g}\n"
            f"- Profit sekarang: {profit:.2f}% (dari average price)\n"
            f"- RSI            : {float(last.get('rsi', 0)) if hasattr(last, 'get') else 0:.1f}\n"
            f"- EMA9           : {ema9_val:.8g}\n"
            f"- EMA26          : {ema26_val:.8g}\n"
            f"- RVOL           : {rvol:.2f}x\n"
            f"- Candle         : {'bullish' if float(last['close']) > float(last['open']) else 'bearish'}\n"
            f"- Resistance     : {res_str}\n\n"
            f"Pertimbangkan: harga di bawah resistance = zona murah untuk DCA. "
            f"Jika profit sudah cukup tinggi atau harga mendekati resistance, pertimbangkan tunda.\n\n"
            f"Jawab HANYA dalam JSON: {{\"approve\": true/false, \"reason\": \"alasan singkat 1 kalimat\"}}"
        )
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 100,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=20
        )
        resp.raise_for_status()
        text = resp.json()['content'][0]['text'].strip()
        text = text.replace("```json","").replace("```","").strip()
        data = json.loads(text)
        return bool(data.get('approve', True)), data.get('reason', '-')
    except Exception as e:
        log(f"   [T2-AI] Gagal evaluasi {symbol}: {e} — default approve")
        return True, "AI gagal — default approve"


def get_atr_pct(df) -> float:
    """Hitung ATR% = ATR / close × 100, pakai 14 candle terakhir."""
    try:
        atr = ta.atr(df['high'], df['low'], df['close'], length=14)
        if atr is None or atr.empty: return 3.0
        atr_val   = float(atr.iloc[-1])
        close_val = float(df['close'].iloc[-1])
        return (atr_val / close_val * 100) if close_val > 0 else 3.0
    except Exception:
        return 3.0


def get_trailing_config(atr_pct: float) -> tuple:
    """
    Return (trailing_deviation_pct, tahan_seconds) berdasarkan ATR%.
    Tipe A: <1%  → dev 0.5%, tahan 300s
    Tipe B: 1-2% → dev 1.0%, tahan 180s
    Tipe C: 2-4% → dev 1.5%, tahan 120s
    Tipe D: 4-7% → dev 2.0%, tahan 60s
    Tipe E: >7%  → dev 2.5%, tahan 30s
    """
    if atr_pct < 1.0:   return (0.5, 300)
    elif atr_pct < 2.0: return (1.0, 180)
    elif atr_pct < 4.0: return (1.5, 120)
    elif atr_pct < 7.0: return (2.0, 60)
    else:               return (2.5, 30)


def send_trailing(symbol: str, trailing_deviation_pct: float) -> bool:
    """
    Aktifkan trailing stop di 3Commas untuk deal aktif.
    Menggunakan endpoint update deal dengan trailing enabled.
    """
    try:
        with active_deals_lock:
            deal_data = active_deals.get(symbol, {})
        deal_id = deal_data.get('deal_id') or deal_data.get('id')

        if not deal_id:
            log(f"⚠️  [T3] Trailing: deal_id tidak ditemukan untuk {symbol}")
            return False

        url = f"https://api.3commas.io/ver1/deals/{deal_id}/update_deal"
        params = {
            "deal_id"                    : deal_id,
            "trailing_enabled"           : True,
            "trailing_deviation"         : trailing_deviation_pct,
        }
        sig = generate_signature(url.replace("https://api.3commas.io",""), params)
        resp = requests.post(
            url,
            headers={
                "APIKEY"   : COMMAS_API_KEY,
                "Signature": sig,
                "Content-Type": "application/json"
            },
            json=params,
            timeout=10
        )
        ok = resp.status_code == 200
        log(f"   [3C] trailing {'aktif' if ok else 'GAGAL'} (status {resp.status_code}): {to_commas_pair(symbol)} dev={trailing_deviation_pct}%")
        return ok
    except Exception as e:
        log(f"⚠️  [T3] Error send_trailing {symbol}: {e}")
        return False


# Cache tahan per pair (untuk AI decision "tahan")
ai_tahan_until = {}


def ai_decide_close_action(symbol: str, entry_price: float, current_data: dict,
                            triggered_by: str, atr_pct: float) -> tuple:
    """
    AI final decision: dari 6 opsi (close/trailing × A/B/C),
    pilih 1 tindakan terbaik.
    Return: (action: str, reason: str)
    action: 'close_A', 'close_B', 'close_C', 'trailing_A', 'trailing_B', 'trailing_C', 'tahan'
    """
    default = (f'close_{triggered_by}', 'Fallback default close')
    try:
        price       = float(current_data.get('close', 0))
        ema9_val    = float(current_data.get('ema9', 0))
        ema26_val   = float(current_data.get('ema26', 0))
        rvol        = float(current_data.get('rvol', 0))
        avg_price   = calc_avg_price(symbol)
        profit_pct  = ((price - avg_price) / avg_price * 100) if avg_price > 0 else 0
        ema_gap_pct = ((ema9_val - ema26_val) / ema26_val * 100) if ema26_val > 0 else 0
        dev, _      = get_trailing_config(atr_pct)

        tipe = 'A' if atr_pct < 1 else ('B' if atr_pct < 2 else ('C' if atr_pct < 4 else ('D' if atr_pct < 7 else 'E')))

        prompt = (
            f"Kamu adalah AI trading assistant untuk pair {to_display_pair(symbol)}.\n"
            f"Kondisi teknikal baru saja terpenuhi (Kondisi {triggered_by}).\n\n"
            f"Data market saat ini:\n"
            f"- Harga entry     : {entry_price:.6g}\n"
            f"- Average price   : {avg_price:.6g}\n"
            f"- Harga sekarang  : {price:.6g}\n"
            f"- Profit          : {profit_pct:.2f}%\n"
            f"- EMA9            : {ema9_val:.6g}\n"
            f"- EMA26           : {ema26_val:.6g}\n"
            f"- Gap EMA9-EMA26  : {ema_gap_pct:.2f}%\n"
            f"- RVOL            : {rvol:.2f}x\n"
            f"- ATR%            : {atr_pct:.2f}% (tipe volatilitas {tipe})\n"
            f"- Trailing dev    : {dev}% (sesuai volatilitas pair)\n\n"
            f"6 opsi yang tersedia:\n"
            f"1. close_A  — Close sekarang via Kondisi A (RVOL)\n"
            f"2. trailing_A — Aktifkan trailing {dev}% via Kondisi A\n"
            f"3. close_B  — Close sekarang via Kondisi B (Profit {PROFIT_THRESHOLD}%)\n"
            f"4. trailing_B — Aktifkan trailing {dev}% via Kondisi B\n"
            f"5. close_C  — Close sekarang via Kondisi C (EMA9 > EMA26)\n"
            f"6. trailing_C — Aktifkan trailing {dev}% via Kondisi C\n"
            f"7. tahan    — Tahan, jangan close atau trailing dulu\n\n"
            f"Kondisi yang baru terpenuhi: {triggered_by}\n"
            f"Pertimbangkan momentum, profit, volatilitas, dan potensi lanjutan.\n"
            f"Trailing cocok kalau momentum masih kuat dan harga berpotensi lanjut naik.\n"
            f"Close cocok kalau momentum melambat atau profit sudah cukup.\n"
            f"Tahan cocok kalau kondisi baru saja terpenuhi tapi momentum masih awal.\n\n"
            f"Jawab JSON persis (tanpa backtick):\n"
            f'{{"action":"close_A|trailing_A|close_B|trailing_B|close_C|trailing_C|tahan",'
            f'"reason":"alasan 1-2 kalimat"}}'
        )

        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={"model": "claude-sonnet-4-20250514", "max_tokens": 200,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=20
        )
        resp.raise_for_status()
        text = resp.json()['content'][0]['text'].strip()
        text = text.replace("```json","").replace("```","").strip()
        data = json.loads(text)

        action = data.get('action', f'close_{triggered_by}')
        reason = data.get('reason', '-')

        valid_actions = ['close_A','trailing_A','close_B','trailing_B','close_C','trailing_C','tahan']
        if action not in valid_actions:
            action = f'close_{triggered_by}'

        log(f"   [AI-Close] {to_display_pair(symbol)}: {action} — {reason}")
        return action, reason

    except Exception as e:
        log(f"⚠️  [AI-Close] Gagal decide {symbol}: {e}")
        return default


def ai_predict_close_priority(symbol: str, entry_price: float, current_data: dict) -> tuple:
    """
    AI prediksi urutan prioritas pengecekan kondisi close untuk pair ini.
    Return: (order: list, reason: str)
    Misal: (['B', 'A', 'C'], 'profit hampir tercapai')
    Semua kondisi tetap aktif — hanya urutan pengecekan yang berubah.
    """
    default_order = ['B', 'A', 'C']

    try:
        price       = current_data.get('close', 0)
        ema9_val    = current_data.get('ema9', 0)
        ema26_val   = current_data.get('ema26', 0)
        rvol        = current_data.get('rvol', 0)
        profit_pct  = ((price - entry_price) / entry_price * 100) if entry_price > 0 else 0
        ema_gap_pct = ((ema9_val - ema26_val) / ema26_val * 100) if ema26_val > 0 else 0

        prompt = (
            f"Kamu adalah analis trading crypto. Untuk pair {to_display_pair(symbol)}, "
            f"prediksi urutan kondisi close long mana yang paling mungkin terpenuhi duluan:\n\n"
            f"Data saat ini:\n"
            f"- Harga entry  : {entry_price:.6g}\n"
            f"- Harga sekarang: {price:.6g}\n"
            f"- Profit saat ini: {profit_pct:.2f}% (target Kondisi B: {PROFIT_THRESHOLD}%)\n"
            f"- EMA9: {ema9_val:.6g}, EMA26: {ema26_val:.6g} "
            f"(gap EMA9-EMA26: {ema_gap_pct:.2f}%, target Kondisi C: EMA9 > EMA26)\n"
            f"- RVOL sekarang: {rvol:.2f}x (target Kondisi A: >= {RVOL_THRESHOLD}x)\n\n"
            f"Kondisi yang tersedia:\n"
            f"- Kondisi A (RVOL)  : RVOL >= {RVOL_THRESHOLD}x + Bullish candle + Elapsed >= 30%\n"
            f"- Kondisi B (Profit): Profit >= {PROFIT_THRESHOLD}%\n"
            f"- Kondisi C (EMA)   : EMA9 > EMA26 + Bullish candle + Elapsed >= 30%\n\n"
            f"Jawab JSON persis (tanpa backtick), urutkan dari yang paling mungkin duluan:\n"
            f'{{"order":["X","Y","Z"],"reason":"alasan singkat 1 kalimat"}}'
        )

        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 150,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=15
        )
        resp.raise_for_status()
        text = resp.json()['content'][0]['text'].strip()
        text = text.replace("```json","").replace("```","").strip()
        data = json.loads(text)

        order  = data.get('order', default_order)
        reason = data.get('reason', '-')

        # Validasi — pastikan semua A, B, C ada
        valid = [x for x in order if x in ['A', 'B', 'C']]
        for c in ['A', 'B', 'C']:
            if c not in valid:
                valid.append(c)

        log(f"   [AI] {to_display_pair(symbol)} prioritas close: {valid} — {reason}")
        return valid, reason

    except Exception as e:
        log(f"⚠️  [AI] Gagal prediksi {symbol}: {e} — pakai default {default_order}")
        return default_order, "Fallback default"


# ── Binance Data ─────────────────────────────────────────────

def get_usdt_spot_pairs():
    try:
        resp = session.get(f"{BASE_URL}/api/v3/exchangeInfo", timeout=15)
        resp.raise_for_status()
        return sorted([
            s['symbol'] for s in resp.json()['symbols']
            if s['quoteAsset'] == 'USDT'
            and s['status'] == 'TRADING'
            and s['isSpotTradingAllowed']
            and s['baseAsset'] not in EXCLUDED_BASE_ASSETS
        ])
    except Exception as e:
        log(f"⚠️  Gagal load pairs: {e}")
        return []

def get_ticker_24h():
    try:
        resp = session.get(f"{BASE_URL}/api/v3/ticker/24hr", timeout=15)
        resp.raise_for_status()
        result = {}
        for d in resp.json():
            result[d['symbol']] = {
                'chg': float(d['priceChangePercent']),
                'vol_usd': float(d['quoteVolume'])  # volume dalam USDT
            }
        return result
    except Exception as e:
        log(f"⚠️  Gagal ticker 24h: {e}")
        return {}

def calc_3d_change(df) -> float:
    """Hitung perubahan % harga 3 hari (18 candle 4H) dari df OHLCV."""
    if df is None or len(df) < 19:
        return None
    price_now  = df.iloc[-1]['close']
    price_3d   = df.iloc[-19]['open']   # 18 candle lalu = 3 hari
    if price_3d == 0:
        return None
    return ((price_now - price_3d) / price_3d) * 100

def get_btc_4h_change() -> float:
    """Ambil perubahan % candle 4H terakhir BTC."""
    try:
        df = get_ohlcv('BTCUSDT', interval='4h', limit=5)
        if df is None or len(df) < 2:
            return 0.0
        last = df.iloc[-1]
        chg  = ((last['close'] - last['open']) / last['open']) * 100
        return chg
    except Exception:
        return 0.0

def get_ohlcv(symbol: str, interval='4h', limit=60):
    try:
        resp = session.get(
            f"{BASE_URL}/api/v3/klines",
            params={'symbol': symbol, 'interval': interval, 'limit': limit},
            timeout=10
        )
        resp.raise_for_status()
        raw = resp.json()
        if len(raw) < 30:
            return None
        df = pd.DataFrame(raw, columns=[
            'ts','open','high','low','close','volume',
            'close_time','qav','trades','tbbav','tbqav','ignore'
        ])
        for col in ['open','high','low','close','volume','ts','close_time']:
            df[col] = pd.to_numeric(df[col])
        return df
    except Exception:
        return None


# ── Helpers ──────────────────────────────────────────────────

def get_elapsed_pct(df: pd.DataFrame) -> float:
    last       = df.iloc[-1]
    now_ms     = time.time() * 1000
    candle_dur = last['close_time'] - last['ts']
    return ((now_ms - last['ts']) / candle_dur * 100) if candle_dur > 0 else 0

def get_support_level(df: pd.DataFrame, strength: int = 2):
    """
    Hitung support level dari pivot low terakhir.
    Menggunakan semua candle closed (exclude candle yang sedang running).
    Pivot low: bar ke-i adalah low terkecil dalam window (i-strength s/d i+strength).
    """
    if df is None or len(df) < (strength * 2 + 3):
        return None

    # Exclude candle terakhir yang masih running
    df_closed = df.iloc[:-1].copy().reset_index(drop=True)
    lows = df_closed['low'].values
    n    = len(lows)

    current_price = df.iloc[-1]['close']
    pivot_lows = []
    # Cari pivot low yang valid: kiri strength candle, kanan minimal 1 candle
    for i in range(strength, n - 1):  # n-1 agar ada minimal 1 candle di kanan
        left_ok  = all(lows[i] < lows[i-j] for j in range(1, strength+1))
        right_ok = all(lows[i] < lows[i+j] for j in range(1, min(strength+1, n-i)))
        if left_ok and right_ok:
            pivot_lows.append(lows[i])

    # Ambil pivot low terakhir yang LEBIH TINGGI dari harga sekarang
    # (support yang bermakna adalah yang di atas current price)
    valid = [p for p in pivot_lows if p > current_price]
    if valid:
        return min(valid)  # ambil support terdekat di atas harga
    return None

def get_resistance_level(df: pd.DataFrame, strength: int = 2):
    """
    Hitung resistance level dari pivot high terakhir (terkonfirmasi).
    Pivot high terkonfirmasi = strength candle di kiri DAN kanan sudah closed.
    Ambil pivot high terdekat di ATAS harga sekarang.
    """
    if df is None or len(df) < (strength * 2 + 3):
        return None

    # Exclude candle terakhir yang masih running
    df_closed = df.iloc[:-1].copy().reset_index(drop=True)
    highs = df_closed['high'].values
    n     = len(highs)

    current_price = df.iloc[-1]['close']
    pivot_highs = []
    for i in range(strength, n - strength):
        left_ok  = all(highs[i] > highs[i-j] for j in range(1, strength+1))
        right_ok = all(highs[i] > highs[i+j] for j in range(1, strength+1))
        if left_ok and right_ok:
            pivot_highs.append(highs[i])

    # Ambil pivot high terdekat di ATAS harga sekarang
    valid = [p for p in pivot_highs if p > current_price]
    if valid:
        return min(valid)
    return None


def should_alert(symbol):
    now = time.time()
    if now - already_alerted.get(symbol, 0) > 4 * 3600:
        already_alerted[symbol] = now
        return True
    return False

def should_confirm_alert(symbol):
    now = time.time()
    if now - confirmed_alerted.get(symbol, 0) > 4 * 3600:
        confirmed_alerted[symbol] = now
        return True
    return False


# ── Kondisi Thread 1 — Screener + Open Long (intrabar) ───────

def check_screener(df, chg_24h, vol_usd=0):
    """
    Kondisi teknikal v2 — Supertrend Near-Switch Uptrend.
    Syarat:
    1. Chg 24H -10% hingga 0%   (difilter di thread1_scan)
    2. Volume 24H >= $5M         (difilter di thread1_scan)
    3. Supertrend masih downtrend (SUPERTd = -1)
    4. Elapsed >= 10%
    5. RSI sekarang > RSI sebelumnya
    6. Stoch K > D
    7. Volume buy (tbbav) candle ini > candle sebelumnya
    8. Harga dalam zona 0% s.d. -3% di bawah EMA50 (4H) — tidak boleh di atas maupun lebih dari 3% di bawah EMA50
    """
    if df is None or len(df) < 25:
        return False, {}

    df    = df.copy()
    close = df['close']
    high  = df['high']
    low   = df['low']
    last  = df.iloc[-1]
    prev  = df.iloc[-2]

    # Syarat 4: Elapsed >= 10%
    elapsed = get_elapsed_pct(df)
    if elapsed < 10.0:
        return False, {}

    # Syarat 3: Supertrend masih downtrend
    try:
        st = ta.supertrend(high, low, close, length=ST_LENGTH, multiplier=ST_MULTIPLIER)
        if st is None or st.empty:
            return False, {}
        col_d = [c for c in st.columns if 'SUPERTd' in c]
        col_s = [c for c in st.columns if 'SUPERTs' in c]
        if not col_d or not col_s:
            return False, {}
        direction = st[col_d[0]].iloc[-1]
        st_line   = st[col_s[0]].iloc[-1]
        if direction != -1:  # -1 = downtrend
            return False, {}
        if pd.isna(st_line):
            return False, {}
    except Exception:
        return False, {}

    # Jarak harga ke ST line (negatif = harga di bawah ST line)
    dist_to_st = ((last['close'] - st_line) / st_line * 100) if st_line > 0 else None

    # Estimasi candle sampai switch: dist / avg_candle_move
    try:
        closes = close.iloc[-6:-1]
        avg_move = closes.pct_change().dropna().abs().mean() * 100
        est_candles = abs(dist_to_st) / avg_move if (avg_move > 0 and dist_to_st is not None) else None
        est_candles = round(est_candles, 1) if est_candles is not None else None
    except Exception:
        est_candles = None

    # Syarat 5: RSI naik
    rsi     = ta.rsi(close, length=14)
    rsi_val = rsi.iloc[-1] if rsi is not None else None
    rsi_prv = rsi.iloc[-2] if rsi is not None else None
    if rsi_val is None or pd.isna(rsi_val) or rsi_prv is None or pd.isna(rsi_prv):
        return False, {}
    if rsi_val <= rsi_prv:
        return False, {}

    # Syarat 6: Stoch K > D
    stoch = ta.stoch(high, low, close, k=14, d=3, smooth_k=1)
    stochk_cur = stochd_cur = None
    if stoch is not None and not stoch.empty:
        col_k = [c for c in stoch.columns if 'STOCHk' in c]
        col_d2 = [c for c in stoch.columns if 'STOCHd' in c]
        if col_k and col_d2:
            stochk_cur = stoch[col_k[0]].iloc[-1]
            stochd_cur = stoch[col_d2[0]].iloc[-1]
    if stochk_cur is None or stochd_cur is None:
        return False, {}
    if stochk_cur <= stochd_cur:
        return False, {}

    # Syarat 7: Volume buy naik
    try:
        vol_buy_cur = float(last['tbbav'])
        vol_buy_prv = float(prev['tbbav'])
        if vol_buy_cur <= vol_buy_prv:
            return False, {}
    except Exception:
        return False, {}

    # Syarat 8: Harga dalam zona 0% s.d. -3% di bawah EMA50 (4H)
    # - Harga >= EMA50         → masih premium, belum cukup koreksi → skip
    # - Harga < EMA50 × 0.97  → sudah terlalu dalam di bawah EMA50  → skip
    # Zona valid: EMA50 × 0.97 ≤ harga < EMA50
    EMA50_MAX_DIST_PCT = -3.0   # % maksimal di bawah EMA50
    ema50       = ta.ema(close, length=50)
    ema50_val   = ema50.iloc[-1] if ema50 is not None else None
    if ema50_val is None or pd.isna(ema50_val):
        return False, {}
    price_now   = float(last['close'])
    ema50_val   = float(ema50_val)
    ema50_lower = ema50_val * (1 + EMA50_MAX_DIST_PCT / 100)   # EMA50 × 0.97
    if price_now >= ema50_val:      # di atas atau sama dengan EMA50 → skip
        return False, {}
    if price_now < ema50_lower:     # lebih dari 3% di bawah EMA50 → skip
        return False, {}

    # Support level untuk referensi AI
    support = get_support_level(df, strength=2)
    dist_to_support = ((last['close'] - support) / support * 100) if support is not None else None

    return True, {
        'price'          : last['close'],
        'st_line'        : st_line,
        'dist_to_st'     : dist_to_st,
        'est_candles'    : est_candles,
        'rsi'            : float(rsi_val),
        'stoch_k'        : float(stochk_cur),
        'stoch_d'        : float(stochd_cur),
        'chg_24h'        : chg_24h,
        'elapsed'        : elapsed,
        'candle_ts'      : last['ts'],
        'vol_usd'        : vol_usd,
        'support'        : support,
        'dist_to_support': dist_to_support,
        'bb_lower'       : 0,
        'bb_pb'          : 0,
        'chg_4h'         : 0,
    }
    if df is None or len(df) < 25:
        return False, {}

    df    = df.copy()
    close = df['close']
    high  = df['high']
    low   = df['low']
    last  = df.iloc[-1]
    prev  = df.iloc[-2]

    # Syarat 6a: Bullish running candle
    if last['close'] <= last['open']:
        return False, {}

    # Syarat 6b: Elapsed >= 10%
    elapsed = get_elapsed_pct(df)
    if elapsed < 10.0:
        return False, {}

    # Hitung BB
    bb      = ta.bbands(close, length=20, std=2.0)
    bb_lower = 0
    if bb is not None and not bb.empty:
        col_l = [c for c in bb.columns if 'BBL' in c]
        if col_l:
            bb_lower = bb[col_l[0]].iloc[-1]

    # Syarat 2: Price < BB Lower
    if bb_lower <= 0 or last['close'] >= bb_lower:
        return False, {}

    # Hitung Stoch
    stoch = ta.stoch(high, low, close, k=14, d=3, smooth_k=1)
    stochk_cur = stochk_prv = stochd_cur = None
    if stoch is not None and not stoch.empty:
        col_k = [c for c in stoch.columns if 'STOCHk' in c]
        col_d = [c for c in stoch.columns if 'STOCHd' in c]
        if col_k and col_d:
            stochk_cur = stoch[col_k[0]].iloc[-1]
            stochk_prv = stoch[col_k[0]].iloc[-2]
            stochd_cur = stoch[col_d[0]].iloc[-1]

    if stochk_cur is None or stochd_cur is None or stochk_prv is None:
        return False, {}

    stochd_prv = stoch[col_d[0]].iloc[-2]

    # Syarat 3: Stoch K > D
    if not (stochk_cur > stochd_cur):
        return False, {}

    # Syarat 4: Stoch D < 23
    if stochd_cur >= STOCH_D_MAX:
        return False, {}

    # Syarat 5: Stoch D sekarang > Stoch D sebelumnya
    if stochd_cur <= stochd_prv:
        return False, {}

    # Hitung RSI
    rsi     = ta.rsi(close, length=14)
    rsi_val = rsi.iloc[-1] if rsi is not None else None
    rsi_prv = rsi.iloc[-2] if rsi is not None else None
    if rsi_val is None or pd.isna(rsi_val) or rsi_prv is None or pd.isna(rsi_prv):
        return False, {}

    # Syarat 6: RSI 25 < RSI < 32
    if rsi_val >= RSI_MAX or rsi_val <= 25.0:
        return False, {}

    # Syarat 7: RSI sekarang > RSI sebelumnya
    if rsi_val <= rsi_prv:
        return False, {}

    # Syarat 9: Volume buy (tbbav) candle ini > candle sebelumnya
    # Tidak dimutlakkan — volume sell tetap negatif, yang dilihat sisi beli
    try:
        vol_buy_cur = float(last['tbbav'])
        vol_buy_prv = float(prev['tbbav'])
        if vol_buy_cur <= vol_buy_prv:
            return False, {}
    except Exception:
        return False, {}

    # Syarat 10: BB Width menyempit (volatilitas menurun = konsolidasi akumulasi)
    # BB Width = (Upper - Lower) / Middle × 100
    # Bandingkan candle sekarang vs rata-rata 5 candle terakhir
    try:
        col_u  = [c for c in bb.columns if 'BBU' in c]
        col_m  = [c for c in bb.columns if 'BBM' in c]
        col_l2 = [c for c in bb.columns if 'BBL' in c]
        if not (col_u and col_m and col_l2):
            return False, {}
        bb_u  = bb[col_u[0]]
        bb_m  = bb[col_m[0]]
        bb_l2 = bb[col_l2[0]]
        width_now  = (bb_u.iloc[-1] - bb_l2.iloc[-1]) / bb_m.iloc[-1] * 100
        width_avg5 = ((bb_u - bb_l2) / bb_m * 100).iloc[-6:-1].mean()
        if pd.isna(width_avg5) or pd.isna(width_now) or width_now >= width_avg5:
            return False, {}
    except Exception:
        return False, {}

    # Syarat 11: Tekanan jual melemah
    # Volume sell = total volume - tbbav
    # Candle sekarang < rata-rata 3 candle sebelumnya
    try:
        vol_total = df['volume'].astype(float)
        vol_buy   = df['tbbav'].astype(float)
        vol_sell  = vol_total - vol_buy
        sell_now  = float(vol_sell.iloc[-1])
        sell_avg3 = float(vol_sell.iloc[-4:-1].mean())
        if pd.isna(sell_avg3) or pd.isna(sell_now) or sell_now >= sell_avg3:
            return False, {}
    except Exception:
        return False, {}

    # Hitung BB %b
    bb_pb = None
    if bb is not None and not bb.empty:
        col_pb = [c for c in bb.columns if 'BBP' in c]
        if col_pb:
            bb_pb = bb[col_pb[0]].iloc[-1]

    # Hitung Support (pivot low strength=2, nearest above current price)
    support = get_support_level(df, strength=2)
    dist_to_support = None
    if support is not None and last['close'] > 0:
        dist_to_support = ((last['close'] - support) / support * 100)  # negatif = harga di bawah support

    return True, {
        'price'           : last['close'],
        'bb_lower'        : bb_lower,
        'bb_pb'           : bb_pb if bb_pb is not None else 0,
        'rsi'             : rsi_val,
        'stoch_k'         : stochk_cur,
        'stoch_d'         : stochd_cur,
        'chg_24h'         : chg_24h,
        'elapsed'         : elapsed,
        'candle_ts'       : last['ts'],
        'vol_usd'         : 0,
        'support'         : support,
        'dist_to_support' : dist_to_support,
    }


# ── Kondisi Thread 2 — Add Funds (ex Thread 3) ───────────────

def check_add_funds_cond(symbol: str) -> tuple:
    """
    Kondisi add funds v5 (sesuai pool v2):
    1. Bullish candle (close > open)
    2. Elapsed >= 13%
    3. Supertrend sudah switch uptrend (SUPERTd = +1) — prediksi terbukti
    4. Resistance terkonfirmasi (pivot high strength=2) + current price < resistance
    5. Add funds count < 1 (dicek di caller)
    6. AI approve (dicek di caller)
    """
    import datetime as dt
    df = get_ohlcv(symbol, interval=CANDLE_INTERVAL, limit=200)
    if df is None or len(df) < 55:
        return False, None
    last  = df.iloc[-1]
    close = df['close']
    high  = df['high']
    low   = df['low']

    # Elapsed
    now_utc             = dt.datetime.now(dt.timezone.utc)
    seconds_in_day      = now_utc.hour * 3600 + now_utc.minute * 60 + now_utc.second
    seconds_into_candle = seconds_in_day % (4 * 3600)
    elapsed             = (seconds_into_candle / (4 * 3600)) * 100

    # Syarat 1: Bullish candle
    c1 = float(last['close']) > float(last['open'])

    # Syarat 2: Elapsed >= 13%
    c2 = elapsed >= TF_ELAPSED_ADDFUNDS

    # Syarat 3: Profit deal saat ini < 2% (harga masih dekat entry, ideal untuk DCA)
    avg_price     = calc_avg_price(symbol)
    current_price = float(last['close'])
    profit_pct    = ((current_price - avg_price) / avg_price * 100) if avg_price > 0 else 0
    c3            = profit_pct < 2.0

    # Syarat 4: Resistance terkonfirmasi + price < resistance
    resistance  = get_resistance_level(df, strength=2)
    if resistance is not None:
        resistance = float(resistance)
    c4          = resistance is not None and current_price < resistance
    dist_to_res = ((current_price - resistance) / resistance * 100) if resistance is not None else None

    log(f"   [T2] {to_display_pair(symbol)}: "
        f"Bull={'[OK]' if c1 else '❌'} "
        f"Elps={elapsed:.0f}%{'[OK]' if c2 else '❌'} "
        f"Profit={profit_pct:.2f}%<2%{'[OK]' if c3 else '❌'} "
        f"Res={'[OK]' if c4 else '❌'}"
        + (f"({resistance:.8g} dist={dist_to_res:.2f}%)" if resistance else "(N/A)"))

    return (c1 and c2 and c3 and c4), {'resistance': resistance, 'dist_to_res': dist_to_res, 'df': df, 'profit_pct': profit_pct}


# ── Kondisi Thread 3 — Close Long (ex Thread 4) ──────────────

def check_close_long(symbol: str, entry_price: float = 0, priority_order: list = None) -> tuple:
    """
    Cek kondisi close long sesuai urutan prioritas dari AI.
    Profit dihitung dari average price (base order + add funds).
    Return: (should_close, triggered_by, details)
    """
    if priority_order is None:
        priority_order = ['B', 'A', 'C']

    df = get_ohlcv(symbol, interval=CANDLE_INTERVAL, limit=60)
    if df is None or len(df) < 30:
        return False, None, {}

    last    = df.iloc[-1]
    close   = df['close']
    elapsed = get_elapsed_pct(df)

    # Gunakan average price (base + add funds) untuk kalkulasi profit
    avg_price  = calc_avg_price(symbol)
    profit_pct = ((last['close'] - avg_price) / avg_price * 100) if avg_price > 0 else 0

    avg_vol = df['volume'].iloc[-(RVOL_PERIOD+1):-1].mean() if len(df) >= RVOL_PERIOD + 1 else 0
    rvol    = last['volume'] / avg_vol if avg_vol > 0 else 0

    ema9      = ta.ema(close, length=9)
    ema26     = ta.ema(close, length=26)
    ema9_val  = ema9.iloc[-1]  if ema9  is not None else None
    ema26_val = ema26.iloc[-1] if ema26 is not None else None

    # Cek sesuai urutan prioritas AI
    for kondisi in priority_order:

        if kondisi == 'B':
            if profit_pct >= PROFIT_THRESHOLD:
                log(f"   [T3-B] {to_display_pair(symbol)}: Profit={profit_pct:.2f}%✅ (avg={avg_price:.6g})")
                return True, 'B', {
                    'profit_pct' : profit_pct,
                    'entry_price': entry_price,
                    'avg_price'  : avg_price,
                    'now_price'  : last['close']
                }
            else:
                log(f"   [T3-B] {to_display_pair(symbol)}: Profit={profit_pct:.2f}%❌ (target {PROFIT_THRESHOLD}%)")

        elif kondisi == 'A':
            cA1 = rvol >= RVOL_THRESHOLD
            cA2 = elapsed >= TF_ELAPSED_CLOSE
            cA3 = last['close'] > last['open']
            cA4 = profit_pct >= PROFIT_MIN_CLOSE
            candle_range = last['high'] - last['low']
            candle_body  = abs(last['close'] - last['open'])
            body_ratio   = (candle_body / candle_range) if candle_range > 0 else 0
            cA5 = body_ratio >= 0.3
            log(f"   [T3-A] {to_display_pair(symbol)}: "
                f"RVOL={rvol:.2f}x{'[OK]' if cA1 else '❌'} "
                f"Elps={elapsed:.0f}%{'[OK]' if cA2 else '❌'} "
                f"Bull={'[OK]' if cA3 else '❌'} "
                f"Profit={profit_pct:.2f}%≥{PROFIT_MIN_CLOSE}%{'[OK]' if cA4 else '❌'} "
                f"Body={body_ratio:.2f}{'[OK]' if cA5 else '❌'}")
            if cA1 and cA2 and cA3 and cA4 and cA5:
                return True, 'A', {'rvol': rvol, 'elapsed': elapsed, 'profit_pct': profit_pct}

        elif kondisi == 'C':
            if ema9_val is not None and ema26_val is not None and \
               not pd.isna(ema9_val) and not pd.isna(ema26_val):
                cC1 = ema9_val > ema26_val
                cC2 = elapsed >= TF_ELAPSED_CLOSE
                cC3 = last['close'] > last['open']
                cC4 = profit_pct >= PROFIT_MIN_CLOSE
                log(f"   [T3-C] {to_display_pair(symbol)}: "
                    f"EMA9>26={'[OK]' if cC1 else '❌'}({ema9_val:.6g}>{ema26_val:.6g}) "
                    f"Elps={elapsed:.0f}%{'[OK]' if cC2 else '❌'} "
                    f"Bull={'[OK]' if cC3 else '❌'} "
                    f"Profit={profit_pct:.2f}%≥{PROFIT_MIN_CLOSE}%{'[OK]' if cC4 else '❌'}")
                if cC1 and cC2 and cC3 and cC4:
                    return True, 'C', {
                        'ema9': ema9_val, 'ema26': ema26_val,
                        'elapsed': elapsed, 'profit_pct': profit_pct
                    }

    return False, None, {}


# ── THREAD 1 ─────────────────────────────────────────────────

def thread1_scan():
    global scan_running, pool_candle_ts, pool_executed, pool_executed_windows
    if scan_running:
        log("⏭️  [T1] Skip — scan sebelumnya masih jalan.")
        return
    scan_running = True
    try:
        log("🔍 [T1] Scan USDT pairs...")

        # ── Filter 1: BTC 4H market dump ──
        btc_chg_4h = get_btc_4h_change()
        log(f"   [T1] BTC 4H: {btc_chg_4h:.2f}%")
        if btc_chg_4h <= BTC_CHG_4H_MAX:
            log(f"🚨 [T1] MARKET DUMP — scan dibatalkan")
            send_telegram(
                f"🚨 <b>MARKET DUMP DETECTED</b>\n"
                f"BTC 4H: <code>{btc_chg_4h:.2f}%</code>\n"
                f"Scan dibatalkan untuk menghindari false signal."
            )
            return

        # Hitung elapsed candle 4H dari waktu lokal (tidak perlu API)
        import datetime as dt
        now_utc             = dt.datetime.now(dt.timezone.utc)
        seconds_in_day      = now_utc.hour * 3600 + now_utc.minute * 60 + now_utc.second
        candle_duration_s   = 4 * 3600
        seconds_into_candle = seconds_in_day % candle_duration_s
        current_elapsed     = (seconds_into_candle / candle_duration_s) * 100
        candle_start_epoch  = int(now_utc.timestamp()) - seconds_into_candle
        current_candle_ts   = candle_start_epoch * 1000

        # Hitung sisa waktu menuju Fase 2 berikutnya (window 1 atau 2)
        next_fase2_seconds = None
        next_window_label  = None
        for wi, (wmin, wmax) in enumerate(TF_PHASE2_WINDOWS):
            if wi not in pool_executed_windows:
                w_start_s = int(wmin / 100 * candle_duration_s)
                if w_start_s > int(seconds_into_candle):
                    next_fase2_seconds = w_start_s - int(seconds_into_candle)
                    next_window_label  = wi + 1
                    break
        if next_fase2_seconds is None:
            sisa_str = "SEKARANG!"
        else:
            sisa_detik_fase2 = next_fase2_seconds
            sisa_menit_fase2 = sisa_detik_fase2 // 60
            sisa_detik_sisa  = sisa_detik_fase2 % 60
            fase2_eta        = datetime.now() + timedelta(seconds=int(sisa_detik_fase2))
            fase2_eta_str    = fase2_eta.strftime("%H:%M")
            label = f" (siklus {next_window_label})" if next_window_label else ""
            if sisa_menit_fase2 > 0:
                sisa_str = f"{sisa_menit_fase2}m {sisa_detik_sisa}d lagi yaitu jam {fase2_eta_str}{label}"
            else:
                sisa_str = f"{sisa_detik_fase2}d lagi yaitu jam {fase2_eta_str}{label}"

        # ── Deteksi candle baru → carry-over kandidat lama ──
        if current_candle_ts != pool_candle_ts:
            with pool_lock:
                old_count = len(pool_candidates)
                carried_over = dict(pool_candidates)  # simpan kandidat lama
                pool_candidates.clear()
                pool_executed = False
                pool_executed_windows.clear()
            pool_notif_sent.clear()
            pool_candle_ts = current_candle_ts
            save_pool_log(candidates={})
            if old_count > 0:
                log(f"   [T1] 🕯️ Candle baru — {old_count} kandidat lama akan di-revalidasi scan ini")
            else:
                log(f"   [T1] 🕯️ Candle baru — pool direset (hapus 0 kandidat lama)")
        else:
            carried_over = {}

        log(f"   [T1] Elapsed: {current_elapsed:.1f}% | Pool: {len(pool_candidates)} kandidat | Fase 2: {sisa_str}")

        # ── Fase 2: window aktif → scan pool dulu, lalu seleksi AI ──
        active_window = None
        for i, (wmin, wmax) in enumerate(TF_PHASE2_WINDOWS):
            if wmin <= current_elapsed <= wmax:
                active_window = i
                break

        # ── Fase 1: Kumpulkan kandidat ke pool ──
        pairs = get_usdt_spot_pairs()
        if not pairs:
            return

        ticker_map = get_ticker_24h()
        matched, errors = [], 0

        for i, symbol in enumerate(pairs, 1):
            try:
                if i % 100 == 0:
                    log(f"   [T1] Progress: {i}/{len(pairs)}...")
                ticker = ticker_map.get(symbol)
                if ticker is None:
                    continue
                chg_24h = ticker["chg"]
                vol_usd = ticker["vol_usd"]

                # Pool v2 filter: chg -10% hingga 0%, volume >= $5M
                if chg_24h < CHG_24H_MIN or chg_24h > CHG_24H_MAX:
                    continue
                if vol_usd < MIN_VOLUME_USD_V2:
                    continue
                if symbol in active_deals:   # skip pair yang sudah jadi active deal
                    continue

                df = get_ohlcv(symbol, interval="4h", limit=60)
                match, vals = check_screener(df, chg_24h, vol_usd)
                if match:
                    vals["vol_usd"] = vol_usd
                    # Hitung chg_4h dari candle running saat ini
                    if df is not None and len(df) >= 1:
                        last_c = df.iloc[-1]
                        chg_4h = ((last_c['close'] - last_c['open']) / last_c['open'] * 100) if last_c['open'] > 0 else 0
                    else:
                        chg_4h = 0
                    vals["chg_4h"] = chg_4h
                    matched.append({"symbol": symbol, **vals})
                    dist_str = f"{vals['dist_to_st']:.2f}%" if vals.get('dist_to_st') is not None else "N/A"
                    est_str  = f"~{vals['est_candles']}c" if vals.get('est_candles') is not None else "?"
                    log(f"   [T1] ✅ POOL: {symbol} RSI={vals['rsi']:.1f} StochK={vals['stoch_k']:.1f} "
                        f"DistST={dist_str} Est={est_str} Chg={vals['chg_24h']:.1f}% Elps={vals['elapsed']:.0f}%")
                time.sleep(0.08)
            except Exception:
                errors += 1

        # Revalidasi carried_over: pair lama yang TIDAK muncul di matched scan ini
        # tapi masih ada di carried_over → cek ulang manual
        revalidated = 0
        if carried_over:
            scanned_symbols = {m["symbol"] for m in matched}
            for sym, vals in carried_over.items():
                if sym in scanned_symbols:
                    continue  # sudah ada di hasil scan baru, skip
                # Re-scan pair ini
                try:
                    ticker = ticker_map.get(sym)
                    if ticker is None:
                        continue
                    chg_24h = ticker["chg"]
                    vol_usd = ticker["vol_usd"]
                    if chg_24h < CHG_24H_MIN or chg_24h > CHG_24H_MAX:
                        continue
                    if vol_usd < MIN_VOLUME_USD_V2:
                        continue
                    if sym in active_deals:
                        continue
                    df = get_ohlcv(sym, interval="4h", limit=60)
                    match, new_vals = check_screener(df, chg_24h, vol_usd)
                    if match:
                        new_vals["vol_usd"] = vol_usd
                        new_vals["first_seen"] = vals.get("first_seen", time.time())
                        if df is not None and len(df) >= 1:
                            last_c = df.iloc[-1]
                            chg_4h = ((last_c['close'] - last_c['open']) / last_c['open'] * 100) if last_c['open'] > 0 else 0
                        else:
                            chg_4h = 0
                        new_vals["chg_4h"] = chg_4h
                        matched.append({"symbol": sym, **new_vals})
                        revalidated += 1
                        dist_str = f"{new_vals['dist_to_st']:.2f}%" if new_vals.get('dist_to_st') is not None else "N/A"
                        est_str  = f"~{new_vals['est_candles']}c" if new_vals.get('est_candles') is not None else "?"
                        log(f"   [T1] 🔄 CARRY-OVER: {sym} RSI={new_vals['rsi']:.1f} StochK={new_vals['stoch_k']:.1f} DistST={dist_str} Est={est_str} Chg={new_vals['chg_24h']:.1f}%")
                    else:
                        log(f"   [T1] ❌ Carry-over {sym} tidak lolos revalidasi — dibuang")
                except Exception:
                    pass

        if revalidated > 0:
            log(f"   [T1] Fase 1: {len(matched) - revalidated} baru + {revalidated} carry-over = {len(matched)} total")
        else:
            log(f"   [T1] Fase 1: {len(matched)} baru ditemukan scan ini")

        if matched:
            now_ts = time.time()
            with pool_lock:
                for m in matched:
                    sym = m["symbol"]
                    if sym not in pool_candidates:
                        m["first_seen"] = now_ts
                    else:
                        m["first_seen"] = pool_candidates[sym].get("first_seen", now_ts)
                    pool_candidates[sym] = m
            save_pool_log(candidates=pool_candidates)
            log(f"   [T1] Pool: {len(pool_candidates)} kandidat | Fase 2: {sisa_str}")

        # ── Setelah scan pool selesai: cek Fase 2 ──
        if active_window is not None:
            if active_window in pool_executed_windows:
                log(f"   [T1] Fase 2 window {active_window+1} sudah dieksekusi — skip.")
                return
            with pool_lock:
                pool_now = dict(pool_candidates)

            if not pool_now:
                log(f"   [T1] Fase 2: pool kosong.")
                return

            log(f"🤖 [T1] FASE 2 — {current_elapsed:.1f}% — AI pilih 1 dari {len(pool_now)} kandidat...")
            pick = ai_pick_best_from_pool(pool_now)
            if not pick:
                return

            best_symbol = pick["best"]
            waktu       = datetime.now().strftime("%d/%m/%Y %H:%M WIB")

            ranking_text = "\n".join([
                f"  {i+1}. {to_display_pair(s)}"
                for i, s in enumerate(pick["ranking"][:8])
            ])
            pool_cands_text = "\n".join([
                f"  {'🥇' if v['symbol'] == best_symbol else '•'} "
                f"{to_display_pair(v['symbol'])} RSI={v['rsi']:.1f} "
                f"Chg={v['chg_24h']:.1f}% "
                f"Pool={int((time.time()-v.get('first_seen',time.time()))/60)}m"
                for v in sorted(pool_now.values(), key=lambda x: x['rsi'])
            ])
            best_data  = pool_now[best_symbol]
            bb_dist    = ((best_data['price'] - best_data['bb_lower']) / best_data['bb_lower'] * 100) if best_data.get('bb_lower', 0) > 0 else 0
            btc_bypass = False  # inisialisasi default — di-set ulang di blok filter BTC di bawah

            send_telegram(
                f"🎯 <b>FASE 2 — SELEKSI AI</b>\n📅 {waktu}\n"
                f"📊 Elapsed: <code>{current_elapsed:.1f}%</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📋 Pool ({len(pool_now)} kandidat):\n{pool_cands_text}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🥇 <b>AI Pilih: {to_display_pair(best_symbol)}</b>\n"
                f"  Harga  : <code>{best_data['price']:.6g}</code>\n"
                f"  RSI    : <code>{best_data['rsi']:.1f}</code>\n"
                f"  Stoch  : K=<code>{best_data['stoch_k']:.1f}</code> D=<code>{best_data['stoch_d']:.1f}</code>\n"
                f"  DistST : <code>{best_data.get('dist_to_st', 0):.2f}%</code>\n"
                f"  Chg24H : <code>{best_data['chg_24h']:.2f}%</code>\n\n"
                f"💡 <i>{pick['reason']}</i>\n\n"
                f"📊 Ranking:\n{ranking_text}\n\n"
                f"  <a href=\"https://www.tradingview.com/chart/?symbol=BINANCE:{best_symbol}\">📈 Chart</a>"
            )
            time.sleep(0.5)

            with active_deals_lock:
                active_count = len(active_deals)

            if active_count >= COMMAS_MAX_ACTIVE_DEALS:
                with active_deals_lock:
                    active_list = ", ".join(active_deals.keys())
                send_telegram(
                    f"⏸️ <b>OPEN LONG DITAHAN</b>\n📅 {waktu}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"🪙 <b>{to_display_pair(best_symbol)}</b> dipilih AI,\n"
                    f"namun slot penuh (<b>{active_count}/{COMMAS_MAX_ACTIVE_DEALS}</b>).\n\n"
                    f"📋 Active deals: <code>{active_list}</code>"
                )
            else:
                if should_confirm_alert(best_symbol):
                    # ── Filter bypass: BTC di bawah EMA20 4H (market lemah) ──
                    btc_df     = get_ohlcv('BTCUSDT', interval='4h', limit=30)
                    btc_bypass = False
                    btc_bypass_reason = None
                    if btc_df is not None and len(btc_df) >= 21:
                        btc_close   = btc_df['close']
                        btc_ema20   = ta.ema(btc_close, length=20)
                        btc_ema20_v = float(btc_ema20.iloc[-1]) if btc_ema20 is not None else None
                        btc_price   = float(btc_df.iloc[-1]['close'])
                        # c-1 dan c-2 diambil dari candle yang sudah tutup
                        # iloc[-1] = running candle (belum tutup) → pakai sebagai harga current
                        # iloc[-2] = candle terakhir yang sudah tutup = c-1
                        # iloc[-3] = candle sebelumnya yang sudah tutup = c-2
                        # Tapi bila running candle OPEN == candle sebelumnya CLOSE (candle baru mulai),
                        # maka c-1 sebenarnya adalah iloc[-2] dan c-2 adalah iloc[-3] — sudah benar.
                        # Fix: gunakan btc_price dari iloc[-1]['close'] (running),
                        # dan c1/c2 dari iloc[-2]/[-3] yang sudah tutup SERTA sudah melewati >5 menit
                        # Syarat boleh open long:
                        # BTC >= EMA20 × 0.98 DAN c-1 bullish DAN c-2 bullish
                        if btc_ema20_v is not None:
                            btc_threshold = btc_ema20_v * 0.98
                            btc_c1_bull   = float(btc_df.iloc[-2]['close']) > float(btc_df.iloc[-2]['open'])  # c-1
                            btc_c2_bull   = float(btc_df.iloc[-3]['close']) > float(btc_df.iloc[-3]['open'])  # c-2
                            # Bila c-1 atau c-2 bearish, fetch ulang sekali setelah delay 3 detik
                            # untuk memastikan data Binance sudah final (bukan data stale)
                            if not btc_c1_bull or not btc_c2_bull:
                                import time as _time
                                _time.sleep(3)
                                btc_df2 = get_ohlcv('BTCUSDT', interval='4h', limit=30)
                                if btc_df2 is not None and len(btc_df2) >= 4:
                                    btc_c1_bull = float(btc_df2.iloc[-2]['close']) > float(btc_df2.iloc[-2]['open'])
                                    btc_c2_bull = float(btc_df2.iloc[-3]['close']) > float(btc_df2.iloc[-3]['open'])
                                    btc_df = btc_df2
                            if btc_price < btc_threshold or not btc_c1_bull or not btc_c2_bull:
                                btc_bypass = True
                                c1_str = "✅" if btc_c1_bull else "❌"
                                c2_str = "✅" if btc_c2_bull else "❌"
                                pct_vs_ema = (btc_price / btc_ema20_v - 1) * 100
                                price_str = "✅" if btc_price >= btc_threshold else "❌"
                                btc_bypass_reason = (
                                    f"BTC≥EMA20×0.98={price_str} {btc_price:,.0f} ({pct_vs_ema:+.1f}% vs EMA20 {btc_ema20_v:,.0f}) | "
                                    f"c-1 bullish={c1_str} c-2 bullish={c2_str}"
                                )

                    if not btc_bypass and btc_df is not None and len(btc_df) >= 21:
                        btc_ema20_v2 = float(ta.ema(btc_df['close'], length=20).iloc[-1])
                        pct_vs_ema2  = (float(btc_df.iloc[-1]['close']) / btc_ema20_v2 - 1) * 100
                        c1_ok = "✅" if float(btc_df.iloc[-2]['close']) > float(btc_df.iloc[-2]['open']) else "❌"
                        c2_ok = "✅" if float(btc_df.iloc[-3]['close']) > float(btc_df.iloc[-3]['open']) else "❌"
                        log(f"   [T1] BTC Lapis2 OK: {float(btc_df.iloc[-1]['close']):,.0f} ({pct_vs_ema2:+.1f}% vs EMA20) | c-1 bullish={c1_ok} c-2 bullish={c2_ok}")

                    if btc_bypass:
                        log(f"⛔ [T1] BYPASS open long {best_symbol}: {btc_bypass_reason}")
                        send_telegram(
                            f"⛔ <b>OPEN LONG DIBYPASS</b>\n📅 {waktu}\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"🪙 <b>{to_display_pair(best_symbol)}</b> dipilih AI tapi dibypass.\n\n"
                            f"❌ Alasan: <i>{btc_bypass_reason}</i>\n\n"
                            f"📋 Pool tetap aktif — kandidat menunggu candle berikutnya.\n"
                            f'  <a href="https://www.tradingview.com/chart/?symbol=BINANCE:BTCUSDT">📈 BTC Chart</a>'
                        )
                    else:
                        # ── Filter bypass: DistST terlalu jauh atau EstCandle terlalu banyak ──
                        dist_to_st    = best_data.get('dist_to_st')
                        est_c         = best_data.get('est_candles')
                        bypass_reason = None

                        if current_elapsed > OPEN_LONG_ELAPSED_MAX:
                            bypass_reason = f"Elapsed {current_elapsed:.1f}% > {OPEN_LONG_ELAPSED_MAX}% (candle hampir tutup, rawan entry di ujung wick)"
                        elif dist_to_st is not None and dist_to_st < DIST_ST_MAX_PCT:
                            bypass_reason = f"DistST {dist_to_st:.2f}% < {DIST_ST_MAX_PCT}% (terlalu jauh dari ST line)"
                        elif est_c is not None and est_c > EST_CANDLE_MAX:
                            bypass_reason = f"EstCandle ~{est_c}c > {EST_CANDLE_MAX}c (terlalu lama menunggu switch uptrend)"

                        if bypass_reason:
                            log(f"⛔ [T1] BYPASS open long {best_symbol}: {bypass_reason}")
                            send_telegram(
                                f"⛔ <b>OPEN LONG DIBYPASS</b>\n📅 {waktu}\n"
                                f"━━━━━━━━━━━━━━━━━━━━\n"
                                f"🪙 <b>{to_display_pair(best_symbol)}</b> dipilih AI tapi dibypass.\n\n"
                                f"❌ Alasan: <i>{bypass_reason}</i>\n\n"
                                f"📊 DistST: <code>{dist_to_st:.2f}%</code> | Est: <code>~{est_c}c</code>\n"
                                f"  <a href=\"https://www.tradingview.com/chart/?symbol=BINANCE:{best_symbol}\">📈 Chart</a>"
                            )
                        else:
                            log(f"🚀 [T1] OPEN LONG Fase 2: {best_symbol} | DistST={dist_to_st:.2f}% Est=~{est_c}c")
                            success      = send_open_long(best_symbol)
                            waktu2       = datetime.now().strftime("%d/%m/%Y %H:%M WIB")
                            dist_st_str  = f"{dist_to_st:.2f}%" if dist_to_st is not None else "N/A"
                            est_jam      = f"~{round(est_c * 4)}j" if est_c is not None else "?"
                            est_str      = f"~{est_c}c ({est_jam})" if est_c is not None else "?"
                            send_telegram(
                                f"🚀 <b>OPEN LONG SIGNAL</b>\n📅 {waktu2}\n"
                                f"━━━━━━━━━━━━━━━━━━━━\n🪙 <b>{to_display_pair(best_symbol)}</b>\n\n"
                                f"✅ Dipilih AI dari {len(pool_now)} kandidat pool\n"
                                f"✅ Elapsed <code>{current_elapsed:.1f}%</code>\n"
                                f"📈 DistST: <code>{dist_st_str}</code> | Est switch uptrend: <code>{est_str}</code>\n\n"
                                f"📡 3Commas: {'✅ Terkirim' if success else '❌ GAGAL'}\n"
                                f"  Pair: <code>{to_commas_pair(best_symbol)}</code>\n"
                                f"  Slot: <code>{active_count + 1}/{COMMAS_MAX_ACTIVE_DEALS}</code>\n"
                                f"  <a href=\"https://www.tradingview.com/chart/?symbol=BINANCE:{best_symbol}\">📈 Chart</a>"
                            )
                            if success:
                                add_to_active_deals(best_symbol, best_data)
                                save_pool_log(winner={
                                    'symbol'     : best_symbol,
                                    'picked_at'  : datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                    'reason'     : pick.get('reason', '-'),
                                    'ranking'    : pick.get('ranking', []),
                                    'deal_opened': True,
                                })

            pool_executed_windows.add(active_window)
            if not btc_bypass:
                with pool_lock:
                    pool_candidates.clear()
                log(f"   [T1] Pool dikosongkan setelah Fase 2 window {active_window+1}.")
            else:
                log(f"   [T1] Pool DIPERTAHANKAN setelah bypass BTC — kandidat menunggu window berikutnya.")
            return

        # ── Notif pool ──
        elapsed_minutes = (time.time() * 1000 - current_candle_ts) / 60000
        for notif_menit in POOL_NOTIF_MINUTES:
            if elapsed_minutes >= notif_menit and notif_menit not in pool_notif_sent:
                pool_notif_sent.add(notif_menit)
                waktu = datetime.now().strftime('%d/%m/%Y %H:%M WIB')
                fase_label = "#1/3" if notif_menit <= 45 else ("#2/3" if notif_menit <= 105 else "#3/3")

                with pool_lock:
                    pool_snap = dict(pool_candidates)

                if pool_snap:
                    pool_text = "\n".join([
                        f"  {i+1}. {to_display_pair(v['symbol'])} "
                        f"RSI={v['rsi']:.1f} Chg={v['chg_24h']:.1f}% "
                        f"Pool={int((time.time()-v.get('first_seen',time.time()))/60)}m"
                        for i, v in enumerate(sorted(pool_snap.values(), key=lambda x: x['rsi']))
                    ])
                    send_telegram(
                        f"📊 <b>POOL UPDATE [{fase_label}] — Menit ke-{notif_menit}</b>\n"
                        f"📅 {waktu} | Elapsed: <code>{current_elapsed:.1f}%</code>\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"📋 {len(pool_snap)} kandidat terkumpul:\n{pool_text}\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"⏳ Fase 2 dalam ±<b>{sisa_str}</b>"
                    )
                    log(f"   [T1] 📢 Pool notif menit-{notif_menit} [{fase_label}] ({len(pool_snap)} kandidat, sisa {sisa_str})")
                else:
                    send_telegram(
                        f"📊 <b>POOL UPDATE [{fase_label}] — Menit ke-{notif_menit}</b>\n"
                        f"📅 {waktu} | Elapsed: <code>{current_elapsed:.1f}%</code>\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"📋 Pool masih kosong — belum ada kandidat yang lolos kondisi.\n"
                        f"⏳ Fase 2 dalam ±<b>{sisa_str}</b>"
                    )
                    log(f"   [T1] 📢 Pool notif menit-{notif_menit} [{fase_label}] — kosong, sisa {sisa_str}")
                break  # hanya 1 notif per scan

    finally:
        scan_running = False


# ── THREAD 2: Add Funds ──────────────────────────────────────

ADD_FUNDS_MAX = 1  # maksimal add funds per deal

def thread2_add_funds():
    with active_deals_lock:
        current = dict(active_deals)
    if not current:
        return
    log(f"💰 [T2] Monitor {len(current)} active deals...")

    # Hitung total add funds terkirim di semua deals
    total_sent = sum(d.get('add_funds_count', 0) for d in current.values())

    for symbol, data in current.items():
        try:
            # Baca langsung dari active_deals (bukan snapshot) agar selalu fresh
            with active_deals_lock:
                live_data = active_deals.get(symbol, data)
            add_count   = live_data.get('add_funds_count', 0)
            skip_until  = live_data.get('add_funds_skip_until', 0)
            entry_price = live_data.get('price', 0)

            # Skip kalau deal ini sudah max add funds
            if add_count >= ADD_FUNDS_MAX:
                log(f"   [T2] {to_display_pair(symbol)}: sudah {add_count}x add funds — skip")
                continue

            # Skip kalau masih dalam periode cooldown (AI bilang jangan)
            if time.time() < skip_until:
                sisa = int((skip_until - time.time()) / 60)
                log(f"   [T2] {to_display_pair(symbol)}: skip {sisa}m lagi (AI: tunda)")
                continue

            # Cek kondisi teknis
            confirmed, cond_data = check_add_funds_cond(symbol)
            if not confirmed:
                continue

            resistance = cond_data.get('resistance') if cond_data else None
            dist_to_res = cond_data.get('dist_to_res') if cond_data else None
            df_af = cond_data.get('df') if cond_data else None

            # Tanya AI: layak add funds sekarang?
            df = df_af if df_af is not None else get_ohlcv(symbol, interval=CANDLE_INTERVAL, limit=60)
            ai_approve, ai_reason = ai_check_add_funds(symbol, entry_price, df)

            waktu = datetime.now().strftime('%d/%m/%Y %H:%M WIB')
            if not ai_approve:
                log(f"   [T2] {to_display_pair(symbol)}: AI tunda add funds — {ai_reason}")
                mark_add_funds_skip(symbol, minutes=30)
                send_telegram(
                    f"🤔 <b>ADD FUNDS DITUNDA</b>\n📅 {waktu}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"🪙 <b>{to_display_pair(symbol)}</b>\n"
                    f"🤖 AI: {ai_reason}\n\n"
                    f"⏳ Akan dicek lagi dalam 30 menit."
                )
                continue

            # Kirim add funds
            res_str = f"{resistance:.8g} (jarak {dist_to_res:.2f}%)" if resistance else "N/A"
            log(f"💸 [T2] ADD FUNDS #{add_count+1}: {symbol}")
            current_price = df.iloc[-1]['close'] if df is not None and len(df) > 0 else 0
            success = send_add_funds(symbol)
            profit_pct_af = cond_data.get('profit_pct', 0) if cond_data else 0
            res_str_clean = res_str.replace('\\', '').replace('<', '').replace('>', '').replace('&', '')
            send_telegram(
                f"💰 <b>ADD FUNDS #{add_count+1}</b>\n📅 {waktu}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n🪙 <b>{to_display_pair(symbol)}</b>\n\n"
                f"✅ Bullish running candle\n"
                f"✅ TF Elapsed ≥ {TF_ELAPSED_ADDFUNDS:.0f}%\n"
                f"✅ Profit {profit_pct_af:.2f}% &lt; 2% (harga masih dekat entry)\n"
                f"✅ Price &lt; Resistance (<code>{res_str_clean}</code>)\n"
                f"🤖 AI: {ai_reason}\n\n"
                f"📡 3Commas: {'✅ Terkirim' if success else '❌ GAGAL'}\n"
                f"  Pair  : <code>{to_commas_pair(symbol)}</code>\n"
                f"  Volume: <code>${ADD_FUNDS_VOLUME} USDT</code> (#{add_count+1}/{ADD_FUNDS_MAX})\n"
                f"  <a href='https://www.tradingview.com/chart/?symbol=BINANCE:{symbol}'>📈 Chart</a>"
            )
            mark_add_funds_sent(symbol, price=current_price)
            total_sent += 1
        except Exception as e:
            import traceback
            log(f"⚠️  [T2] Error {symbol}: {e}\n{traceback.format_exc()}")


# ── THREAD 3: Close Long (ex Thread 4) ───────────────────────

# Cache prioritas AI per pair
ai_priority_cache = {}
AI_PRIORITY_TTL   = 10 * 60  # 10 menit

def get_ai_priority(symbol: str, entry_price: float, df) -> tuple:
    """Ambil prioritas dari cache atau minta AI kalau sudah expired."""
    now = time.time()
    cached = ai_priority_cache.get(symbol)
    if cached and (now - cached[2]) < AI_PRIORITY_TTL:
        return cached[0], cached[1]

    # Hitung data untuk AI
    last      = df.iloc[-1]
    ema9      = ta.ema(df['close'], length=9)
    ema26     = ta.ema(df['close'], length=26)
    avg_vol   = df['volume'].iloc[-(RVOL_PERIOD+1):-1].mean() if len(df) >= RVOL_PERIOD+1 else 0
    rvol      = last['volume'] / avg_vol if avg_vol > 0 else 0

    current_data = {
        'close': last['close'],
        'ema9' : ema9.iloc[-1]  if ema9  is not None else 0,
        'ema26': ema26.iloc[-1] if ema26 is not None else 0,
        'rvol' : rvol,
    }

    order, reason = ai_predict_close_priority(symbol, entry_price, current_data)
    ai_priority_cache[symbol] = (order, reason, now)
    return order, reason


def thread3_close_long():
    with active_deals_lock:
        current = dict(active_deals)
    if not current:
        return
    log(f"🔒 [T3] Monitor {len(current)} active deals untuk close...")

    for symbol, data in current.items():
        try:
            # Cek apakah sedang dalam mode tahan
            tahan_until = ai_tahan_until.get(symbol, 0)
            if time.time() < tahan_until:
                sisa = int(tahan_until - time.time())
                log(f"   [T3] {to_display_pair(symbol)}: tahan {sisa}d lagi")
                continue

            entry_price = data.get('price', 0)

            df = get_ohlcv(symbol, interval=CANDLE_INTERVAL, limit=60)
            if df is None or len(df) < 30:
                continue

            # Hitung ATR% untuk konfigurasi volatilitas
            atr_pct = get_atr_pct(df)
            _, tahan_secs = get_trailing_config(atr_pct)

            # Prioritas urutan dari AI (cache)
            priority_order, _ = get_ai_priority(symbol, entry_price, df)

            should_close, triggered_by, details = check_close_long(
                symbol, entry_price, priority_order
            )

            if not should_close:
                continue

            # ── AI Final Decision: 6 opsi ──
            last     = df.iloc[-1]
            ema9     = ta.ema(df['close'], length=9)
            ema26    = ta.ema(df['close'], length=26)
            avg_vol  = df['volume'].iloc[-(RVOL_PERIOD+1):-1].mean()
            rvol     = float(last['volume']) / avg_vol if avg_vol > 0 else 0
            current_data_ai = {
                'close': float(last['close']),
                'ema9' : float(ema9.iloc[-1])  if ema9  is not None else 0,
                'ema26': float(ema26.iloc[-1]) if ema26 is not None else 0,
                'rvol' : rvol,
            }
            action, ai_reason = ai_decide_close_action(
                symbol, entry_price, current_data_ai, triggered_by, atr_pct
            )

            dev, _ = get_trailing_config(atr_pct)
            waktu  = datetime.now().strftime('%d/%m/%Y %H:%M WIB')
            tipe_vol = 'A' if atr_pct < 1 else ('B' if atr_pct < 2 else ('C' if atr_pct < 4 else ('D' if atr_pct < 7 else 'E')))

            if action == 'tahan':
                ai_tahan_until[symbol] = time.time() + tahan_secs
                log(f"   [T3] {to_display_pair(symbol)}: AI tahan {tahan_secs}d — {ai_reason}")
                send_telegram(
                    f"⏸️ <b>TAHAN — AI Putuskan Tunggu</b>\n📅 {waktu}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n🪙 <b>{to_display_pair(symbol)}</b>\n\n"
                    f"Kondisi {triggered_by} terpenuhi tapi AI memilih tahan.\n"
                    f"🤖 <i>{ai_reason}</i>\n"
                    f"⏳ Cek lagi dalam <b>{tahan_secs}d</b> (volatilitas tipe {tipe_vol}, ATR={atr_pct:.1f}%)"
                )
                continue

            elif action.startswith('trailing_'):
                log(f"🔀 [T3] TRAILING [{triggered_by}]: {symbol} dev={dev}%")
                success = send_trailing(symbol, dev)
                send_telegram(
                    f"🔀 <b>TRAILING AKTIF</b>\n📅 {waktu}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n🪙 <b>{to_display_pair(symbol)}</b>\n\n"
                    f"Kondisi {triggered_by} terpenuhi → AI pilih trailing.\n"
                    f"📉 Trailing deviation: <code>{dev}%</code> (ATR={atr_pct:.1f}%, tipe {tipe_vol})\n"
                    f"🤖 <i>{ai_reason}</i>\n\n"
                    f"📡  3Commas: {'✅ Trailing aktif' if success else '❌ GAGAL'}\n"
                    f"  Pair: <code>{to_commas_pair(symbol)}</code>\n"
                    f"  <a href='https://www.tradingview.com/chart/?symbol=BINANCE:{symbol}'>📈 Chart</a>"
                )

            else:
                # close_A / close_B / close_C
                log(f"🔴 [T3] CLOSE LONG [{triggered_by}]: {symbol}")
                success = send_close_long(symbol)

                if triggered_by == 'A':
                    kondisi_text = (
                        f"✅ <b>Kondisi A</b> (RVOL)\n"
                        f"  RVOL    : <code>{details['rvol']:.2f}x</code> ≥ {RVOL_THRESHOLD}x\n"
                        f"  Elapsed : <code>{details['elapsed']:.0f}%</code>\n"
                        f"  Bullish running candle"
                    )
                elif triggered_by == 'B':
                    kondisi_text = (
                        f"✅ <b>Kondisi B</b> (Profit)\n"
                        f"  Entry  : <code>{details['entry_price']:.6g}</code>\n"
                        f"  Harga  : <code>{details['now_price']:.6g}</code>\n"
                        f"  Profit : <code>{details['profit_pct']:.2f}%</code> ≥ {PROFIT_THRESHOLD}%"
                    )
                elif triggered_by == 'C':
                    kondisi_text = (
                        f"✅ <b>Kondisi C</b> (EMA9 &gt; EMA26)\n"
                        f"  EMA9   : <code>{details['ema9']:.6g}</code>\n"
                        f"  EMA26  : <code>{details['ema26']:.6g}</code>\n"
                        f"  Elapsed: <code>{details['elapsed']:.0f}%</code>\n"
                        f"  Bullish running candle"
                    )

                send_telegram(
                    f"🔴 <b>CLOSE LONG SIGNAL</b>\n📅 {waktu}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n🪙 <b>{to_display_pair(symbol)}</b>\n\n"
                    f"<b>Tertrigger oleh:</b>\n{kondisi_text}\n\n"
                    f"🤖 AI putuskan: <b>CLOSE</b>\n<i>{ai_reason}</i>\n"
                    f"📊 ATR={atr_pct:.1f}% (tipe {tipe_vol})\n\n"
                    f"📡 3Commas: {'✅ Terkirim' if success else '❌ GAGAL'}\n"
                    f"  Pair: <code>{to_commas_pair(symbol)}</code>\n"
                    f"  <a href='https://www.tradingview.com/chart/?symbol=BINANCE:{symbol}'>📈 Chart</a>"
                )
                if success:
                    remove_from_active_deals(symbol)
                    ai_priority_cache.pop(symbol, None)
                    ai_tahan_until.pop(symbol, None)

        except Exception as e:
            import traceback
            log(f"⚠️  [T3] Error {symbol}: {e}\n{traceback.format_exc()}")


# ── Thread Loops ─────────────────────────────────────────────

def run_thread1():
    while True:
        thread1_scan()
        time.sleep(SCAN_INTERVAL * 60)

def run_thread2():
    time.sleep(10)
    while True:
        thread2_add_funds()
        time.sleep(ADDFUNDS_INTERVAL)

def run_thread3():
    time.sleep(15)
    while True:
        thread3_close_long()
        time.sleep(CLOSE_INTERVAL)


# ── Entry Point ──────────────────────────────────────────────

if __name__ == '__main__':
    log("=" * 65)
    log("  BINANCE SCREENER → AI → TELEGRAM + 3COMMAS")
    log("  T1: Screener + Open Long  tiap 3 menit")
    log("  T2: Add Funds             tiap 15 detik")
    log("  T3: Close Long            tiap 15 detik")
    log("=" * 65)
    log("  T = Thread (proses paralel). Ada 3 thread:")
    log("  T1=Screener+OpenLong(3m) | T2=AddFunds(15d) | T3=CloseLong(15d)")
    log("  URUTAN EKSEKUSI T1:")
    log("  Lapis1 (BTC candle 4H berjalan ≤−3% dari open candle → scan batal) → Fase1 (pool 8 syarat)")
    log("  → Fase2 Window1(50-59m) / Window2(110-119m) / Window3(170-209m)")
    log("    └─ Gerbang: Lapis2(BTC EMA20+c1c2) → Elapsed≤75%")
    log("               → DistST≥−7.5% → EstCandle≤8 → OPEN LONG")
    log("=" * 65)

    load_active_deals()
    load_pool_from_log()

    log("📤 Tes Telegram...")
    with active_deals_lock:
        deals_now = list(active_deals.keys())
    send_telegram(
        "✅ <b>Binance Screener aktif!</b>\n"
        f"🔍 T1 Screener+Open Long: tiap <b>{SCAN_INTERVAL} menit</b>\n"
        f"💰 T2 Add Funds         : tiap <b>{ADDFUNDS_INTERVAL} detik</b> | Vol: <b>${ADD_FUNDS_VOLUME}</b>\n"
        f"🔴 T3 Close Long        : tiap <b>{CLOSE_INTERVAL} detik</b>\n"
        f"🤖 AI: Claude Sonnet\n"
        f"📋 Active deals: <code>{', '.join(deals_now) if deals_now else 'kosong'}</code>\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "🧵 <b>T = Thread</b> (proses paralel, ada 3):\n"
        "   T1 = Screener + Open Long (tiap 3 menit)\n"
        "   T2 = Add Funds (tiap 15 detik)\n"
        "   T3 = Close Long (tiap 15 detik)\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📌 <b>Urutan Eksekusi T1:</b>\n"
        "1️⃣ <b>Lapis 1</b> — BTC candle 4H berjalan ≤−3% dari open candle itu → scan batal\n"
        "2️⃣ <b>Fase 1</b> — Pool: 8 syarat (Chg, Vol, ST, Elapsed, RSI, Stoch, VolBuy, EMA50 zona −3%)\n"
        "3️⃣ <b>Fase 2</b> — Window 1(50-59m) / 2(110-119m) / 3(170-209m)\n"
        "   <b>Lapis 2</b> BTC: harga≥EMA20×0.98 + c-1 &amp; c-2 bullish\n"
        "   Bypass: Elapsed&gt;75% | DistST&lt;−7.5% | EstCandle&gt;8\n"
        "   ✅ Lolos semua → <b>OPEN LONG</b> via 3Commas"
    )
    log("✅ Telegram OK.")

    t1 = threading.Thread(target=run_thread1, daemon=True, name="T1-Screener")
    t2 = threading.Thread(target=run_thread2, daemon=True, name="T2-AddFunds")
    t3 = threading.Thread(target=run_thread3, daemon=True, name="T3-CloseLong")

    t1.start(); t2.start(); t3.start()

    log("\n⏰ 3 Thread aktif. Tekan Ctrl+C untuk berhenti.\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log("🛑 Script dihentikan.")
        sys.exit(0)
