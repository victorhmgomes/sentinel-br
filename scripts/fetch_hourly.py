"""
fetch_hourly.py — Candles horários (1h) para detecção intraday.

Cobertura: últimos 30 dias × 24h = 720 pontos por série.
Venues/pares (5 séries iniciais; foco nos 3 maiores BR players):
 - Binance Spot:  BTCBRL, USDTBRL
 - Mercado Bitcoin v4: BTC-BRL, USDT-BRL
 - Foxbit v3: btcbrl

Saída: data/ohlcv_hourly.json

Schema: {
  "generated_at": iso,
  "window": {"from": "...", "to": "...", "days": 30},
  "hourly": {
    "<venue>": {
      "<symbol>": [{ts, date, hour_utc, open, high, low, close,
                    volume_base, volume_quote}, ...],
      ...
    }
  }
}
"""
from __future__ import annotations
import json, time, datetime as dt
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)

UA = "Mozilla/5.0 (sentinel-br-hourly/0.1)"
DAYS = 30


def http_get(url: str, params: dict | None = None, retries: int = 3, sleep: float = 1.0):
    if params:
        url = f"{url}?{urlencode(params)}"
    last_err = None
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
            with urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as e:
            last_err = e
            time.sleep(sleep * (attempt + 1))
    raise RuntimeError(f"falha em {url}: {last_err}")


def row_from_ts(ts_ms: int) -> dict:
    """Helper: transforma timestamp em (date, hour_utc) canônicos."""
    d = dt.datetime.utcfromtimestamp(ts_ms / 1000)
    return {"date": d.strftime("%Y-%m-%d"), "hour_utc": d.hour}


# ---------- Binance ----------
BINANCE = "https://data-api.binance.vision/api/v3/klines"

def fetch_binance_hourly(symbol: str, start_ms: int, end_ms: int):
    out = []
    cursor = start_ms
    while cursor < end_ms:
        chunk = http_get(BINANCE, {
            "symbol":    symbol,
            "interval":  "1h",
            "startTime": cursor,
            "endTime":   end_ms,
            "limit":     1000,
        })
        if not chunk:
            break
        for k in chunk:
            ts = int(k[0])
            hd = row_from_ts(ts)
            out.append({
                "ts":            ts,
                "date":          hd["date"],
                "hour_utc":      hd["hour_utc"],
                "open":          float(k[1]),
                "high":          float(k[2]),
                "low":           float(k[3]),
                "close":         float(k[4]),
                "volume_base":   float(k[5]),
                "volume_quote":  float(k[7]),
                "trades":        int(k[8]),
            })
        cursor = int(chunk[-1][0]) + 3_600_000  # +1h
        if len(chunk) < 1000:
            break
        time.sleep(0.2)
    return out


# ---------- Mercado Bitcoin v4 ----------
MB = "https://api.mercadobitcoin.net/api/v4/candles"

def fetch_mb_hourly(symbol: str, start_s: int, end_s: int):
    """MB: resolution=60 (minutos) = 1h. countback pra paginar p/ trás."""
    out = []
    cursor_end = end_s
    while True:
        data = http_get(MB, {
            "symbol":     symbol,
            "resolution": "1h",     # MB v4 aceita: 1m, 15m, 1h, 3h, 1d, 1w, 1M
            "to":         cursor_end,
            "countback":  500,
        })
        if not data or not data.get("t"):
            break
        n = len(data["t"])
        for i in range(n):
            ts = int(data["t"][i])
            if ts < start_s:
                continue
            hd = row_from_ts(ts * 1000)
            close = float(data["c"][i])
            vol_b = float(data["v"][i])
            out.append({
                "ts":            ts * 1000,
                "date":          hd["date"],
                "hour_utc":      hd["hour_utc"],
                "open":          float(data["o"][i]),
                "high":          float(data["h"][i]),
                "low":           float(data["l"][i]),
                "close":         close,
                "volume_base":   vol_b,
                "volume_quote":  vol_b * close,   # MB não retorna quote
                "trades":        None,
            })
        earliest = int(data["t"][0])
        if earliest <= start_s:
            break
        cursor_end = earliest - 1
        time.sleep(0.2)
    # dedup & sort
    seen = set(); dedup = []
    for row in sorted(out, key=lambda r: r["ts"]):
        if row["ts"] in seen: continue
        seen.add(row["ts"]); dedup.append(row)
    return dedup


# ---------- Foxbit v3 ----------
FOXBIT = "https://api.foxbit.com.br/rest/v3/markets/{sym}/candlesticks"

def fetch_foxbit_hourly(symbol: str, start_ms: int, end_ms: int):
    out = []
    cursor = start_ms
    while cursor < end_ms:
        data = http_get(FOXBIT.format(sym=symbol), {
            "interval":   "1h",
            "start_time": cursor,
            "end_time":   end_ms,
            "limit":      500,
        })
        if not data:
            break
        for k in data:
            try:
                ts = int(k[0])
                hd = row_from_ts(ts)
                out.append({
                    "ts":            ts,
                    "date":          hd["date"],
                    "hour_utc":      hd["hour_utc"],
                    "open":          float(k[1]),
                    "high":          float(k[2]),
                    "low":           float(k[3]),
                    "close":         float(k[4]),
                    "volume_base":   float(k[6]),
                    "volume_quote":  float(k[7]),
                    "trades":        int(k[8]) if len(k) > 8 else None,
                })
            except Exception:
                continue
        cursor = int(data[-1][0]) + 3_600_000
        if len(data) < 500:
            break
        time.sleep(0.2)
    seen = set(); dedup = []
    for row in sorted(out, key=lambda r: r["ts"]):
        if row["ts"] in seen: continue
        seen.add(row["ts"]); dedup.append(row)
    return dedup


def main():
    now = dt.datetime.utcnow()
    start = now - dt.timedelta(days=DAYS)
    start_ms = int(start.timestamp() * 1000)
    end_ms   = int(now.timestamp() * 1000)
    start_s  = start_ms // 1000
    end_s    = end_ms // 1000

    out = {
        "generated_at": now.isoformat() + "Z",
        "window": {"from": start.strftime("%Y-%m-%d"),
                   "to":   now.strftime("%Y-%m-%d"),
                   "days": DAYS},
        "hourly": {},
    }

    print(f"[hourly 1h / últimos {DAYS}d]")

    print("[1/3] Binance BRL...")
    bn = {}
    for sym in ("BTCBRL", "USDTBRL"):
        try:
            bn[sym] = fetch_binance_hourly(sym, start_ms, end_ms)
            print(f"    {sym}: {len(bn[sym])} horas")
        except Exception as e:
            print(f"    {sym} FAIL: {e}")
            bn[sym] = []
    out["hourly"]["binance_brl"] = bn

    print("[2/3] Mercado Bitcoin...")
    mb = {}
    for sym in ("BTC-BRL", "USDT-BRL"):
        try:
            mb[sym] = fetch_mb_hourly(sym, start_s, end_s)
            print(f"    {sym}: {len(mb[sym])} horas")
        except Exception as e:
            print(f"    {sym} FAIL: {e}")
            mb[sym] = []
    out["hourly"]["mercado_bitcoin"] = mb

    print("[3/3] Foxbit...")
    fx = {}
    for sym in ("btcbrl",):
        try:
            fx[sym] = fetch_foxbit_hourly(sym, start_ms, end_ms)
            print(f"    {sym}: {len(fx[sym])} horas")
        except Exception as e:
            print(f"    {sym} FAIL: {e}")
            fx[sym] = []
    out["hourly"]["foxbit"] = fx

    out_path = DATA / "ohlcv_hourly.json"
    out_path.write_text(json.dumps(out))
    n_total = sum(len(s) for v in out["hourly"].values() for s in v.values())
    print(f"\nsalvo em {out_path} ({out_path.stat().st_size/1024:.1f} KB, {n_total} horas totais)")


if __name__ == "__main__":
    main()
