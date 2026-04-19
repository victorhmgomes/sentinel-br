"""
fetch_data.py — Conectores para dados públicos de exchanges e on-chain.

Coleta 1 ano de OHLCV diário (volume em unidades e em quote currency) de:
 - Binance Spot (BTCUSDT, ETHUSDT, USDTTRY placeholder global)
 - Mercado Bitcoin v4 (BTC-BRL, USDT-BRL, ETH-BRL)
 - Foxbit v3 (btcbrl, usdtbrl, ethbrl)
 - Blockstream / mempool.space (on-chain BTC: txs, hashrate, mempool)

Saída: cryptofraud/data/raw.json
"""

from __future__ import annotations
import json
import time
import datetime as dt
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)

UA = "Mozilla/5.0 (cryptofraud-prototype/0.1)"


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


# ---------- Binance ----------
BINANCE = "https://data-api.binance.vision/api/v3/klines"

def fetch_binance_daily(symbol: str, start_ms: int, end_ms: int):
    """Retorna lista de dicts {ts, open, high, low, close, volume_base, volume_quote, trades}."""
    out = []
    cursor = start_ms
    while cursor < end_ms:
        chunk = http_get(BINANCE, {
            "symbol": symbol,
            "interval": "1d",
            "startTime": cursor,
            "endTime": end_ms,
            "limit": 1000,
        })
        if not chunk:
            break
        for k in chunk:
            out.append({
                "ts": int(k[0]),
                "date": dt.datetime.utcfromtimestamp(k[0] / 1000).strftime("%Y-%m-%d"),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume_base": float(k[5]),
                "volume_quote": float(k[7]),
                "trades": int(k[8]),
            })
        cursor = int(chunk[-1][0]) + 86_400_000
        if len(chunk) < 1000:
            break
        time.sleep(0.2)
    return out


# ---------- Mercado Bitcoin v4 ----------
MB = "https://api.mercadobitcoin.net/api/v4/candles"

def fetch_mb_daily(symbol: str, start_s: int, end_s: int):
    """MB retorna até ~1000 pontos por request. Usamos countback."""
    out = []
    cursor_end = end_s
    while True:
        data = http_get(MB, {
            "symbol": symbol,
            "resolution": "1d",
            "to": cursor_end,
            "countback": 300,
        })
        if not data or not data.get("t"):
            break
        n = len(data["t"])
        for i in range(n):
            ts = int(data["t"][i])
            if ts < start_s:
                continue
            out.append({
                "ts": ts * 1000,
                "date": dt.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d"),
                "open": float(data["o"][i]),
                "high": float(data["h"][i]),
                "low": float(data["l"][i]),
                "close": float(data["c"][i]),
                "volume_base": float(data["v"][i]),
                "volume_quote": float(data["v"][i]) * float(data["c"][i]),
                "trades": None,
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

def fetch_foxbit_daily(symbol: str, start_ms: int, end_ms: int):
    out = []
    cursor = start_ms
    while cursor < end_ms:
        data = http_get(FOXBIT.format(sym=symbol), {
            "interval": "1d",
            "start_time": cursor,
            "end_time": end_ms,
            "limit": 500,
        })
        if not data:
            break
        for k in data:
            try:
                out.append({
                    "ts": int(k[0]),
                    "date": dt.datetime.utcfromtimestamp(int(k[0]) / 1000).strftime("%Y-%m-%d"),
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume_base": float(k[6]),
                    "volume_quote": float(k[7]),
                    "trades": int(k[8]) if len(k) > 8 else None,
                })
            except Exception:
                continue
        cursor = int(data[-1][0]) + 86_400_000
        if len(data) < 500:
            break
        time.sleep(0.2)
    # dedup
    seen=set(); dedup=[]
    for row in sorted(out, key=lambda r: r["ts"]):
        if row["ts"] in seen: continue
        seen.add(row["ts"]); dedup.append(row)
    return dedup


# ---------- Blockstream / mempool.space ----------
def fetch_btc_onchain(days: int = 365):
    """Série diária de hashrate + dificuldade + pools agregados (mempool.space)."""
    try:
        hashrate = http_get(f"https://mempool.space/api/v1/mining/hashrate/1y")
        return {
            "hashrates": hashrate.get("hashrates", [])[-days:],
            "difficulty": hashrate.get("difficulty", [])[-days:],
            "currentHashrate": hashrate.get("currentHashrate"),
            "currentDifficulty": hashrate.get("currentDifficulty"),
        }
    except Exception as e:
        print(f"  [on-chain warn] {e}")
        return {"hashrates": [], "difficulty": []}


def main():
    now = dt.datetime.utcnow()
    start = now - dt.timedelta(days=365)
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(now.timestamp() * 1000)
    start_s = start_ms // 1000
    end_s = end_ms // 1000

    raw = {
        "generated_at": now.isoformat() + "Z",
        "window": {"from": start.strftime("%Y-%m-%d"), "to": now.strftime("%Y-%m-%d")},
        "sources": {},
    }

    print("[1/3] Binance...")
    bn = {}
    for sym in ("BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"):
        try:
            bn[sym] = fetch_binance_daily(sym, start_ms, end_ms)
            print(f"    {sym}: {len(bn[sym])} dias")
        except Exception as e:
            print(f"    {sym} FAIL: {e}")
            bn[sym] = []
    raw["sources"]["binance"] = bn

    print("[1b/3] Binance BRL (venue BR)...")
    bnbr = {}
    for sym in ("BTCBRL", "ETHBRL", "USDTBRL", "SOLBRL"):
        try:
            bnbr[sym] = fetch_binance_daily(sym, start_ms, end_ms)
            print(f"    {sym}: {len(bnbr[sym])} dias")
        except Exception as e:
            print(f"    {sym} FAIL: {e}")
            bnbr[sym] = []
    raw["sources"]["binance_brl"] = bnbr

    print("[2/3] Mercado Bitcoin...")
    mb = {}
    for sym in ("BTC-BRL", "USDT-BRL", "ETH-BRL", "SOL-BRL"):
        try:
            mb[sym] = fetch_mb_daily(sym, start_s, end_s)
            print(f"    {sym}: {len(mb[sym])} dias")
        except Exception as e:
            print(f"    {sym} FAIL: {e}")
            mb[sym] = []
    raw["sources"]["mercado_bitcoin"] = mb

    print("[2b/3] Foxbit...")
    fx = {}
    for sym in ("btcbrl", "usdtbrl", "ethbrl"):
        try:
            fx[sym] = fetch_foxbit_daily(sym, start_ms, end_ms)
            print(f"    {sym}: {len(fx[sym])} dias")
        except Exception as e:
            print(f"    {sym} FAIL: {e}")
            fx[sym] = []
    raw["sources"]["foxbit"] = fx

    print("[3/3] On-chain BTC...")
    raw["sources"]["onchain_btc"] = fetch_btc_onchain()
    print(f"    hashrate points: {len(raw['sources']['onchain_btc'].get('hashrates', []))}")

    out_path = DATA / "raw.json"
    out_path.write_text(json.dumps(raw))
    print(f"\nsalvo em {out_path} ({out_path.stat().st_size/1024:.1f} KB)")


if __name__ == "__main__":
    main()
