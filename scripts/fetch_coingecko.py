"""
fetch_coingecko.py — puxa volume 30d por exchange do CoinGecko (free, sem key)
como validação cruzada do volume multi-exchange. Também grava o ranking atual 24h.

Endpoints:
  GET /exchanges/{id}/volume_chart?days=30  → [[ts, vol_btc], ...]
  GET /exchanges?per_page=250&page=1         → metadata (trust_score, volume atual, país)
"""
import json, time, datetime as dt, urllib.request, urllib.error
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = Path(__file__).resolve().parent.parent
OUT  = ROOT / "data" / "coingecko.json"

UA = {"User-Agent": "sentinel-br/1.0", "Accept": "application/json"}

# id CoinGecko → nome de exibição (16 exchanges de interesse)
EXCHANGES = {
    # BR
    "mercado_bitcoin": "Mercado Bitcoin",
    "foxbit":          "Foxbit",
    "novadax":         "NovaDAX",
    "bitso":           "Bitso",
    # globais
    "binance":         "Binance",
    "bybit_spot":      "Bybit",
    "okex":            "OKX",
    "gdax":            "Coinbase",
    "kraken":          "Kraken",
    "kucoin":          "KuCoin",
    "bitget":          "Bitget",
    "mxc":             "MEXC",
    "huobi":           "HTX",
    "gate":            "Gate",
    "bitfinex":        "Bitfinex",
    "bitstamp":        "Bitstamp",
}

BASE = "https://api.coingecko.com/api/v3"


def get_json(url, timeout=15, retries=3):
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            last = f"HTTP {e.code}"
            if e.code == 429:
                time.sleep(8)
            else:
                time.sleep(1)
        except Exception as e:
            last = f"{type(e).__name__}: {e}"
            time.sleep(1)
    raise RuntimeError(f"fail after {retries}: {last}")


def fetch_volume_chart(eid: str):
    data = get_json(f"{BASE}/exchanges/{eid}/volume_chart?days=30")
    # CoinGecko retorna string para não perder precisão → converter
    out = []
    for ts_ms, vol_str in data:
        d = dt.datetime.fromtimestamp(ts_ms / 1000, tz=dt.timezone.utc).date().isoformat()
        out.append({"date": d, "vol_btc": float(vol_str)})
    return out


def main():
    t0 = time.time()
    print(f"[coingecko] coletando volume 30d de {len(EXCHANGES)} exchanges…")

    # 1) ranking + metadata
    try:
        rank = get_json(f"{BASE}/exchanges?per_page=250&page=1")
    except Exception as e:
        print(f"  ranking falhou: {e}")
        rank = []

    meta = {}
    for e in rank:
        if e["id"] in EXCHANGES:
            meta[e["id"]] = {
                "name":          e["name"],
                "country":       e.get("country"),
                "trust_score":   e.get("trust_score"),
                "year":          e.get("year_established"),
                "url":           e.get("url"),
                "vol_24h_btc":   e.get("trade_volume_24h_btc"),
                "vol_24h_btc_normalized": e.get("trade_volume_24h_btc_normalized"),
            }

    # 2) volume 30d — sequencial (CoinGecko free é agressivo com rate limit)
    series = {}
    for eid, nome in EXCHANGES.items():
        try:
            s = fetch_volume_chart(eid)
            series[eid] = s
            last_vol = s[-1]["vol_btc"] if s else None
            print(f"  {nome:20s} {eid:20s} n={len(s):3d}  último={last_vol:,.0f} BTC")
        except Exception as e:
            series[eid] = []
            print(f"  {nome:20s} {eid:20s} ERRO: {e}")
        time.sleep(2.5)  # ~24 req/min (limite demo ≈ 30/min)

    # 3) agregados — volume médio / total 30d
    stats = {}
    for eid, s in series.items():
        if not s:
            continue
        vals = [r["vol_btc"] for r in s]
        stats[eid] = {
            "name":     EXCHANGES[eid],
            "n_days":   len(vals),
            "avg_btc":  sum(vals) / len(vals),
            "sum_btc":  sum(vals),
            "max_btc":  max(vals),
            "min_btc":  min(vals),
            "last_btc": vals[-1],
        }

    out = {
        "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        "source":       "coingecko",
        "days":         30,
        "exchanges":    EXCHANGES,
        "meta":         meta,
        "series":       series,
        "stats":        stats,
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, default=str))
    dt_ms = (time.time() - t0) * 1000
    print(f"[coingecko] gravado em {OUT} ({OUT.stat().st_size/1024:.1f} KB · {dt_ms:.0f} ms)")


if __name__ == "__main__":
    main()
