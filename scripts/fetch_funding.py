"""
fetch_funding.py — Funding rate de perpétuos BTC/ETH em 3 venues.

Funding rate é o pagamento periódico (a cada 8h em geral) entre longs e shorts
em contratos perpétuos. Quando funding fica MUITO negativo, significa que há
forte pressão vendedora (mais shorts pagando longs) — típico em fases de
cash-out forçado ou em pânico defensivo após incidente.

Endpoints (sem key):
  Binance: https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=1000
  Bybit:   https://api.bybit.com/v5/market/funding/history?category=linear&symbol=BTCUSDT&limit=200
  OKX:     https://www.okx.com/api/v5/public/funding-rate-history?instId=BTC-USDT-SWAP&limit=100

Output: data/funding.json com séries diárias agregadas (média das 3 leituras/dia)
        para BTC e ETH em 3 venues, +alertas Z-score.
"""
from __future__ import annotations
import json, time, datetime as dt, urllib.request, urllib.error
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent
OUT  = ROOT / "data" / "funding.json"
UA   = {"User-Agent": "sentinel-br/1.0", "Accept": "application/json"}


def get_json(url, timeout=15, retries=3):
    last = None
    for _ in range(retries):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            last = f"HTTP {e.code}"
            time.sleep(1 if e.code != 429 else 6)
        except Exception as e:
            last = f"{type(e).__name__}: {e}"
            time.sleep(1)
    raise RuntimeError(f"fail after {retries}: {last}")


def to_date(ts_ms):
    return dt.datetime.fromtimestamp(int(ts_ms) / 1000, tz=dt.timezone.utc).date().isoformat()


def fetch_binance(symbol, limit=1000):
    """Binance retorna ~3 amostras/dia; 1000 ≈ 333 dias."""
    rows = get_json(f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={symbol}&limit={limit}")
    return [(to_date(r["fundingTime"]), float(r["fundingRate"])) for r in rows]


def fetch_bybit(symbol, limit=200):
    d = get_json(f"https://api.bybit.com/v5/market/funding/history?category=linear&symbol={symbol}&limit={limit}")
    out = []
    for r in d.get("result", {}).get("list", []):
        out.append((to_date(r["fundingRateTimestamp"]), float(r["fundingRate"])))
    return out


def fetch_okx(inst, limit=100):
    """OKX limita a 100; paginamos com `before` para puxar mais."""
    out = []
    cursor = ""
    pages = 0
    while pages < 6:  # 6 × 100 = 600 amostras (~200 dias)
        url = f"https://www.okx.com/api/v5/public/funding-rate-history?instId={inst}&limit={limit}"
        if cursor:
            url += f"&after={cursor}"
        d = get_json(url)
        rows = d.get("data", [])
        if not rows: break
        for r in rows:
            out.append((to_date(r["fundingTime"]), float(r.get("realizedRate") or r["fundingRate"])))
        cursor = rows[-1]["fundingTime"]
        pages += 1
        time.sleep(0.3)
    return out


def daily_mean(samples):
    """[(date, rate), ...] → {date: mean_rate}"""
    g = defaultdict(list)
    for d_, r in samples:
        g[d_].append(r)
    return {d_: sum(rs)/len(rs) for d_, rs in g.items()}


def rolling_z(dates, vals, window=30):
    import statistics as st
    out = []
    for i, v in enumerate(vals):
        if i < window: out.append(None); continue
        win = vals[i - window:i]
        mu = sum(win)/window
        sd = st.pstdev(win) or 1e-12
        out.append(round((v - mu)/sd, 3))
    return out


VENUES = {
    "BTC": {
        "Binance": ("BTCUSDT",       fetch_binance, 1000),
        "Bybit":   ("BTCUSDT",       fetch_bybit,   200),
        "OKX":     ("BTC-USDT-SWAP", fetch_okx,     100),
    },
    "ETH": {
        "Binance": ("ETHUSDT",       fetch_binance, 1000),
        "Bybit":   ("ETHUSDT",       fetch_bybit,   200),
        "OKX":     ("ETH-USDT-SWAP", fetch_okx,     100),
    },
}


def main():
    t0 = time.time()
    out_series = {}
    alerts = []

    for asset, venue_map in VENUES.items():
        out_series[asset] = {}
        for venue, (sym, fn, lim) in venue_map.items():
            try:
                samples = fn(sym, lim)
                daily   = daily_mean(samples)
                ds      = sorted(daily.keys())
                vals    = [daily[d] for d in ds]
                zs      = rolling_z(ds, vals, window=30)
                out_series[asset][venue] = [
                    {"date": d, "rate": v, "z": z} for d, v, z in zip(ds, vals, zs)
                ]
                # alertas: funding extremamente negativo (Z ≤ -3 OU rate ≤ -0.05% absoluto)
                for d, v, z in zip(ds, vals, zs):
                    if (z is not None and z <= -3) or v <= -0.0005:
                        sev = "critical" if (v <= -0.001) else "high" if (v <= -0.0005 or (z and z <= -4)) else "medium"
                        alerts.append({
                            "asset": asset, "venue": venue, "date": d,
                            "rate": round(v, 6), "z": z, "severity": sev,
                        })
                print(f"  {asset:3s} {venue:8s} {sym:18s} n_days={len(ds):3d}  "
                      f"min={min(vals):.6f}  max={max(vals):.6f}")
            except Exception as e:
                out_series[asset][venue] = []
                print(f"  {asset:3s} {venue:8s} ERRO: {e}")
            time.sleep(0.4)

    # dedupe alertas por (asset, venue, date), mantendo a maior severidade
    sev_rank = {"medium":1, "high":2, "critical":3}
    keyed = {}
    for a in alerts:
        k = (a["asset"], a["venue"], a["date"])
        if k not in keyed or sev_rank[a["severity"]] > sev_rank[keyed[k]["severity"]]:
            keyed[k] = a
    alerts = sorted(keyed.values(), key=lambda x: (x["date"], x["asset"]))

    out = {
        "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        "series":       out_series,
        "alerts":       alerts,
        "summary": {
            "n_alerts":          len(alerts),
            "n_critical":        sum(1 for a in alerts if a["severity"] == "critical"),
            "n_high":            sum(1 for a in alerts if a["severity"] == "high"),
            "min_rate":          min((p["rate"] for v in out_series.values() for vv in v.values() for p in vv),
                                     default=0),
            "max_rate":          max((p["rate"] for v in out_series.values() for vv in v.values() for p in vv),
                                     default=0),
        },
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, default=str))
    elapsed = int((time.time() - t0) * 1000)
    print(f"[funding] gravado em {OUT} ({OUT.stat().st_size/1024:.1f} KB · {elapsed}ms)")
    print(f"          alerts: {len(alerts)} ({out['summary']['n_critical']} critical, "
          f"{out['summary']['n_high']} high)")


if __name__ == "__main__":
    main()
