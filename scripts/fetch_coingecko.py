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


def get_json(url, timeout=8, retries=2):
    """CoinGecko free é rate-limitado; prioriza falhar rápido em vez de
    segurar o workflow. Em 429, devolve None (pula em vez de esperar)."""
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            last = f"HTTP {e.code}"
            # Em rate-limit ou indisponibilidade, desiste — stale > empty
            if e.code in (429, 503, 502, 504):
                return None
            time.sleep(0.5)
        except Exception as e:
            last = f"{type(e).__name__}: {e}"
            time.sleep(0.5)
    raise RuntimeError(f"fail after {retries}: {last}")


def fetch_volume_chart(eid: str):
    data = get_json(f"{BASE}/exchanges/{eid}/volume_chart?days=30")
    if data is None:
        return None  # rate-limited / indisponível → caller mantém stale
    out = []
    for ts_ms, vol_str in data:
        d = dt.datetime.fromtimestamp(ts_ms / 1000, tz=dt.timezone.utc).date().isoformat()
        out.append({"date": d, "vol_btc": float(vol_str)})
    return out


def main():
    t0 = time.time()
    BUDGET = 180.0  # segundos — se estourar, aborta e preserva arquivo anterior
    print(f"[coingecko] coletando volume 30d de {len(EXCHANGES)} exchanges…")

    # 1) ranking + metadata (opcional — se falhar, segue em frente)
    rank = []
    try:
        r = get_json(f"{BASE}/exchanges?per_page=250&page=1")
        rank = r or []
        if not rank:
            print("  ranking: rate-limited, pulando (mantém meta anterior)")
    except Exception as e:
        print(f"  ranking falhou: {e}")

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

    # 2) volume 30d — sequencial (CoinGecko free é agressivo com rate limit).
    # series[eid] = None → indisponível (mantém do anterior); [] → sem dados
    series = {}
    n_ok = n_skip = 0
    for eid, nome in EXCHANGES.items():
        if time.time() - t0 > BUDGET:
            print(f"  ⏱ budget {BUDGET:.0f}s estourado — abortando restantes")
            break
        try:
            s = fetch_volume_chart(eid)
            if s is None:
                series[eid] = None
                n_skip += 1
                print(f"  {nome:20s} {eid:20s} rate-limited")
            else:
                series[eid] = s
                n_ok += 1
                last_vol = s[-1]["vol_btc"] if s else None
                print(f"  {nome:20s} {eid:20s} n={len(s):3d}  último={last_vol:,.0f} BTC")
        except Exception as e:
            series[eid] = None
            n_skip += 1
            print(f"  {nome:20s} {eid:20s} ERRO: {e}")
        time.sleep(1.5)  # ~40 req/min — dentro do limite demo

    print(f"[coingecko] ok={n_ok} skip={n_skip}")

    # Se TUDO falhou, não sobrescreve — preserva snapshot anterior
    if n_ok == 0:
        if OUT.exists():
            print(f"[coingecko] ⚠ todos falharam — mantendo arquivo anterior ({OUT})")
            return
        # Se não existe nenhum arquivo anterior, grava vazio pra não quebrar build
        print("[coingecko] ⚠ todos falharam e sem arquivo anterior — gravando shell vazio")

    # Se algumas exchanges rate-limited (series[eid] is None), tenta manter
    # a série anterior do disco pra não sumir com a exchange do dashboard.
    if any(v is None for v in series.values()) and OUT.exists():
        try:
            prev = json.loads(OUT.read_text(encoding="utf-8"))
            prev_series = prev.get("series", {})
            prev_meta = prev.get("meta", {})
            for eid, v in list(series.items()):
                if v is None and eid in prev_series and prev_series[eid]:
                    series[eid] = prev_series[eid]
                    print(f"  ↻ reusando série anterior de {eid} ({len(series[eid])} dias)")
            # Mescla meta: se ranking desta rodada veio vazio, usa a anterior
            if not meta and prev_meta:
                meta = prev_meta
                print(f"  ↻ reusando meta anterior ({len(meta)} exchanges)")
        except Exception as e:
            print(f"  [warn] não consegui ler arquivo anterior: {e}")

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
