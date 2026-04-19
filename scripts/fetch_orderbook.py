"""
fetch_orderbook.py — Snapshot consolidado de múltiplos pares em BRL.

Pares suportados: BTC-BRL, ETH-BRL, USDT-BRL, SOL-BRL.
Venues: 4 BR + até 12 globais (via conversão USDT-BRL).

Cada run busca em paralelo (ThreadPool) os books de todos os venues/pares e
produz data/orderbook.json:
{
  generated_at, usdtbrl, pairs: {
     "BTC-BRL": {venues_total, venues_online, books, health, consolidated, vwap, depth_chart, arb},
     "ETH-BRL": {...},
     ...
  }
}
"""
from __future__ import annotations
import json, time, datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)

UA = "Mozilla/5.0 (sentinel-br-prototype/0.3)"
TIMEOUT = 8
DEPTH = 50
DEGRADED_MS = 1000
STALE_MS    = 3500


def http_json(url):
    t0 = time.perf_counter()
    req = Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    with urlopen(req, timeout=TIMEOUT) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    return data, int((time.perf_counter() - t0) * 1000)


def quality(lat_ms, age_ms):
    if lat_ms < DEGRADED_MS and age_ms < DEGRADED_MS: return "online"
    if lat_ms < STALE_MS and age_ms < STALE_MS:        return "degraded"
    return "stale"


def usdtbrl_rate():
    try:
        d, _ = http_json("https://api.mercadobitcoin.net/api/v4/tickers?symbols=USDT-BRL")
        return float(d[0]["last"])
    except Exception:
        return 5.00


# ============================================================
# Per-venue adapters — parametrizados por par
# ============================================================

def ad_mb(pair_brl):
    d, lat = http_json(f"https://api.mercadobitcoin.net/api/v4/{pair_brl}/orderbook?limit={DEPTH}")
    ts = int(d.get("timestamp", 0)) // 1_000_000_000
    return "Mercado Bitcoin", "BR", "REST", pair_brl, d["bids"], d["asks"], 1.0, ts, lat


def ad_foxbit(pair_fx):
    d, lat = http_json(f"https://api.foxbit.com.br/rest/v3/markets/{pair_fx}/orderbook")
    ts = int(d.get("timestamp", 0)) // 1000
    return "Foxbit", "BR", "REST", pair_fx, d["bids"], d["asks"], 1.0, ts, lat


def ad_novadax(pair_nd):
    d, lat = http_json(f"https://api.novadax.com/v1/market/depth?symbol={pair_nd}&limit={DEPTH}")
    p = d["data"]; ts = int(p.get("timestamp", 0)) // 1000
    return "NovaDAX", "BR", "REST", pair_nd, p["bids"], p["asks"], 1.0, ts, lat


def ad_bitpreco(pair_bp):
    d, lat = http_json(f"https://api.bitpreco.com/{pair_bp}/orderbook")
    if not d.get("success"):
        raise ValueError("bitpreco: success=false")
    # níveis em formato dict {price, amount}
    bids = [[r["price"], r["amount"]] for r in d["bids"][:DEPTH]]
    asks = [[r["price"], r["amount"]] for r in d["asks"][:DEPTH]]
    return "BitPreço", "BR", "REST", pair_bp, bids, asks, 1.0, int(time.time()), lat


def ad_ripio(pair_rp):
    d, lat = http_json(f"https://api.ripiotrade.co/v4/public/orders/level-2?pair={pair_rp}&limit={DEPTH}")
    if d.get("error_code"):
        raise ValueError(f"ripio: {d.get('message')}")
    p = d.get("data") or {}
    bids = [[r["price"], r["amount"]] for r in p.get("bids", [])[:DEPTH]]
    asks = [[r["price"], r["amount"]] for r in p.get("asks", [])[:DEPTH]]
    return "Ripio Trade", "BR", "REST", pair_rp, bids, asks, 1.0, int(time.time()), lat


def ad_bitso(pair_bs):
    d, lat = http_json(f"https://api.bitso.com/v3/order_book/?book={pair_bs}&aggregate=true")
    p = d["payload"]
    bids = [[r["price"], r["amount"]] for r in p["bids"][:DEPTH]]
    asks = [[r["price"], r["amount"]] for r in p["asks"][:DEPTH]]
    ts = int(dt.datetime.fromisoformat(p["updated_at"].replace("Z","+00:00")).timestamp())
    return "Bitso BR", "BR", "REST", pair_bs, bids, asks, 1.0, ts, lat


def ad_binance(sym, conv):
    d, lat = http_json(f"https://data-api.binance.vision/api/v3/depth?symbol={sym}&limit={DEPTH}")
    return "Binance", "Global", "REST", sym, d["bids"], d["asks"], conv, int(time.time()), lat


def ad_okx(inst, conv):
    d, lat = http_json(f"https://www.okx.com/api/v5/market/books?instId={inst}&sz={DEPTH}")
    p = d["data"][0]; ts = int(p["ts"]) // 1000
    return "OKX", "Global", "REST", inst, p["bids"], p["asks"], conv, ts, lat


def ad_bybit(sym, conv):
    d, lat = http_json(f"https://api.bybit.com/v5/market/orderbook?category=spot&symbol={sym}&limit={DEPTH}")
    r = d["result"]; ts = int(r["ts"]) // 1000
    return "Bybit", "Global", "REST", sym, r["b"], r["a"], conv, ts, lat


def ad_bitget(sym, conv):
    d, lat = http_json(f"https://api.bitget.com/api/v2/spot/market/orderbook?symbol={sym}&type=step0&limit={DEPTH}")
    p = d["data"]
    return "Bitget", "Global", "REST", sym, p["bids"], p["asks"], conv, int(time.time()), lat


def ad_kraken(pair, conv):
    d, lat = http_json(f"https://api.kraken.com/0/public/Depth?pair={pair}&count={DEPTH}")
    k = list(d["result"].keys())[0]; r = d["result"][k]
    return "Kraken", "Global", "REST", pair, r["bids"], r["asks"], conv, int(time.time()), lat


def ad_kucoin(sym, conv):
    d, lat = http_json(f"https://api.kucoin.com/api/v1/market/orderbook/level2_20?symbol={sym}")
    p = d["data"]; ts = int(p["time"]) // 1000
    return "KuCoin", "Global", "REST", sym, p["bids"], p["asks"], conv, ts, lat


def ad_coinbase(pid, conv):
    d, lat = http_json(f"https://api.exchange.coinbase.com/products/{pid}/book?level=2")
    return "Coinbase", "Global", "REST", pid, d["bids"][:DEPTH], d["asks"][:DEPTH], conv, int(time.time()), lat


def ad_bitstamp(pid, conv):
    d, lat = http_json(f"https://www.bitstamp.net/api/v2/order_book/{pid}/")
    ts = int(d["timestamp"])
    return "Bitstamp", "Global", "REST", pid, d["bids"][:DEPTH], d["asks"][:DEPTH], conv, ts, lat


def ad_bitfinex(sym, conv):
    d, lat = http_json(f"https://api-pub.bitfinex.com/v2/book/{sym}/P0?len=25")
    bids, asks = [], []
    for row in d:
        p, _cnt, amt = row[0], row[1], row[2]
        (bids if amt > 0 else asks).append([p, abs(amt)])
    bids.sort(key=lambda r: -r[0]); asks.sort(key=lambda r: r[0])
    return "Bitfinex", "Global", "REST", sym, bids, asks, conv, int(time.time()), lat


def ad_gate(sym, conv):
    d, lat = http_json(f"https://api.gateio.ws/api/v4/spot/order_book?currency_pair={sym}&limit={DEPTH}")
    ts = int(d.get("update", time.time()*1000)) // 1000
    return "Gate.io", "Global", "REST", sym, d["bids"], d["asks"], conv, ts, lat


def ad_htx(sym, conv):
    d, lat = http_json(f"https://api.huobi.pro/market/depth?symbol={sym}&type=step0&depth=20")
    tick = d["tick"]; ts = int(tick["ts"]) // 1000
    return "HTX", "Global", "REST", sym, tick["bids"], tick["asks"], conv, ts, lat


def ad_mexc(sym, conv):
    d, lat = http_json(f"https://api.mexc.com/api/v3/depth?symbol={sym}&limit={DEPTH}")
    ts = int(d.get("timestamp", time.time()*1000)) // 1000
    return "MEXC", "Global", "REST", sym, d["bids"], d["asks"], conv, ts, lat


# ============================================================
# Mapa de pares por venue.  None = venue não oferece esse par.
# ============================================================
# Para cada venue global, preferimos o par nativo em BRL quando existir (Binance,
# Bybit, OKX, KuCoin, HTX, MEXC já listam spot BRL direto — é onde o cash-out
# brasileiro ocorre em liquidez global). Para Kraken/Coinbase/Bitstamp/Bitfinex/
# Gate/Bitget cai no par USDT e aplica conversão USDT-BRL.
PAIR_SPEC = {
    "BTC-BRL": {
        "MB": "BTC-BRL", "Foxbit": "btcbrl", "NovaDAX": "BTC_BRL",
        "BitPreço": "btc-brl", "Ripio": "BTC_BRL", "Bitso": "btc_brl",
        "Binance": "BTCBRL",    # direto
        "Bybit":   "BTCBRL",    # direto
        "OKX":     "BTC-BRL",   # direto
        "KuCoin":  "BTC-BRL",   # direto
        "MEXC":    "BTCBRL",    # direto
        "Bitget":  "BTCUSDT",   "Kraken": "XBTUSDT",
        "Coinbase":"BTC-USD",   "Bitstamp":"btcusd",
        "Bitfinex":"tBTCUSD",   "Gate.io": "BTC_USDT",
        "HTX":     "btcusdt",   # HTX não tem par BRL — cai via USDT
    },
    "ETH-BRL": {
        "MB": "ETH-BRL", "Foxbit": "ethbrl", "NovaDAX": "ETH_BRL",
        "BitPreço": "eth-brl", "Ripio": "ETH_BRL", "Bitso": "eth_brl",
        "Binance": "ETHBRL",    # direto
        "Bybit":   "ETHBRL",    # direto
        "OKX":     "ETH-BRL",   # direto
        "KuCoin":  "ETH-BRL",   # direto
        "MEXC":    "ETHBRL",    # direto
        "Bitget":  "ETHUSDT",   "Kraken": "ETHUSDT",
        "Coinbase":"ETH-USD",   "Bitstamp":"ethusd",
        "Bitfinex":"tETHUSD",   "Gate.io": "ETH_USDT",
        "HTX":     "ethusdt",
    },
    "USDT-BRL": {
        "MB": "USDT-BRL", "Foxbit": "usdtbrl", "NovaDAX": "USDT_BRL",
        "BitPreço": "usdt-brl", "Ripio": "USDT_BRL",
        "Binance": "USDTBRL",   # direto
        "Bybit":   "USDTBRL",   # direto
        "OKX":     "USDT-BRL",  # direto
        # HTX e MEXC não expõem USDT-BRL público (err: invalid symbol / HTTP 400)
    },
    "SOL-BRL": {
        "MB": "SOL-BRL", "Foxbit": "solbrl", "NovaDAX": "SOL_BRL",
        "BitPreço": "sol-brl", "Ripio": "SOL_BRL",
        "Binance": "SOLBRL",    # direto
        "Bybit":   "SOLBRL",    # direto
        "OKX":     "SOL-BRL",   # direto
        "MEXC":    "SOLBRL",    # direto
        "Bitget":  "SOLUSDT",   "Kraken": "SOLUSDT",
        "KuCoin":  "SOL-USDT",  "Coinbase":"SOL-USD",
        "Bitfinex":"tSOLUSD",   "Gate.io": "SOL_USDT",
        "HTX":     "solusdt",
    },
}

ADAPTER = {
    "MB":       lambda sym, conv: ad_mb(sym),
    "Foxbit":   lambda sym, conv: ad_foxbit(sym),
    "NovaDAX":  lambda sym, conv: ad_novadax(sym),
    "BitPreço": lambda sym, conv: ad_bitpreco(sym),
    "Ripio":    lambda sym, conv: ad_ripio(sym),
    "Bitso":    lambda sym, conv: ad_bitso(sym),
    "Binance":  ad_binance,  "OKX":       ad_okx,
    "Bybit":    ad_bybit,    "Bitget":    ad_bitget,
    "Kraken":   ad_kraken,   "KuCoin":    ad_kucoin,
    "Coinbase": ad_coinbase, "Bitstamp":  ad_bitstamp,
    "Bitfinex": ad_bitfinex, "Gate.io":   ad_gate,
    "HTX":      ad_htx,      "MEXC":      ad_mexc,
}


def _ok(levels, conv):
    out = []
    for r in levels[:DEPTH]:
        try:
            out.append({"price_brl": round(float(r[0]) * conv, 2), "size": float(r[1])})
        except Exception:
            continue
    return out


def run_adapter(venue_key, pair_brl, sym_or_id, conv):
    try:
        fn = ADAPTER[venue_key]
        # Se o símbolo contém "BRL" (qualquer caixa), o book já está em BRL:
        # não precisa converter. Isso cobre BTCBRL, BTC-BRL, btcbrl, USDT-BRL, etc.
        sym_conv = 1.0 if "BRL" in sym_or_id.upper() else conv
        venue, cat, typ, pair_native, bids, asks, own_conv, ts_exch, lat_ms = fn(sym_or_id, sym_conv)
        bids_n = _ok(bids, conv=own_conv)
        asks_n = _ok(asks, conv=own_conv)
        if not bids_n or not asks_n: raise ValueError("livro vazio")
        best_bid = bids_n[0]["price_brl"]; best_ask = asks_n[0]["price_brl"]
        mid = (best_bid + best_ask) / 2
        spread_bps = round((best_ask - best_bid) / mid * 10000, 2) if mid else None
        age_ms = max(0, int(time.time()*1000) - ts_exch*1000) if ts_exch else lat_ms
        return {
            "venue": venue, "category": cat, "type": typ,
            "pair_native": pair_native, "pair_brl": pair_brl,
            "best_bid": best_bid, "best_ask": best_ask, "mid": round(mid, 2),
            "spread_bps": spread_bps, "latency_ms": lat_ms, "age_ms": age_ms,
            "ts_exch": ts_exch, "ts_local": int(time.time()),
            "bids": bids_n[:30], "asks": asks_n[:30],
            "quality": quality(lat_ms, age_ms), "error": None,
        }
    except Exception as e:
        return {"venue": venue_key, "category": "?", "type": "REST",
                "pair_native": sym_or_id, "pair_brl": pair_brl,
                "best_bid": None, "best_ask": None, "mid": None,
                "spread_bps": None, "latency_ms": None, "age_ms": None,
                "ts_exch": None, "ts_local": int(time.time()),
                "bids": [], "asks": [], "quality": "error",
                "error": f"{type(e).__name__}: {e}"}


def build_consolidated(books):
    b, a = [], []
    for x in books:
        if x["quality"] in ("stale", "error"): continue
        for lvl in x["bids"]: b.append({**lvl, "venue": x["venue"]})
        for lvl in x["asks"]: a.append({**lvl, "venue": x["venue"]})
    b.sort(key=lambda r: -r["price_brl"])
    a.sort(key=lambda r:  r["price_brl"])
    top_b = b[0] if b else None
    top_a = a[0] if a else None
    mid = spread = spread_pct = None
    if top_b and top_a:
        mid = (top_b["price_brl"] + top_a["price_brl"]) / 2
        spread = top_a["price_brl"] - top_b["price_brl"]
        spread_pct = round(spread / mid * 100, 4) if mid else None
    return {"best_bid": top_b, "best_ask": top_a, "mid": mid,
            "spread_brl": spread, "spread_pct": spread_pct,
            "bids": b[:200], "asks": a[:200]}


def vwap(levels, target):
    rem = target; cost = 0.0; got = 0.0
    for lvl in levels:
        take = min(lvl["size"], rem)
        cost += take * lvl["price_brl"]; got += take; rem -= take
        if rem <= 1e-9: break
    return None if got <= 0 else {
        "target": target, "filled": round(got, 6),
        "avg_price": round(cost/got, 2), "fully_filled": rem <= 1e-9}


def build_pair(pair_brl, conv, vwap_targets):
    spec = PAIR_SPEC[pair_brl]
    jobs = [(venue, sym) for venue, sym in spec.items()]
    books = [None] * len(jobs)
    with ThreadPoolExecutor(max_workers=min(16, len(jobs))) as ex:
        fut_map = {ex.submit(run_adapter, venue, pair_brl, sym, conv): i
                   for i, (venue, sym) in enumerate(jobs)}
        for fut in as_completed(fut_map):
            i = fut_map[fut]
            books[i] = fut.result()
    books.sort(key=lambda b: b["venue"])
    cons = build_consolidated(books)

    vwaps_bid, vwaps_ask = [], []
    for sz in vwap_targets:
        if cons["bids"]: vwaps_bid.append({**(vwap(cons["bids"], sz) or {}), "side":"bid"})
        if cons["asks"]: vwaps_ask.append({**(vwap(cons["asks"], sz) or {}), "side":"ask"})

    mid = cons["mid"] or 0
    depth = {"bins": [], "venues": {}}
    if mid:
        lo, hi = mid*0.98, mid*1.02
        n = 40; step = (hi - lo) / n
        depth["bins"] = [round(lo + i*step, 4) for i in range(n+1)]
        for b in books:
            if b["quality"] in ("stale", "error"): continue
            arr = [0.0] * n
            for lvl in b["bids"] + b["asks"]:
                p = lvl["price_brl"]
                if p < lo or p > hi: continue
                idx = min(int((p - lo) / step), n-1)
                arr[idx] += lvl["size"]
            depth["venues"][b["venue"]] = arr

    health = [{
        "venue": b["venue"], "category": b["category"], "type": b["type"],
        "books": f"{len(b['bids']) + len(b['asks'])}/{2*DEPTH}",
        "latency_ms": b["latency_ms"], "age_ms": b["age_ms"],
        "spread_pct": round((b["spread_bps"] or 0)/100, 3) if b["spread_bps"] else None,
        "quality": b["quality"], "error": b.get("error"),
        "best_bid": b["best_bid"], "best_ask": b["best_ask"],
    } for b in books]

    return {
        "pair": pair_brl,
        "venues_total": len(books),
        "venues_online": sum(1 for b in books if b["quality"] == "online"),
        "venues_degraded": sum(1 for b in books if b["quality"] == "degraded"),
        "venues_stale": sum(1 for b in books if b["quality"] in ("stale","error")),
        "books": books, "health": health,
        "consolidated": {
            "best_bid": cons["best_bid"], "best_ask": cons["best_ask"],
            "mid": cons["mid"], "spread_brl": cons["spread_brl"],
            "spread_pct": cons["spread_pct"],
            "top_bids": cons["bids"][:25], "top_asks": cons["asks"][:25],
        },
        "vwap": {"bids": vwaps_bid, "asks": vwaps_ask},
        "depth_chart": depth,
        "arb": sorted([
            {"venue": b["venue"], "best_bid": b["best_bid"], "best_ask": b["best_ask"]}
            for b in books if b["best_bid"] and b["best_ask"]
        ], key=lambda x: -x["best_bid"]),
    }


def main():
    t0 = time.perf_counter()
    conv = usdtbrl_rate()
    print(f"[sentinel] USDT-BRL = {conv}")

    target_map = {
        "BTC-BRL":  [0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0],
        "ETH-BRL":  [1, 5, 10, 25, 50, 100, 250],
        "USDT-BRL": [10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000],
        "SOL-BRL":  [10, 25, 50, 100, 250, 500, 1000],
    }

    out_pairs = {}
    for pair in ("BTC-BRL", "ETH-BRL", "USDT-BRL", "SOL-BRL"):
        print(f"[sentinel] {pair}...")
        out_pairs[pair] = build_pair(pair, conv, target_map[pair])
        p = out_pairs[pair]
        cons = p["consolidated"]
        if cons["mid"]:
            print(f"   mid={cons['mid']:.4f}  spread={cons['spread_pct']}%  "
                  f"{p['venues_online']}/{p['venues_total']} online")

    elapsed = int((time.perf_counter() - t0) * 1000)
    out = {
        "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        "elapsed_ms": elapsed,
        "usdtbrl": conv,
        "pairs": out_pairs,
    }
    path = DATA / "orderbook.json"
    path.write_text(json.dumps(out, default=str))
    print(f"[sentinel] {path.stat().st_size/1024:.1f} KB em {elapsed}ms · {len(out_pairs)} pares")


if __name__ == "__main__":
    main()
