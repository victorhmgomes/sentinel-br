"""
fetchers.py — fetchers leves de orderbook para o watcher (sub-segundo total).
Inclui Binance (4 pares BRL), Mercado Bitcoin, Foxbit, e BTC-USDT global como FX.
"""
from __future__ import annotations
import json, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

UA = {"User-Agent": "sentinel-br/1.0", "Accept": "application/json"}


def _get(url: str, timeout: float = 6.0) -> Optional[dict]:
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


def _book(bids_raw, asks_raw, n: int = 50) -> dict:
    """Normaliza pra {best_bid, best_ask, bids:[(px,qty)..], asks:[..]}"""
    bids = [(float(p), float(q)) for p, q in (bids_raw or [])[:n]]
    asks = [(float(p), float(q)) for p, q in (asks_raw or [])[:n]]
    return {
        "best_bid": bids[0][0] if bids else 0.0,
        "best_ask": asks[0][0] if asks else 0.0,
        "bids": bids, "asks": asks,
    }


def _book_from_dict_levels(bids_raw, asks_raw, n: int = 50,
                           px_key: str = "price", qty_key: str = "amount") -> dict:
    """Mesma normalização, mas aceita níveis como dict {price, amount}
    (padrão de BitPreço e Ripio Trade)."""
    def _pairs(raw):
        out = []
        for lvl in (raw or [])[:n]:
            try: out.append((float(lvl[px_key]), float(lvl[qty_key])))
            except (KeyError, TypeError, ValueError): continue
        return out
    bids = _pairs(bids_raw)
    asks = _pairs(asks_raw)
    return {
        "best_bid": bids[0][0] if bids else 0.0,
        "best_ask": asks[0][0] if asks else 0.0,
        "bids": bids, "asks": asks,
    }


# -------- Binance (4 pares BRL + BTCUSDT global como FX) --------
BINANCE_PAIRS = {
    "BTC-BRL":  "BTCBRL",
    "ETH-BRL":  "ETHBRL",
    "USDT-BRL": "USDTBRL",
    "SOL-BRL":  "SOLBRL",
}

def fetch_binance() -> dict:
    out = {}
    def one(asset, sym):
        d = _get(f"https://api.binance.com/api/v3/depth?symbol={sym}&limit=50")
        if not d: return asset, None
        return asset, _book(d.get("bids"), d.get("asks"))
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [ex.submit(one, a, s) for a, s in BINANCE_PAIRS.items()]
        for f in as_completed(futs):
            a, b = f.result()
            if b: out[a] = b
    return out


def fetch_btc_usdt_global() -> float:
    d = _get("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT")
    if not d: return 0.0
    try: return float(d["price"])
    except Exception: return 0.0


# -------- Mercado Bitcoin (BTC, ETH, USDT, SOL · BRL) --------
MB_PAIRS = {
    "BTC-BRL":  "BTC-BRL",
    "ETH-BRL":  "ETH-BRL",
    "USDT-BRL": "USDT-BRL",
    "SOL-BRL":  "SOL-BRL",
}

def fetch_mb() -> dict:
    out = {}
    def one(asset, sym):
        d = _get(f"https://api.mercadobitcoin.net/api/v4/{sym}/orderbook?limit=50")
        if not d: return asset, None
        return asset, _book(d.get("bids"), d.get("asks"))
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [ex.submit(one, a, s) for a, s in MB_PAIRS.items()]
        for f in as_completed(futs):
            a, b = f.result()
            if b: out[a] = b
    return out


# -------- Foxbit --------
FOXBIT_PAIRS = {
    "BTC-BRL":  "btcbrl",
    "ETH-BRL":  "ethbrl",
    "USDT-BRL": "usdtbrl",
    "SOL-BRL":  "solbrl",
}

def fetch_foxbit() -> dict:
    out = {}
    def one(asset, sym):
        d = _get(f"https://api.foxbit.com.br/rest/v3/markets/{sym}/orderbook?depth=50")
        if not d: return asset, None
        # foxbit retorna {bids:[[px,qty]..], asks:[[px,qty]..]}
        bids = d.get("bids") or d.get("buy") or []
        asks = d.get("asks") or d.get("sell") or []
        return asset, _book(bids, asks)
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [ex.submit(one, a, s) for a, s in FOXBIT_PAIRS.items()]
        for f in as_completed(futs):
            a, b = f.result()
            if b: out[a] = b
    return out


# -------- BitPreço (BTC, ETH, USDT, SOL · BRL) --------
# Endpoint: https://api.bitpreco.com/{pair}/orderbook
# Schema: {success, bids:[{price, amount, id}, ...], asks:[...]}
BITPRECO_PAIRS = {
    "BTC-BRL":  "btc-brl",
    "ETH-BRL":  "eth-brl",
    "USDT-BRL": "usdt-brl",
    "SOL-BRL":  "sol-brl",
}

def fetch_bitpreco() -> dict:
    out = {}
    def one(asset, sym):
        d = _get(f"https://api.bitpreco.com/{sym}/orderbook")
        if not d or not d.get("success"):
            return asset, None
        return asset, _book_from_dict_levels(d.get("bids"), d.get("asks"))
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [ex.submit(one, a, s) for a, s in BITPRECO_PAIRS.items()]
        for f in as_completed(futs):
            a, b = f.result()
            if b: out[a] = b
    return out


# -------- Ripio Trade (BTC, ETH, USDT, SOL · BRL) --------
# Endpoint: https://api.ripiotrade.co/v4/public/orders/level-2?pair=BTC_BRL
# Schema: {data:{bids:[{price, amount}], asks:[{price, amount}]}, error_code, message}
RIPIO_PAIRS = {
    "BTC-BRL":  "BTC_BRL",
    "ETH-BRL":  "ETH_BRL",
    "USDT-BRL": "USDT_BRL",
    "SOL-BRL":  "SOL_BRL",
}

def fetch_ripio() -> dict:
    out = {}
    def one(asset, sym):
        d = _get(f"https://api.ripiotrade.co/v4/public/orders/level-2?pair={sym}&limit=50")
        if not d or d.get("error_code"):
            return asset, None
        data = d.get("data") or {}
        return asset, _book_from_dict_levels(data.get("bids"), data.get("asks"))
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [ex.submit(one, a, s) for a, s in RIPIO_PAIRS.items()]
        for f in as_completed(futs):
            a, b = f.result()
            if b: out[a] = b
    return out


# -------- NovaDAX (BTC, ETH, USDT, SOL · BRL) --------
NOVADAX_PAIRS = {
    "BTC-BRL":  "BTC_BRL",
    "ETH-BRL":  "ETH_BRL",
    "USDT-BRL": "USDT_BRL",
    "SOL-BRL":  "SOL_BRL",
}

def fetch_novadax() -> dict:
    out = {}
    def one(asset, sym):
        d = _get(f"https://api.novadax.com/v1/market/depth?symbol={sym}&limit=50")
        if not d or d.get("code") != "A10000":
            return asset, None
        data = d.get("data") or {}
        return asset, _book(data.get("bids"), data.get("asks"))
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [ex.submit(one, a, s) for a, s in NOVADAX_PAIRS.items()]
        for f in as_completed(futs):
            a, b = f.result()
            if b: out[a] = b
    return out


# -------- snap consolidado --------
def fetch_snapshot() -> dict:
    """Captura paralela de todas as venues. Tempo total ~ <2s típico."""
    t0 = time.time()
    snap = {}
    with ThreadPoolExecutor(max_workers=7) as ex:
        f_b = ex.submit(fetch_binance)
        f_m = ex.submit(fetch_mb)
        f_f = ex.submit(fetch_foxbit)
        f_n = ex.submit(fetch_novadax)
        f_bp = ex.submit(fetch_bitpreco)
        f_r = ex.submit(fetch_ripio)
        f_g = ex.submit(fetch_btc_usdt_global)
        snap["Binance"]         = f_b.result()  or {}
        snap["Mercado Bitcoin"] = f_m.result()  or {}
        snap["Foxbit"]          = f_f.result()  or {}
        snap["NovaDAX"]         = f_n.result()  or {}
        snap["BitPreço"]        = f_bp.result() or {}
        snap["Ripio Trade"]     = f_r.result()  or {}
        snap["_meta"] = {
            "btc_usdt_global": f_g.result(),
            "elapsed_ms": int((time.time() - t0) * 1000),
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
    return snap
