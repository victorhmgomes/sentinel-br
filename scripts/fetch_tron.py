"""
fetch_tron.py — Snapshot TRON / USDT-TRC20 para o Sentinel BR.

Endpoints TronScan (públicos, sem key):
  GET /api/stats/overview?days=365                           → estatísticas diárias da rede
  GET /api/token_trc20?contract=<USDT>&showAll=1             → metadados USDT
  GET /api/token_trc20/holders?contract_address=<USDT>       → top holders (exchanges taggeadas)
  GET /api/token_trc20/transfers?trc20Id=<USDT>&limit=50     → transferências recentes

Por quê TRON?
  TRON (USDT-TRC20) é a rail dominante para cash-out pós-PIX no ecossistema BR.
  Taxas ínfimas (< US$ 1), confirmação rápida (~3s), disponível em todas as
  corretoras brasileiras grandes. Sinqia/C&M/BTG — todos com cash-out via TRC20.

Métricas geradas:
  • daily_series: active_account_number, newAddressSeen, usdt_transaction (365d)
  • z_scores: rolling 30d mean/σ sobre a série usdt_transaction
  • hot_wallet_balances: top 30 holders com balanço + tag + share of supply
  • exchange_balances: holders taggeados (Binance/Bybit/OKX/Kraken/Gate/Bitfinex/Bitget)
  • recent_transfers: amostra das últimas 50 transferências (para dashboard)
"""
from __future__ import annotations
import json, time, datetime as dt, urllib.request, urllib.error
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT  = ROOT / "data" / "tron.json"

USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
BASE          = "https://apilist.tronscanapi.com/api"
UA            = {"User-Agent": "sentinel-br/1.0", "Accept": "application/json"}


def get_json(url, timeout=20, retries=3):
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            last = f"HTTP {e.code}"
            time.sleep(2 if e.code != 429 else 8)
        except Exception as e:
            last = f"{type(e).__name__}: {e}"
            time.sleep(2)
    raise RuntimeError(f"fail after {retries}: {last}")


def rolling_z(vals, window=30):
    """Z-score rolling baseado em média/σ da janela anterior (não inclui t)."""
    import statistics as st
    out = []
    for i, v in enumerate(vals):
        if i < window:
            out.append(None); continue
        win = vals[i - window:i]
        mu = sum(win) / window
        try:
            sd = st.pstdev(win) or 1e-9
        except Exception:
            sd = 1e-9
        out.append(round((v - mu) / sd, 3))
    return out


def fetch_overview():
    """Diário da rede TRON — 365 dias."""
    d = get_json(f"{BASE}/stats/overview?days=365")
    rows = d.get("data", []) or []
    series = []
    for row in rows:
        ts_ms = row.get("date") or 0
        d_str = (dt.datetime.fromtimestamp(ts_ms / 1000, tz=dt.timezone.utc)
                 .date().isoformat()) if ts_ms else row.get("dateDayStr")
        series.append({
            "date":            d_str,
            "active_accounts": int(row.get("active_account_number") or 0),
            "new_addresses":   int(row.get("newAddressSeen") or 0),
            "usdt_tx":         int(row.get("usdt_transaction") or 0),
            "total_tx":        int(row.get("newTransactionSeen") or row.get("totalTransaction") or 0),
        })
    series.sort(key=lambda r: r["date"])  # ascending
    return series


def fetch_token_info():
    d = get_json(f"{BASE}/token_trc20?contract={USDT_CONTRACT}&showAll=1")
    lst = d.get("trc20_tokens", []) or []
    if not lst:
        return {}
    t = lst[0]
    return {
        "symbol":        t.get("symbol"),
        "name":          t.get("name"),
        "holders_count": t.get("holders_count"),
        "total_supply":  float(t.get("total_supply_with_decimals", 0)) / 1e6,
        "issue_addr":    t.get("issue_address"),
        "issue_ts":      t.get("issue_ts"),
        "price_usd":     t.get("price") or 1.0,
    }


def fetch_holders(n=30):
    d = get_json(f"{BASE}/token_trc20/holders?contract_address={USDT_CONTRACT}"
                 f"&limit={n}&start=0")
    rows = d.get("trc20_tokens", []) or []
    out = []
    for i, h in enumerate(rows):
        bal = float(h.get("balance", 0)) / 1e6
        tag = (h.get("addressTag") or "").strip()
        pubtag = (h.get("publicTagDesc") or "").strip()
        # Heurística de exchange — qualquer tag com nome de VASP conhecido.
        ex = None
        t_low = (tag + " " + pubtag).lower()
        for pat in ("binance", "bybit", "okx", "kraken", "gate", "bitfinex",
                    "bitget", "kucoin", "huobi", "htx", "mexc", "bitstamp",
                    "coinbase", "tether"):
            if pat in t_low:
                ex = pat.capitalize() if pat != "htx" else "HTX"
                break
        out.append({
            "rank":    i + 1,
            "address": h.get("holder_address"),
            "balance": bal,
            "tag":     tag or pubtag,
            "exchange": ex,
        })
    return out


def fetch_recent_transfers(n=50):
    d = get_json(f"{BASE}/token_trc20/transfers?limit={n}&start=0"
                 f"&trc20Id={USDT_CONTRACT}")
    rows = d.get("token_transfers", []) or []
    out = []
    for t in rows:
        amt = float(t.get("quant", 0)) / 1e6
        from_tag = ""
        to_tag = ""
        ft = t.get("from_address_tag") or {}
        tt = t.get("to_address_tag") or {}
        if isinstance(ft, dict):
            from_tag = ft.get("from_address_tag") or ""
        if isinstance(tt, dict):
            to_tag = tt.get("to_address_tag") or ""
        out.append({
            "ts":      t.get("block_ts"),
            "hash":    t.get("transaction_id"),
            "amount":  amt,
            "from":    t.get("from_address"),
            "to":      t.get("to_address"),
            "from_tag": from_tag,
            "to_tag":   to_tag,
        })
    # ordenar por amount desc
    out.sort(key=lambda r: -r["amount"])
    return out


def main():
    t0 = time.time()
    print("[tron] stats/overview 365d…")
    series = fetch_overview()
    print(f"  {len(series)} dias  (primeiro: {series[0]['date']}  último: {series[-1]['date']})")

    # Z-scores na série de transações USDT
    usdt_vals = [r["usdt_tx"] for r in series]
    zs = rolling_z(usdt_vals, window=30)
    alerts = []
    for r, z in zip(series, zs):
        if z is not None and z >= 2:
            sev = "critical" if z >= 4 else "high" if z >= 3 else "medium"
            alerts.append({"date": r["date"], "z": z, "severity": sev, "value": r["usdt_tx"]})

    print("[tron] token info + top holders…")
    info = fetch_token_info()
    time.sleep(1)
    holders = fetch_holders(n=30)
    time.sleep(1)

    print("[tron] recent transfers…")
    transfers = fetch_recent_transfers(n=50)

    # Estatísticas de exchange balances
    ex_balances = {}
    for h in holders:
        if h["exchange"] and h["exchange"] != "Tether":
            ex_balances.setdefault(h["exchange"], {"total": 0.0, "wallets": []})
            ex_balances[h["exchange"]]["total"] += h["balance"]
            ex_balances[h["exchange"]]["wallets"].append(
                {"address": h["address"], "balance": h["balance"], "tag": h["tag"]}
            )
    ex_rank = sorted(ex_balances.items(), key=lambda kv: -kv[1]["total"])

    out = {
        "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        "contract":     USDT_CONTRACT,
        "info":         info,
        "series":       series,
        "z_scores":     zs,
        "alerts":       alerts,
        "holders_top":  holders,
        "exchange_balances": [
            {"exchange": k, **v} for k, v in ex_rank
        ],
        "recent_transfers": transfers,
        "summary": {
            "days":             len(series),
            "avg_usdt_tx":      sum(usdt_vals) / len(usdt_vals) if usdt_vals else 0,
            "max_usdt_tx":      max(usdt_vals) if usdt_vals else 0,
            "last_usdt_tx":     usdt_vals[-1] if usdt_vals else 0,
            "alerts_total":     len(alerts),
            "alerts_high":      sum(1 for a in alerts if a["severity"] in ("high","critical")),
            "alerts_critical":  sum(1 for a in alerts if a["severity"] == "critical"),
            "top_tagged_exchange_bal": sum(v["total"] for _, v in ex_rank),
        },
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, default=str))
    elapsed = int((time.time() - t0) * 1000)
    print(f"[tron] gravado em {OUT} ({OUT.stat().st_size/1024:.1f} KB · {elapsed}ms)")
    print(f"       alerts 365d: {len(alerts)} total ({out['summary']['alerts_high']} high+, "
          f"{out['summary']['alerts_critical']} critical)")
    print(f"       exchanges tagged: {len(ex_rank)}  total bal = "
          f"{out['summary']['top_tagged_exchange_bal']:,.0f} USDT")


if __name__ == "__main__":
    main()
