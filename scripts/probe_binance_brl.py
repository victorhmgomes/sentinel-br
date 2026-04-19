"""
probe_binance_brl.py — testa quais pares -BRL estão ativos na Binance hoje.
Roda /api/v3/exchangeInfo e filtra; depois testa /depth pra cada par.
"""
import json, time, urllib.request

UA = {"User-Agent": "sentinel-br/1.0", "Accept": "application/json"}

def get(url, timeout=15):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())

# 1) lista todos os símbolos -BRL ativos
info = get("https://api.binance.com/api/v3/exchangeInfo")
brl = [s for s in info["symbols"] if s["symbol"].endswith("BRL") and s["status"] == "TRADING"]
print(f"BINANCE pares -BRL ativos: {len(brl)}")
for s in brl:
    print(f"  {s['symbol']:14s} base={s['baseAsset']:6s} quote={s['quoteAsset']}")

# 2) testa /depth e /ticker/24hr pros que interessam
WANT = ["BTCBRL", "ETHBRL", "USDTBRL", "SOLBRL"]
print("\nProfundidade e volume 24h:")
for sym in WANT:
    if not any(s["symbol"] == sym for s in brl):
        print(f"  {sym}: NÃO LISTADO ATIVO")
        continue
    try:
        t0 = time.time()
        d = get(f"https://api.binance.com/api/v3/depth?symbol={sym}&limit=100")
        lat = int((time.time()-t0)*1000)
        n_bid = len(d.get("bids", []))
        n_ask = len(d.get("asks", []))
        best_bid = float(d["bids"][0][0]) if n_bid else 0
        best_ask = float(d["asks"][0][0]) if n_ask else 0
        spread = (best_ask - best_bid) / ((best_ask + best_bid)/2) * 100 if best_bid and best_ask else 0

        t = get(f"https://api.binance.com/api/v3/ticker/24hr?symbol={sym}")
        vol_brl = float(t.get("quoteVolume", 0))
        n_trades = int(t.get("count", 0))

        print(f"  {sym:8s}  bid={best_bid:>14,.4f}  ask={best_ask:>14,.4f}  "
              f"spread={spread:+.4f}%  bids={n_bid:3d}  asks={n_ask:3d}  "
              f"vol24h=R$ {vol_brl:>15,.0f}  trades24h={n_trades:>7,}  lat={lat}ms")
    except Exception as e:
        print(f"  {sym:8s}  ERRO: {e}")
    time.sleep(0.3)
