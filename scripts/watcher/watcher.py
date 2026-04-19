"""
watcher.py — loop principal do Sentinel BR streaming.

  - a cada N segundos (default 30s), captura snapshot consolidado
  - mantém janela rolling em memória (deque com últimos K snapshots)
  - dispara todos os detectores
  - dedupe básico: não repete o MESMO alert rule+venue+asset dentro de M segundos
  - envia para sinks (stdout + JSONL + _live.json)

Uso:
  python3 scripts/watcher/watcher.py
  python3 scripts/watcher/watcher.py --interval 30 --min-severity medium

Interromper com Ctrl+C.
"""
from __future__ import annotations
import argparse, signal, sys, time
from collections import deque
from pathlib import Path

# garante que roda tanto como script quanto como módulo
HERE = Path(__file__).resolve().parent
SCRIPTS = HERE.parent  # scripts/
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))
# também aceita rodar de dentro do próprio pacote
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

try:
    from watcher.fetchers  import fetch_snapshot
    from watcher.detectors import run_all
    from watcher.sinks     import default_sink
except ModuleNotFoundError:
    # fallback quando o próprio arquivo é chamado como `watcher/watcher.py`
    from fetchers  import fetch_snapshot
    from detectors import run_all
    from sinks     import default_sink


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--interval", type=int, default=30, help="segundos entre capturas (default 30)")
    p.add_argument("--window",   type=int, default=40, help="tamanho da janela rolling (default 40 snapshots)")
    p.add_argument("--dedupe",   type=int, default=300, help="dedupe em segundos (default 300s = 5min)")
    p.add_argument("--min-severity", default="medium",
                   choices=["info","medium","high","critical"])
    p.add_argument("--data-root", default="data/alerts",
                   help="diretório de alerts JSONL (default data/alerts)")
    return p.parse_args()


class DedupeKey:
    """Evita repetir (rule, venue, asset) em janela curta."""
    def __init__(self, ttl_sec: int):
        self.ttl = ttl_sec
        self.mem: dict[tuple, float] = {}

    def seen(self, a) -> bool:
        k = (a.rule, a.venue, a.asset)
        now = time.time()
        # limpa expirados
        for kk in list(self.mem):
            if now - self.mem[kk] > self.ttl:
                self.mem.pop(kk, None)
        if k in self.mem:
            return True
        self.mem[k] = now
        return False


def pretty_header(interval, window, min_sev):
    bar = "═" * 70
    lines = [
        bar,
        "  SENTINEL BR · streaming watcher",
        f"  interval={interval}s  window={window}  min_severity={min_sev}",
        "  venues: Binance (âncora) · Mercado Bitcoin · Foxbit",
        "  ativos: BTC-BRL · ETH-BRL · USDT-BRL · SOL-BRL",
        "  Ctrl+C pra parar.",
        bar,
    ]
    print("\n".join(lines))


def main():
    args = parse_args()
    sink = default_sink(min_severity=args.min_severity, root=args.data_root)
    dedupe = DedupeKey(ttl_sec=args.dedupe)
    state: deque = deque(maxlen=args.window)

    # SIGINT limpinho
    stopping = {"flag": False}
    def _stop(*_):
        stopping["flag"] = True
        print("\nparando (aguardando ciclo terminar)...")
    signal.signal(signal.SIGINT, _stop)

    pretty_header(args.interval, args.window, args.min_severity)

    tick = 0
    while not stopping["flag"]:
        tick += 1
        t0 = time.time()
        try:
            snap = fetch_snapshot()
        except Exception as e:
            print(f"[tick {tick}] ERRO fetch: {e}", file=sys.stderr)
            time.sleep(args.interval)
            continue

        meta = snap.get("_meta", {})
        # sumário compacto
        def _mid(v, a):
            b = snap.get(v, {}).get(a, {})
            return b.get("best_bid"), b.get("best_ask")
        bbid, bask = _mid("Binance", "BTC-BRL")
        mbid, mask = _mid("Mercado Bitcoin", "BTC-BRL")
        fbid, fask = _mid("Foxbit", "BTC-BRL")
        print(f"[{meta.get('ts','')}] tick #{tick}  fetch={meta.get('elapsed_ms','?')}ms  "
              f"Binance BTC-BRL bid/ask={bbid}/{bask}  MB={mbid}/{mask}  Fox={fbid}/{fask}  "
              f"BTC-USDT={meta.get('btc_usdt_global'):.0f}")

        alerts = run_all(snap, state)
        n_new = 0
        for a in alerts:
            if dedupe.seen(a):
                continue
            sink.emit(a)
            n_new += 1
        if alerts and n_new == 0:
            print(f"   ({len(alerts)} alerta(s) suprimido(s) por dedupe)")
        elif alerts:
            print(f"   → {n_new} novo(s) alerta(s)")

        state.append(snap)

        # respeita o interval mesmo com fetch variado
        elapsed = time.time() - t0
        sleep_for = max(0, args.interval - elapsed)
        # sleep interrompível
        for _ in range(int(sleep_for)):
            if stopping["flag"]: break
            time.sleep(1)

    print("watcher encerrado.")


if __name__ == "__main__":
    main()
