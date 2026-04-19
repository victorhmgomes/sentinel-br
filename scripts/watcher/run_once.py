"""
run_once.py — versão single-shot do watcher para cron (GitHub Actions).

Diferença do watcher.py:
  - sem loop infinito; executa 1 ciclo e sai.
  - carrega janela rolling de data/alerts/_state.json (se existir) e salva no final.
  - usa default_sink() com Telegram habilitado (se env setado).

Uso:
  python3 scripts/watcher/run_once.py
  python3 scripts/watcher/run_once.py --min-severity high --dedupe 900

Vars de ambiente:
  TELEGRAM_BOT_TOKEN  — token do bot (ex: 123456:ABC...)
  TELEGRAM_CHAT_ID    — id do chat (ex: 987654321 ou -100123456789 p/ grupo)

Saída: JSONL de alertas em data/alerts/YYYY-MM-DD.jsonl + _live.json rolling.
"""
from __future__ import annotations
import argparse, json, sys, time
from collections import deque
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCRIPTS = HERE.parent
ROOT = SCRIPTS.parent
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

try:
    from watcher.fetchers  import fetch_snapshot
    from watcher.detectors import run_all
    from watcher.sinks     import default_sink
except ModuleNotFoundError:
    from fetchers  import fetch_snapshot
    from detectors import run_all
    from sinks     import default_sink


STATE_PATH = Path("data/alerts/_state.json")
DEDUPE_PATH = Path("data/alerts/_dedupe.json")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--window",   type=int, default=40,
                   help="tamanho da janela rolling (default 40 snapshots)")
    p.add_argument("--dedupe",   type=int, default=900,
                   help="dedupe TTL em segundos (default 900s = 15min, match com cron)")
    p.add_argument("--min-severity", default="medium",
                   choices=["info","medium","high","critical"])
    p.add_argument("--telegram-min-severity", default="high",
                   choices=["info","medium","high","critical"],
                   help="severidade mínima pro Telegram (default high = não spammar)")
    p.add_argument("--data-root", default="data/alerts")
    return p.parse_args()


def load_state(path: Path, window: int) -> deque:
    """Reconstrói janela rolling de estado persistido (best-effort)."""
    if not path.exists():
        return deque(maxlen=window)
    try:
        raw = json.loads(path.read_text() or "[]")
        d = deque(maxlen=window)
        for snap in raw[-window:]:
            d.append(snap)
        return d
    except Exception as e:
        print(f"[state] falha ao ler {path}: {e}", file=sys.stderr)
        return deque(maxlen=window)


def save_state(path: Path, state: deque) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(list(state), ensure_ascii=False))


def load_dedupe(path: Path, ttl: int) -> dict:
    """Lê map (rule,venue,asset) -> last_ts. Limpa expirados."""
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text() or "{}")
    except Exception:
        return {}
    now = time.time()
    return {k: v for k, v in raw.items() if now - v <= ttl}


def save_dedupe(path: Path, d: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(d, ensure_ascii=False))


def main():
    args = parse_args()
    sink = default_sink(
        min_severity=args.min_severity,
        root=args.data_root,
        telegram=True,
        telegram_min_severity=args.telegram_min_severity,
    )

    state = load_state(STATE_PATH, args.window)
    dedupe = load_dedupe(DEDUPE_PATH, args.dedupe)

    t0 = time.time()
    try:
        snap = fetch_snapshot()
    except Exception as e:
        print(f"[run_once] ERRO fetch: {e}", file=sys.stderr)
        sys.exit(1)

    meta = snap.get("_meta", {})
    print(f"[{meta.get('ts','')}] run_once  fetch={meta.get('elapsed_ms','?')}ms  "
          f"window={len(state)}  dedupe_active={len(dedupe)}")

    alerts = run_all(snap, state)
    n_new, n_suppressed = 0, 0
    now = time.time()
    for a in alerts:
        key = f"{a.rule}|{a.venue}|{a.asset}"
        last = dedupe.get(key)
        if last and (now - last) < args.dedupe:
            n_suppressed += 1
            continue
        sink.emit(a)
        dedupe[key] = now
        n_new += 1

    if alerts:
        print(f"  → {n_new} novo(s) alerta(s), {n_suppressed} suprimido(s) por dedupe")
    else:
        print("  → nenhum alerta disparou")

    state.append(snap)
    save_state(STATE_PATH, state)
    save_dedupe(DEDUPE_PATH, dedupe)

    print(f"done em {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
