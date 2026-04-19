"""
run_correlator.py — lê alertas do watcher e gera data/events.json.

Input:
  - data/alerts/*.jsonl         (histórico rotativo diário)
  - data/alerts/_live.json      (janela ao vivo, top 200)

Output:
  - data/events.json            {"generated_at": iso, "events": [...], "summary": {...}}

Uso:
  python3 scripts/watcher/run_correlator.py
  python3 scripts/watcher/run_correlator.py --dedup 90 --cooldown 600
"""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
from datetime import datetime, timezone

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))
from correlator import correlate, summarize, DEFAULTS

ROOT = HERE.parent.parent  # cryptofraud/
ALERT_ROOT = ROOT / "data" / "alerts"
OUT = ROOT / "data" / "events.json"


def _load_jsonl(p: Path) -> list[dict]:
    out = []
    try:
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        pass
    return out


def _load_json_array(p: Path) -> list[dict]:
    try:
        raw = p.read_text(encoding="utf-8")
        if not raw.strip():
            return []
        v = json.loads(raw)
        return v if isinstance(v, list) else []
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def load_all_alerts(root: Path) -> list[dict]:
    """
    Une:
      1. todos os data/alerts/YYYY-MM-DD.jsonl (histórico completo)
      2. data/alerts/_live.json (janela recente — pode ter itens não flushados ao jsonl?)
    Deduplica por (ts, rule, asset, venue) que é chave natural pra mesma emissão.
    """
    seen = set()
    out = []

    # histórico rotativo
    for p in sorted(root.glob("*.jsonl")):
        for a in _load_jsonl(p):
            k = (a.get("ts"), a.get("rule"), a.get("asset"), a.get("venue"))
            if k in seen:
                continue
            seen.add(k)
            out.append(a)

    # live
    for a in _load_json_array(root / "_live.json"):
        k = (a.get("ts"), a.get("rule"), a.get("asset"), a.get("venue"))
        if k in seen:
            continue
        seen.add(k)
        out.append(a)

    return out


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--dedup",    type=int, default=DEFAULTS["dedup_s"],
                   help=f"dedup por (rule,asset) em segundos (default {DEFAULTS['dedup_s']})")
    p.add_argument("--cooldown", type=int, default=DEFAULTS["cooldown_s"],
                   help=f"fecha evento após N seg sem alerta novo (default {DEFAULTS['cooldown_s']})")
    p.add_argument("--min-distinct", type=int, default=DEFAULTS["min_distinct_for_event"],
                   help=f"≥ N detectores distintos pra virar 'correlacionado' (default {DEFAULTS['min_distinct_for_event']})")
    p.add_argument("--alerts-root", default=str(ALERT_ROOT),
                   help=f"raiz dos alertas (default {ALERT_ROOT})")
    p.add_argument("--out", default=str(OUT),
                   help=f"saída events.json (default {OUT})")
    return p.parse_args()


def main():
    args = parse_args()
    alerts = load_all_alerts(Path(args.alerts_root))
    print(f"lidos {len(alerts)} alerta(s) de {args.alerts_root}")

    events = correlate(alerts, params={
        "dedup_s":   args.dedup,
        "cooldown_s": args.cooldown,
        "min_distinct_for_event": args.min_distinct,
    })
    summ = summarize(events)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "params": {
            "dedup_s":   args.dedup,
            "cooldown_s": args.cooldown,
            "min_distinct_for_event": args.min_distinct,
        },
        "summary": summ,
        "events":  events,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    # resumo no stdout
    print(f"→ {out_path}")
    print(f"  eventos: {summ['n_total']}  (correlacionados: {summ['n_correlated']}  singletons: {summ['n_singleton']})")
    print(f"  convergência máxima: {summ['max_convergence']} detectores")
    if summ["by_severity"]:
        sev_order = ["critical", "high", "medium", "singleton"]
        txt = "  ".join(f"{s}:{summ['by_severity'].get(s, 0)}" for s in sev_order if s in summ["by_severity"])
        print(f"  por severidade: {txt}")
    if summ["by_asset"]:
        txt = "  ".join(f"{a}:{n}" for a, n in summ["by_asset"].items())
        print(f"  por ativo: {txt}")


if __name__ == "__main__":
    main()
