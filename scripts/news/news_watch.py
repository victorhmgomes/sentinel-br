"""
news_watch.py — pipeline completo:
  1. fetch_all (RSS paralelo)
  2. classify_all (scoring por keywords)
  3. correlate com alertas do watcher
  4. escreve data/news.json pro dashboard

Uso:
  python3 scripts/news/news_watch.py
  python3 scripts/news/news_watch.py --min-sev low    # mais permissivo
  python3 scripts/news/news_watch.py --limit 120
"""
from __future__ import annotations
import argparse, json, sys, time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from fetcher    import fetch_all
from classifier import classify_all, top_relevant
from correlator import correlate, load_watcher_alerts, summarize

ROOT = HERE.parent.parent  # cryptofraud/
DATA = ROOT / "data"
ALERTS_DIR = DATA / "alerts"
DASH_JSON  = DATA / "dashboard.json"


def load_dashboard_alerts(path: Path) -> list[dict]:
    """Normaliza alertas do dashboard.json pro schema esperado pelo correlator
    (ts ISO, rule, venue). Esses alertas têm apenas `date` (YYYY-MM-DD) —
    ancoramos em 12:00 UTC do dia pra janela ±h funcionar."""
    if not path.exists():
        return []
    try:
        d = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    raw = d.get("alerts", []) or []
    out: list[dict] = []
    for a in raw:
        date = a.get("date") or a.get("ts")
        if not date:
            continue
        # se já for ISO com hora, mantém; senão ancora em 12:00 UTC
        ts = date if "T" in str(date) else f"{date}T12:00:00+00:00"
        out.append({
            "ts": ts,
            "rule": a.get("metric") or a.get("rule"),
            "severity": a.get("severity"),
            "venue": a.get("source") or a.get("venue"),
            "asset": a.get("asset"),
            "narrative": a.get("narrative") or a.get("metric"),
        })
    return out


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--min-sev", default="medium",
                   choices=["low","medium","high","critical"])
    p.add_argument("--min-br-score", type=int, default=1,
                   help="foco BR mínimo; 0=neutro, 1=exige ≥1 sinal BR líquido, 2=mais estrito")
    p.add_argument("--limit", type=int, default=80,
                   help="máx itens a persistir (default 80)")
    p.add_argument("--back-hours", type=int, default=72,
                   help="janela atrás: quantas horas antes da notícia procurar alertas")
    p.add_argument("--forward-hours", type=int, default=72)
    p.add_argument("--out", default=str(DATA / "news.json"))
    return p.parse_args()


def main():
    args = parse_args()
    t0 = time.time()

    print("→ fetching RSS feeds...", file=sys.stderr)
    raw = fetch_all()
    print(f"  {len(raw)} itens únicos", file=sys.stderr)

    print("→ classifying...", file=sys.stderr)
    classed = classify_all(raw)
    relevant = top_relevant(classed, min_sev=args.min_sev, min_br_score=args.min_br_score)
    print(f"  {len(relevant)} itens >= {args.min_sev} com foco BR ≥ {args.min_br_score}",
          file=sys.stderr)

    print("→ loading watcher alerts...", file=sys.stderr)
    live_alerts = load_watcher_alerts(ALERTS_DIR) if ALERTS_DIR.exists() else []
    dash_alerts = load_dashboard_alerts(DASH_JSON)
    alerts = live_alerts + dash_alerts
    print(f"  {len(live_alerts)} watcher (live) + {len(dash_alerts)} dashboard.json = {len(alerts)} alertas",
          file=sys.stderr)

    print("→ correlating...", file=sys.stderr)
    corr = correlate(relevant, alerts,
                     back_hours=args.back_hours,
                     forward_hours=args.forward_hours)
    # Ordena: corroboração maior primeiro, depois severidade, depois data
    sev_order = {"critical":4,"high":3,"medium":2,"low":1,"noise":0}
    corr.sort(key=lambda x: (
        x.get("_corroboration_score",0),
        sev_order.get(x.get("_severity","noise"),0),
        x.get("ts_iso","")
    ), reverse=True)

    corr = corr[:args.limit]
    summ = summarize(corr)
    summ["generated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    summ["runtime_sec"]  = round(time.time() - t0, 2)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"summary": summ, "items": corr}
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"\n✓ {out_path}  ({out_path.stat().st_size/1024:.1f} KB)", file=sys.stderr)
    print(f"  itens persistidos: {len(corr)}", file=sys.stderr)
    print(f"  com corroboração do watcher: {summ['with_watcher_corroboration']}", file=sys.stderr)
    print(f"  por severidade: {summ['by_severity']}", file=sys.stderr)
    print(f"  runtime: {summ['runtime_sec']}s", file=sys.stderr)


if __name__ == "__main__":
    main()
