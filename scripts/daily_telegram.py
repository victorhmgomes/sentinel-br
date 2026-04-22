"""
daily_telegram.py — envia alertas de severidade ≥ high do dashboard.json
para o Telegram, uma vez por dia (pós detect_anomalies + backtest + ensemble).

Diferente do sink do watcher (que fire-and-forget em cada tick), este script:
  - lê data/dashboard.json
  - pega alertas de HOJE (ou opcionalmente últimos N dias via --days)
  - filtra severity >= threshold
  - dedup por (date,source,metric) pra não repetir se reexecutado
  - emite via TelegramSink reaproveitado do watcher

Credenciais em env:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
  (se ausentes, o script loga e sai silenciosamente — mesma ergonomia do sink)

Dedup state em data/alerts/_daily_telegram_sent.json
"""
from __future__ import annotations
import argparse, json, sys, datetime as dt
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DASH = ROOT / "data" / "dashboard.json"
STATE = ROOT / "data" / "alerts" / "_daily_telegram_sent.json"

sys.path.insert(0, str(ROOT / "scripts" / "watcher"))
from sinks import TelegramSink  # noqa: E402

SEV_RANK = {"medium": 2, "high": 3, "critical": 4}


def load_state() -> set[str]:
    if not STATE.exists():
        return set()
    try:
        data = json.loads(STATE.read_text())
        return set(data.get("sent", []))
    except Exception:
        return set()


def save_state(sent: set[str]) -> None:
    STATE.parent.mkdir(parents=True, exist_ok=True)
    STATE.write_text(json.dumps({"sent": sorted(sent)}, indent=2))


def alert_key(a: dict) -> str:
    """Chave estável para dedup. Data + source + metric + valor arredondado."""
    v = a.get("value")
    v_str = f"{v:.2f}" if isinstance(v, (int, float)) else str(v)
    return f"{a.get('date')}|{a.get('source')}|{a.get('metric')}|{v_str}"


def to_sink_format(a: dict) -> dict:
    """Converte alerta do detect_anomalies (schema diário) para schema do
    TelegramSink do watcher (que espera venue/rule/ts/narrative/context)."""
    metric = a.get("metric", "?")
    src    = a.get("source", "?")
    asset  = a.get("asset", "?")
    val    = a.get("value")
    sev    = a.get("severity", "medium")
    date   = a.get("date", "")

    # narrative por metric
    narr_map = {
        "volume_zscore":    f"Volume Z={val} — pico de liquidez fora do baseline 30d.",
        "stablecoin_flight": f"USDT/BTC ratio Z={val} — rotação anormal para stablecoin.",
        "br_premium":        f"Premium BR = {val}% — desvio de paridade teórica.",
        "price_volume_divergence": f"Volume Z={val} sem movimento de preço — possível wash-trade.",
        "ring_of_mules":     f"{val} séries BR acionando simultaneamente — frota de mulas.",
        "ensemble":          f"{val} detectores corroborando — corroboração cruzada.",
    }
    narr = narr_map.get(metric, f"{metric} = {val}")

    tags = a.get("tags", [])
    corr = a.get("corroborating_metrics")
    ctx  = {"value": val, "tags": ",".join(tags[:4])}
    if corr:
        ctx["signals"] = ",".join(corr)

    return {
        "ts":       f"{date}T00:00:00Z",
        "venue":    src,
        "asset":    asset,
        "rule":     metric,
        "severity": sev,
        "narrative": narr,
        "context":  ctx,
    }


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--min-sev", default="high",
                   choices=["medium", "high", "critical"],
                   help="severidade mínima para disparar (default: high)")
    p.add_argument("--days", type=int, default=1,
                   help="janela em dias para trás (default 1 = só hoje)")
    p.add_argument("--dry-run", action="store_true",
                   help="não envia, só imprime o que enviaria")
    return p.parse_args()


def main():
    args = parse_args()

    if not DASH.exists():
        print(f"[daily_telegram] {DASH} não encontrado — rode detect_anomalies.py antes", file=sys.stderr)
        sys.exit(0)

    d = json.loads(DASH.read_text())
    alerts = d.get("alerts", [])
    if not alerts:
        print("[daily_telegram] nenhum alerta no dashboard.json")
        return

    today = dt.date.today()
    cutoff = (today - dt.timedelta(days=args.days - 1)).isoformat()
    min_rank = SEV_RANK[args.min_sev]

    # filtra severidade + janela
    fresh = [
        a for a in alerts
        if a.get("date", "") >= cutoff
        and SEV_RANK.get(a.get("severity"), 0) >= min_rank
    ]

    if not fresh:
        print(f"[daily_telegram] sem alertas >= {args.min_sev} desde {cutoff}")
        return

    # dedup contra state (para não reenviar se o cron rodar 2x no mesmo dia)
    sent = load_state()
    new_alerts = [a for a in fresh if alert_key(a) not in sent]
    if not new_alerts:
        print(f"[daily_telegram] {len(fresh)} alertas >= {args.min_sev}, todos já enviados")
        return

    print(f"[daily_telegram] {len(new_alerts)} novo(s) alerta(s) >= {args.min_sev} para enviar")

    sink = TelegramSink(min_severity=args.min_sev)
    if not sink.enabled and not args.dry_run:
        print("[daily_telegram] Telegram desativado (env ausente) — encerrando sem erro")
        return

    # Header opcional: manchete consolidada se 3+ alertas
    if len(new_alerts) >= 3:
        by_sev = {}
        for a in new_alerts:
            by_sev[a["severity"]] = by_sev.get(a["severity"], 0) + 1
        header = {
            "ts":       f"{today.isoformat()}T00:00:00Z",
            "venue":    "Sentinel BR",
            "asset":    "multi",
            "rule":     "daily_roll_up",
            "severity": max(new_alerts, key=lambda a: SEV_RANK.get(a["severity"], 0))["severity"],
            "narrative": f"{len(new_alerts)} novos alertas ≥ {args.min_sev} hoje.",
            "context":  {f"n_{k}": v for k, v in by_sev.items()},
        }
        if args.dry_run:
            print("DRY-RUN:", header)
        else:
            sink.emit(header)

    for a in new_alerts:
        payload = to_sink_format(a)
        if args.dry_run:
            print("DRY-RUN:", payload)
        else:
            sink.emit(payload)
        sent.add(alert_key(a))

    if not args.dry_run:
        save_state(sent)
        print(f"[daily_telegram] enviado. state gravado em {STATE}")


if __name__ == "__main__":
    main()
