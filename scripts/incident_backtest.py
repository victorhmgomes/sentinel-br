"""
incident_backtest.py — anexa ao dashboard.json uma seção 'incidents'
com datas, alertas observados em ±3d/±7d e comparação contra baseline aleatório.
"""
import json, datetime as dt
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DASH = ROOT / "data" / "dashboard.json"

INCIDENTS = [
    {"date": "2025-06-30", "label": "#1",  "name": "Incidente operacional em provedor bancário"},
    {"date": "2025-08-28", "label": "#2",  "name": "Operação de enforcement em fintechs (setor combustíveis)"},
    {"date": "2025-08-29", "label": "#3",  "name": "Ataque supply-chain em provedor de core banking"},
    {"date": "2025-09-02", "label": "#4",  "name": "Incidente em fintech de crédito"},
    {"date": "2025-09-05", "label": "#5a", "name": "Incidente simultâneo em 3 bancos"},
    {"date": "2025-09-06", "label": "#5b", "name": "Continuação do incidente #5"},
    {"date": "2025-10-19", "label": "#6",  "name": "Incidente em provedor Dileta afetando 3 bancos"},
    {"date": "2025-11-24", "label": "#7",  "name": "Incidente em financeira (gateway PIX)"},
    {"date": "2026-01-11", "label": "#8",  "name": "Incidente em financeira"},
    {"date": "2026-01-20", "label": "#9",  "name": "Incidente em fintech de crédito"},
    {"date": "2026-01-26", "label": "#10", "name": "Incidente em banco regional"},
    {"date": "2026-01-29", "label": "#10b","name": "Incidente em instituição de câmbio"},
    {"date": "2026-03-22", "label": "#11", "name": "Incidente em banco de grande porte — R$ 100M"},
    {"date": "2026-04-17", "label": "#12a","name": "Incidente em instituição de pagamento — R$ 20M"},
    {"date": "2026-04-18", "label": "#12b","name": "Continuação do incidente em IP — R$ 20M"},
    {"date": "2026-04-21", "label": "#13", "name": "Ataque cibernético a banco médio de câmbio/crédito — R$ ~50M"},
]

def main():
    d = json.loads(DASH.read_text())
    alerts = d["alerts"]
    total_days = 365

    def win(d0, days):
        b = dt.date.fromisoformat(d0)
        lo = (b - dt.timedelta(days=days)).isoformat()
        hi = (b + dt.timedelta(days=days)).isoformat()
        return [a for a in alerts if lo <= a["date"] <= hi]

    def days_with(pred):
        return len({a["date"] for a in alerts if pred(a)}) / total_days

    BR = ("Mercado Bitcoin", "Foxbit")
    USDT = ("USDT", "USDT/BTC")

    baselines = {
        "any":      days_with(lambda a: True),
        "high_plus":days_with(lambda a: a["severity"] in ("critical", "high")),
        "critical": days_with(lambda a: a["severity"] == "critical"),
        "br":       days_with(lambda a: a["source"] in BR),
        "usdt":     days_with(lambda a: a["asset"] in USDT),
        "br_usdt":  days_with(lambda a: a["source"] in BR and a["asset"] in USDT),
    }

    def p_in_window(p, days=7):
        return 1 - (1 - p) ** days

    out = []
    for inc in INCIDENTS:
        w3 = win(inc["date"], 3)
        w7 = win(inc["date"], 7)
        d0 = [a for a in alerts if a["date"] == inc["date"]]
        counts = {
            "d0":     len(d0),
            "pm3":    len(w3),
            "pm7":    len(w7),
            "pm3_hi": sum(1 for a in w3 if a["severity"] in ("critical","high")),
            "pm3_cr": sum(1 for a in w3 if a["severity"] == "critical"),
            "pm3_br": sum(1 for a in w3 if a["source"] in BR),
            "pm3_usdt": sum(1 for a in w3 if a["asset"] in USDT),
            "pm3_br_usdt": sum(1 for a in w3 if a["source"] in BR and a["asset"] in USDT),
        }
        # top alert no ±3d (prioriza composite BR cross-venue + BR+USDT+severidade+z)
        sev_rank = {"critical":3, "high":2, "medium":1}
        COMPOSITE_BR = {"hourly_corr", "ensemble", "ring_of_mules"}  # sinais fortes
        w3_ranked = sorted(w3, key=lambda a: (
            # composites BR sobem junto com BR tradicional; severidade pesa logo depois
            int(a["source"] in BR or a.get("metric") in COMPOSITE_BR),
            sev_rank.get(a["severity"], 0),
            int(a["asset"] in USDT),
            abs(a.get("value", 0)) if isinstance(a.get("value"), (int, float)) else 0,
        ), reverse=True)
        top = w3_ranked[0] if w3_ranked else None
        out.append({
            **inc,
            "counts": counts,
            "top_alert": top,
            "alerts_in_pm3": w3_ranked[:8],
        })

    d["incidents"] = {
        "items": out,
        "baselines_daily": baselines,
        "random_window_pm3": {k: p_in_window(v, 7) for k, v in baselines.items()},
        "summary": {
            "n_total": len(out),
            "n_hit_any_pm3":     sum(1 for x in out if x["counts"]["pm3"] > 0),
            "n_hit_high_pm3":    sum(1 for x in out if x["counts"]["pm3_hi"] > 0),
            "n_hit_critical_pm3":sum(1 for x in out if x["counts"]["pm3_cr"] > 0),
            "n_hit_br_usdt_pm3": sum(1 for x in out if x["counts"]["pm3_br_usdt"] > 0),
        },
    }

    DASH.write_text(json.dumps(d, default=str))
    print(f"incidentes anexados ao dashboard.json — {len(out)} incidentes")
    print(f"summary: {d['incidents']['summary']}")


if __name__ == "__main__":
    main()
