"""
tron_detector.py — detector tron_outflow (saída de USDT TRC-20).

Lê data/tron.json (série diária + z_scores já computados pelo fetcher),
emite alertas no schema unificado do dashboard quando |Z| ≥ 3.

Schema esperado em tron.json:
  series: [{date, usdt_tx, total_tx, active_accounts, ...}, ...]
  z_scores: [float, ...]  (mesma ordem da series, Z-score de usdt_tx vs janela 30d)

Severidade (calibrada empiricamente sobre 365d de USDT TRC-20):
  z ≥ +1.8  → high      (volume on-chain anormalmente alto = cash-out massivo)
  z ≥ +2.2  → critical
Z negativo (volume baixo) NÃO é alerta: o sinal de interesse é OUTFLOW
elevado (cash-out PIX→TRC20). Z-scores TRON são comprimidos vs os de
exchanges BR porque a base diária é enorme (~2.3M USDT-tx/dia), então
thresholds são menores que os 3/4 usados em volume_zscore.

Cross-correlação com USDT-BRL (opcional, fase 2):
  Se cross_usdt_brl_dates for fornecido, marca tag "corr:usdt_brl_pm1d"
  para alertas TRON que caem em ±1d de um pico USDT-BRL.
"""
from __future__ import annotations
import json
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parent.parent
TRON_JSON = ROOT / "data" / "tron.json"


def _severity(z) -> str | None:
    """Apenas Z positivo conta (outflow elevado = cash-out)."""
    if z is None:
        return None
    try:
        zf = float(z)
    except (TypeError, ValueError):
        return None
    if zf >= 2.2: return "critical"
    if zf >= 1.8: return "high"
    return None


def build_tron_outflow_alerts(
    cross_usdt_brl_dates: Iterable[str] | None = None,
) -> tuple[list[dict], dict]:
    """Retorna (alerts, summary). Vazio se tron.json não existir."""
    if not TRON_JSON.exists():
        return [], {"enabled": False, "reason": "tron.json não encontrado"}

    try:
        d = json.loads(TRON_JSON.read_text(encoding="utf-8"))
    except Exception as e:
        return [], {"enabled": False, "reason": f"parse error: {e}"}

    series = d.get("series", [])
    zs = d.get("z_scores", [])
    if not series or not zs or len(series) != len(zs):
        return [], {"enabled": False, "reason": "series/z_scores ausentes ou desalinhados"}

    cross_set = set(cross_usdt_brl_dates or [])

    alerts: list[dict] = []
    for row, z in zip(series, zs):
        sev = _severity(z)
        if not sev:
            continue
        date = row.get("date")
        if not date:
            continue
        tags = ["on-chain", "tron", "usdt_trc20"]
        if cross_set and date in cross_set:
            tags.append("corr:usdt_brl_pm1d")
            # corroboração on/off-chain escala severidade
            if sev == "high":
                sev = "critical"
        alerts.append({
            "date": date,
            "source": "TRON network",
            "asset": "USDT (TRC-20)",
            "metric": "tron_outflow",
            "value": round(z, 2),
            "severity": sev,
            "price": None,
            "volume": row.get("usdt_tx"),
            "tags": tags,
        })

    by_sev = {"critical": 0, "high": 0}
    for a in alerts:
        by_sev[a["severity"]] = by_sev.get(a["severity"], 0) + 1

    summary = {
        "enabled": True,
        "n_days_scored": len(zs),
        "n_alerts": len(alerts),
        "by_severity": by_sev,
        "n_cross_corr": sum(1 for a in alerts if "corr:usdt_brl_pm1d" in a.get("tags", [])),
        "contract": d.get("contract"),
        "asset_info": d.get("info", {}).get("symbol"),
    }
    return alerts, summary


if __name__ == "__main__":
    alerts, summ = build_tron_outflow_alerts()
    print(json.dumps(summ, indent=2))
    print(f"\nalerts ({len(alerts)}):")
    for a in alerts[:10]:
        print(" ", a["date"], a["severity"], "Z=" + str(a["value"]), "vol=" + str(a["volume"]))
