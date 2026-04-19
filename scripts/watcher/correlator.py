"""
correlator.py — agrega alertas independentes em EVENTOS correlacionados.

Motivação: cada detector dispara isoladamente; um ataque real acende vários
deles em janela curta (price_burst + depth_drop + decouple + spread_spike ao
mesmo tempo, tipicamente). Uma camada de cima que olha "≥N detectores
distintos dentro de Δt no mesmo ativo" vira um super-alerta de altíssima
precisão e serve como dedup natural.

Schema de entrada (alertas): dicts com os campos:
  - rule        (str): nome do detector
  - severity    (str): "medium" | "high" | "critical" | "info"
  - venue       (str)
  - asset       (str): ex "BTC-BRL", "USDT-BRL"
  - value       (num)
  - threshold   (num)
  - narrative   (str)
  - context     (dict)
  - ts          (str ISO-8601)

Schema de saída (eventos):
  {
    "event_id": "evt_YYYYMMDD_HHMM_<asset>",
    "asset": "BTC-BRL",
    "opened_at": "2026-04-19T14:37:00+00:00",
    "closed_at": "2026-04-19T14:41:30+00:00",
    "duration_s": 270,
    "distinct_detectors": ["price_burst", "depth_drop", ...],
    "n_distinct_detectors": 4,
    "n_alerts": 23,
    "max_alert_severity": "high",
    "event_severity": "critical",
    "narrative": "4 sinais convergentes em BTC-BRL entre 14:37 e 14:41 (~4min): ...",
    "alerts": [ ... alertas contribuintes ... ]
  }
"""
from __future__ import annotations
from collections import defaultdict
from datetime import datetime, timezone
from typing import Iterable


# ---------- parâmetros padrão ----------
DEFAULTS = {
    # Janela de dedup por (rule, asset): repetições do mesmo sinal dentro desse
    # intervalo contam como 1 sinal pro evento, mas contribuem com severidade máxima.
    "dedup_s": 60,

    # Cooldown: um evento fica "aberto" enquanto chegam novos alertas. Se não
    # chega nada novo nesse tempo, o evento fecha. Próximo alerta no mesmo
    # ativo abre evento novo.
    "cooldown_s": 300,

    # Limite mínimo de detectores distintos pra considerar "evento correlacionado"
    # (com N_min=1 viram "singletons" — também registrados, severity_event = "singleton").
    "min_distinct_for_event": 2,
}

# Mapa rule → family (para apresentação mais limpa no dashboard)
RULE_FAMILY = {
    "binance_spread_spike":       "spread_spike",
    "binance_depth_drop":         "depth_drop",
    "binance_price_burst":        "price_burst",
    "binance_vs_br_divergence":   "divergence",
    "br_pre_spike":               "pre_spike",
    "usdt_brl_decouple":          "decouple",
    "stablecoin_flight":          "stablecoin_flight",
}

_SEV_ORDER = {"info": 0, "medium": 1, "high": 2, "critical": 3}
_SEV_REV   = {v: k for k, v in _SEV_ORDER.items()}


def _parse_ts(s: str) -> datetime:
    """Aceita ISO-8601 com ou sem timezone. Retorna tz-aware (UTC se ausente)."""
    s = s.replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _max_sev(a: str, b: str) -> str:
    return _SEV_REV[max(_SEV_ORDER.get(a, 0), _SEV_ORDER.get(b, 0))]


def _roll_up_severity(max_alert_sev: str, n_distinct: int) -> str:
    """Severidade do evento agregado.

    Regra: convergência entre detectores distintos escala a severidade:
      - 1 detector  -> "singleton"  (ainda registrado mas não é evento "real")
      - 2 detectores-> max(medium, max_alert_sev)
      - 3 detectores-> max(high,   max_alert_sev)
      - 4+          -> "critical" sempre

    Isso captura a intuição: quando 4 detectores independentes concordam,
    a probabilidade de falso positivo é esmagadoramente menor que cada um
    sozinho — independente da severidade individual.
    """
    if n_distinct <= 1:
        return "singleton"
    if n_distinct >= 4:
        return "critical"
    base = {2: "medium", 3: "high"}[n_distinct]
    return _max_sev(base, max_alert_sev)


def _event_id(asset: str, opened_at: datetime) -> str:
    return "evt_{ymd}_{hm}_{asset}".format(
        ymd=opened_at.strftime("%Y%m%d"),
        hm=opened_at.strftime("%H%M"),
        asset=asset.replace("-", "").lower(),
    )


def _build_narrative(asset: str, opened_at: datetime, closed_at: datetime,
                     families: list[str], max_alert_sev: str, n_alerts: int) -> str:
    dur = int((closed_at - opened_at).total_seconds())
    dur_txt = f"{dur//60}min{dur%60:02d}s" if dur >= 60 else f"{dur}s"
    fam_txt = ", ".join(families)
    hr = opened_at.strftime("%H:%M")
    hr_end = closed_at.strftime("%H:%M")
    n = len(families)
    if n <= 1:
        return (f"Sinal isolado em {asset} às {hr} ({fam_txt}). "
                f"{n_alerts} alerta(s) bruto(s). Severidade máxima: {max_alert_sev}.")
    return (f"{n} sinais convergentes em {asset} entre {hr}-{hr_end} (~{dur_txt}): "
            f"{fam_txt}. {n_alerts} alerta(s) bruto(s). "
            f"Severidade máxima vista: {max_alert_sev}.")


# ---------- core ----------
def correlate(alerts: Iterable[dict], params: dict | None = None) -> list[dict]:
    """
    Agrega alertas em eventos por (asset). Janela deslizante com cooldown.

    Args:
        alerts: iterável de dicts com schema descrito no topo do arquivo.
        params: override de DEFAULTS (dedup_s, cooldown_s, min_distinct_for_event).

    Returns:
        Lista de dicts de evento (ordenados por opened_at desc).
    """
    p = dict(DEFAULTS)
    if params:
        p.update(params)
    dedup_s    = p["dedup_s"]
    cooldown_s = p["cooldown_s"]
    min_dist   = p["min_distinct_for_event"]

    # parse e ordena por ts
    sortable = []
    for a in alerts:
        try:
            t = _parse_ts(a["ts"])
        except Exception:
            continue
        sortable.append((t, a))
    sortable.sort(key=lambda x: x[0])

    # agrupa por asset
    by_asset: dict[str, list] = defaultdict(list)
    for t, a in sortable:
        by_asset[a.get("asset", "UNKNOWN")].append((t, a))

    events: list[dict] = []
    for asset, seq in by_asset.items():
        open_ev = None
        # _last_seen_sig: {(rule): datetime} — dedup por regra dentro do evento
        for t, a in seq:
            if open_ev is None:
                open_ev = _new_open_event(asset, t, a)
                continue
            # se passou do cooldown desde o último alerta novo, fecha e abre novo
            if (t - open_ev["_last_t"]).total_seconds() > cooldown_s:
                events.append(_finalize(open_ev, min_dist))
                open_ev = _new_open_event(asset, t, a)
                continue
            # mesmo evento: decide se é repetição (dedup) ou novo sinal
            rule = a.get("rule", "?")
            last_seen_for_rule = open_ev["_last_seen"].get(rule)
            is_dup = (last_seen_for_rule is not None and
                      (t - last_seen_for_rule).total_seconds() < dedup_s)
            # sempre registra o alerta cru e atualiza max severity
            open_ev["alerts"].append(a)
            open_ev["_last_seen"][rule] = t
            open_ev["_last_t"] = t
            open_ev["_max_alert_sev"] = _max_sev(
                open_ev["_max_alert_sev"], a.get("severity", "info"))
            if not is_dup:
                # é um "novo sinal" daquele detector (fora da janela de dedup)
                open_ev["_n_signals_per_rule"][rule] += 1
            open_ev["_closed_at"] = t
        if open_ev is not None:
            events.append(_finalize(open_ev, min_dist))

    # ordenação final: mais recente primeiro
    events.sort(key=lambda e: e["opened_at"], reverse=True)
    return events


def _new_open_event(asset: str, t: datetime, first_alert: dict) -> dict:
    rule = first_alert.get("rule", "?")
    return {
        "asset": asset,
        "_opened_at": t,
        "_closed_at": t,
        "_last_t": t,
        "_last_seen": {rule: t},
        "_n_signals_per_rule": defaultdict(int, {rule: 1}),
        "_max_alert_sev": first_alert.get("severity", "info"),
        "alerts": [first_alert],
    }


def _finalize(ev: dict, min_dist: int) -> dict:
    opened = ev["_opened_at"]
    closed = ev["_closed_at"]
    distinct_rules  = list(ev["_n_signals_per_rule"].keys())
    n_distinct      = len(distinct_rules)
    families        = [RULE_FAMILY.get(r, r) for r in distinct_rules]
    max_alert_sev   = ev["_max_alert_sev"]
    event_sev       = _roll_up_severity(max_alert_sev, n_distinct)
    n_alerts        = len(ev["alerts"])
    dur_s           = int((closed - opened).total_seconds())

    out = {
        "event_id":             _event_id(ev["asset"], opened),
        "asset":                ev["asset"],
        "opened_at":            opened.isoformat(),
        "closed_at":            closed.isoformat(),
        "duration_s":           dur_s,
        "distinct_detectors":   families,
        "distinct_rules":       distinct_rules,  # nome raw, pra referência
        "n_distinct_detectors": n_distinct,
        "n_alerts":             n_alerts,
        "max_alert_severity":   max_alert_sev,
        "event_severity":       event_sev,
        "is_correlated":        n_distinct >= min_dist,
        "narrative":            _build_narrative(
            ev["asset"], opened, closed, families, max_alert_sev, n_alerts),
        "alerts":               ev["alerts"],
    }
    return out


# ---------- resumo pro dashboard ----------
def summarize(events: list[dict]) -> dict:
    """Gera estatísticas agregadas pra KPIs da seção."""
    n_total = len(events)
    correlated = [e for e in events if e["is_correlated"]]
    n_corr  = len(correlated)
    n_sing  = n_total - n_corr
    by_sev = defaultdict(int)
    by_asset = defaultdict(int)
    max_conv = 0
    for e in events:
        by_sev[e["event_severity"]] += 1
        by_asset[e["asset"]] += 1
        max_conv = max(max_conv, e["n_distinct_detectors"])
    return {
        "n_total":          n_total,
        "n_correlated":     n_corr,
        "n_singleton":      n_sing,
        "max_convergence":  max_conv,
        "by_severity":      dict(by_sev),
        "by_asset":         dict(by_asset),
    }


# ---------- smoke test ----------
if __name__ == "__main__":
    # pequeno teste sintético: 4 sinais em BTC-BRL em janela curta => evento crítico
    sample = [
        {"rule": "binance_price_burst",      "severity": "high",     "venue": "Binance", "asset": "BTC-BRL", "ts": "2026-04-19T14:37:00+00:00", "value": 7.2, "threshold": 6.0},
        {"rule": "binance_depth_drop",       "severity": "medium",   "venue": "Binance", "asset": "BTC-BRL", "ts": "2026-04-19T14:37:30+00:00", "value": 0.41, "threshold": 0.35},
        {"rule": "binance_spread_spike",     "severity": "high",     "venue": "Binance", "asset": "BTC-BRL", "ts": "2026-04-19T14:38:10+00:00", "value": 2.3, "threshold": 1.25},
        {"rule": "usdt_brl_decouple",        "severity": "critical", "venue": "Binance", "asset": "USDT-BRL","ts": "2026-04-19T14:39:00+00:00", "value": 1.2, "threshold": 0.30},
        # depois do cooldown: novo evento
        {"rule": "binance_price_burst",      "severity": "medium",   "venue": "Binance", "asset": "BTC-BRL", "ts": "2026-04-19T15:10:00+00:00", "value": 6.5, "threshold": 6.0},
    ]
    import json
    evs = correlate(sample)
    print(json.dumps({"events": evs, "summary": summarize(evs)}, indent=2, default=str))
