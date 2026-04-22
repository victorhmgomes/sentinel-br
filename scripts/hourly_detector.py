"""
hourly_detector.py — HourlyCorr: volume 1h anormal corroborado entre exchanges BR.

Entrada:  data/ohlcv_hourly.json (produzido por fetch_hourly.py)
Saída:    lista de alertas no schema do dashboard + seção "hourly_corr".

Lógica:
 1. Para cada (venue, symbol), calcula baseline por hora-do-dia (0-23) usando
    a mediana do volume_quote nas últimas 14 dias em cada hora. Isso respeita
    sazonalidade intraday (3h BRT é diferente de 14h BRT).
 2. Para cada (date, hour), computa ratio = vol / median_hod. "Hot" se >= 2.0.
 3. Cross-venue: se 3+ (venue, symbol) combos tão "hot" na mesma (date, hour_utc),
    emite alerta hourly_corr. Severity: high se n>=3, critical se n>=5.
 4. Roll-up diário: um alerta por data, com a hora de pico + contexto.

Por que hora-do-dia baseline? Porque na 20/04 o provedor viu Z enorme em USDT às
horas de pico do mercado — comparar contra baseline da mesma HoD evita gordura
de "nem sempre a janela do dia inteiro é uniforme". Sem isso, madrugadas 3h
parecem sempre anômalas contra o dia inteiro.
"""
from __future__ import annotations
import json, statistics
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HOURLY = ROOT / "data" / "ohlcv_hourly.json"

# thresholds
RATIO_HOT     = 3.0   # vol / median_hod >= 3.0 = "hot hour" (era 2.0, barulhento)
MIN_VOL_BRL   = 50_000  # piso absoluto: hora com vol<50k BRL ignorada (thin-market)
MIN_BASELINE  = 6     # precisa de pelo menos 6 pontos p/ baseline de hora-do-dia
CORR_THRESH   = 3     # cross-venue: 3+ VENUES DISTINTOS hot na mesma (date, hour)
BASELINE_DAYS = 14    # janela rolling para median_hod
LOOKBACK_DAYS = 30    # só avalia as últimas 30 días — todo o histórico fetch

SEV_RANK = {"medium": 2, "high": 3, "critical": 4}

# mapeamento venue interno -> source canônico usado nos outros alertas
VENUE_LABELS = {
    "binance_brl":      "Binance",
    "mercado_bitcoin":  "Mercado Bitcoin",
    "foxbit":           "Foxbit",
}


def _hod_baseline(rows: list[dict], asof_ts_ms: int) -> dict[int, float]:
    """Retorna median(vol_quote) por hour_utc usando APENAS os 14d anteriores a asof.
    Isso garante que o baseline não inclua a hora atual no cálculo."""
    cutoff_lo = asof_ts_ms - BASELINE_DAYS * 86_400_000
    by_hour: dict[int, list[float]] = defaultdict(list)
    for r in rows:
        if r["ts"] >= asof_ts_ms:
            continue
        if r["ts"] < cutoff_lo:
            continue
        v = r.get("volume_quote")
        if v is None or v <= 0:
            continue
        by_hour[r["hour_utc"]].append(v)
    return {
        h: statistics.median(vs)
        for h, vs in by_hour.items()
        if len(vs) >= MIN_BASELINE
    }


def _scan_venue_hot_hours(venue: str, symbol: str, rows: list[dict]) -> list[dict]:
    """Para uma série (venue, symbol), retorna hot hours: [{ts, date, hour_utc, ratio, volume}]."""
    if not rows:
        return []
    rows_sorted = sorted(rows, key=lambda r: r["ts"])
    latest_ts = rows_sorted[-1]["ts"]
    scan_lo = latest_ts - LOOKBACK_DAYS * 86_400_000

    hot = []
    for r in rows_sorted:
        if r["ts"] < scan_lo:
            continue
        baseline = _hod_baseline(rows_sorted, r["ts"])
        med = baseline.get(r["hour_utc"])
        if not med:
            continue
        v = r.get("volume_quote") or 0
        if v < MIN_VOL_BRL:
            continue
        ratio = v / med
        if ratio >= RATIO_HOT:
            hot.append({
                "ts":         r["ts"],
                "date":       r["date"],
                "hour_utc":   r["hour_utc"],
                "ratio":      round(ratio, 2),
                "volume":     round(v, 2),
                "baseline":   round(med, 2),
                "venue":      venue,
                "symbol":     symbol,
            })
    return hot


def build_hourly_corr_alerts(hourly_path: Path = HOURLY) -> tuple[list[dict], dict]:
    """Retorna (alerts, summary_section).

    alerts: lista no schema padrão do dashboard (date, source, asset, metric,
            value, severity, tags, hot_hours, corroborating_venues)
    summary_section: dict pronto pra ir no dashboard.json[\"hourly_corr\"]
    """
    if not hourly_path.exists():
        return [], {
            "enabled": False,
            "reason":  f"{hourly_path.name} ausente — rode fetch_hourly.py antes",
        }

    raw = json.loads(hourly_path.read_text())
    h_data = raw.get("hourly", {})

    # 1. Scan de hot hours por venue-symbol
    all_hot: list[dict] = []
    series_summary: list[dict] = []
    for venue, syms in h_data.items():
        for sym, rows in syms.items():
            hot = _scan_venue_hot_hours(venue, sym, rows)
            all_hot.extend(hot)
            series_summary.append({
                "venue":    VENUE_LABELS.get(venue, venue),
                "symbol":   sym,
                "n_rows":   len(rows),
                "n_hot":    len(hot),
                "top_ratio": max((h["ratio"] for h in hot), default=0.0),
            })

    # 2. Corroboração cross-venue por (date, hour_utc)
    by_slot: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for h in all_hot:
        by_slot[(h["date"], h["hour_utc"])].append(h)

    # 3. Agrega por (date) — um alerta por dia, consolidando todas as hot hours
    alerts_by_date: dict[str, dict] = {}
    for (date, hour), hits in by_slot.items():
        # corroboração REAL: 3+ venues DISTINTOS (não combos de mesmo venue).
        # Binance:BTCBRL + Binance:USDTBRL contam como 1 venue, não 2.
        unique_venues = {h["venue"] for h in hits}
        unique_combos = {(h["venue"], h["symbol"]) for h in hits}
        if len(unique_venues) < CORR_THRESH:
            continue

        # existing alert do dia? aumenta
        a = alerts_by_date.get(date)
        if a is None:
            a = {
                "date":        date,
                "source":      "cross-venue",
                "asset":       "multi",
                "metric":      "hourly_corr",
                "value":       0,       # peak n_venues
                "severity":    "high",
                "price":       None,
                "volume":      None,
                "tags":        ["intraday", "hourly_corr", "cross-venue", "BR"],
                "hot_hours":   [],
                "corroborating_venues": [],
            }
            alerts_by_date[date] = a

        venues_fmt = sorted(f"{VENUE_LABELS.get(v, v)}:{s}" for (v, s) in unique_combos)
        top_ratio = max(h["ratio"] for h in hits)
        a["hot_hours"].append({
            "hour_utc":     hour,
            "n_venues":     len(unique_venues),
            "n_combos":     len(unique_combos),
            "venues":       venues_fmt,
            "top_ratio":    round(top_ratio, 2),
        })
        a["value"] = max(a["value"], len(unique_venues))
        for v in venues_fmt:
            if v not in a["corroborating_venues"]:
                a["corroborating_venues"].append(v)
        # severity bump: >=3 venues E >=4 combos OU ratio >=10
        if len(unique_venues) >= 3 and (len(unique_combos) >= 4 or top_ratio >= 10):
            a["severity"] = "critical"
        # tag com horário de pico (BRT = UTC-3)
        tag_hour = f"hour_{hour:02d}Z"
        if tag_hour not in a["tags"]:
            a["tags"].append(tag_hour)

    # ordena hot_hours por hora
    alerts = []
    for date in sorted(alerts_by_date.keys()):
        a = alerts_by_date[date]
        a["hot_hours"].sort(key=lambda x: x["hour_utc"])
        alerts.append(a)

    summary = {
        "enabled":      True,
        "generated_at": raw.get("generated_at"),
        "window":       raw.get("window"),
        "thresholds": {
            "ratio_hot":    RATIO_HOT,
            "corr_thresh":  CORR_THRESH,
            "baseline_days": BASELINE_DAYS,
            "lookback_days": LOOKBACK_DAYS,
        },
        "series":       series_summary,
        "n_hot_slots":  len(by_slot),
        "n_alerts":     len(alerts),
        "description":  (
            "HourlyCorr — volume 1h >= 2× mediana da mesma hora-do-dia "
            "(baseline 14d), com 3+ exchanges BR corroborando na mesma janela."
        ),
    }
    return alerts, summary


if __name__ == "__main__":
    alerts, summary = build_hourly_corr_alerts()
    print(f"HourlyCorr: {summary.get('n_alerts', 0)} alerta(s) em {summary.get('n_hot_slots', 0)} slot(s) quentes")
    for s in summary.get("series", []):
        print(f"  {s['venue']:<18} {s['symbol']:<10} rows={s['n_rows']:4d} hot={s['n_hot']:3d} peak_ratio={s['top_ratio']}")
    for a in alerts:
        print(f"  [{a['severity']:<8}] {a['date']} n_venues={a['value']} venues={','.join(a['corroborating_venues'][:4])}")
