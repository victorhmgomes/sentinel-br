"""
detect_anomalies.py — Motor de indicadores + alertas.

Entrada:  data/raw.json
Saída:    data/dashboard.json  (consumido pelo index.html)

Indicadores calculados:
 1. Volume Z-score (janela 30d)  → pico de liquidez
 2. Volume / mediana 30d          → ratio robusto
 3. EWMA(span=14) vs long (90d)   → shift de regime
 4. Stablecoin flight: USDT-BRL vol / BTC-BRL vol (proxy de fuga p/ cripto)
 5. Premium BR: (BTC-BRL) / (BTCUSDT * USDT-BRL) − 1
 6. Dispersão de volume (Herfindahl inverso por exchange) — concentração
 7. Price-volume divergence      → volume alto sem movimento de preço
 8. On-chain: hashrate Z-score (sanidade, não é fraude)
"""
from __future__ import annotations
import json, math, statistics, sys, datetime as dt
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
# Permite importar hourly_detector sem precisar rodar do diretório scripts/
sys.path.insert(0, str(Path(__file__).resolve().parent))
RAW  = ROOT / "data" / "raw.json"
OUT  = ROOT / "data" / "dashboard.json"

SINQIA_DATE = "2025-08-29"  # ataque à Sinqia
ALERT_LEVELS = {"critical": 4.0, "high": 3.0, "medium": 2.0}


def rolling(stat, series, window):
    out = [None] * len(series)
    for i in range(len(series)):
        w = [x for x in series[max(0, i - window):i] if x is not None]
        if len(w) >= max(5, window // 3):
            out[i] = stat(w)
    return out


def mean(xs): return sum(xs) / len(xs) if xs else None
def std(xs):
    if len(xs) < 2: return 0.0
    m = mean(xs); return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))
def median(xs): return statistics.median(xs) if xs else None
def mad(xs):
    if not xs: return 0.0
    med = median(xs); return median([abs(x - med) for x in xs])


def compute_series(rows, key="volume_quote"):
    dates = [r["date"] for r in rows]
    vals  = [r.get(key) for r in rows]
    price = [r.get("close") for r in rows]
    mean30  = rolling(mean, vals, 30)
    std30   = rolling(std, vals, 30)
    med30   = rolling(median, vals, 30)
    mad30   = rolling(mad, vals, 30)
    mean90  = rolling(mean, vals, 90)

    zscore = [
        ((v - m) / s) if (v is not None and m is not None and s and s > 0) else None
        for v, m, s in zip(vals, mean30, std30)
    ]
    robust_z = [
        ((v - m) / (1.4826 * d)) if (v is not None and m is not None and d and d > 0) else None
        for v, m, d in zip(vals, med30, mad30)
    ]
    ratio = [
        (v / m) if (v is not None and m) else None
        for v, m in zip(vals, med30)
    ]
    regime = [
        ((m30 - m90) / m90) if (m30 is not None and m90) else None
        for m30, m90 in zip(mean30, mean90)
    ]

    # price-volume divergence: dias com vol alto (z>2) e |ret|<0.5% sobre janela
    divergence = []
    for i, (v, m, s) in enumerate(zip(vals, mean30, std30)):
        if i == 0 or v is None or m is None or s is None or s == 0:
            divergence.append(None); continue
        z = (v - m) / s
        p0, p1 = price[i-1], price[i]
        ret = abs((p1 - p0) / p0) if p0 else 0
        divergence.append(1.0 if (z > 2 and ret < 0.005) else 0.0)

    return {
        "dates": dates, "volume": vals, "price": price,
        "mean30": mean30, "std30": std30, "zscore": zscore,
        "robust_z": robust_z, "ratio": ratio, "regime": regime,
        "divergence": divergence,
    }


def align_by_date(series_list):
    """Interseção de datas entre múltiplas séries (mesmo dia)."""
    date_sets = [set(s["dates"]) for s in series_list]
    common = sorted(set.intersection(*date_sets))
    aligned = []
    for s in series_list:
        idx = {d: i for i, d in enumerate(s["dates"])}
        aligned.append({k: [v[idx[d]] if k != "dates" else d for d in common] if isinstance(v, list) else v
                        for k, v in s.items()})
    return common, aligned


def severity(z):
    if z is None: return None
    for name, thr in ALERT_LEVELS.items():
        if z >= thr: return name
    return None


def alerts_from_series(dates, zscore, source_name, asset, price, volume, extra_tags=None):
    out = []
    for i, (d, z) in enumerate(zip(dates, zscore)):
        sev = severity(z)
        if not sev: continue
        tags = list(extra_tags or [])
        out.append({
            "date": d,
            "source": source_name,
            "asset": asset,
            "metric": "volume_zscore",
            "value": round(z, 2),
            "severity": sev,
            "price": price[i],
            "volume": volume[i],
            "tags": tags,
        })
    return out


def build_br_premium(mb_btc, bn_btc, mb_usdt):
    """Premium BR = BTC-BRL / (BTCUSDT * USDT-BRL) − 1 .  Desvio >2% é anomalia."""
    idx_bn = {r["date"]: r["close"] for r in bn_btc}
    idx_usdt = {r["date"]: r["close"] for r in mb_usdt}
    out = []
    for r in mb_btc:
        d = r["date"]
        if d in idx_bn and d in idx_usdt and idx_usdt[d] > 0:
            fair = idx_bn[d] * idx_usdt[d]
            prem = (r["close"] / fair) - 1
            out.append({"date": d, "premium": prem, "br_price": r["close"], "fair": fair})
    return out


def main():
    raw = json.loads(RAW.read_text())
    bn = raw["sources"]["binance"]
    bnbr = raw["sources"].get("binance_brl", {}) or {}
    mb = raw["sources"]["mercado_bitcoin"]
    fx = raw["sources"]["foxbit"]
    oc = raw["sources"]["onchain_btc"]

    # ---------- métricas por par/exchange ----------
    # Venues BR (usados no ring-of-mules): MB, Foxbit, Binance BRL.
    # NovaDAX/Bitso BR/BitPreço/Ripio Trade ainda não expõem klines históricos
    # públicos sem auth; quando expuserem, entram aqui automaticamente.
    BR_VENUE_SET = {"Mercado Bitcoin", "Foxbit", "Binance BRL"}
    metrics = {}
    for venue, pack, assets in [
        ("Binance",  bn, {"BTCUSDT":"BTC", "ETHUSDT":"ETH", "BNBUSDT":"BNB", "SOLUSDT":"SOL", "XRPUSDT":"XRP"}),
        ("Binance BRL", bnbr, {"BTCBRL":"BTC", "ETHBRL":"ETH", "USDTBRL":"USDT", "SOLBRL":"SOL"}),
        ("Mercado Bitcoin", mb, {"BTC-BRL":"BTC", "USDT-BRL":"USDT", "ETH-BRL":"ETH", "SOL-BRL":"SOL"}),
        ("Foxbit",   fx, {"btcbrl":"BTC", "usdtbrl":"USDT", "ethbrl":"ETH"}),
    ]:
        for sym, asset in assets.items():
            rows = pack.get(sym, [])
            if len(rows) < 60: continue
            s = compute_series(rows, key="volume_quote")
            key = f"{venue}|{asset}|{sym}"
            metrics[key] = {"venue": venue, "asset": asset, "symbol": sym, **s}

    # ---------- alertas ----------
    alerts = []
    for key, m in metrics.items():
        tags = []
        if m["asset"] == "USDT": tags.append("stablecoin")
        if m["venue"] in BR_VENUE_SET: tags.append("BR")
        if m["venue"] == "Binance": tags.append("global")
        a = alerts_from_series(m["dates"], m["zscore"], m["venue"], m["asset"],
                               m["price"], m["volume"], extra_tags=tags)
        alerts.extend(a)

    # Indicadores agregados BR: USDT/BTC ratio por venue, concentração entre venues
    def series_by_date(rows, field):
        return {r["date"]: r.get(field) for r in rows}

    # Stablecoin flight (MB): USDT-BRL vol / BTC-BRL vol
    flight = []
    if mb.get("USDT-BRL") and mb.get("BTC-BRL"):
        u = series_by_date(mb["USDT-BRL"], "volume_quote")
        b = series_by_date(mb["BTC-BRL"],  "volume_quote")
        for d in sorted(set(u) & set(b)):
            bu, bb = u[d] or 0, b[d] or 0
            if bb > 0:
                flight.append({"date": d, "ratio": bu / bb})
        vals = [x["ratio"] for x in flight]
        mean30 = rolling(mean, vals, 30)
        std30  = rolling(std,  vals, 30)
        for i, row in enumerate(flight):
            if mean30[i] is not None and std30[i] and std30[i] > 0:
                z = (row["ratio"] - mean30[i]) / std30[i]
                row["zscore"] = z
                sev = severity(z)
                if sev:
                    alerts.append({
                        "date": row["date"], "source": "Mercado Bitcoin",
                        "asset": "USDT/BTC", "metric": "stablecoin_flight",
                        "value": round(z, 2), "severity": sev,
                        "price": None, "volume": row["ratio"],
                        "tags": ["stablecoin", "BR", "laundering_proxy"],
                    })
            else:
                row["zscore"] = None

    # Premium BR arbitragem
    premium = build_br_premium(mb.get("BTC-BRL", []), bn.get("BTCUSDT", []), mb.get("USDT-BRL", []))
    prem_vals = [p["premium"] for p in premium]
    prem_mean = rolling(mean, prem_vals, 30)
    prem_std  = rolling(std,  prem_vals, 30)
    for i, row in enumerate(premium):
        if prem_mean[i] is not None and prem_std[i] and prem_std[i] > 0:
            row["zscore"] = (row["premium"] - prem_mean[i]) / prem_std[i]
            if abs(row["zscore"]) >= 3 and abs(row["premium"]) > 0.01:
                sev = "high" if abs(row["zscore"]) < 4 else "critical"
                alerts.append({
                    "date": row["date"], "source": "Arbitragem BR",
                    "asset": "BTC", "metric": "br_premium",
                    "value": round(row["premium"] * 100, 2),
                    "severity": sev,
                    "price": row["br_price"], "volume": None,
                    "tags": ["BR", "arbitrage", "laundering_proxy"],
                })
        else:
            row["zscore"] = None

    # Concentração BR: share diário do MB vs (MB+Foxbit) em BTC-BRL
    conc = []
    if mb.get("BTC-BRL") and fx.get("btcbrl"):
        m_v = series_by_date(mb["BTC-BRL"], "volume_quote")
        f_v = series_by_date(fx["btcbrl"],  "volume_quote")
        for d in sorted(set(m_v) & set(f_v)):
            total = (m_v[d] or 0) + (f_v[d] or 0)
            if total > 0:
                conc.append({
                    "date": d,
                    "mb_share": (m_v[d] or 0) / total,
                    "fx_share": (f_v[d] or 0) / total,
                    "total_brl": total,
                })

    # ---------- Heatmap: matriz (série × data × z-score) ----------
    # Formato compacto para renderizar widget tipo contribution-graph do GitHub.
    # Cada série vira uma "linha"; cada dia vira uma célula com Z-score.
    heatmap_series = []
    for key, m in metrics.items():
        # Usar z robusto quando disponível, fallback para z padrão
        cells = []
        for d, z in zip(m["dates"], m["zscore"]):
            if z is None:
                cells.append({"date": d, "z": None, "sev": None})
                continue
            sev = "critical" if z >= 4 else "high" if z >= 3 else "medium" if z >= 2 else None
            cells.append({"date": d, "z": round(z, 2), "sev": sev})
        heatmap_series.append({
            "key":    key,
            "venue":  m["venue"],
            "asset":  m["asset"],
            "symbol": m["symbol"],
            "cells":  cells,
        })
    # Ordem: BR primeiro, depois globais; dentro de cada grupo por asset
    venue_order = {"Mercado Bitcoin": 0, "Foxbit": 1, "Binance BRL": 2, "Binance": 3}
    heatmap_series.sort(key=lambda s: (venue_order.get(s["venue"], 9), s["asset"], s["symbol"]))

    # ---------- Ring-of-mules: dias com ≥3 séries BR em Z≥2 ----------
    # BR = Mercado Bitcoin + Foxbit + Binance BRL (todas as séries dessas exchanges).
    # Quando NovaDAX/Bitso BR/BitPreço/Ripio expuserem klines, entram aqui.
    br_keys = [k for k, m in metrics.items() if m["venue"] in BR_VENUE_SET]
    br_metrics = [metrics[k] for k in br_keys]
    # Coletar todas as datas únicas das séries BR
    all_br_dates = sorted(set(d for m in br_metrics for d in m["dates"]))
    ring_days = []
    for d in all_br_dates:
        hits = []
        for m in br_metrics:
            if d in m["dates"]:
                idx = m["dates"].index(d)
                z = m["zscore"][idx]
                if z is not None and z >= 2:
                    sev = "critical" if z >= 4 else "high" if z >= 3 else "medium"
                    hits.append({"venue": m["venue"], "asset": m["asset"],
                                 "symbol": m["symbol"], "z": round(z, 2), "sev": sev})
        # Requer ≥3 séries BR acionando E ≥2 venues distintos (senão é concentração
        # idiossincrática do mesmo venue, não "ring" multi-corretora).
        n_venues_hit = len({h["venue"] for h in hits})
        if len(hits) >= 3 and n_venues_hit >= 2:
            max_z  = max(h["z"] for h in hits)
            n_crit = sum(1 for h in hits if h["sev"] == "critical")
            n_high = sum(1 for h in hits if h["sev"] == "high")
            # severidade: ≥3 venues distintos OR (≥2 crit) → critical;
            # ≥2 venues + (≥2 high OR ≥1 crit) → high; senão medium
            if (n_venues_hit >= 3) or (n_crit >= 2) or (n_crit >= 1 and n_high >= 2):
                r_sev = "critical"
            elif n_high >= 2 or n_crit >= 1:
                r_sev = "high"
            else:
                r_sev = "medium"
            ring_days.append({
                "date": d, "n_hits": len(hits), "n_venues": n_venues_hit,
                "max_z": max_z, "severity": r_sev, "hits": hits,
            })
            # Propagar pros alertas principais
            alerts.append({
                "date": d, "source": "BR multi-venue", "asset": "multi",
                "metric": "ring_of_mules", "value": len(hits), "severity": r_sev,
                "price": None, "volume": None,
                "tags": ["BR", "ring_of_mules", "composite", f"{n_venues_hit}venues"],
            })
    # Base rate para interpretação: % de dias de 12m que disparam
    base_rate = (len(ring_days) / len(all_br_dates)) if all_br_dates else 0

    # ---------- HourlyCorr: volume 1h corroborado cross-venue ----------
    # Granularidade horária sobre Binance BRL + MB + Foxbit (30d × 24h).
    # Detecta pré-posição tipo 20/04/26 que baseline diário miss.
    try:
        from hourly_detector import build_hourly_corr_alerts
        hourly_alerts, hourly_summary = build_hourly_corr_alerts()
        alerts.extend(hourly_alerts)
        print(f"[hourly_corr] {len(hourly_alerts)} alertas cross-venue (horário)")
    except Exception as e:
        print(f"[hourly_corr] skip: {e}")
        hourly_alerts, hourly_summary = [], {"enabled": False, "reason": str(e)}

    # ---------- Ensemble severity: detectores co-acionando mesmo dia/venue ----------
    # Provider benchmark (20/04/26 MB): stablecoin_flight (medium) sozinho.
    # O provider vizinho disparou 3/8 agentes (Stablecoin + SingleExch +
    # HourlyCorr) e classificou HIGH. Mesmo sem os agentes faltando, nós
    # podemos escalar por corroboração: 2+ metrics distintos na mesma
    # (date, source) → high; 3+ → critical. Se algum dos signals original
    # já é critical, preserva. ring_of_mules e ensemble são excluídos
    # do agrupamento (já são composites).
    SEV_RANK = {"medium": 2, "high": 3, "critical": 4}
    COMPOSITE_METRICS = {"ring_of_mules", "ensemble", "hourly_corr"}
    by_day_source: dict[tuple[str,str], list[dict]] = defaultdict(list)
    for a in alerts:
        if a.get("metric") in COMPOSITE_METRICS:
            continue
        by_day_source[(a["date"], a["source"])].append(a)

    ensemble_alerts = []
    ensemble_days: list[dict] = []  # para KPI / heatmap
    for (d, src), group in by_day_source.items():
        metrics_fired = sorted({a["metric"] for a in group})
        if len(metrics_fired) < 2:
            continue
        n = len(metrics_fired)
        max_individual = max((SEV_RANK.get(a["severity"], 0) for a in group), default=0)
        # escalada por corroboração
        if n >= 3 or max_individual >= SEV_RANK["critical"]:
            e_sev = "critical"
        else:
            e_sev = "high"
        top_abs = max(
            (abs(a["value"]) if isinstance(a.get("value"), (int, float)) else 0)
            for a in group
        )
        ensemble_alerts.append({
            "date": d, "source": src, "asset": "multi",
            "metric": "ensemble",
            "value": n,  # quantidade de metrics distintos corroborando
            "severity": e_sev,
            "price": None, "volume": None,
            "tags": ["ensemble", "composite", f"{n}signals"] +
                    (["BR"] if src in BR_VENUE_SET else []),
            "corroborating_metrics": metrics_fired,
            "top_zscore": round(top_abs, 2),
        })
        ensemble_days.append({
            "date": d, "source": src,
            "n_signals": n, "severity": e_sev,
            "metrics": metrics_fired,
            "top_zscore": round(top_abs, 2),
        })

    alerts.extend(ensemble_alerts)
    ensemble_days.sort(key=lambda x: (x["date"], x["source"]))

    # ---------- KPIs headline ----------
    last_30 = sorted(set(a["date"] for a in alerts))[-30:] if alerts else []
    total_alerts = len(alerts)
    by_sev = {s: sum(1 for a in alerts if a["severity"] == s) for s in ALERT_LEVELS}
    by_source = {}
    for a in alerts:
        by_source[a["source"]] = by_source.get(a["source"], 0) + 1
    by_asset = {}
    for a in alerts:
        by_asset[a["asset"]] = by_asset.get(a["asset"], 0) + 1

    # Zoom Sinqia (±14 dias)
    sinqia_start = (dt.date.fromisoformat(SINQIA_DATE) - dt.timedelta(days=14)).isoformat()
    sinqia_end   = (dt.date.fromisoformat(SINQIA_DATE) + dt.timedelta(days=30)).isoformat()
    sinqia_alerts = [a for a in alerts if sinqia_start <= a["date"] <= sinqia_end]

    # ---------- saída ----------
    out = {
        "generated_at": raw["generated_at"],
        "window": raw["window"],
        "sinqia": {
            "date": SINQIA_DATE,
            "zoom_from": sinqia_start,
            "zoom_to": sinqia_end,
            "alerts_in_window": sinqia_alerts,
            "context": (
                "Ataque à Sinqia em 29/08/2025 desviou entre R$ 420M e R$ 710M "
                "via PIX para contas laranjas, com suspeita de posterior conversão em cripto."
            ),
        },
        "kpis": {
            "total_alerts_12m": total_alerts,
            "alerts_by_severity": by_sev,
            "alerts_by_source": by_source,
            "alerts_by_asset": by_asset,
            "last_30_days_with_alerts": len(last_30),
            "data_sources_count": sum(len(v) for v in [bn, mb, fx]),
        },
        "series": {k: {
            "venue": v["venue"], "asset": v["asset"], "symbol": v["symbol"],
            "dates": v["dates"], "volume": v["volume"], "price": v["price"],
            "mean30": v["mean30"], "zscore": v["zscore"], "ratio": v["ratio"],
            "divergence": v["divergence"],
        } for k, v in metrics.items()},
        "stablecoin_flight": flight,
        "br_premium": premium,
        "br_concentration": conc,
        "heatmap": {
            "series": heatmap_series,
            "legend": {"medium": [2, 3], "high": [3, 4], "critical": [4, 99]},
        },
        "ring_of_mules": {
            "days":      ring_days,
            "n_days":    len(ring_days),
            "base_rate": round(base_rate * 100, 2),  # em %
            "threshold": "≥3 séries BR com Z≥2 no mesmo dia",
            "venues":    sorted(BR_VENUE_SET),
        },
        "ensemble": {
            "days":       ensemble_days,
            "n_days":     len(ensemble_days),
            "threshold":  "≥2 metrics distintos mesma (date,source) → high; ≥3 → critical",
            "description": "Agrega detectores que disparam no mesmo dia/venue para escalar severidade por corroboração.",
        },
        "hourly_corr": hourly_summary,
        "onchain_btc": {
            "hashrates": oc.get("hashrates", []),
            "difficulty": oc.get("difficulty", []),
        },
        "alerts": sorted(alerts, key=lambda a: (a["date"], -ALERT_LEVELS.get(a["severity"], 0))),
        "indicators_doc": [
            {"id": "volume_zscore", "name": "Pico de volume (Z-score 30d)",
             "desc": "Detecta picos de liquidez em um par/exchange comparando volume diário com a média móvel e desvio-padrão dos últimos 30 dias. Z>3 indica outlier estatisticamente significativo — padrão clássico de cash-out via cripto.",
             "trigger": "Z ≥ 3 (high), Z ≥ 4 (critical)"},
            {"id": "stablecoin_flight", "name": "Fuga para stablecoin",
             "desc": "Razão entre volume USDT-BRL e BTC-BRL. Saltos indicam rotação massiva para stablecoin — técnica comum de lavagem em VASPs por preservar valor durante o layering.",
             "trigger": "Z ≥ 3 sobre janela 30d"},
            {"id": "br_premium", "name": "Premium BR vs. mercado global",
             "desc": "Desvio entre preço BTC-BRL e preço teórico (BTCUSDT × USDT-BRL). Prêmios anormais sinalizam demanda doméstica súbita — frequentemente associada a cash-out pós-incidente.",
             "trigger": "|Z| ≥ 3 e desvio > 1%"},
            {"id": "price_volume_divergence", "name": "Volume sem movimento de preço",
             "desc": "Volume Z>2 com retorno diário < 0,5%. Sugere wash-trading, fracionamento ou layering — não há pressão direcional real sobre o book.",
             "trigger": "z_vol > 2 e |retorno| < 0,5%"},
            {"id": "orderbook_depth", "name": "Profundidade anômala do book (roadmap)",
             "desc": "Requer snapshot autenticado do orderbook. Detecta paredes súbitas e remoção massiva de liquidez — proxy de manipulação e front-running de depósito ilícito.",
             "trigger": "variação de profundidade L10 > 3σ"},
            {"id": "withdrawal_dispersion", "name": "Dispersão de saques on-chain (roadmap)",
             "desc": "Número de destinos únicos de saques BTC/ETH/stablecoin por dia por exchange. Pulverização alta = layering. Requer atribuição VASP on-chain (conectores de inteligência de blockchain).",
             "trigger": "Entropia de Shannon dos destinos > p95 12m"},
            {"id": "pix_to_crypto_lag", "name": "Lag PIX→cripto (roadmap)",
             "desc": "Tempo entre depósito PIX e primeira saída on-chain por conta. Lags < 10min com valor alto indicam automação de laundering.",
             "trigger": "lag < 600s e valor > R$ 50k"},
            {"id": "smurfing", "name": "Smurfing / estruturação",
             "desc": "Muitos depósitos/saques pequenos abaixo dos limites usuais de triagem. Detectável via contagem de trades diária e tamanho médio.",
             "trigger": "contagem +3σ com ticket médio < R$ 5k"},
            {"id": "ring_of_mules", "name": "Ring of mules (BR multi-venue)",
             "desc": "Dia em que 3 ou mais séries BR (Mercado Bitcoin + Foxbit + Binance BRL, qualquer ativo) acionam Z≥2 simultaneamente. Indica frota de mulas operando em paralelo em vários VASPs no mesmo evento. Quando NovaDAX, Bitso BR, BitPreço e Ripio Trade expuserem klines públicos, entram automaticamente.",
             "trigger": "≥3 séries BR com Z≥2 no mesmo dia"},
            {"id": "ensemble", "name": "Ensemble severity (corroboração cruzada)",
             "desc": "Agrega detectores independentes que disparam na mesma (data, exchange). Se 2+ metrics distintos corroboram, classifica como high; 3+ como critical. Inspirado no voting de agentes do benchmark externo (Stablecoin + SingleExch + HourlyCorr = 3/8 = HIGH). Reduz falso-negativo de detector solo em regime ambíguo.",
             "trigger": "≥2 metrics distintos mesma (date,source) → high; ≥3 → critical"},
            {"id": "hourly_corr", "name": "HourlyCorr (intraday cross-venue)",
             "desc": "Granularidade horária sobre Binance BRL + Mercado Bitcoin + Foxbit. Para cada (venue, par), compara volume da hora contra a mediana da mesma hora-do-dia nos últimos 14d. Se 3+ venues distintos acionam ratio ≥3 na mesma hora UTC, emite alerta cross-venue — pega pré-posição pre-ataque que o baseline diário suaviza.",
             "trigger": "ratio ≥3 em ≥3 venues distintos na mesma (date, hour_utc); 4+ combos ou ratio ≥10 → critical"},
            {"id": "tron_outflow", "name": "Saída de TRC-20 USDT (roadmap on-chain)",
             "desc": "Soma diária de saídas USDT de hot-wallets de exchange na rede TRON. Cash-out BR pós-PIX rota majoritária via TRC-20. Correlação cruzada com pico USDT-BRL local em ±24h.",
             "trigger": "outflow Z≥2 e correlação com pico USDT-BRL ≥ 0.6"},
            {"id": "funding_rate", "name": "Funding rate de perpétuos",
             "desc": "Funding rate fortemente negativo em BTC/ETH perpétuos = pressão vendedora anormal (cash-out forçado ou hedge defensivo após incidente).",
             "trigger": "rate ≤ −0,05% por 8h (high) ou ≤ −0,1% (critical)"},
        ],
    }
    OUT.write_text(json.dumps(out, default=str))
    size_kb = OUT.stat().st_size / 1024
    print(f"dashboard.json gerado — {size_kb:.1f} KB")
    print(f"total de alertas: {total_alerts}  (crit {by_sev.get('critical',0)}, high {by_sev.get('high',0)}, med {by_sev.get('medium',0)})")
    print(f"alertas na janela Sinqia: {len(sinqia_alerts)}")


if __name__ == "__main__":
    main()
