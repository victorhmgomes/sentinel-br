"""
build_dashboard.py — injeta data/dashboard.json dentro de dashboard/index.html.

Saídas:
  - {ROOT}/dashboard/index.built.html   (build local sempre)
  - {ROOT}/index.html                   (cópia pra root — GitHub Pages)
  - $SENTINEL_OUTPUTS/sentinel-br.html  (se env setado — sandbox local)
"""
from pathlib import Path
import json, os, shutil

ROOT = Path(__file__).resolve().parent.parent
TEMPLATE = ROOT / "dashboard" / "index.html"
DATA     = ROOT / "data" / "dashboard.json"
_EXTRA_OUT = os.environ.get("SENTINEL_OUTPUTS", "").strip()
OUTPUTS  = Path(_EXTRA_OUT) if _EXTRA_OUT else None

html = TEMPLATE.read_text(encoding="utf-8")
data = DATA.read_text(encoding="utf-8")
# Escapar </script> que por acaso esteja nos dados
data_safe = data.replace("</script", "<\\/script")
out_html = html.replace("__DATA__", data_safe)

# Pré-renderiza a manchete (lead) com valores reais já no HTML,
# para que o texto "13 de 14 ataques..." apareça mesmo sem JS e seja
# indexável por Ctrl+F no source.
try:
    _d = json.loads(data)
    _summ = (_d.get("incidents") or {}).get("summary") or {}
    _hit = _summ.get("n_hit_any_pm3", "—")
    _tot = _summ.get("n_total", "—")
    out_html = out_html.replace("__LEAD_HITS__",     f"{_hit} de {_tot}")
    out_html = out_html.replace("__LEAD_HIT_RATE__", str(_hit))
    out_html = out_html.replace("__LEAD_HIT_TOTAL__", str(_tot))
    print(f"manchete pré-renderizada: {_hit} de {_tot}")
except Exception as e:
    out_html = out_html.replace("__LEAD_HITS__",     "—")
    out_html = out_html.replace("__LEAD_HIT_RATE__", "—")
    out_html = out_html.replace("__LEAD_HIT_TOTAL__", "—")
    print(f"[warn] nao foi possivel pre-renderizar manchete: {e}")

# Orderbook ao vivo (se existir)
OB = ROOT / "data" / "orderbook.json"
if OB.exists():
    ob_safe = OB.read_text(encoding="utf-8").replace("</script", "<\\/script")
    out_html = out_html.replace("__OB__", ob_safe)
    print(f"orderbook embutido: {OB.stat().st_size/1024:.1f} KB")
else:
    out_html = out_html.replace("__OB__", "null")
    print("orderbook.json ausente — seção ao vivo virá vazia")

# CoinGecko 30d por exchange (se existir)
CG = ROOT / "data" / "coingecko.json"
if CG.exists():
    cg_safe = CG.read_text(encoding="utf-8").replace("</script", "<\\/script")
    out_html = out_html.replace("__CG__", cg_safe)
    print(f"coingecko embutido: {CG.stat().st_size/1024:.1f} KB")
else:
    out_html = out_html.replace("__CG__", "null")
    print("coingecko.json ausente — widget de volume por exchange virá vazio")

# TRON USDT TRC-20 (se existir)
TRON = ROOT / "data" / "tron.json"
if TRON.exists():
    tron_safe = TRON.read_text(encoding="utf-8").replace("</script", "<\\/script")
    out_html = out_html.replace("__TRON__", tron_safe)
    print(f"tron embutido: {TRON.stat().st_size/1024:.1f} KB")
else:
    out_html = out_html.replace("__TRON__", "null")
    print("tron.json ausente — seção on-chain virá vazia")

# Feed ao vivo de alertas do watcher (se existir)
LIVE = ROOT / "data" / "alerts" / "_live.json"
if LIVE.exists():
    live_safe = LIVE.read_text(encoding="utf-8").replace("</script", "<\\/script")
    out_html = out_html.replace("__LIVE__", live_safe)
    print(f"live alerts embutidos: {LIVE.stat().st_size/1024:.1f} KB")
else:
    out_html = out_html.replace("__LIVE__", "[]")
    print("_live.json ausente — feed ao vivo virá vazio")

# Funding rate perpétuos (se existir)
FUND = ROOT / "data" / "funding.json"
if FUND.exists():
    fund_safe = FUND.read_text(encoding="utf-8").replace("</script", "<\\/script")
    out_html = out_html.replace("__FUNDING__", fund_safe)
    print(f"funding embutido: {FUND.stat().st_size/1024:.1f} KB")
else:
    out_html = out_html.replace("__FUNDING__", "null")
    print("funding.json ausente — seção de funding virá vazia")

# Corroboração com imprensa (se existir)
NEWS = ROOT / "data" / "news.json"
if NEWS.exists():
    news_raw = NEWS.read_text(encoding="utf-8")
    news_safe = news_raw.replace("</script", "<\\/script")
    out_html = out_html.replace("__NEWS__", news_safe)
    print(f"news embutido: {NEWS.stat().st_size/1024:.1f} KB")
    # Pré-renderiza KPIs da seção imprensa pra que apareçam mesmo sem JS
    try:
        _nd = json.loads(news_raw)
        _ns = _nd.get("summary") or {}
        _nb = _ns.get("by_severity") or {}
        _ntotal = len(_nd.get("items") or [])
        _nhigh  = (_nb.get("high") or 0) + (_nb.get("critical") or 0)
        _nmed   = _nb.get("medium") or 0
        _ncorr  = _ns.get("with_watcher_corroboration") or 0
        out_html = out_html.replace("__PRESS_TOTAL__", str(_ntotal))
        out_html = out_html.replace("__PRESS_HIGH__",  str(_nhigh))
        out_html = out_html.replace("__PRESS_MED__",   str(_nmed))
        out_html = out_html.replace("__PRESS_CORR__",  str(_ncorr))
        print(f"  kpis imprensa: total={_ntotal} high={_nhigh} med={_nmed} corr={_ncorr}")
    except Exception as e:
        for ph in ("__PRESS_TOTAL__","__PRESS_HIGH__","__PRESS_MED__","__PRESS_CORR__"):
            out_html = out_html.replace(ph, "—")
        print(f"  [warn] falha ao pre-render kpis imprensa: {e}")
else:
    out_html = out_html.replace("__NEWS__", "null")
    for ph in ("__PRESS_TOTAL__","__PRESS_HIGH__","__PRESS_MED__","__PRESS_CORR__"):
        out_html = out_html.replace(ph, "—")
    print("news.json ausente — seção de imprensa virá vazia")

# Calibração provisória do stablecoin_flight (sintético · sanity check)
FLIGHT = ROOT / "data" / "flight_calibration.json"
if FLIGHT.exists():
    flight_raw = FLIGHT.read_text(encoding="utf-8")
    flight_safe = flight_raw.replace("</script", "<\\/script")
    out_html = out_html.replace("__FLIGHT__", flight_safe)
    print(f"flight_calibration embutido: {FLIGHT.stat().st_size/1024:.1f} KB")
    try:
        _fd = json.loads(flight_raw)
        _fs = _fd.get("summary") or {}
        out_html = out_html.replace("__FLIGHT_TPR__",
            f"{int(round((_fs.get('tp_rate') or 0)*100))}%")
        out_html = out_html.replace("__FLIGHT_FPR__",
            f"{int(round((_fs.get('fp_rate') or 0)*100))}%")
        out_html = out_html.replace("__FLIGHT_F1__",
            f"{(_fs.get('f1') or 0):.2f}")
        out_html = out_html.replace("__FLIGHT_N__",
            str(_fs.get("n_scenarios") or "—"))
        print(f"  kpis flight: TPR={_fs.get('tp_rate')} FPR={_fs.get('fp_rate')} F1={_fs.get('f1')}")
    except Exception as e:
        for ph in ("__FLIGHT_TPR__","__FLIGHT_FPR__","__FLIGHT_F1__","__FLIGHT_N__"):
            out_html = out_html.replace(ph, "—")
        print(f"  [warn] falha ao pre-render kpis flight: {e}")
else:
    out_html = out_html.replace("__FLIGHT__", "null")
    for ph in ("__FLIGHT_TPR__","__FLIGHT_FPR__","__FLIGHT_F1__","__FLIGHT_N__"):
        out_html = out_html.replace(ph, "—")
    print("flight_calibration.json ausente — subseção stablecoin_flight virá vazia")

# Calibração & FP/FN (se existir)
CALIB = ROOT / "data" / "calibration.json"
if CALIB.exists():
    calib_safe = CALIB.read_text(encoding="utf-8").replace("</script", "<\\/script")
    out_html = out_html.replace("__CALIB__", calib_safe)
    print(f"calibration embutido: {CALIB.stat().st_size/1024:.1f} KB")
else:
    out_html = out_html.replace("__CALIB__", "null")
    print("calibration.json ausente — seção de calibração virá vazia")

# Eventos correlacionados (correlator) — super-alertas agregados do watcher
EVENTS = ROOT / "data" / "events.json"
if EVENTS.exists():
    events_raw = EVENTS.read_text(encoding="utf-8")
    events_safe = events_raw.replace("</script", "<\\/script")
    out_html = out_html.replace("__EVENTS__", events_safe)
    print(f"events embutido: {EVENTS.stat().st_size/1024:.1f} KB")
    # pré-renderiza KPIs da seção de eventos
    try:
        _ed = json.loads(events_raw)
        _es = _ed.get("summary") or {}
        _etot  = _es.get("n_total", 0)
        _ecorr = _es.get("n_correlated", 0)
        _emaxc = _es.get("max_convergence", 0)
        _esev  = _es.get("by_severity") or {}
        _ecrit = _esev.get("critical", 0)
        out_html = out_html.replace("__EV_TOTAL__",   str(_etot))
        out_html = out_html.replace("__EV_CORR__",    str(_ecorr))
        out_html = out_html.replace("__EV_MAXCONV__", str(_emaxc))
        out_html = out_html.replace("__EV_CRIT__",    str(_ecrit))
        print(f"  kpis eventos: total={_etot} corr={_ecorr} maxConv={_emaxc} crit={_ecrit}")
    except Exception as e:
        for ph in ("__EV_TOTAL__","__EV_CORR__","__EV_MAXCONV__","__EV_CRIT__"):
            out_html = out_html.replace(ph, "—")
        print(f"  [warn] falha ao pre-render kpis de eventos: {e}")
else:
    out_html = out_html.replace("__EVENTS__", "null")
    for ph in ("__EV_TOTAL__","__EV_CORR__","__EV_MAXCONV__","__EV_CRIT__"):
        out_html = out_html.replace(ph, "—")
    print("events.json ausente — seção de eventos virá vazia")

local = ROOT / "dashboard" / "index.built.html"
local.write_text(out_html, encoding="utf-8")
print(f"build local: {local} ({local.stat().st_size/1024:.1f} KB)")

# Cópia pra raiz do repo — index.html que o GitHub Pages serve
root_index = ROOT / "index.html"
root_index.write_text(out_html, encoding="utf-8")
print(f"root index: {root_index} ({root_index.stat().st_size/1024:.1f} KB)")

# Saída extra opcional (sandbox local ou deploy manual)
if OUTPUTS is not None:
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    final = OUTPUTS / "sentinel-br.html"
    shutil.copy2(local, final)
    print(f"publicado em: {final}")
