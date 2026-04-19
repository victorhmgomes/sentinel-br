"""
run_backtest.py — harness de calibração de thresholds com FP/FN.

Fluxo:
  1. Coleta N baselines reais (ou carrega cache) → cada um é um snap completo
     do fetch_snapshot() do watcher.
  2. Usa os primeiros N-1 como state (histórico estável) e o último como "now".
  3. Mede FP atual: em quantos baselines (re-interpretados como "now" com
     histórico = demais) cada detector dispara. Idealmente → 0.
  4. Para cada cenário (6), varre intensidade ∈ GRID. Para cada ponto,
     perturba uma cópia do snap "now" e roda o detector alvo → TP/FN.
  5. Para cada parâmetro principal de threshold, varre valores e recalcula
     FP rate + detection curve. Grava `data/calibration.json`.

Uso:
  python3 scripts/backtest/run_backtest.py --collect 25 --interval 2.5
  python3 scripts/backtest/run_backtest.py              # usa cache
  python3 scripts/backtest/run_backtest.py --max-fp 0.02  # alvo de FP 2%

Saída:
  data/backtest/snapshots.jsonl   (cache dos baselines)
  data/calibration.json           (resultado + recomendações)
"""
from __future__ import annotations
import argparse, copy, json, sys, time
from collections import deque
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from watcher.fetchers import fetch_snapshot
from watcher.detectors import (
    THRESHOLDS, apply_overrides,
    detect_binance_spread_spike, detect_binance_depth_drop,
    detect_binance_price_burst, detect_binance_vs_br_divergence,
    detect_br_pre_spike, detect_usdt_brl_decouple,
)
from backtest.perturb import (
    PERTURBATIONS, GRIDS, SCENARIO_TO_DETECTOR,
)

DATA       = ROOT / "data"
CACHE_PATH = DATA / "backtest" / "snapshots.jsonl"
OUT_PATH   = DATA / "calibration.json"

DETECTORS = {
    "binance_spread_spike":    detect_binance_spread_spike,
    "binance_depth_drop":      detect_binance_depth_drop,
    "binance_price_burst":     detect_binance_price_burst,
    "binance_vs_br_divergence":detect_binance_vs_br_divergence,
    "br_pre_spike":            detect_br_pre_spike,
    "usdt_brl_decouple":       detect_usdt_brl_decouple,
}

# Para cada detector, quais chaves do THRESHOLDS fazem sentido sweepar (med/high/crit)
# e a grade de valores para varrer.
SWEEPS = {
    "binance_spread_spike": {
        # nova chave pós-redesign (p95-based). Range: 1.0 = exatamente igual ao p95
        # (trivial), 5.0 = só ataques agressivos.
        "spread_spike_ratio_p95_med": [1.0, 1.25, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0],
    },
    "binance_depth_drop": {
        "depth_drop_pct_med":      [0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.80],
    },
    "binance_price_burst": {
        "price_burst_z_med":       [2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 10.0, 12.0],
    },
    "binance_vs_br_divergence": {
        "divergence_gap_pct_med":  [0.20, 0.30, 0.40, 0.50, 0.75, 1.00, 1.25, 1.50, 2.00, 2.50],
    },
    "br_pre_spike": {
        "pre_spike_local_pct_med": [0.20, 0.30, 0.40, 0.50, 0.60, 0.75, 1.00, 1.25],
    },
    "usdt_brl_decouple": {
        "decouple_gap_pct_med":    [0.30, 0.50, 0.75, 1.00, 1.25, 1.50, 2.00, 2.50],
    },
}


# ---------- coleta / cache ----------
def collect_baselines(n: int, interval: float) -> list[dict]:
    """Coleta n snapshots live, com interval segundos entre cada um."""
    snaps = []
    for i in range(n):
        t0 = time.time()
        snap = fetch_snapshot()
        ok = bool(snap.get("Binance")) and bool(snap.get("Mercado Bitcoin"))
        took = time.time() - t0
        print(f"  [{i+1}/{n}] fetch={'ok' if ok else 'FAIL'} em {took:.2f}s", file=sys.stderr)
        if ok:
            snaps.append(snap)
        if i < n - 1:
            time.sleep(max(0, interval - took))
    return snaps


def save_cache(snaps: list[dict]) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with CACHE_PATH.open("w", encoding="utf-8") as f:
        for s in snaps:
            f.write(json.dumps(s, ensure_ascii=False) + "\n")


def load_cache() -> list[dict]:
    if not CACHE_PATH.exists():
        return []
    out = []
    with CACHE_PATH.open(encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if ln:
                try:
                    out.append(json.loads(ln))
                except Exception:
                    pass
    return out


# ---------- helpers ----------
def snaps_to_state(snaps: list[dict]) -> deque:
    """Transforma lista em deque pra servir de state (histórico)."""
    return deque(snaps, maxlen=max(50, len(snaps)+5))


def run_detector(detector_name: str, snap: dict, state: deque) -> list:
    fn = DETECTORS[detector_name]
    try:
        return fn(snap, state)
    except Exception as e:
        print(f"[{detector_name}] erro: {e}", file=sys.stderr)
        return []


def measure_baseline_fp(detector_name: str, snaps: list[dict]) -> dict:
    """Mede taxa de FP "leave-one-out": para cada i, usa snaps[:i]+snaps[i+1:]
    como state e snaps[i] como now. Se o detector dispara → FP."""
    fires = 0
    total = 0
    for i in range(len(snaps)):
        state = snaps_to_state(snaps[:i] + snaps[i+1:])
        alerts = run_detector(detector_name, snaps[i], state)
        total += 1
        if alerts:
            fires += 1
    return {
        "total": total, "fires": fires,
        "fp_rate": (fires / total) if total else 0.0,
    }


def measure_detection_curve(scenario: str, now_snap: dict, state: deque) -> list[dict]:
    """Para cada intensidade do grid, perturba now_snap e mede se o detector
    dispara + severidade."""
    detector_name, _thr_key = SCENARIO_TO_DETECTOR[scenario]
    perturb_fn = PERTURBATIONS[scenario]
    grid = GRIDS[scenario]
    out = []
    for intensity in grid:
        # intensity = 0 é baseline puro (controle — mede FP)
        if intensity == 0.0:
            perturbed = copy.deepcopy(now_snap)
            meta = {"applied": False, "scenario": scenario, "intensity": 0.0}
        else:
            if scenario == "spread_spike":
                perturbed, meta = perturb_fn(now_snap, target_pct=intensity)
            elif scenario == "depth_drop":
                perturbed, meta = perturb_fn(now_snap, drop_frac=intensity)
            elif scenario == "price_burst":
                perturbed, meta = perturb_fn(now_snap, shift_pct=intensity)
            elif scenario == "divergence":
                perturbed, meta = perturb_fn(now_snap, binance_shift_pct=intensity)
            elif scenario == "pre_spike":
                perturbed, meta = perturb_fn(now_snap, local_shift_pct=intensity)
            elif scenario == "decouple":
                perturbed, meta = perturb_fn(now_snap, shift_pct=intensity)
            else:
                raise ValueError(scenario)
        alerts = run_detector(detector_name, perturbed, state)
        out.append({
            "intensity": intensity,
            "n_alerts": len(alerts),
            "severities": [a.severity for a in alerts],
            "top_value": (alerts[0].value if alerts else None),
        })
    return out


def sweep_threshold(detector_name: str, thr_key: str,
                    thr_values: list[float],
                    snaps: list[dict]) -> list[dict]:
    """Para cada valor de threshold, mede FP (leave-one-out) e curve de
    detection (cenário correspondente). Restaura threshold no fim."""
    # encontra cenário inverso (detector_name → scenario)
    scenario = None
    for sc, (det, _k) in SCENARIO_TO_DETECTOR.items():
        if det == detector_name:
            scenario = sc
            break
    if scenario is None:
        raise ValueError(f"sem cenário pra {detector_name}")

    # now + state fixos (último snap é now, demais são state)
    now_snap  = snaps[-1]
    state_0   = snaps_to_state(snaps[:-1])

    results = []
    original = THRESHOLDS[thr_key]
    try:
        for v in thr_values:
            apply_overrides({thr_key: v})
            fp = measure_baseline_fp(detector_name, snaps)
            curve = measure_detection_curve(scenario, now_snap, state_0)
            # menor intensidade que disparou (intensity > 0 é ataque real)
            min_fire = None
            for pt in curve:
                if pt["n_alerts"] > 0 and pt["intensity"] > 0:
                    min_fire = pt["intensity"]; break
            n_fire          = sum(1 for pt in curve if pt["n_alerts"] > 0 and pt["intensity"] > 0)
            n_total_attacks = sum(1 for pt in curve if pt["intensity"] > 0)
            tp_rate = (n_fire / n_total_attacks) if n_total_attacks else 0.0
            # F1 aproximado: precision = tp/(tp+fp), recall = tp_rate
            tp = n_fire
            fp_abs = fp["fires"]
            precision = tp / (tp + fp_abs) if (tp + fp_abs) else 0.0
            recall = tp_rate
            f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
            results.append({
                "threshold": v,
                "fp_rate": fp["fp_rate"],
                "fp_fires": fp["fires"], "fp_total": fp["total"],
                "tp_rate": tp_rate,
                "tp_fires": n_fire, "tp_total": n_total_attacks,
                "min_intensity_fired": min_fire,
                "precision": precision, "recall": recall, "f1": f1,
            })
    finally:
        apply_overrides({thr_key: original})
    return results


def recommend_threshold(sweep: list[dict], max_fp: float) -> dict:
    """Pick o menor threshold (mais sensível) que mantém fp_rate <= max_fp.
    Se nenhum atende, pega o que dá menor FP."""
    ok = [r for r in sweep if r["fp_rate"] <= max_fp]
    if ok:
        # entre os aceitáveis, o menor threshold maximiza recall
        best = min(ok, key=lambda r: r["threshold"])
        return {
            "threshold": best["threshold"],
            "reason": f"menor threshold com fp_rate <= {max_fp:.2%}",
            "fp_rate": best["fp_rate"],
            "tp_rate": best["tp_rate"],
            "f1": best["f1"],
        }
    # fallback: maior F1
    best = max(sweep, key=lambda r: r["f1"])
    return {
        "threshold": best["threshold"],
        "reason": f"nenhum ponto <= max_fp; usando maior F1 ({best['f1']:.3f})",
        "fp_rate": best["fp_rate"],
        "tp_rate": best["tp_rate"],
        "f1": best["f1"],
    }


# ---------- main ----------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--collect", type=int, default=0,
                   help="coleta N baselines live (e sobrescreve cache)")
    p.add_argument("--interval", type=float, default=2.5,
                   help="segundos entre coletas live")
    p.add_argument("--max-fp", type=float, default=0.02,
                   help="taxa alvo máxima de FP para recomendação (default 2%)")
    p.add_argument("--out", default=str(OUT_PATH))
    return p.parse_args()


def main():
    args = parse_args()
    t0 = time.time()

    # 1. carrega ou coleta baselines
    if args.collect > 0:
        print(f"→ coletando {args.collect} baselines live (interval={args.interval}s)...",
              file=sys.stderr)
        snaps = collect_baselines(args.collect, args.interval)
        if len(snaps) < 5:
            print(f"FATAL: só coletei {len(snaps)} snapshots — precisa >= 5",
                  file=sys.stderr)
            sys.exit(1)
        save_cache(snaps)
        print(f"  cache salvo: {CACHE_PATH} ({len(snaps)} snaps)", file=sys.stderr)
    else:
        snaps = load_cache()
        if len(snaps) < 5:
            print(f"FATAL: cache vazio/insuficiente ({len(snaps)}). "
                  f"Rode primeiro com --collect 20", file=sys.stderr)
            sys.exit(1)
        print(f"→ usando cache: {len(snaps)} baselines", file=sys.stderr)

    # 2. FP atual (thresholds atuais) e detection curve (intensidade → fires)
    print(f"→ medindo FP atual e detection curves (thresholds vigentes)...",
          file=sys.stderr)
    baseline_fp = {}
    for det in DETECTORS:
        baseline_fp[det] = measure_baseline_fp(det, snaps)
    now_snap = snaps[-1]; state_0 = snaps_to_state(snaps[:-1])
    detection_curves = {}
    for sc in GRIDS:
        detection_curves[sc] = measure_detection_curve(sc, now_snap, state_0)

    # 3. sweep por detector
    print(f"→ sweep de thresholds (6 detectores × grid)...", file=sys.stderr)
    sweeps = {}
    recommendations = {}
    for det, params in SWEEPS.items():
        sweeps[det] = {}
        for thr_key, thr_values in params.items():
            print(f"    {det}:{thr_key}  ({len(thr_values)} pontos)", file=sys.stderr)
            sw = sweep_threshold(det, thr_key, thr_values, snaps)
            sweeps[det][thr_key] = sw
            recommendations[thr_key] = recommend_threshold(sw, args.max_fp)
            recommendations[thr_key]["current"] = THRESHOLDS[thr_key]

    # 4. resumo compacto pra dashboard
    summary = []
    for det in DETECTORS:
        row = {
            "detector": det,
            "fp_current": baseline_fp[det]["fp_rate"],
            "fp_fires":   baseline_fp[det]["fires"],
            "fp_total":   baseline_fp[det]["total"],
        }
        sw = SWEEPS.get(det, {})
        if sw:
            thr_key = next(iter(sw.keys()))
            row["threshold_key"]        = thr_key
            row["threshold_current"]    = THRESHOLDS[thr_key]
            row["threshold_recommended"]= recommendations[thr_key]["threshold"]
            row["fp_recommended"]       = recommendations[thr_key]["fp_rate"]
            row["tp_recommended"]       = recommendations[thr_key]["tp_rate"]
            row["f1_recommended"]       = recommendations[thr_key]["f1"]
            row["reason"]               = recommendations[thr_key]["reason"]
        summary.append(row)

    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "runtime_sec":  round(time.time() - t0, 2),
        "n_baselines":  len(snaps),
        "max_fp_target": args.max_fp,
        "thresholds_current": dict(THRESHOLDS),
        "baseline_fp":  baseline_fp,
        "detection_curves": detection_curves,
        "sweeps":       sweeps,
        "recommendations": recommendations,
        "summary":      summary,
    }
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"\n✓ {out_path}  ({out_path.stat().st_size/1024:.1f} KB)", file=sys.stderr)
    print(f"  runtime: {payload['runtime_sec']}s   baselines: {len(snaps)}",
          file=sys.stderr)
    print(f"\nRECOMENDAÇÕES (max_fp={args.max_fp:.0%}):", file=sys.stderr)
    for row in summary:
        if "threshold_key" not in row: continue
        print(f"  {row['detector']:28s}  "
              f"{row['threshold_key']:30s}  "
              f"{row['threshold_current']:>6} → {row['threshold_recommended']:>6}  "
              f"(fp={row['fp_recommended']:.1%} tp={row['tp_recommended']:.0%} "
              f"f1={row['f1_recommended']:.2f})",
              file=sys.stderr)


if __name__ == "__main__":
    main()
