"""
run_flight_backtest.py — backtest provisório do detector stablecoin_flight.

Contexto: stablecoin_flight é um detector de CONJUNÇÃO (2+ de 3 condições numa
janela de ~3 snaps), então o perturbador precisa construir uma *trajetória* de
4-5 snaps, não um snap único. O resto dos detectores é pontual (snap vs. state
de baseline) e usa o harness run_backtest.py.

Este script:
  1. busca um snapshot REAL como ponto de partida
  2. constrói 5 cenários: 2 "attack" (deveriam disparar), 3 "clean" (não deveriam)
  3. roda detect_stablecoin_flight com thresholds atuais em cada cenário
  4. imprime tabela TP/FP e grava `data/flight_calibration.json`

Limitações honestas:
  - sample size=5, SEM dados históricos reais de ataque (o evento Sinqia/HSBC
    não tem orderbook BR arquivado em granularidade de segundo);
  - thresholds são "palpite educado" validado por sintético. Pra virar
    calibração definitiva precisa de (a) golden set de ataques reais com
    orderbook L2 ou (b) sintéticos mais realistas derivados de Z-scores
    históricos de cada sinal componente.
  - o resultado serve como SANITY (o detector faz o que diz fazer) e
    DOCUMENTA os thresholds assumidos, não como medida de TP/FP em produção.
"""
from __future__ import annotations
import sys, copy, json
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from watcher.fetchers  import fetch_snapshot
from watcher.detectors import detect_stablecoin_flight, THRESHOLDS


# ---------- construção de trajetórias ----------
def _shift_price(book: dict, pct: float) -> dict:
    if not book: return book
    f = 1.0 + pct / 100.0
    bb = (book.get("best_bid") or 0.0) * f
    ba = (book.get("best_ask") or 0.0) * f
    return {
        "best_bid": bb, "best_ask": ba,
        "bids": [(px * f, qty) for px, qty in book.get("bids", [])],
        "asks": [(px * f, qty) for px, qty in book.get("asks", [])],
    }


def _force_imbalance(book: dict, target_imb: float) -> dict:
    """Ajusta quantidades de bids/asks no ±2% pra produzir imbalance target."""
    if not book: return book
    bb = book.get("best_bid") or 0.0
    ba = book.get("best_ask") or 0.0
    if bb <= 0 or ba <= 0: return book
    mid = (bb + ba) / 2; band = 0.02 * mid
    # total "antes" dentro do ±2%
    bids_in = [(px, qty) for px, qty in book.get("bids", []) if px >= mid - band]
    asks_in = [(px, qty) for px, qty in book.get("asks", []) if px <= mid + band]
    bids_out = [(px, qty) for px, qty in book.get("bids", []) if px <  mid - band]
    asks_out = [(px, qty) for px, qty in book.get("asks", []) if px >  mid + band]
    # queremos (B-A)/(B+A) = t  =>  B = (1+t)/(1-t) * A
    # fixa A=1 (qty base), calcula B
    tot_a = sum(q for _, q in asks_in) or 1e-9
    target_b = (1 + target_imb) / (1 - target_imb) * tot_a
    tot_b = sum(q for _, q in bids_in) or 1e-9
    scale_b = target_b / tot_b
    bids_in2 = [(px, qty * scale_b) for px, qty in bids_in]
    return {
        "best_bid": bb, "best_ask": ba,
        "bids": bids_in2 + bids_out,
        "asks": asks_in + asks_out,
    }


def _force_decouple(snap: dict, decouple_pct: float) -> dict:
    """Força o mid de USDT-BRL a ficar decouple_pct acima do FX implícito
    (btc_brl / btc_usdt_global). Não mexe em BTC-BRL nem no btc_usdt_global."""
    s = copy.deepcopy(snap)
    btc = (s.get("Binance", {}) or {}).get("BTC-BRL", {})
    btc_brl = ((btc.get("best_bid") or 0) + (btc.get("best_ask") or 0)) / 2
    btc_usd = (s.get("_meta", {}) or {}).get("btc_usdt_global", 0)
    if not (btc_brl and btc_usd): return s
    fx = btc_brl / btc_usd
    target_mid = fx * (1 + decouple_pct / 100.0)
    # ajusta best_bid/ask em torno do target mid
    usdt = s["Binance"].setdefault("USDT-BRL", {})
    if usdt:
        cur_bb = usdt.get("best_bid") or 0.0
        cur_ba = usdt.get("best_ask") or 0.0
        if cur_bb and cur_ba:
            cur_mid = (cur_bb + cur_ba) / 2
            factor = target_mid / cur_mid if cur_mid else 1.0
            s["Binance"]["USDT-BRL"] = _shift_price(usdt, (factor - 1) * 100)
    return s


def _build_attack_trajectory(baseline: dict, *,
                              n_snaps: int = 5,
                              risk_drop_pct_total: float = 1.5,
                              usdt_imbalance: float = 0.5,
                              decouple_pct: float = 1.0) -> list:
    """Sequência de snaps simulando fuga para stablecoin.
    Risk assets caem linearmente; USDT-BRL recebe imbalance e decouple no último
    snap. Os snaps intermediários representam estado em progresso."""
    traj = []
    step = risk_drop_pct_total / (n_snaps - 1)
    for i in range(n_snaps):
        cur_drop = -step * i
        s = copy.deepcopy(baseline)
        for asset in ("BTC-BRL", "ETH-BRL", "SOL-BRL"):
            book = (s.get("Binance", {}) or {}).get(asset)
            if book:
                s["Binance"][asset] = _shift_price(book, cur_drop)
        if i == n_snaps - 1:
            # último snap: imbalance + decouple
            usdt = (s.get("Binance", {}) or {}).get("USDT-BRL")
            if usdt:
                s["Binance"]["USDT-BRL"] = _force_imbalance(usdt, usdt_imbalance)
            s = _force_decouple(s, decouple_pct)
        traj.append(s)
    return traj


def _build_clean_trajectory(baseline: dict, n_snaps: int = 5,
                            drift_pct: float = 0.05) -> list:
    """Trajetória "limpa": pequenas oscilações aleatórias, sem padrão de fuga."""
    import random
    rng = random.Random(42)
    traj = []
    for i in range(n_snaps):
        s = copy.deepcopy(baseline)
        for asset in ("BTC-BRL","ETH-BRL","USDT-BRL","SOL-BRL"):
            book = (s.get("Binance", {}) or {}).get(asset)
            if book:
                s["Binance"][asset] = _shift_price(book, rng.uniform(-drift_pct, drift_pct))
        traj.append(s)
    return traj


# ---------- avaliação ----------
def evaluate_trajectory(traj: list) -> tuple[int, list]:
    """Roda detect_stablecoin_flight em cada snap com state rolling.
    Retorna (n_alerts_totais, lista de alertas)."""
    state = deque(maxlen=30)
    alerts_out = []
    for snap in traj:
        als = detect_stablecoin_flight(snap, state)
        for a in als:
            alerts_out.append({"rule": a.rule, "severity": a.severity,
                                "asset": a.asset, "value": a.value,
                                "narrative": a.narrative,
                                "context": a.context})
        state.append(snap)
    return len(alerts_out), alerts_out


# ---------- cenários ----------
SCENARIOS = [
    # (nome, expected_fire, builder, params)
    ("attack_severo",   True,  "attack",
        {"risk_drop_pct_total": 2.0, "usdt_imbalance": 0.55, "decouple_pct": 1.2}),
    ("attack_moderado", True,  "attack",
        {"risk_drop_pct_total": 1.0, "usdt_imbalance": 0.40, "decouple_pct": 0.5}),
    ("attack_borderline", True, "attack",
        {"risk_drop_pct_total": 0.6, "usdt_imbalance": 0.36, "decouple_pct": 0.31}),
    ("clean_baseline",  False, "clean",
        {"drift_pct": 0.05}),
    ("clean_volatil",   False, "clean",
        {"drift_pct": 0.20}),
]


def run() -> dict:
    print("[flight-bt] fetching baseline snapshot…")
    baseline = fetch_snapshot()
    if not baseline.get("Binance"):
        raise SystemExit("no binance data — can't run backtest")
    # smoke: precisa ter BTC/ETH/SOL/USDT-BRL
    required = ("BTC-BRL","ETH-BRL","SOL-BRL","USDT-BRL")
    missing = [a for a in required if a not in baseline.get("Binance", {})]
    if missing:
        raise SystemExit(f"baseline incompleto, faltou: {missing}")

    print("[flight-bt] thresholds atuais:")
    for k, v in THRESHOLDS.items():
        if k.startswith("flight_"):
            print(f"    {k} = {v}")

    results = []
    for name, expect, kind, params in SCENARIOS:
        if kind == "attack":
            traj = _build_attack_trajectory(baseline, **params)
        else:
            traj = _build_clean_trajectory(baseline, **params)
        n_alerts, alerts = evaluate_trajectory(traj)
        fired = n_alerts > 0
        result = "TP" if (fired and expect) else \
                 "FN" if (not fired and expect) else \
                 "FP" if (fired and not expect) else "TN"
        print(f"  {name:20s} expect={str(expect):5s} fired={str(fired):5s} n_alerts={n_alerts:2d}  [{result}]")
        if alerts:
            for a in alerts:
                print(f"     -> sev={a['severity']:8s} | {a['narrative'][:90]}")
        results.append({
            "scenario": name,
            "kind": kind,
            "params": params,
            "expected_fire": expect,
            "actual_fire": fired,
            "n_alerts": n_alerts,
            "outcome": result,
            "alerts": alerts,
        })

    tp = sum(1 for r in results if r["outcome"] == "TP")
    fn = sum(1 for r in results if r["outcome"] == "FN")
    fp = sum(1 for r in results if r["outcome"] == "FP")
    tn = sum(1 for r in results if r["outcome"] == "TN")
    summary = {
        "n_scenarios":  len(results),
        "tp": tp, "fn": fn, "fp": fp, "tn": tn,
        "tp_rate": tp / max(1, tp + fn),
        "fp_rate": fp / max(1, fp + tn),
        "f1": (2*tp) / max(1, 2*tp + fp + fn),
    }
    print(f"\n[flight-bt] resumo: TP={tp} FN={fn} FP={fp} TN={tn} | "
          f"TPR={summary['tp_rate']:.0%} FPR={summary['fp_rate']:.0%} F1={summary['f1']:.2f}")

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "detector": "stablecoin_flight",
        "note": ("Calibração provisória. Cenários sintéticos derivados do mesmo "
                 "snap baseline real, variando parâmetros conforme docstring do "
                 "detector. Resultado indica sanidade do detector e documenta "
                 "thresholds atuais; não deve ser lido como FP/TP de produção "
                 "(sample=5, sem golden set de ataques reais com orderbook L2)."),
        "thresholds": {k: v for k, v in THRESHOLDS.items() if k.startswith("flight_")},
        "scenarios":  results,
        "summary":    summary,
    }
    return out


if __name__ == "__main__":
    data = run()
    outp = ROOT / "data" / "flight_calibration.json"
    outp.write_text(json.dumps(data, indent=2, default=str))
    print(f"[flight-bt] salvo em {outp}")
