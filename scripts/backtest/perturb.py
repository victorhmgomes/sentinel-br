"""
perturb.py — injeta "assinaturas de ataque" em snapshots reais de baseline.

Cada perturbador pega um snapshot REAL (coletado via fetch_snapshot()) e produz
uma cópia profunda com uma assinatura sintética do tipo de fraude que cada
detector deveria flagrar. A intensidade é parametrizável — isso permite varrer
uma curva detection-rate × threshold no harness de backtest.

Regras:
  * sempre retornamos um snap NOVO (deep copy) — o baseline original fica
    intocado pra reuso em múltiplos cenários
  * prices/qty são manipulados com aritmética consistente (se mexe em mid,
    mexe em best_bid/best_ask E nos níveis)
  * cada função retorna (snap_perturbado, metadata_da_perturbacao)

Mapeamento perturbador → detector alvo:
  perturb_spread_spike   →  detect_binance_spread_spike
  perturb_depth_drop     →  detect_binance_depth_drop
  perturb_price_burst    →  detect_binance_price_burst
  perturb_divergence     →  detect_binance_vs_br_divergence
  perturb_pre_spike      →  detect_br_pre_spike     (mexe no snap "agora", mantém state[-1])
  perturb_decouple       →  detect_usdt_brl_decouple
"""
from __future__ import annotations
import copy
from typing import Tuple


# ---------- helpers ----------
def _deepcopy_snap(snap: dict) -> dict:
    """Clona snap preservando listas de tuplas (bids/asks)."""
    return copy.deepcopy(snap)


def _scale_prices(book: dict, factor: float) -> dict:
    """Multiplica todos os preços (best_bid/ask + níveis) por factor.
    Mantém quantidades. Não muta o original."""
    bb = book.get("best_bid", 0.0) or 0.0
    ba = book.get("best_ask", 0.0) or 0.0
    return {
        "best_bid": bb * factor,
        "best_ask": ba * factor,
        "bids": [(px * factor, qty) for px, qty in book.get("bids", [])],
        "asks": [(px * factor, qty) for px, qty in book.get("asks", [])],
    }


def _scale_qty(book: dict, factor: float, band_pct: float = 0.02) -> dict:
    """Multiplica qty dos níveis DENTRO de ±band_pct do mid por factor.
    Níveis fora da banda permanecem inalterados."""
    bb = book.get("best_bid", 0.0) or 0.0
    ba = book.get("best_ask", 0.0) or 0.0
    if bb <= 0 or ba <= 0:
        return dict(book)
    mid = (bb + ba) / 2
    band = band_pct * mid
    lo = mid - band
    hi = mid + band

    def _scale_side(levels, inside_check):
        out = []
        for px, qty in levels:
            if inside_check(px):
                out.append((px, qty * factor))
            else:
                out.append((px, qty))
        return out

    return {
        "best_bid": bb,
        "best_ask": ba,
        "bids": _scale_side(book.get("bids", []), lambda px: px >= lo),
        "asks": _scale_side(book.get("asks", []), lambda px: px <= hi),
    }


def _widen_spread(book: dict, factor: float) -> dict:
    """Mantém mid estável e multiplica o spread por factor, empurrando
    best_bid pra baixo e best_ask pra cima. Níveis internos também são
    deslocados pra não haver cross-book."""
    bb = book.get("best_bid", 0.0) or 0.0
    ba = book.get("best_ask", 0.0) or 0.0
    if bb <= 0 or ba <= 0 or ba <= bb:
        return dict(book)
    mid = (bb + ba) / 2
    half_spread = (ba - bb) / 2
    new_half = half_spread * factor
    new_bb = mid - new_half
    new_ba = mid + new_half
    # desloca o top: bids acima do novo best_bid descem pra new_bb
    #                asks abaixo do novo best_ask sobem pra new_ba
    new_bids = []
    for px, qty in book.get("bids", []):
        if px > new_bb:
            new_bids.append((new_bb, qty))
        else:
            new_bids.append((px, qty))
    new_asks = []
    for px, qty in book.get("asks", []):
        if px < new_ba:
            new_asks.append((new_ba, qty))
        else:
            new_asks.append((px, qty))
    return {
        "best_bid": new_bb,
        "best_ask": new_ba,
        "bids": new_bids,
        "asks": new_asks,
    }


# ---------- perturbadores (um por detector) ----------

def perturb_spread_spike(
    snap: dict,
    asset: str = "BTC-BRL",
    target_pct: float = 0.10,
    venue: str = "Binance",
) -> Tuple[dict, dict]:
    """Força o spread da Binance/asset a ficar em target_pct (valor absoluto em %).
    Alvo: detect_binance_spread_spike.

    Nota: valor = 0 mede o baseline (sem perturbação). Ponto de controle (FP).

    target_pct típicos: 0.02 (piso), 0.05 (med provável), 0.10 (high), 0.30 (crit)."""
    s = _deepcopy_snap(snap)
    book = s.get(venue, {}).get(asset)
    if not book:
        return s, {"applied": False, "reason": f"no book for {venue}/{asset}"}
    if target_pct <= 0:
        return s, {"applied": False, "scenario": "spread_spike",
                   "target_pct": 0.0, "reason": "baseline (identidade)"}
    bb = book.get("best_bid", 0.0) or 0.0
    ba = book.get("best_ask", 0.0) or 0.0
    if bb <= 0 or ba <= 0:
        return s, {"applied": False, "reason": f"invalid book for {venue}/{asset}"}
    mid = (bb + ba) / 2
    cur_spread = ba - bb
    cur_pct = cur_spread / mid * 100
    # fator para chegar em target_pct
    factor = (target_pct / cur_pct) if cur_pct > 0 else 1.0
    s[venue][asset] = _widen_spread(book, factor)
    new_bb = s[venue][asset]["best_bid"]; new_ba = s[venue][asset]["best_ask"]
    return s, {
        "applied": True,
        "scenario": "spread_spike",
        "venue": venue, "asset": asset,
        "target_pct": target_pct,
        "factor_applied": factor,
        "original_spread_pct": cur_pct,
        "new_spread_pct": (new_ba - new_bb) / ((new_ba + new_bb) / 2) * 100,
    }


def perturb_depth_drop(
    snap: dict,
    asset: str = "BTC-BRL",
    drop_frac: float = 0.60,
    venue: str = "Binance",
) -> Tuple[dict, dict]:
    """Remove drop_frac da profundidade dentro de ±2% do mid.
    Alvo: detect_binance_depth_drop.

    drop_frac típicos: 0.30 (não deve disparar), 0.55 (medium), 0.72 (high), 0.85 (crit)."""
    s = _deepcopy_snap(snap)
    book = s.get(venue, {}).get(asset)
    if not book:
        return s, {"applied": False, "reason": f"no book for {venue}/{asset}"}
    keep = max(0.0, 1.0 - drop_frac)
    s[venue][asset] = _scale_qty(book, keep, band_pct=0.02)
    return s, {
        "applied": True,
        "scenario": "depth_drop",
        "venue": venue, "asset": asset,
        "drop_frac": drop_frac,
    }


def perturb_price_burst(
    snap: dict,
    asset: str = "BTC-BRL",
    shift_pct: float = 1.0,
    venue: str = "Binance",
) -> Tuple[dict, dict]:
    """Desloca o mid da Binance/asset em shift_pct (positivo=alta, negativo=queda).
    Alvo: detect_binance_price_burst.

    Nota: o detector usa Z-score do Δ% sobre histórico. Um shift de 1% já é
    enorme num horizonte de segundos — depois do histórico estável, qualquer
    shift >= 0.3% deve ser flagrado.

    shift_pct típicos: 0.1 (não deve), 0.5 (medium), 1.0 (high), 2.0 (crit)."""
    s = _deepcopy_snap(snap)
    book = s.get(venue, {}).get(asset)
    if not book:
        return s, {"applied": False, "reason": f"no book for {venue}/{asset}"}
    factor = 1.0 + (shift_pct / 100.0)
    s[venue][asset] = _scale_prices(book, factor)
    return s, {
        "applied": True,
        "scenario": "price_burst",
        "venue": venue, "asset": asset,
        "shift_pct": shift_pct,
    }


def perturb_divergence(
    snap: dict,
    asset: str = "BTC-BRL",
    binance_shift_pct: float = 0.5,
) -> Tuple[dict, dict]:
    """Desloca SÓ a Binance — cria gap vs média MB+Foxbit.
    Alvo: detect_binance_vs_br_divergence.

    binance_shift_pct típicos: 0.1 (não), 0.35 (medium), 0.7 (high), 1.2 (crit)."""
    s = _deepcopy_snap(snap)
    book = s.get("Binance", {}).get(asset)
    if not book:
        return s, {"applied": False, "reason": f"no binance book for {asset}"}
    factor = 1.0 + (binance_shift_pct / 100.0)
    s["Binance"][asset] = _scale_prices(book, factor)
    return s, {
        "applied": True,
        "scenario": "divergence",
        "asset": asset,
        "binance_shift_pct": binance_shift_pct,
    }


def perturb_pre_spike(
    snap: dict,
    local_venue: str = "Mercado Bitcoin",
    asset: str = "BTC-BRL",
    local_shift_pct: float = 0.6,
) -> Tuple[dict, dict]:
    """Desloca SÓ MB ou Foxbit no snap 'agora', mantendo Binance intacta.
    O detector compara com state[-1] (snap anterior) — passe o baseline
    original como state[-1] no harness.
    Alvo: detect_br_pre_spike.

    local_shift_pct típicos: 0.2 (não), 0.5 (medium), 1.0 (high)."""
    s = _deepcopy_snap(snap)
    book = s.get(local_venue, {}).get(asset)
    if not book:
        return s, {"applied": False, "reason": f"no book for {local_venue}/{asset}"}
    factor = 1.0 + (local_shift_pct / 100.0)
    s[local_venue][asset] = _scale_prices(book, factor)
    return s, {
        "applied": True,
        "scenario": "pre_spike",
        "venue": local_venue, "asset": asset,
        "local_shift_pct": local_shift_pct,
    }


def perturb_decouple(
    snap: dict,
    shift_pct: float = 0.8,
) -> Tuple[dict, dict]:
    """Desloca USDT-BRL na Binance sem mexer em BTC-BRL nem no BTC-USDT global.
    Isso abre um gap entre USDT-BRL observado e FX implícito (btc_brl/btc_usdt).
    Alvo: detect_usdt_brl_decouple.

    shift_pct típicos: 0.3 (não), 0.7 (medium), 1.1 (high), 1.6 (crit)."""
    s = _deepcopy_snap(snap)
    book = s.get("Binance", {}).get("USDT-BRL")
    if not book:
        return s, {"applied": False, "reason": "no binance USDT-BRL book"}
    factor = 1.0 + (shift_pct / 100.0)
    s["Binance"]["USDT-BRL"] = _scale_prices(book, factor)
    return s, {
        "applied": True,
        "scenario": "decouple",
        "shift_pct": shift_pct,
    }


# ---------- registry ----------
PERTURBATIONS = {
    "spread_spike":  perturb_spread_spike,
    "depth_drop":    perturb_depth_drop,
    "price_burst":   perturb_price_burst,
    "divergence":    perturb_divergence,
    "pre_spike":     perturb_pre_spike,
    "decouple":      perturb_decouple,
}


# Intensity grids — cada ponto é um cenário de backtest.
# O ponto 0 (inativo) serve pra medir FP quando o detector "vê" uma cópia
# idêntica do baseline.
GRIDS = {
    # spread_spike: valores em % absoluto (target do spread). 0=baseline (FP).
    # piso do detector é 0.02%, então valores <= 0.02 não devem disparar.
    "spread_spike": [0.0, 0.015, 0.02, 0.03, 0.05, 0.10, 0.20, 0.50, 1.00],
    "depth_drop":   [0.0, 0.2, 0.35, 0.5, 0.6, 0.7, 0.8, 0.9],
    "price_burst":  [0.0, 0.05, 0.1, 0.2, 0.4, 0.7, 1.0, 1.5, 2.0, 3.0],
    "divergence":   [0.0, 0.1, 0.2, 0.35, 0.5, 0.7, 1.0, 1.5, 2.0],
    "pre_spike":    [0.0, 0.1, 0.2, 0.4, 0.6, 0.8, 1.2, 1.8],
    "decouple":     [0.0, 0.2, 0.4, 0.6, 0.8, 1.1, 1.5, 2.0],
}


# Mapeia cenário -> (nome do detector que deveria disparar, parâmetro do THRESHOLDS
# que representa o "limite mínimo" de disparo pra sweep).
# Usado no harness pra decidir "esse ponto é TP ou FN".
SCENARIO_TO_DETECTOR = {
    "spread_spike":  ("binance_spread_spike",   "spread_spike_ratio_p95_med"),
    "depth_drop":    ("binance_depth_drop",     "depth_drop_pct_med"),
    "price_burst":   ("binance_price_burst",    "price_burst_z_med"),
    "divergence":    ("binance_vs_br_divergence","divergence_gap_pct_med"),
    "pre_spike":     ("br_pre_spike",           "pre_spike_local_pct_med"),
    "decouple":      ("usdt_brl_decouple",      "decouple_gap_pct_med"),
}
