"""
detectors.py — sinais on-line do watcher Sentinel BR.

Filosofia: cada detector recebe (snap, state) onde:
  snap   = última leitura consolidada (dict por venue/asset)
  state  = histórico rolling (deque dos últimos N snapshots)

E retorna uma lista de Alert(...) ou [] se nada disparou.

Foco: BINANCE como âncora ("o lugar a defender") + cross-venue contra MB e Foxbit
para sinais antecipatórios (BR pre-spike).
"""
from __future__ import annotations
from dataclasses import dataclass, asdict
from collections import deque
from datetime import datetime, timezone
from statistics import median, pstdev, mean
from typing import Iterable, Optional


# ---------- thresholds (parametrizáveis via backtest) ----------
# Medium = limite inferior que dispara o alerta.
# High/Critical = escalonamentos de severidade.
# Podem ser mutados em runtime (ex: scripts/backtest/run_backtest.py).
THRESHOLDS: dict = {
    # binance_spread_spike (redesign 2026-04-19) — ratio sobre p95 histórico
    # + piso absoluto. Substituiu a versão antiga que usava mediana (FP ~35%
    # porque mediana é instável em books com spread tick-level).
    # Lógica: ignora spreads < piso_absoluto (ruído tick) e compara com p95
    # histórico (resistente a outliers) multiplicado por fator de folga.
    # Calibrado 2026-04-19: ratio_p95_med 2.0 → 1.25 (fp=0%, tp=75%, f1=0.86).
    # Os 3 pontos "perdidos" estão abaixo do piso (não são ataques reais).
    "spread_spike_min_abs_pct":    0.02,  # 2 bps — abaixo disso é tick, não ataque
    "spread_spike_ratio_p95_med":  1.25,
    "spread_spike_ratio_p95_high": 2.0,
    "spread_spike_ratio_p95_crit": 3.0,
    "spread_spike_min_history":    10,

    # binance_depth_drop — queda % da profundidade vs mediana
    # Calibrado 2026-04-19: med 0.50 → 0.35 (fp=0%, tp=71%, f1=0.83).
    "depth_drop_pct_med":          0.35,
    "depth_drop_pct_high":         0.55,
    "depth_drop_pct_crit":         0.70,
    "depth_drop_min_history":      10,

    # binance_price_burst — |Z-score| do Δ% vs distribuição histórica de Δ
    # Calibrado 2026-04-19: med 3.0 → 6.0 (fp=0%, tp=100%, f1=1.00).
    # O valor alto reflete que o std dev histórico é pequeno — qualquer shift
    # percentual vira Z-score grande. 6σ evita FP de ruído de alta-frequência.
    "price_burst_z_med":           6.0,
    "price_burst_z_high":          8.0,
    "price_burst_z_crit":         10.0,
    "price_burst_min_history":    15,

    # binance_vs_br_divergence — gap % absoluto entre Binance e média MB+Foxbit
    # Calibrado 2026-04-19: med 0.30 → 0.75 (fp=0%, tp=38%, f1=0.55).
    # O valor antigo (0.30) produzia 100% FP porque MB+Foxbit estão
    # ESTRUTURALMENTE ~0.3% acima da Binance (prêmio BR). 0.75% pega desvios
    # acima do prêmio normal. TP baixa é esperada: o detector só flagra ataques
    # grandes. Complementar com pre_spike pra cobrir early signals.
    "divergence_gap_pct_med":      0.75,
    "divergence_gap_pct_high":     1.25,
    "divergence_gap_pct_crit":     2.00,

    # br_pre_spike — Δ% local em 1 leitura, com Binance ainda parado
    # Calibrado 2026-04-19: med 0.40 → 0.20 (fp=0%, tp=86%, f1=0.92).
    "pre_spike_local_pct_med":     0.20,
    "pre_spike_local_pct_high":    0.50,
    "pre_spike_binance_max_pct":   0.10,

    # usdt_brl_decouple — gap % de USDT-BRL vs FX implícito
    # Calibrado 2026-04-19: med 0.60 → 0.30 (fp=0%, tp=86%, f1=0.92).
    # USDT-BRL é estruturalmente próximo do FX BTC/USDT → gaps > 0.3% SÃO
    # anomalias reais (pressão atípica por stablecoin local).
    "decouple_gap_pct_med":        0.30,
    "decouple_gap_pct_high":       0.60,
    "decouple_gap_pct_crit":       1.00,

    # stablecoin_flight — detector multi-sinal de fuga para USDT.
    # Dispara por CONJUNÇÃO: 2+ das 3 condições a seguir (olhando ~3 snaps = ~90s):
    #   (A) queda coordenada em BTC/ETH/SOL-BRL Binance < -X%
    #   (B) imbalance bid-heavy no livro de USDT-BRL > Y (compra unilateral)
    #   (C) decouple USDT-BRL vs FX implícito > Z% (prêmio atípico)
    # Ortogonal aos 6 detectores anteriores: pega o PADRÃO MACRO de laundering
    # (layering fase 1) e não o tick único. Valores iniciais são palpite
    # educado — a calibrar com backtest (roadmap).
    "flight_risk_drop_pct":        0.50,   # Δ% combinada BTC+ETH+SOL (negativa)
    "flight_usdt_imbalance_min":   0.35,   # imbalance [-1..+1] em USDT-BRL
    "flight_usdt_decouple_pct":    0.30,   # gap USDT-BRL vs FX implícito
    "flight_lookback_snaps":       3,      # janela de ~90s em interval=30s
    "flight_min_history":          4,
    "flight_risk_drop_crit_pct":   1.50,   # queda severa pra escalar pra critical
    "flight_decouple_crit_pct":    0.80,
}

def apply_overrides(overrides: dict) -> dict:
    """Atualiza THRESHOLDS in-place e retorna o dicionário antigo pra rollback."""
    old = {k: THRESHOLDS[k] for k in overrides if k in THRESHOLDS}
    THRESHOLDS.update({k: v for k, v in overrides.items() if k in THRESHOLDS})
    return old


# ---------- modelo ----------
@dataclass
class Alert:
    rule:       str        # ex: "binance_vs_br_divergence"
    severity:   str        # "critical" | "high" | "medium" | "info"
    venue:      str        # "Binance" | "Mercado Bitcoin" | "Foxbit" | "—"
    asset:      str        # "BTC-BRL" | "USDT-BRL" | ...
    value:      float      # observado (formato cru)
    threshold:  float      # gatilho usado
    narrative:  str        # frase em PT pra leigo
    context:    dict       # tudo o que ajudar diagnóstico
    ts:         str        # ISO UTC

    def to_dict(self) -> dict:
        return asdict(self)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------- helpers de book ----------
def _mid(book: dict) -> float:
    bid = book.get("best_bid"); ask = book.get("best_ask")
    if bid and ask: return (bid + ask) / 2
    return 0.0


def _spread_pct(book: dict) -> float:
    bid = book.get("best_bid"); ask = book.get("best_ask")
    if not (bid and ask) or bid <= 0 or ask <= 0: return 0.0
    return (ask - bid) / ((ask + bid) / 2) * 100


def _depth_pm2(book: dict) -> float:
    """Soma do volume base nos níveis dentro de ±2% do mid."""
    mid = _mid(book)
    if mid == 0: return 0.0
    band = 0.02 * mid
    bids = sum(qty for px, qty in book.get("bids", []) if px >= mid - band)
    asks = sum(qty for px, qty in book.get("asks", []) if px <= mid + band)
    return bids + asks


def _imbalance(book: dict) -> float:
    """[-1..+1] · positivo = bid-heavy, negativo = ask-heavy."""
    mid = _mid(book)
    if mid == 0: return 0.0
    band = 0.02 * mid
    bids = sum(qty for px, qty in book.get("bids", []) if px >= mid - band)
    asks = sum(qty for px, qty in book.get("asks", []) if px <= mid + band)
    tot = bids + asks
    if tot == 0: return 0.0
    return (bids - asks) / tot


def _zscore(x: float, history: Iterable[float]) -> float:
    h = [v for v in history if v is not None]
    if len(h) < 5: return 0.0
    mu = mean(h); sd = pstdev(h) or 1e-12
    return (x - mu) / sd


# ---------- detectores ----------
def _p95(values: list[float]) -> float:
    """p95 "barato" — nth-item, sem scipy. Para n<20, retorna o máximo."""
    if not values: return 0.0
    xs = sorted(values)
    n = len(xs)
    if n < 20:
        return xs[-1]
    k = int(0.95 * (n - 1))
    return xs[k]


def detect_binance_spread_spike(snap: dict, state: deque) -> list[Alert]:
    """Spread Binance BRL > T × p95 histórico, E spread atual > piso absoluto.

    Redesign (2026-04-19): antes usava mediana, o que produzia ~35% FP em books
    com spread tick-level (ETH-BRL oscila entre 0.0001% e 0.017% só por book
    fino, sem ataque). p95 é robusto a outliers; piso absoluto elimina casos
    de "mediana quase-zero → qualquer tick é X vezes maior".
    """
    T_med  = THRESHOLDS["spread_spike_ratio_p95_med"]
    T_high = THRESHOLDS["spread_spike_ratio_p95_high"]
    T_crit = THRESHOLDS["spread_spike_ratio_p95_crit"]
    N_min  = THRESHOLDS["spread_spike_min_history"]
    floor  = THRESHOLDS["spread_spike_min_abs_pct"]
    out = []
    for asset, book in snap.get("Binance", {}).items():
        sp = _spread_pct(book)
        if sp < floor:  # ruído tick-level — ignora
            continue
        history = [_spread_pct(s.get("Binance", {}).get(asset, {})) for s in state]
        history = [h for h in history if h > 0]
        if len(history) < N_min: continue
        p95 = _p95(history)
        # baseline efetivo = max(piso, p95); evita ratio explodir quando p95 é quase-zero
        baseline = max(floor, p95)
        ratio = sp / baseline
        if ratio >= T_med:
            sev = "critical" if ratio >= T_crit else "high" if ratio >= T_high else "medium"
            med = median(history)
            out.append(Alert(
                rule="binance_spread_spike", severity=sev,
                venue="Binance", asset=asset,
                value=round(sp, 4), threshold=round(T_med * baseline, 4),
                narrative=(f"Spread Binance {asset} em {sp:.4f}% — {ratio:.1f}× o p95 "
                           f"recente ({p95:.4f}%) e acima do piso de {floor:.3f}%."),
                context={"spread_pct": sp, "p95_pct": p95, "median_pct": med,
                         "baseline_pct": baseline, "ratio": ratio, "floor_pct": floor},
                ts=_now_iso(),
            ))
    return out


def detect_binance_depth_drop(snap: dict, state: deque) -> list[Alert]:
    """Profundidade ±2% caiu > T_med % vs mediana."""
    T_med  = THRESHOLDS["depth_drop_pct_med"]
    T_high = THRESHOLDS["depth_drop_pct_high"]
    T_crit = THRESHOLDS["depth_drop_pct_crit"]
    N_min  = THRESHOLDS["depth_drop_min_history"]
    out = []
    for asset, book in snap.get("Binance", {}).items():
        d = _depth_pm2(book)
        history = [_depth_pm2(s.get("Binance", {}).get(asset, {})) for s in state]
        history = [h for h in history if h > 0]
        if len(history) < N_min: continue
        med = median(history)
        if med <= 0: continue
        drop = 1 - (d / med)
        if drop >= T_med:
            sev = "critical" if drop >= T_crit else "high" if drop >= T_high else "medium"
            out.append(Alert(
                rule="binance_depth_drop", severity=sev,
                venue="Binance", asset=asset,
                value=round(d, 4), threshold=round(T_med * med, 4),
                narrative=f"Profundidade Binance {asset} caiu {drop*100:.0f}% (de {med:.2f} → {d:.2f} unidades em ±2%).",
                context={"depth_now": d, "depth_median": med, "drop_pct": drop},
                ts=_now_iso(),
            ))
    return out


def detect_binance_price_burst(snap: dict, state: deque) -> list[Alert]:
    """Mid-price move com |Z-score| > T_med vs últimas N leituras."""
    T_med  = THRESHOLDS["price_burst_z_med"]
    T_high = THRESHOLDS["price_burst_z_high"]
    T_crit = THRESHOLDS["price_burst_z_crit"]
    N_min  = THRESHOLDS["price_burst_min_history"]
    out = []
    for asset, book in snap.get("Binance", {}).items():
        m = _mid(book)
        history = [_mid(s.get("Binance", {}).get(asset, {})) for s in state]
        history = [h for h in history if h > 0]
        if len(history) < N_min: continue
        last = history[-1]
        if last <= 0: continue
        chg = (m - last) / last * 100
        chg_history = []
        for i in range(1, len(history)):
            if history[i-1] > 0:
                chg_history.append((history[i] - history[i-1]) / history[i-1] * 100)
        z = _zscore(chg, chg_history)
        if abs(z) >= T_med:
            direction = "ALTA" if chg > 0 else "QUEDA"
            sev = "critical" if abs(z) >= T_crit else "high" if abs(z) >= T_high else "medium"
            out.append(Alert(
                rule="binance_price_burst", severity=sev,
                venue="Binance", asset=asset,
                value=round(chg, 4), threshold=round(T_med * pstdev(chg_history), 4),
                narrative=f"Movimento brusco de {direction} em Binance {asset}: {chg:+.3f}% (Z={z:+.2f}).",
                context={"pct_change": chg, "z": z, "n_history": len(chg_history)},
                ts=_now_iso(),
            ))
    return out


# Lista canônica das venues BR usadas pelos detectores cross-venue.
# Ampliada em 2026-04-19: adicionadas NovaDAX, BitPreço, Ripio Trade para
# aumentar robustez (antes só MB+Foxbit). Min 2 venues respondendo pra calcular
# média — evita dar peso a 1 exchange quebrada.
BR_VENUES = ("Mercado Bitcoin", "Foxbit", "NovaDAX", "BitPreço", "Ripio Trade")


def detect_binance_vs_br_divergence(snap: dict, state: deque) -> list[Alert]:
    """Binance BRL vs média das venues BR (MB, Foxbit, NovaDAX, BitPreço, Ripio).
       Sinaliza arbitragem aberta — fluxo PIX local divergindo de Binance.
       Robustez: média dos quotes BR é estimador mais estável que par único."""
    out = []
    binance = snap.get("Binance", {})
    for asset, b_book in binance.items():
        b_mid = _mid(b_book)
        if not b_mid: continue
        local_mids = []
        for v_name in BR_VENUES:
            v_snap = snap.get(v_name, {}) or {}
            v_book = v_snap.get(asset, {}) if v_snap else {}
            v_mid = _mid(v_book)
            if v_mid > 0: local_mids.append((v_name, v_mid))
        if len(local_mids) < 2: continue  # precisa ≥2 venues BR vivas
        avg_local = mean([m for _, m in local_mids])
        gap_pct = (b_mid - avg_local) / avg_local * 100
        T_med  = THRESHOLDS["divergence_gap_pct_med"]
        T_high = THRESHOLDS["divergence_gap_pct_high"]
        T_crit = THRESHOLDS["divergence_gap_pct_crit"]
        if abs(gap_pct) >= T_med:
            sev = "critical" if abs(gap_pct) >= T_crit else "high" if abs(gap_pct) >= T_high else "medium"
            who = "acima" if gap_pct > 0 else "abaixo"
            venues_txt = "+".join(n.split()[0] for n, _ in local_mids)  # ex "Mercado+Foxbit+NovaDAX"
            out.append(Alert(
                rule="binance_vs_br_divergence", severity=sev,
                venue="Binance", asset=asset,
                value=round(gap_pct, 4), threshold=T_med,
                narrative=(f"Binance {asset} está {gap_pct:+.3f}% {who} da média BR "
                           f"({venues_txt}; n={len(local_mids)}). Arb aberta — "
                           f"possível pressão local de fluxo PIX/cash-out."),
                context={
                    "binance_mid": b_mid,
                    "local_mids": dict(local_mids),
                    "avg_local": avg_local,
                    "gap_pct": gap_pct,
                    "n_br_venues": len(local_mids),
                },
                ts=_now_iso(),
            ))
    return out


def detect_br_pre_spike(snap: dict, state: deque) -> list[Alert]:
    """Lead indicator: qualquer venue BR se move antes da Binance.
       Se mid de alguma das 5 venues BR muda ≥ T_med em 1 leitura E Binance ainda
       não moveu (Δ ≤ B_max), sinaliza pressão local antes da arbitragem fechar.
       É o melhor candidato para 'evento BR antes do mercado global perceber'.

       Versão 2026-04-19: amplia de 2 para 5 venues. Concordância entre 2+ venues
       locais vira um sinal mais forte (reduz FP de venue com book ruim)."""
    out = []
    if not state: return out
    prev = state[-1]
    binance_now = snap.get("Binance", {})
    binance_prev = prev.get("Binance", {})
    T_med  = THRESHOLDS["pre_spike_local_pct_med"]
    T_high = THRESHOLDS["pre_spike_local_pct_high"]
    B_max  = THRESHOLDS["pre_spike_binance_max_pct"]

    # Coleta de candidatos por (venue, asset) dentro da janela
    # Depois consolida por asset pra reportar convergência BR cross-venue.
    by_asset: dict[str, list] = {}
    for v_name in BR_VENUES:
        cur = snap.get(v_name, {}) or {}
        old = prev.get(v_name, {}) or {}
        for asset, book in cur.items():
            m_now = _mid(book); m_prev = _mid(old.get(asset, {}))
            if not (m_now and m_prev): continue
            local_chg = (m_now - m_prev) / m_prev * 100
            b_now = _mid(binance_now.get(asset, {}))
            b_prev = _mid(binance_prev.get(asset, {}))
            if not (b_now and b_prev): continue
            b_chg = (b_now - b_prev) / b_prev * 100
            if abs(local_chg) >= T_med and abs(b_chg) <= B_max:
                by_asset.setdefault(asset, []).append({
                    "venue": v_name, "local_chg": local_chg, "b_chg": b_chg,
                    "m_prev": m_prev, "m_now": m_now,
                    "b_prev": b_prev, "b_now": b_now,
                })

    for asset, hits in by_asset.items():
        # Maior magnitude domina severidade
        biggest = max(hits, key=lambda h: abs(h["local_chg"]))
        max_chg = biggest["local_chg"]
        n_venues_br = len(hits)
        # escalona: 2+ venues concordando vira "high" mesmo sem cruzar T_high
        if abs(max_chg) >= T_high or n_venues_br >= 2:
            sev = "high"
        else:
            sev = "medium"
        direction = "ALTA" if max_chg > 0 else "QUEDA"
        venues_txt = ", ".join(h["venue"] for h in hits)
        out.append(Alert(
            rule="br_pre_spike", severity=sev,
            venue=biggest["venue"], asset=asset,
            value=round(max_chg, 4), threshold=T_med,
            narrative=(f"{n_venues_br} venue(s) BR movendo em {asset} ({direction}): "
                       f"{venues_txt}; maior Δ={max_chg:+.3f}% (em {biggest['venue']}) "
                       f"mas Binance {biggest['b_chg']:+.3f}% (quase parada). Pressão "
                       f"local antes do mercado global reagir."),
            context={
                "n_venues_br":      n_venues_br,
                "max_local_chg_pct": max_chg,
                "binance_change_pct": biggest["b_chg"],
                "hits":             hits,
            },
            ts=_now_iso(),
        ))
    return out


def detect_usdt_brl_decouple(snap: dict, state: deque) -> list[Alert]:
    """USDT-BRL deslocando do FX implícito (BTC-BRL / BTC-USDT).
       Se USDT-BRL diverge > 0.6% do FX teórico, há pressão atípica de demanda
       por stablecoin local — assinatura clássica de cash-out de fraude."""
    out = []
    binance = snap.get("Binance", {})
    btc_brl = _mid(binance.get("BTC-BRL", {}))
    btc_usd = snap.get("_meta", {}).get("btc_usdt_global", 0)  # vem do fetch global
    usdt_brl = _mid(binance.get("USDT-BRL", {}))
    if not (btc_brl and btc_usd and usdt_brl): return out
    fx_implied = btc_brl / btc_usd
    gap_pct = (usdt_brl - fx_implied) / fx_implied * 100
    T_med  = THRESHOLDS["decouple_gap_pct_med"]
    T_high = THRESHOLDS["decouple_gap_pct_high"]
    T_crit = THRESHOLDS["decouple_gap_pct_crit"]
    if abs(gap_pct) >= T_med:
        sev = "critical" if abs(gap_pct) >= T_crit else "high" if abs(gap_pct) >= T_high else "medium"
        direction = "PRÊMIO" if gap_pct > 0 else "DESCONTO"
        out.append(Alert(
            rule="usdt_brl_decouple", severity=sev,
            venue="Binance", asset="USDT-BRL",
            value=round(gap_pct, 4), threshold=T_med,
            narrative=(f"USDT-BRL com {direction} de {abs(gap_pct):.2f}% sobre o FX implícito "
                       f"(R$ {usdt_brl:.4f} vs R$ {fx_implied:.4f}). Pressão atípica de "
                       f"demanda por stablecoin local — sinal de cash-out de fraude."),
            context={
                "usdt_brl_observed": usdt_brl,
                "fx_implied_btc": fx_implied,
                "btc_brl": btc_brl, "btc_usdt_global": btc_usd,
                "gap_pct": gap_pct,
            },
            ts=_now_iso(),
        ))
    return out


def detect_stablecoin_flight(snap: dict, state: deque) -> list[Alert]:
    """Fuga para stablecoin: ativos de risco caindo E pressão compradora em USDT-BRL
    (bid-heavy) E/OU decouple positivo. Padrão forense de laundering pós-hack
    (conversão rápida de ativos voláteis em USDT pra preservar valor no layering).

    Dispara por CONJUNÇÃO: 2+ das 3 condições numa janela de ~lookback snaps.
      (A) Δ% médio ponderado de BTC+ETH+SOL-BRL Binance < -flight_risk_drop_pct
      (B) imbalance bid-heavy do USDT-BRL > flight_usdt_imbalance_min
      (C) gap USDT-BRL vs FX implícito (decouple positivo) > flight_usdt_decouple_pct
    """
    out = []
    binance = snap.get("Binance", {})
    if not binance: return out

    N = THRESHOLDS["flight_lookback_snaps"]
    N_min = THRESHOLDS["flight_min_history"]
    if len(state) < N_min: return out

    past = state[-N] if len(state) >= N else state[0]
    past_binance = past.get("Binance", {})

    # ----- (A) queda coordenada em ativos de risco -----
    drops = []
    for asset in ("BTC-BRL", "ETH-BRL", "SOL-BRL"):
        m_now  = _mid(binance.get(asset, {}))
        m_prev = _mid(past_binance.get(asset, {}))
        if m_now and m_prev:
            drops.append((m_now - m_prev) / m_prev * 100)
    if len(drops) < 2: return out  # precisa ≥2 ativos pra dizer "coordenado"
    risk_drop = mean(drops)  # negativo se o mercado caiu em média
    cond_A = (risk_drop <= -THRESHOLDS["flight_risk_drop_pct"])

    # ----- (B) pressão compradora em USDT-BRL -----
    usdt_book = binance.get("USDT-BRL", {})
    imb = _imbalance(usdt_book) if usdt_book else 0.0
    cond_B = (imb >= THRESHOLDS["flight_usdt_imbalance_min"])

    # ----- (C) decouple USDT-BRL vs FX implícito -----
    btc_brl = _mid(binance.get("BTC-BRL", {}))
    btc_usd = snap.get("_meta", {}).get("btc_usdt_global", 0)
    usdt_brl = _mid(usdt_book)
    decouple_pct = 0.0
    if btc_brl and btc_usd and usdt_brl:
        fx_implied = btc_brl / btc_usd
        decouple_pct = (usdt_brl - fx_implied) / fx_implied * 100
    cond_C = (decouple_pct >= THRESHOLDS["flight_usdt_decouple_pct"])

    conds = sum([cond_A, cond_B, cond_C])
    if conds < 2:
        return out

    # severidade: 2/3 -> medium; 3/3 -> high; escala para critical se queda
    # severa + decouple grande (sinal de estresse real)
    sev = "medium"
    if conds == 3:
        sev = "high"
    if (risk_drop <= -THRESHOLDS["flight_risk_drop_crit_pct"] and
        decouple_pct >= THRESHOLDS["flight_decouple_crit_pct"]):
        sev = "critical"

    parts = []
    if cond_A: parts.append(f"ativos de risco em queda média {risk_drop:+.2f}%")
    if cond_B: parts.append(f"USDT-BRL bid-heavy (imbalance {imb:+.2f})")
    if cond_C: parts.append(f"prêmio USDT-BRL {decouple_pct:+.2f}%")
    narr = (
        f"Fuga para stablecoin: {' · '.join(parts)}. "
        f"Padrão de laundering pós-hack — conversão de BTC/ETH/SOL em "
        f"USDT-BRL para preservar valor durante layering."
    )

    out.append(Alert(
        rule="stablecoin_flight", severity=sev,
        venue="Binance", asset="USDT-BRL",
        value=round(decouple_pct, 4),
        threshold=THRESHOLDS["flight_usdt_decouple_pct"],
        narrative=narr,
        context={
            "risk_drop_pct":     round(risk_drop, 4),
            "usdt_imbalance":    round(imb, 4),
            "usdt_decouple_pct": round(decouple_pct, 4),
            "conds_met":         conds,
            "lookback_snaps":    N,
            "risk_drops_by_asset": {
                a: round(d, 4) for a, d in zip(("BTC-BRL","ETH-BRL","SOL-BRL"), drops)
            },
        },
        ts=_now_iso(),
    ))
    return out


# ---------- registry ----------
DETECTORS = [
    detect_binance_spread_spike,
    detect_binance_depth_drop,
    detect_binance_price_burst,
    detect_binance_vs_br_divergence,
    detect_br_pre_spike,
    detect_usdt_brl_decouple,
    detect_stablecoin_flight,
]


def run_all(snap: dict, state: deque) -> list[Alert]:
    out = []
    for fn in DETECTORS:
        try:
            out.extend(fn(snap, state))
        except Exception as e:
            print(f"[detector {fn.__name__}] erro: {e}")
    return out
