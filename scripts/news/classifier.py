"""
classifier.py — scoring keyword-based de itens de imprensa.

Foco explícito: hacking, incidentes cibernéticos, PLD/CFT (prevenção à lavagem
de dinheiro e combate ao financiamento do terrorismo). Notícias que são
puramente sobre "crypto" ou "PIX" sem o ângulo de ataque/lavagem são
classificadas como noise e não aparecem na seção "Imprensa corrobora".

Cada item recebe score por categoria (attack, launder, cft, pix, crypto, value).
Severidade final exige `attack OR launder OR cft` — pix/crypto entram só como
*contexto* que escala a severidade, nunca como sinal único.
"""
from __future__ import annotations
import re
from typing import Iterable

# --- dicionários de termos ---
# peso do termo, sentence-insensitive
ATTACK = {
    # Ataque/hacking direto
    r"\bataque\b": 3, r"\bataques\b": 3,
    r"\bhack\w*\b": 4, r"\binvas\w*\b": 4,
    r"\bransomware\b": 5, r"\bmalware\b": 4, r"\bphishing\b": 4,
    r"\bciberat\w+\b": 5, r"\bcibercrim\w+\b": 5, r"\bciberseguran\w+\b": 3,
    r"\bexploit\w*\b": 4, r"\bzero-?day\b": 5, r"\bbackdoor\b": 4,
    r"\bbreach\b": 4, r"\bdata\s+breach\b": 5,
    # Vazamento / incidente
    r"\bincidente de seguran\w+\b": 4, r"\bincidente cibern\w+\b": 5,
    r"\bvazamento\b": 3, r"\bvazou\b": 3, r"\bvazaram\b": 3,
    r"\bvazamento de dados\b": 4,
    # Roubo / desvio / fraude
    r"\bfraude\b": 3, r"\bfraudes\b": 3, r"\bfraudul\w+\b": 3,
    r"\bgolpe\b": 2, r"\bgolpes\b": 2,
    r"\bdesvi\w+\b": 3,   # desvio, desviados
    r"\broubo\b": 3, r"\broubado\w*\b": 3, r"\bfurto\b": 2,
    r"\bextors\w+\b": 3, r"\bsequestro de dados\b": 5,
}
PIX = {
    r"\bPIX\b": 3,
    r"\bSinqia\b": 5, r"\bPismo\b": 4, r"\bC&M Software\b": 5,
    r"\bBanco Central\b": 2, r"\bBCB\b": 2, r"\bBC\b": 1,
    r"\bSPB\b": 3, r"\bSTR\b": 1, r"\bSPI\b": 2,
    r"\binstitui\w+ de pagament\w*\b": 2,
    r"\bfintech\b": 1,
}
CRYPTO = {
    r"\bbitcoin\b": 2, r"\bcripto\b": 2, r"\bcriptomoed\w+\b": 2,
    r"\bstablecoin\w*\b": 3, r"\bUSDT\b": 3, r"\bTether\b": 3,
    r"\bexchange\b": 2, r"\bcorretora\b": 2,
    r"\bMercado Bitcoin\b": 3, r"\bFoxbit\b": 3, r"\bBinance\b": 3,
    r"\bwallet\b": 2, r"\bcarteira\b": 1,
    r"\bon-?chain\b": 2, r"\bblockchain\b": 1,
    r"\bTRON\b": 2, r"\bTRC-?20\b": 3, r"\bERC-?20\b": 2,
}
LAUNDER = {
    # Lavagem / PLD
    r"\blavagem\b": 4, r"\blavand\w+\b": 3, r"\blavador\w*\b": 4,
    r"\boculta\w+ (de )?patrim\w+\b": 4, r"\bocultar origem\b": 3,
    r"\bestrutura\w+\b": 1,            # estruturação
    r"\bsmurfing\b": 4, r"\bfracionamento\b": 3,
    r"\blayering\b": 4, r"\bplacement\b": 3, r"\bintegration\b": 2,
    r"\bmixer\b": 4, r"\btumbler\b": 4, r"\bcoin\s*join\b": 4,
    r"\bmul\w+ banc\w+\b": 3, r"\bconta\s+laranja\w*\b": 4, r"\bdoleiro\w*\b": 3,
    # Instituições PLD
    r"\bCOAF\b": 4, r"\bCoaf\b": 4,
    r"\bMPF\b": 2, r"\bPF\b": 2, r"\bPol\w+cia Federal\b": 3,
    r"\bdeflagr\w+\b": 3, r"\bopera\w+ policial\b": 3,
}
# CFT — Combate ao Financiamento do Terrorismo e regulação PLD/AML/FATF
CFT = {
    # Siglas brasileiras
    r"\bPLD\b": 4, r"\bPLDFT\w*\b": 5, r"\bPLD[/\-]?FT\w*\b": 5,
    r"\bPLD[/\-]?CFT\w*\b": 5, r"\bPLD[/\-]?CFTP\w*\b": 5,
    r"\bCFT\b": 4, r"\bCFTP\b": 4, r"\bFPADM\b": 4,
    r"\bpreven\w+ [aà] lavagem\b": 4,
    r"\bcombate.*financiamento.*terror\w*\b": 5,
    r"\bfinanciamento do terror\w*\b": 5, r"\bterrorismo\b": 3,
    # Regulação BR
    r"\blei 9\.?613\b": 4, r"\bcircular 3\.?978\b": 4, r"\bcircular 3978\b": 4,
    r"\bresolu\w+ BCB\s*\d+\b": 3, r"\binstru\w+ CVM\s*\d+\b": 2,
    # Compliance / KYC / VASP
    r"\bcompliance\b": 2, r"\bKYC\b": 3, r"\bKYC/AML\b": 4,
    r"\bAML\b": 4, r"\bCTF\b": 4, r"\bCAAF\b": 3,
    r"\bdue diligence\b": 2, r"\btravel rule\b": 4,
    r"\bVASP\w*\b": 3, r"\bprestador.*ativos.*virtuais\b": 4,
    # Regulação internacional
    r"\bMiCA\b": 2, r"\bFATF\b": 3, r"\bGAFI\b": 3, r"\bFinCEN\b": 3,
    # Sanções / embargos
    r"\bOFAC\b": 3, r"\bsan[çc][ãa]o\w*\b": 3, r"\bembargo\w*\b": 3,
    r"\blista de sanções\b": 4, r"\bSDN list\b": 4,
}
VALUE = {
    r"\bR\$\s*\d": 2,                  # menciona valor em reais
    r"\bmilh\w+\b": 1, r"\bbilh\w+\b": 2,
    r"\bUS\$\s*\d": 1,
}

CATS: dict[str, dict] = {
    "attack":  ATTACK,
    "launder": LAUNDER,
    "cft":     CFT,
    "pix":     PIX,
    "crypto":  CRYPTO,
    "value":   VALUE,
}

# compila uma vez
_COMPILED = {cat: [(re.compile(p, re.IGNORECASE), w) for p, w in d.items()]
             for cat, d in CATS.items()}

# ---------------- FOCO BR ----------------
# Sinais positivos: termos que indicam relevância pro mercado brasileiro.
BR_POSITIVE = {
    r"\bBrasil\b": 2, r"\bbrasileir[ao]s?\b": 2, r"\bBR\b": 1,
    r"\bBras[ií]lia\b": 1,
    r"\bS[aã]o Paulo\b": 1, r"\bRio de Janeiro\b": 1, r"\bBelo Horizonte\b": 1,
    r"\bR\$\s*\d": 3,
    r"\bPIX\b": 3, r"\bBCB\b": 2, r"\bBanco Central\b": 2, r"\bBacen\b": 2, r"\bCOPOM\b": 2,
    r"\bSelic\b": 1, r"\bTR\b": 0,
    r"\bBTG\b": 3, r"\bIta[uú]\b": 3, r"\bBradesco\b": 3, r"\bSantander\b": 2,
    r"\bBRB\b": 3, r"\bInter\b": 2, r"\bNubank\b": 3, r"\bPicPay\b": 3,
    r"\bC6\b": 3, r"\bMercado Pago\b": 3, r"\bNext\b": 1, r"\bStone\b": 2,
    r"\bXP\b": 2, r"\bOriginal\b": 1, r"\bSafra\b": 2,
    r"\bMercado Bitcoin\b": 4, r"\bFoxbit\b": 4, r"\bNovaDAX\b": 3, r"\bBitso\b": 3,
    r"\bSinqia\b": 5, r"\bPismo\b": 4, r"\bC&M Software\b": 5, r"\bSPB\b": 2,
    r"\bPol[ií]cia Federal\b": 3, r"\bPF\b": 2, r"\bMPF\b": 2, r"\bSTF\b": 1,
    r"\bCVM\b": 2, r"\bReceita Federal\b": 2, r"\bCoaf\b": 3, r"\bCGU\b": 2,
    r"\bINSS\b": 2, r"\bMaster\b": 2,      # caso BRB/Master
    r"\bgolpe do Pix\b": 3,
    r"\bBrasil Cash\b": 3, r"\bPefisa\b": 3, r"\bBanco do Nordeste\b": 3,
    r"\bAsa\b": 2, r"\bSAQ\b": 2,
}
# Sinais negativos: marcadores de notícia *exclusivamente* estrangeira.
BR_NEGATIVE = {
    # Rússia e variações
    r"\bR[uú]ssia\b": 3, r"\bruss[oa]s?\b": 3, r"\bKremlin\b": 3, r"\bPutin\b": 3,
    r"\bMoscou\b": 3, r"\bMoscovo\b": 3,
    # Ucrânia
    r"\bUcr[aâ]nia\b": 2, r"\bucranian[oa]s?\b": 2, r"\bKiev\b": 2,
    # EUA
    r"\bEUA\b": 2, r"\bEstados Unidos\b": 2, r"\bWashington\b": 2,
    r"\bTrump\b": 2, r"\bBiden\b": 2, r"\bSEC\b": 2, r"\bFBI\b": 2,
    r"\bDOJ\b": 2, r"\bDEA\b": 2, r"\bIRS\b": 1,
    r"\bNova York\b": 2, r"\bNew York\b": 2, r"\bUS\$\s*\d": 2,
    # Exchanges US-focused
    r"\bBitfinex\b": 3, r"\bCoinbase\b": 2, r"\bKraken\b": 2, r"\bGemini\b": 2,
    # América Latina (quando não BR)
    r"\bArgentina\b": 2, r"\bargentin[ao]s?\b": 2, r"\bBuenos Aires\b": 2,
    r"\bVenezuela\b": 2, r"\bvenezuelan[oa]s?\b": 2,
    r"\bCuba\b": 2, r"\bM[eé]xico\b": 2, r"\bmexican[oa]s?\b": 2,
    r"\bChile\b": 1, r"\bColombia\b": 1,
    # Ásia
    r"\bChina\b": 2, r"\bchin[eê]s\w*\b": 2, r"\bchinesa?s?\b": 2,
    r"\bPequim\b": 2, r"\bXangai\b": 2, r"\bHong Kong\b": 2,
    r"\bJap[ãa]o\b": 1, r"\bT[óo]quio\b": 1, r"\bCoreia\b": 1,
    r"\b[ií]ndia\b": 1,
    # Europa
    r"\bEuropa\b": 1, r"\beurope[ui]a?s?\b": 1,
    r"\bUni[aã]o Europeia\b": 2, r"\bPar[ií]s\b": 1, r"\bLondres\b": 2,
    r"\bFran[çc]a\b": 1, r"\bfranc[êe]s\w*\b": 1,
    r"\bAlemanha\b": 1, r"\balem[ãa]o\w*\b": 1,
    r"\bIt[aá]lia\b": 1, r"\bitalian[oa]s?\b": 1,
    r"\bEspanha\b": 1, r"\bespanhol\w*\b": 1,
    # Oriente Médio
    r"\bIr[aã]\b": 2, r"\biranian[oa]s?\b": 2, r"\bOrmuz\b": 2,
    r"\bIsrael\b": 1, r"\bisraelens\w*\b": 1,
    # Paraguai (só como marcador, muitas vezes aparece em crime tripla-fronteira)
    r"\bParaguai\b": 1, r"\bparaguai[oa]s?\b": 1,
}
_BR_POS = [(re.compile(p, re.IGNORECASE), w) for p, w in BR_POSITIVE.items()]
_BR_NEG = [(re.compile(p, re.IGNORECASE), w) for p, w in BR_NEGATIVE.items()]


def br_focus(text: str) -> dict:
    """Retorna {pos, neg, score}. score>0 => foco Brasil; score<0 => estrangeira."""
    pos = sum(w for rx, w in _BR_POS if rx.search(text))
    neg = sum(w for rx, w in _BR_NEG if rx.search(text))
    return {"pos": pos, "neg": neg, "score": pos - neg}


def score_item(item: dict) -> dict:
    """Retorna o item enriquecido com `_score`, `_severity` e `_br_focus`."""
    text_raw = " ".join([item.get("title",""), item.get("summary","")])
    text = text_raw.lower()
    score: dict[str, int] = {}
    hits:  dict[str, list[str]] = {}
    total = 0
    for cat, rules in _COMPILED.items():
        cat_score = 0
        cat_hits: list[str] = []
        for rx, w in rules:
            m = rx.search(text)
            if m:
                cat_score += w
                cat_hits.append(m.group(0))
        score[cat] = cat_score
        hits[cat] = cat_hits
        total += cat_score

    # foco BR (usa texto original pra respeitar case-sensitive quando útil)
    brf = br_focus(text_raw)

    # severidade: foco em hack/incidente/PLD-CFT.
    # Notícias puramente "crypto" ou puramente "PIX" (sem ataque nem lavagem
    # nem CFT) caem em noise e NÃO entram na seção "Imprensa corrobora".
    # pix/crypto entram só como CONTEXTO que escala a severidade.
    has_attack = score["attack"] >= 3
    has_laund  = score["launder"]>= 3
    has_cft    = score["cft"]    >= 3
    has_pix    = score["pix"]    >= 2
    has_crypto = score["crypto"] >= 2
    has_context = has_pix or has_crypto
    has_core    = has_attack or has_laund or has_cft

    sev = "noise"
    if not has_core:
        # sem ângulo de hack/lavagem/CFT -> não é o tipo de notícia que queremos
        sev = "noise"
    elif has_attack and has_laund:
        # hack + lavagem => criticidade máxima
        sev = "critical"
    elif (has_attack or has_laund or has_cft) and has_context:
        # hack/lavagem/CFT com menção a PIX ou cripto => high (exatamente o fit)
        sev = "high"
    elif has_attack or has_laund or has_cft:
        # hack/lavagem/CFT sem contexto cripto/PIX — ainda relevante, medium
        sev = "medium"

    # se a notícia tem foco estrangeiro (neg>>pos) e nenhum marcador BR forte,
    # rebaixa. Evita "Rússia", "Bitfinex", "Argentina", etc poluindo.
    if brf["score"] < -1 and brf["pos"] < 3:
        sev_order = ["noise","low","medium","high","critical"]
        idx = sev_order.index(sev)
        sev = sev_order[max(0, idx - 2)]   # rebaixa 2 níveis (high→low, medium→noise)

    item = dict(item)
    item["_score"] = score
    item["_hits"]  = {k: v for k, v in hits.items() if v}
    item["_total"] = total
    item["_severity"] = sev
    item["_br_focus"] = brf
    return item


def classify_all(items: Iterable[dict]) -> list[dict]:
    return [score_item(it) for it in items]


def top_relevant(items: Iterable[dict], min_sev: str = "medium",
                 min_br_score: int = 0) -> list[dict]:
    """
    Filtra por severidade mínima E por foco BR mínimo.
    min_br_score=0 => aceita neutro (sem negatividade estrangeira).
    min_br_score=1 => exige pelo menos 1 marcador BR positivo líquido.
    """
    order = {"noise": 0, "low": 1, "medium": 2, "high": 3, "critical": 4}
    thr = order.get(min_sev, 2)
    out = []
    for it in items:
        if order.get(it.get("_severity","noise"),0) < thr:
            continue
        brf = it.get("_br_focus", {"score": 0})
        if brf.get("score", 0) < min_br_score:
            continue
        out.append(it)
    out.sort(key=lambda x: (order[x["_severity"]], x.get("_br_focus",{}).get("score",0), x["_total"]), reverse=True)
    return out


if __name__ == "__main__":
    # smoke test
    sample = [
        {"title": "Polícia Federal deflagra operação contra lavagem via USDT",
         "summary": "O grupo usava corretoras para converter reais em stablecoins."},
        {"title": "BC anuncia nova taxa de juros",
         "summary": "Comitê manteve a Selic inalterada."},
        {"title": "Hackers invadem fintech ligada ao PIX e desviam R$ 420 milhões",
         "summary": "Valores foram convertidos em bitcoin e stablecoins segundo investigação."},
    ]
    for s in classify_all(sample):
        print(f"[{s['_severity']:<8s}] total={s['_total']:2d}  {s['title']}")
        print(f"   hits: {s['_hits']}")
