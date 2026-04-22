"""
classifier.py — scoring keyword-based de itens de imprensa.

Foco explícito: INCIDENTES CIBERNÉTICOS contra instituições financeiras
brasileiras (tipo Sinqia, C&M Software, Banco Rendimento, HSBC, BTG).
Notícias políticas, judiciais, regulatórias genéricas, fofoca corporativa
ou fraudes sem vínculo cyber são classificadas como `noise`.

Gating (tightened 2026-04-22):
  Severidade >= medium REQUER um sinal CYBER_HARD (hack/invasão/ransomware/
  malware/ciberataque/vazamento de dados/incidente cibernético/exploit/etc).
  Fraude/golpe/desvio puros SEM sinal cyber explícito NÃO qualificam.
  Exceção: fraude + nome de FI brasileira (Sinqia/Pismo/C&M/Rendimento...)
  pode escalar pra medium se o contexto for financeiro-cyber.

Cada item recebe score por categoria (cyber, attack, launder, cft, pix,
crypto, value, political_noise). A severidade final pondera tudo.
"""
from __future__ import annotations
import re
from typing import Iterable

# --- dicionários de termos ---
# peso do termo, sentence-insensitive

# CYBER_HARD: termos strictly cyber — gate obrigatório pra sev >= medium.
# Peso alto porque cada hit é sinal forte.
CYBER_HARD = {
    r"\bhack\w*\b": 5, r"\bhacker\w*\b": 5,
    r"\binvas[aãoõ]\w*\b": 5,          # invasão, invadir, invadiram
    r"\bransomware\b": 6, r"\bmalware\b": 5, r"\bphishing\b": 5,
    r"\bciberat\w+\b": 6, r"\bcibercrim\w+\b": 6, r"\bciberseguran\w+\b": 3,
    r"\bataque cibern\w+\b": 6, r"\bataque hacker\b": 6,
    r"\bataques? de hackers?\b": 5,
    r"\bexploit\w*\b": 5, r"\bzero-?day\b": 6, r"\bbackdoor\b": 5,
    r"\bbreach\b": 5, r"\bdata\s+breach\b": 6,
    r"\bincidente de seguran\w+\b": 5, r"\bincidente cibern\w+\b": 6,
    r"\bvazamento de dados\b": 5, r"\bvaz\w+ dados pessoais\b": 4,
    r"\bsequestro de dados\b": 6,
    r"\bddos\b": 4, r"\bnegação de serviço\b": 4,
    r"\bsistema comprometido\b": 4, r"\bsistemas comprometidos\b": 4,
    r"\bintrusão\w*\b": 4,
    r"\bapt\s*\d*\b": 3,                # APT groups
    r"\bmalicioso\w*\b": 2, r"\bsupply[- ]chain\b": 3,
}
# ATTACK: sinais de crime financeiro gerais (fraude, golpe, desvio) — SEM
# sinal cyber puro, NÃO qualificam. Servem pra escalar severidade quando
# coexistem com CYBER_HARD ou com um alvo financeiro nomeado.
ATTACK = {
    r"\bataque\b": 2, r"\bataques\b": 2,  # diluído — "ataque" genérico é fraco
    r"\bfraude\b": 3, r"\bfraudes\b": 3, r"\bfraudul\w+\b": 3,
    r"\bgolpe\b": 2, r"\bgolpes\b": 2,
    r"\bdesvi\w+\b": 3,
    r"\broubo\b": 2, r"\broubado\w*\b": 2, r"\bfurto\b": 2,
    r"\bextors\w+\b": 3,
    r"\bvazamento\b": 2, r"\bvazou\b": 2, r"\bvazaram\b": 2,
    # cyber-adjacent soft
    r"\bameaça\w*\b": 1, r"\balerta\s+ciber\w*\b": 2,
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

# NAMED_FI: instituições financeiras BR conhecidas ou providers de core banking
# que são alvos potenciais de incidente cyber. Se aparecer junto com ATTACK
# (fraude/golpe), vira "medium" mesmo sem CYBER_HARD (o caso "R$ 50M desviados
# do Banco Rendimento" que não diz 'hack' mas é claramente cyber).
NAMED_FI = {
    # Providers/core banking/processadores — foco principal
    r"\bSinqia\b": 6, r"\bPismo\b": 5, r"\bC&M Software\b": 6,
    r"\bDileta\b": 5, r"\bEvertec\b": 4, r"\bTSys\b": 3,
    # Bancos BR (alvos típicos de ataque)
    r"\bBanco Rendimento\b": 6,     # "rendimento" sozinho é genérico (juros) → não matcha
    r"\bHSBC\b": 5, r"\bBTG\b": 5, r"\bBTG Pactual\b": 5,
    r"\bBRB\b": 4, r"\bBanco de Brasília\b": 4,
    r"\bIta[uú]\b": 3, r"\bBradesco\b": 3, r"\bSantander\b": 3,
    r"\bBanco do Brasil\b": 3, r"\bCaixa\b": 3,
    r"\bBanco Master\b": 4, r"\bMaster\b": 2,
    r"\bNubank\b": 4, r"\bPicPay\b": 4, r"\bC6\b": 4,
    r"\bInter\b": 3, r"\bOriginal\b": 2, r"\bSafra\b": 3,
    r"\bMercado Pago\b": 4, r"\bStone\b": 3, r"\bPagBank\b": 3,
    r"\bPagSeguro\b": 3, r"\bCielo\b": 3, r"\bRede\b": 2,
    r"\bXP\b": 2, r"\bBanco do Nordeste\b": 3, r"\bBanrisul\b": 3,
    # Exchanges BR
    r"\bMercado Bitcoin\b": 4, r"\bFoxbit\b": 4,
    r"\bNovaDAX\b": 3, r"\bBitso\b": 3, r"\bBinance\s+Brasil\b": 3,
    # Fintechs de crédito
    r"\bBrasil Cash\b": 4, r"\bPefisa\b": 3, r"\bAsa\b": 2,
    # Instituições de pagamento/financeiras genéricas se ligadas a cyber
    r"\bgateway\s+PIX\b": 3, r"\bprovedor.*core\s+banking\b": 5,
    r"\bfintech.*cr[eé]dito\b": 3,
}

# POLITICAL_NOISE: penaliza notícias que são política/judicial/fofoca
# corporativa. Se o peso político é alto e o cyber_hard é zero,
# a notícia é rebaixada pra noise mesmo com launder/fraude hits.
POLITICAL_NOISE = {
    r"\bSTF\b": 3, r"\bSupremo Tribunal\b": 3,
    r"\bcomiss[ãa]o\s+(parlamentar|mista|especial)\b": 2,
    r"\baudi[êe]ncia\s+p[uú]blica\b": 2,
    r"\bCVM\s+est[aá]\b": 2,             # análise regulatória genérica
    r"\bprocesso\s+de\s+asilo\b": 3,
    r"\bex-presidente\s+do\b": 3,        # "ex-presidente do BRB" etc
    r"\bpris[ãa]o\s+de\s+ex\b": 3,
    r"\bpris[ãa]o\s+preventiva\b": 2,
    r"\bPapuda\b": 4,                    # presídio = política
    r"\bempresas\s+de\s+fachada\b": 3,
    r"\bconex[õo]es\s+pol[ií]ticas\b": 4,
    r"\bescritorial?\b|\bgabinete\s+\b": 2,
    r"\bdelegad[oa]\b": 2, r"\bRamagem\b": 4,
    r"\bVorcaro\b": 3,                   # caso BRB/Master: fofoca corp
    r"\bCOPOM\b": 2, r"\bSelic\b": 2,
    r"\bBNDES\b": 2,
    r"\bimpeachment\b": 4,
    r"\bCongresso\s+Nacional\b": 2,
    r"\b(PT|PL|PSDB|PSL|MDB|Novo|PSOL|Republicanos)\b": 2,  # siglas partidárias
    r"\baudi[êe]ncia\s+de\s+cust[oó]dia\b": 3,
    r"\bemenda\s+constitucional\b": 3,
    r"\brelator[íi]a\b|\bvoto\s+do\s+relator\b": 2,
    r"\boperação\s+(lava\s+jato|zelot[eo]s|greenfield)\b": 2,  # old ops
    r"\bfoto\s+do\b": 3,                 # "Veja foto" = gossip
    r"\bdiz\s+[Aa]\s+PF\b": 2,           # discourse fluff
}

# COMMENTARY_NOISE: notícia de opinião/análise/meta-discussão sobre
# cyber, não incidente concreto. "Santander defende criação de fórum pra
# analisar ataques" tem hacker×2 no texto mas é CISO dando entrevista, não
# é um ataque ao Santander. Sem isso, análises e op-eds vazam pra high.
COMMENTARY_NOISE = {
    r"\bdefende\s+(a\s+)?cria[çc][ãa]o\b": 4,
    r"\bdefende\s+(o\s+)?uso\b": 3,
    r"\bprop[õo]e\s+(a\s+)?cria[çc][ãa]o\b": 4,
    r"\bf[óo]rum\s+setorial\b": 4,
    r"\bf[óo]rum\s+para\s+analisar\b": 4,
    r"\bmesa\s+redonda\b": 3,
    r"\bpainel\s+sobre\b": 2,
    r"\bdebate\s+sobre\b": 2,
    r"\bseminários?\s+sobre\b": 2,
    r"\bconfer[êe]ncia\s+sobre\b": 2,
    r"\baumentar\s+a\s+coopera[çc][ãa]o\b": 3,
    r"\bcoopera[çc][ãa]o\s+setorial\b": 3,
    r"\bCISO\b": 3,
    r"\bchefe\s+de\s+seguran[çc]a\s+da\s+informa[çc][ãa]o\b": 3,
    r"\bdiretor\s+de\s+seguran[çc]a\b": 2,
    r"\baumentar\s+controles\b": 2,
    r"\beleva\s+n[ií]veis?\s+m[ií]nimos?\b": 2,
    r"\bentrevista\s+exclusiva\b": 2,
    r"\b(diz|afirma)\s+(que\s+)?o\b": 1,
    r"\bopina[çc][ãa]o\b": 2,
    r"\ban[áa]lise\s+(do\s+|da\s+)?mercado\b": 2,
    r"\bpara\s+o\s+executivo\b": 2,
    r"\bsegundo\s+o\s+(executivo|CISO|diretor|especialista)\b": 3,
}

# INCIDENT_VERBS: verbos/expressões que indicam INCIDENTE CONCRETO (não
# comentário/análise). Presença de pelo menos um é requisito forte pra
# severidade ≥ medium em tópicos de cyber. "Banco Rendimento SOFREU ataque"
# qualifica; "Santander DEFENDE fórum para analisar ataques" não.
INCIDENT_VERBS = {
    r"\bsofr\w+\s+(um\s+|uma\s+)?ataque\b": 4,
    r"\bsofr\w+\s+(um\s+|uma\s+)?invas[ãa]o\b": 4,
    r"\bsofr\w+\s+(um\s+|uma\s+)?incidente\b": 3,
    r"\b[eé]\s+alvo\s+de\b": 4,
    r"\bfoi\s+alvo\b": 4,
    r"\bser\s+alvo\s+de\b": 3,
    r"\bteve\s+(seus?\s+)?(sistemas?|rede|dados?)\b": 3,
    r"\batingiu\s+(o\s+|a\s+|os\s+|as\s+)?sistemas?\b": 3,
    r"\bparalisou\s+(as\s+)?opera[çc][õo]es\b": 4,
    r"\bdeixou\s+fora\s+do\s+ar\b": 3,
    r"\binterrom\w+\s+(servi[çc]os?|opera[çc][õo]es)\b": 3,
    r"\bfoi\s+(hackead|invadid|atacad)\w+\b": 4,
    r"\bhouve\s+um\s+ataque\b": 3,
    r"\bataque\s+a[o]?\s+\w+\s+(bank|banco|sistema|plataforma)\b": 3,
    r"\balerta\s+clientes\b": 3,          # "Banco X alerta clientes sobre ataque"
    r"\bconfirma\s+(o\s+|a\s+)?(ataque|incidente|invas[ãa]o)\b": 4,
    r"\bvazamento\s+(de\s+dados|confirmado)\b": 4,
    r"\bsequestro\s+de\s+dados\b": 4,
    r"\broubo\s+de\s+(dados|R\$|US\$|fundos|cripto)\b": 3,
    r"\bR\$\s*\d+[\d.,]*\s*(milh|milhões|bilh)": 2,   # valor concreto da perda
}

CATS: dict[str, dict] = {
    "cyber":   CYBER_HARD,
    "attack":  ATTACK,
    "launder": LAUNDER,
    "cft":     CFT,
    "pix":     PIX,
    "crypto":  CRYPTO,
    "value":   VALUE,
    "named_fi":        NAMED_FI,
    "political_noise": POLITICAL_NOISE,
    "commentary":      COMMENTARY_NOISE,
    "incident_verbs":  INCIDENT_VERBS,
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
    # NAMED_FI propaga pra BR-focus: mencionar uma FI brasileira nomeada
    # (Banco Rendimento, Sinqia, Pismo, C&M, MB, Foxbit, BTG, etc.) já é
    # foco BR por definição, mesmo que o texto não diga "Brasil" ou "R$".
    # Sem isso, matérias como "Banco Rendimento sofre ataque hacker" cujas
    # manchetes não mencionam Brasil explicitamente caíam no filtro BR.
    if score["named_fi"] >= 4:
        bump = max(2, score["named_fi"] // 3)
        brf = {"pos": brf["pos"] + bump, "neg": brf["neg"],
               "score": (brf["pos"] + bump) - brf["neg"],
               "named_fi_bump": bump}

    # --- Gating tightened (2026-04-22) ---
    # Queremos SÓ notícias de incidente cibernético contra FI brasileira
    # (tipo Sinqia/C&M Software, Banco Rendimento/BTG/HSBC, Mercado Bitcoin).
    # Fraude genérica sem cyber e lavagem sem ataque ficam em noise.
    # Cyber term sozinho (ex.: "invasão") sem contexto financeiro/digital
    # também NÃO qualifica — "invasão de pneus" não é cyber.
    cyber_score    = score["cyber"]
    strong_cyber   = cyber_score >= 6    # ciberataque/ransomware/incidente cibern (auto)
    weak_cyber     = cyber_score >= 4    # hack/invasão solo — precisa contexto
    has_attack     = score["attack"]     >= 3
    has_laund      = score["launder"]    >= 4
    has_laund_soft = score["launder"]    >= 2
    has_cft        = score["cft"]        >= 4
    has_pix        = score["pix"]        >= 2
    has_crypto     = score["crypto"]     >= 2
    has_named_fi   = score["named_fi"]   >= 4
    pol_noise      = score["political_noise"]
    commentary     = score.get("commentary", 0)
    incident_verb_score = score.get("incident_verbs", 0)
    has_incident   = incident_verb_score >= 3   # pelo menos 1 verbo forte

    has_context    = has_pix or has_crypto

    # Cyber efetivo: strong passa sozinho; weak exige contexto (FI nomeada,
    # cripto/PIX, ou lavagem). Caso contrário "invasão de pneus", "ataque
    # ao governo", "hackers políticos" vazam pra noise.
    has_cyber = strong_cyber or (
        weak_cyber and (has_named_fi or has_context or has_laund_soft)
    )

    # "core": gate principal pra severidade >= medium.
    has_core = (
        has_cyber
        or (has_attack and has_named_fi and (has_context or has_laund_soft))
        or has_cft
    )

    sev = "noise"
    if not has_core:
        sev = "noise"
    elif has_cyber and has_laund:
        sev = "critical"                          # cyber + lavagem = critical
    elif has_cyber and (has_context or has_named_fi):
        sev = "high"                              # cyber + cripto/PIX/FI = high
    elif has_cyber:
        sev = "medium"                            # cyber sozinho = medium
    elif has_attack and has_named_fi:
        # fraude/desvio + FI brasileira nomeada = medium mesmo sem hack explícito
        # (caso "R$ 50M desviados do Banco Rendimento" sem palavra 'hack')
        sev = "high" if has_context else "medium"
    elif has_laund or has_cft:
        sev = "medium"                            # lavagem/CFT fortes sem cyber

    # Penalidade política: se a notícia tem muito peso político/judicial e
    # ZERO cyber explícito, rebaixa. Pega "STF envia investigações",
    # "prisão de ex-presidente", "Vorcaro", "Ramagem", etc.
    if pol_noise >= 4 and not has_cyber:
        sev_order = ["noise","low","medium","high","critical"]
        idx = sev_order.index(sev)
        sev = sev_order[max(0, idx - 2)]
    elif pol_noise >= 3 and not has_cyber:
        sev_order = ["noise","low","medium","high","critical"]
        idx = sev_order.index(sev)
        sev = sev_order[max(0, idx - 1)]

    # Penalidade COMENTÁRIO/OPINIÃO: notícia em que o peso vem de
    # análise/opinião ao invés de fato concreto. "Santander defende criação
    # de fórum para analisar ataques" tem cyber=13 mas é entrevista.
    # Se commentary é alto E não há verbo de incidente, rebaixa 2 níveis.
    if commentary >= 5 and not has_incident:
        sev_order = ["noise","low","medium","high","critical"]
        idx = sev_order.index(sev)
        sev = sev_order[max(0, idx - 2)]
    elif commentary >= 3 and not has_incident:
        sev_order = ["noise","low","medium","high","critical"]
        idx = sev_order.index(sev)
        sev = sev_order[max(0, idx - 1)]

    # Requisito de verbo de incidente: pra high/critical, o texto precisa
    # conter pelo menos um verbo de incidente concreto ("sofreu ataque",
    # "foi alvo de", "paralisou operações"). Caso contrário, cap em medium.
    # Exceção: launder+cyber (rotina de PLD/CFT frequentemente não tem
    # verbo de incidente) ou CFT puro.
    if not has_incident and not has_laund and not has_cft:
        sev_order = ["noise","low","medium","high","critical"]
        if sev_order.index(sev) > sev_order.index("medium"):
            sev = "medium"

    # se a notícia tem foco estrangeiro (neg>>pos) e nenhum marcador BR forte,
    # rebaixa. Evita "Rússia", "Bitfinex", "Argentina", etc poluindo.
    if brf["score"] < -1 and brf["pos"] < 3:
        sev_order = ["noise","low","medium","high","critical"]
        idx = sev_order.index(sev)
        sev = sev_order[max(0, idx - 2)]

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
    # smoke test — deve filtrar ruído político e manter cyber-real
    sample = [
        # POSITIVOS — devem entrar como medium/high/critical
        {"title": "Hackers invadem fintech ligada ao PIX e desviam R$ 420 milhões",
         "summary": "Valores foram convertidos em bitcoin e stablecoins."},
        {"title": "Banco Rendimento sofre ciberataque e perde R$ 50 milhões",
         "summary": "Invasão atingiu sistemas de câmbio na madrugada."},
        {"title": "Ataque hacker atinge provedor Sinqia e afeta 3 bancos brasileiros",
         "summary": "Incidente de segurança causou indisponibilidade no PIX."},
        {"title": "Ransomware atinge Mercado Bitcoin e paralisa saques de USDT",
         "summary": "Exchange confirma vazamento de dados e sequestro de sistemas."},
        # NOISE (política/judicial/regulatório/fofoca) — devem cair pra noise
        {"title": "STF envia investigações de operações Rejeito e Intrafortis para MG",
         "summary": "Ministro Moraes redistribuiu processos ao juízo federal."},
        {"title": "Prisão de ex-presidente do BRB levará a conexões políticas",
         "summary": "Paulo Henrique Costa está na Papuda. Vorcaro comentou."},
        {"title": "CVM está protegendo o investidor? Dino amplia audiência pública",
         "summary": "Relatoria aguarda manifestação dos reguladores."},
        {"title": "Como policiais recuperam conversas do celular e da nuvem",
         "summary": "Técnicas de extração forense em investigações."},
        {"title": "BC anuncia nova taxa de juros",
         "summary": "COPOM manteve a Selic inalterada."},
        # BORDERLINE — fraude sem cyber, sem FI nomeada = noise
        {"title": "Polícia Federal deflagra operação contra lavagem via USDT",
         "summary": "Grupo convertia reais em stablecoins."},
    ]
    print(f"{'sev':<10s} {'pol':>3s} {'cyb':>3s} {'att':>3s} {'fi':>3s}  title")
    for s in classify_all(sample):
        sc = s["_score"]
        print(f"[{s['_severity']:<8s}] "
              f"{sc.get('political_noise',0):>3d} "
              f"{sc.get('cyber',0):>3d} "
              f"{sc.get('attack',0):>3d} "
              f"{sc.get('named_fi',0):>3d}  "
              f"{s['title'][:80]}")
