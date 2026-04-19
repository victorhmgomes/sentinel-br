"""
feeds.py — lista curada de RSS brasileiros relevantes para fraude/cripto/PIX.
Todos são públicos e sem autenticação.
"""
from __future__ import annotations

FEEDS = [
    # --- Economia / Mercado ---
    {"source": "G1 Economia",      "url": "https://g1.globo.com/rss/g1/economia/",                       "bucket": "economia"},
    {"source": "Folha Mercado",    "url": "https://feeds.folha.uol.com.br/mercado/rss091.xml",           "bucket": "economia"},
    {"source": "InfoMoney",        "url": "https://www.infomoney.com.br/feed/",                          "bucket": "economia"},
    {"source": "Valor Investe",    "url": "https://valorinveste.globo.com/rss/valorinveste/",            "bucket": "economia"},
    {"source": "MoneyTimes",       "url": "https://www.moneytimes.com.br/feed/",                         "bucket": "economia"},

    # --- Cripto BR ---
    {"source": "Portal do Bitcoin","url": "https://portaldobitcoin.uol.com.br/feed/",                    "bucket": "cripto"},
    {"source": "Livecoins",        "url": "https://livecoins.com.br/feed/",                              "bucket": "cripto"},
    {"source": "Criptofácil",      "url": "https://www.criptofacil.com/feed/",                           "bucket": "cripto"},
    {"source": "MoneyTimes Cripto","url": "https://www.moneytimes.com.br/tag/criptomoedas/feed/",        "bucket": "cripto"},

    # --- Tecnologia / Segurança ---
    {"source": "Olhar Digital",    "url": "https://olhardigital.com.br/feed/",                           "bucket": "seguranca"},
    {"source": "Tecmundo",         "url": "https://rss.tecmundo.com.br/feed",                            "bucket": "seguranca"},

    # --- Polícia / Operações ---
    {"source": "Agência Brasil Justiça", "url": "https://agenciabrasil.ebc.com.br/rss/justica/feed.xml", "bucket": "policia"},
    {"source": "G1 Política",            "url": "https://g1.globo.com/rss/g1/politica/",                 "bucket": "policia"},
]
