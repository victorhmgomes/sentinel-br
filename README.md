# Sentinel BR

Plataforma de inteligência de fraude para exchanges brasileiras de criptoativos, 100% baseada em dados públicos. Monitora 16 exchanges (4 BR + 12 globais) e aciona alertas quando o comportamento de preço, volume ou orderbook foge do padrão.

> No caso **Sinqia / HSBC (R$ 420–710M, 29/08/2025)**, um sinal forte foi disparado **7 dias antes** da divulgação pública do ataque. Dos 15 incidentes bancários dos últimos 12 meses, **14 deixaram rastro detectável** em exchanges de cripto.

## O que você encontra aqui

- **Coletores** (`scripts/fetch_*.py`) — puxam OHLCV diário de Binance, Mercado Bitcoin, Foxbit, NovaDAX, Bitso BR, BitPreço, Ripio Trade, CoinGecko, TRON on-chain e funding perpétuo.
- **Livro consolidado em tempo real** (`scripts/fetch_orderbook.py`) — snapshot L2 de 4 pares (BTC/ETH/USDT/SOL) em BRL através dos 6 venues BR + Binance global, com conversão cambial normalizada.
- **Detector de anomalias** (`scripts/detect_anomalies.py`) — Z-score sobre volume diário, divergence multi-venue, ring-of-mules (concordância de alertas em ≥2 venues BR no mesmo dia), stablecoin flight, pre-attack spike.
- **Watcher 30s** (`scripts/watcher/`) — 6 detectores Binance-centric rodando em loop com sinks em stdout colorido + JSONL.
- **Correlação com imprensa** (`scripts/news/`) — classificador por keywords focado em hacking, incidentes cibernéticos e PLD/CFT. Cruza alertas com manchetes dos veículos brasileiros.
- **Backtest** (`scripts/backtest/` + `scripts/incident_backtest.py`) — perturbador de snapshots (6 cenários), harness FP/FN com sweep de threshold, validação contra 15 incidentes anonimizados.
- **Dashboard** (`dashboard/index.html` + `index.html` no root) — HTML standalone com dados inline, modo leigo/expert, glossário, pronto para publicação estática.

## Como reproduzir

```bash
# 1. coleta (roda ~2 min; só precisa internet)
python3 scripts/fetch_data.py
python3 scripts/fetch_orderbook.py
python3 scripts/fetch_coingecko.py
python3 scripts/fetch_funding.py
python3 scripts/fetch_tron.py
python3 scripts/news/fetcher.py

# 2. análise
python3 scripts/detect_anomalies.py
python3 scripts/incident_backtest.py
python3 scripts/news/correlator.py
python3 scripts/backtest/run_backtest.py

# 3. build do dashboard
python3 scripts/build_dashboard.py
# abre dashboard/index.built.html no navegador
```

## Estrutura

```
sentinel-br/
├── index.html              # dashboard publicável (cópia do build)
├── dashboard/
│   ├── index.html          # template-fonte do dashboard
│   └── index.built.html    # build com dados embutidos
├── scripts/
│   ├── fetch_*.py          # coletores de dados públicos
│   ├── detect_anomalies.py # Z-score + ring-of-mules
│   ├── incident_backtest.py
│   ├── build_dashboard.py
│   ├── backtest/           # harness FP/FN
│   ├── news/               # RSS + classifier + correlator
│   └── watcher/            # loop 30s Binance-centric
└── data/
    ├── raw.json            # 1 ano OHLCV diário, todas as venues
    ├── orderbook.json      # snapshot L2 consolidado
    ├── dashboard.json      # saída agregada do build
    ├── news.json           # últimas manchetes classificadas
    └── ...
```

## Deploy (GitHub Pages + cron 15min)

O repositório publica o dashboard em GitHub Pages e atualiza os dados sozinho via GitHub Actions.

- **`.github/workflows/refresh.yml`** — roda a cada 15 min: orderbook + funding + watcher single-shot + rebuild. Faz commit de volta no `main`.
- **`.github/workflows/refresh-daily.yml`** — 03:00 UTC: pipeline completo (OHLCV 12 meses, CoinGecko, TRON, imprensa, detectores, incident backtest, correlator).

Para ativar:
1. **Settings → Pages**: Source = *Deploy from a branch*, Branch = `main`, Folder = `/` (root). URL fica em `https://<usuario>.github.io/sentinel-br/`.
2. **Settings → Actions → General**: em *Workflow permissions*, marcar *Read and write permissions* (necessário para o bot commitar o rebuild).
3. (Opcional) **Settings → Secrets and variables → Actions** — para alertas no Telegram:
   - `TELEGRAM_BOT_TOKEN` — token do bot (criado via [@BotFather](https://t.me/BotFather))
   - `TELEGRAM_CHAT_ID` — id do chat (use [@userinfobot](https://t.me/userinfobot) ou a API getUpdates)

Sem os secrets, o watcher segue rodando normalmente e persistindo alertas em `data/alerts/*.jsonl`; só o envio pro Telegram é pulado.

## Fontes públicas usadas

Binance Data Portal · Mercado Bitcoin v4 · Foxbit v3 · NovaDAX v2 · Bitso BR · BitPreço · Ripio Trade · CoinGecko · mempool.space · TRON TronGrid · RSS dos principais veículos econômicos brasileiros.

## Licença

Código sob MIT. Dados coletados são públicos e pertencem às respectivas exchanges/APIs.

## Autor

Victor Gomes — projeto desenhado como resposta ao ataque Sinqia/HSBC de 29/08/2025.
