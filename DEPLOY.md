# Deploy Sentinel BR — passo a passo

Sequência para colocar o dashboard no ar e ativar alertas do Telegram. Tempo total: ~10 min.

---

## 1 · Subir código para o GitHub

### 1a. Apagar a pasta `.git` local

O Claude tentou init o repo mas o filesystem montado bloqueou. No Windows, abra o PowerShell (ou o terminal do VS Code) na pasta `Fraude\sentinel-br`:

```powershell
cd "C:\Users\victo\OneDrive\Documents\Fraude\sentinel-br"
if (Test-Path .git) { Remove-Item -Recurse -Force .git }
```

### 1b. Init, commit e push

O repo `victorhmgomes/sentinel-br` já existe no GitHub com a versão antiga. Vamos sobrescrever com o código novo (Telegram + cron + toggle EN/PT):

```powershell
cd "C:\Users\victo\OneDrive\Documents\Fraude\sentinel-br"
git init -b main
git config user.email "victorhmgomes@gmail.com"
git config user.name  "Victor Gomes"
git add .
git commit -m "Deploy pipeline + Telegram alerts + EN/PT toggle"
git remote add origin https://github.com/victorhmgomes/sentinel-br.git
git push -u origin main --force
```

> ⚠️ O `--force` é necessário porque o repo remoto tem 1 commit antigo (do upload via Web UI de mais cedo) com histórico diferente. **Nenhum dado importante será perdido** — aquele commit só tinha a versão velha do dashboard sem o toggle.

Se o `git push` pedir autenticação: use um **Personal Access Token** (Settings → Developer settings → PATs → *Fine-grained*, só `Contents: Read/Write` no repo sentinel-br).

---

## 2 · Tornar o repo público

Para GitHub Pages sem custo e Actions ilimitado:

1. `https://github.com/victorhmgomes/sentinel-br` → **Settings** → **General**
2. Rolar até **Danger Zone** → **Change repository visibility** → **Make public**
3. Confirmar digitando `victorhmgomes/sentinel-br`

---

## 3 · Ativar GitHub Pages

1. **Settings** → **Pages** (menu lateral)
2. **Source**: `Deploy from a branch`
3. **Branch**: `main` / `/ (root)` → **Save**
4. Aguarde ~1 min e a URL aparece no topo: `https://victorhmgomes.github.io/sentinel-br/`

---

## 4 · Liberar write permission para Actions

Precisa disso pro bot commitar o rebuild a cada 15 min:

1. **Settings** → **Actions** → **General**
2. Rolar até **Workflow permissions**
3. Marcar **Read and write permissions**
4. **Save**

---

## 5 · Criar o bot do Telegram

1. No Telegram, abra [@BotFather](https://t.me/BotFather) e envie `/newbot`
2. Dê um nome (ex: `Sentinel BR Alerts`) e um username terminando em `bot` (ex: `sentinelbr_alerts_bot`)
3. O BotFather responde com um **token** no formato `123456789:AAEhBP0av...` — **copie e guarde**

### 5a. Pegar seu chat_id

1. No Telegram, abra [@userinfobot](https://t.me/userinfobot) e envie `/start`
2. Ele responde com seu **ID** (algo como `987654321`) — **copie e guarde**
3. Abra o bot que você acabou de criar e envie qualquer mensagem (ex: `/start`). Isso "autoriza" ele a te escrever.

### 5b. Para alertas em grupo (opcional)

Se quiser receber em um grupo em vez de privado:

1. Crie/abra um grupo, adicione o bot como membro
2. Mande qualquer mensagem no grupo
3. Abra no navegador: `https://api.telegram.org/bot<SEU_TOKEN>/getUpdates`
4. Procure `"chat":{"id":-100123456789, ...}` — esse número negativo é o chat_id do grupo

---

## 6 · Configurar Secrets no GitHub

1. **Settings** → **Secrets and variables** → **Actions** → **New repository secret**
2. Adicione dois secrets:
   - Nome: `TELEGRAM_BOT_TOKEN` · Valor: `123456789:AAEhBP0av...` (token do passo 5)
   - Nome: `TELEGRAM_CHAT_ID` · Valor: `987654321` (id do passo 5a ou 5b)

---

## 7 · Disparar o primeiro run

Não precisa esperar 15 min:

1. **Actions** (tab no topo do repo) → **refresh-live** → **Run workflow** → **Run workflow**
2. Aguarde ~1 min. Se a primeira execução funcionar, você recebe **a primeira mensagem no Telegram** (só se houver alerta high/critical no snapshot atual) e o dashboard é republicado.

Para testar o Telegram sem depender de alerta high, troque o `--telegram-min-severity high` para `medium` (ou até `info`) em `.github/workflows/refresh.yml` temporariamente.

---

## Troubleshooting rápido

| Sintoma | Causa provável | Solução |
|---|---|---|
| `git push` pede senha e rejeita | Sem PAT configurado | Gere um token fine-grained (repo-only) e use como senha |
| Actions falha "refusing to allow..." | Workflow permissions em Read-only | Passo 4 |
| Pages 404 depois de habilitar | Primeiro build ainda rodando | Aguarde 2-3 min; cheque Actions → *pages build and deployment* |
| Mensagem não chega no Telegram | Secret errado ou bot sem "start" | Cheque `TELEGRAM_BOT_TOKEN` exato (sem espaços) e abra o bot uma vez para mandar `/start` |
| Commit do bot falha em loop | `.nojekyll` ausente + Jekyll remove arquivo | Confirme que `.nojekyll` foi commitado (git ls-files \| grep nojekyll) |
