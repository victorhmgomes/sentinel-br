// scripts/test_dashboard.mjs — smoke test do dashboard buildado (jsdom).
//
// Carrega index.html num DOM headless, executa o JS inline com Chart.js
// stubado (sem rede, sem canvas) e falha o processo se:
//   - qualquer renderer lançou exceção (window.__sentinelErrors ou 'error' event)
//   - I18N não inicializou ou o idioma default não é 'en'
//   - painéis-chave ficaram vazios (indicators, semáforo, exec24h, incidents)
//   - a manchete não foi pintada no formato esperado
//   - o toggle PT→EN→PT gera erro
//
// Uso:  node scripts/test_dashboard.mjs [caminho/para/index.html]
// Exit: 0 = ok · 1 = falhou (mensagens em stderr)
//
// Motivação: em 2026-06-09 um `.filter` chamado num objeto (não array) dentro
// de um renderer parou silenciosamente a execução do script; metade dos
// painéis ficou vazia e o i18n não aplicou. Este teste roda no CI antes do
// commit, então um erro desse tipo bloqueia o push em vez de ir pro Pages.

import { JSDOM, VirtualConsole } from 'jsdom';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const file = resolve(process.argv[2] || 'index.html');
const html = readFileSync(file, 'utf8');

const failures = [];
const fail = (msg) => failures.push(msg);
const ok = (cond, msg) => { if (!cond) fail(msg); };

// ---- console virtual: captura erros, silencia ruído de canvas ----
const vc = new VirtualConsole();
const consoleErrors = [];
vc.on('error', (...a) => consoleErrors.push(a.map(String).join(' ')));
vc.on('jsdomError', (e) => {
  const m = String(e && e.message || e);
  if (/getContext|canvas/i.test(m)) return; // esperado sem pacote canvas
  consoleErrors.push('jsdomError: ' + m);
});

// ---- Chart.js stub: o CDN não é carregado (resources não 'usable') ----
function chartStub(window) {
  const inst = () => ({
    destroy() {}, update() {}, resize() {}, render() {},
    data: { labels: [], datasets: [] }, options: {}, canvas: null, ctx: null,
  });
  const Chart = function () { return inst(); };
  Chart.register = () => {};
  Chart.defaults = { color: '', borderColor: '', font: { family: '' }, plugins: {} };
  Chart.helpers = {};
  window.Chart = Chart;
}

const dom = new JSDOM(html, {
  runScripts: 'dangerously',
  pretendToBeVisual: true,
  virtualConsole: vc,
  beforeParse(window) {
    chartStub(window);
    window.__uncaught = [];
    window.addEventListener('error', (e) => {
      window.__uncaught.push((e && e.message) || String(e));
    });
    // requestAnimationFrame síncrono pra qualquer render diferido
    window.requestAnimationFrame = (cb) => { cb(0); return 0; };
  },
});

const { window } = dom;
const { document } = window;

// jsdom executa <script> inline síncrono no parse; damos um tick pra
// eventuais setTimeout(…, 0) de renderers.
await new Promise((r) => setTimeout(r, 50));

// ---- 1. nenhum erro de renderer ----
const rendererErrors = window.__sentinelErrors || [];
for (const e of rendererErrors) fail(`renderer "${e.name}" lançou: ${e.message}`);
for (const m of window.__uncaught) fail(`erro uncaught: ${m}`);
ok(Array.isArray(window.__sentinelErrors), 'window.__sentinelErrors não existe — helper safe() ausente?');

// ---- 2. i18n ----
ok(typeof window.I18N === 'object', 'I18N não definido');
ok(window.I18N && window.I18N.lang === 'en', `idioma default deveria ser 'en', é '${window.I18N && window.I18N.lang}'`);
const indTitle = document.querySelector('[data-i18n="ind.title"]');
ok(indTitle && /methodology/i.test(indTitle.textContent), `ind.title não traduzido: "${indTitle && indTitle.textContent}"`);

// ---- 3. painéis-chave populados ----
const DATA = JSON.parse(document.getElementById('DATA').textContent);
const nInd = document.querySelectorAll('#indicators .indicator').length;
const nIndExpected = (DATA.indicators_doc || []).length;
ok(nInd === nIndExpected && nInd > 0, `#indicators: ${nInd} cards (esperado ${nIndExpected})`);

ok(document.querySelectorAll('#semaforo .sem-card').length === 3, '#semaforo não tem 3 cards');
ok(document.querySelectorAll('#exec24h-bullets li').length >= 1, '#exec24h-bullets vazio');
ok(!/^[—\-\s]*$/.test(document.getElementById('exec24h-status').textContent), '#exec24h-status não pintado');
ok(document.querySelectorAll('#inc-summary .kpi').length >= 3, '#inc-summary sem KPIs');
ok(document.querySelectorAll('#tbl-incidents tbody tr').length === (DATA.incidents?.items || []).length,
   '#tbl-incidents: linhas != incidentes');
ok(/^\d+$/.test(document.getElementById('kpi-total').textContent.trim()), '#kpi-total não numérico');
const nBub = document.querySelectorAll('#incident-map .bub').length;
ok(nBub === (DATA.incidents?.items || []).length && nBub > 0, `#incident-map: ${nBub} bolhas (esperado ${(DATA.incidents?.items||[]).length})`);
ok(document.querySelectorAll('#imap-legend span').length === 4, '#imap-legend não pintada');

// ---- 4. manchete ----
const lead = document.getElementById('lead-hits');
ok(lead && /^\d+ of \d+$/.test(lead.textContent.trim()), `#lead-hits em EN deveria ser "N of M", é "${lead && lead.textContent}"`);
const headline = document.querySelector('[data-i18n-html="lead.headline"]');
ok(headline && /bank attacks/.test(headline.textContent), 'manchete não está em EN');

// ---- 5. toggle de idioma não pode gerar erro ----
const before = rendererErrors.length + window.__uncaught.length;
try {
  window.I18N.setLang('pt');
  // apply() re-escreve o innerHTML da manchete → #lead-hits é um nó novo; re-consultar
  const leadPt = document.getElementById('lead-hits');
  ok(leadPt && /^\d+ de \d+$/.test(leadPt.textContent.trim()),
     `#lead-hits em PT deveria ser "N de M", é "${leadPt && leadPt.textContent}"`);
  ok(document.querySelectorAll('#indicators .indicator').length === nIndExpected, 'indicators sumiram após toggle PT');
  const indPt = document.querySelector('[data-i18n="ind.title"]');
  ok(indPt && /metodologia/i.test(indPt.textContent), 'ind.title não voltou pra PT');
  window.I18N.setLang('en');
  const leadEn = document.getElementById('lead-hits');
  ok(leadEn && /^\d+ of \d+$/.test(leadEn.textContent.trim()), 'volta pra EN não repintou #lead-hits');
} catch (e) {
  fail('setLang lançou: ' + e.message);
}
const after = (window.__sentinelErrors || []).length + window.__uncaught.length;
ok(after === before, `toggle de idioma gerou ${after - before} erro(s)`);

// ---- 6. console.error de renderers (safe() loga lá) ----
const sentinelConsole = consoleErrors.filter((m) => m.includes('[sentinel]'));
for (const m of sentinelConsole) fail('console: ' + m.slice(0, 200));

// ---- resultado ----
if (failures.length) {
  console.error(`\n✗ smoke test FALHOU (${failures.length}):`);
  for (const f of failures) console.error('  - ' + f);
  process.exit(1);
}
console.log(`✓ smoke test OK — ${nInd} indicators · lang=${window.I18N.lang} · 0 erros de renderer`);
process.exit(0);
