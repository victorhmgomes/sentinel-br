// scripts/test_dashboard.mjs — smoke test do dashboard buildado (jsdom).
//
// Carrega index.html num DOM headless, executa o JS inline e falha se:
//   - qualquer renderer lançou exceção (window.__sentinelErrors / 'error')
//   - I18N não inicializou ou o idioma default não é 'en'
//   - painéis-chave ficaram vazios (mapa, agora, evidência, sinais, volume, detectores)
//   - o toggle EN→PT→EN gera erro ou não traduz
//   - o modo tech não expõe o método
//
// Uso:  node scripts/test_dashboard.mjs [caminho/para/index.html]
// Exit: 0 = ok · 1 = falhou (mensagens em stderr)

import { JSDOM, VirtualConsole } from 'jsdom';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const file = resolve(process.argv[2] || 'index.html');
const html = readFileSync(file, 'utf8');
const failures = [];
const fail = (m) => failures.push(m);
const ok = (c, m) => { if (!c) fail(m); };

const vc = new VirtualConsole();
const consoleErrors = [];
vc.on('error', (...a) => consoleErrors.push(a.map(String).join(' ')));
vc.on('jsdomError', (e) => { const m = String(e && e.message || e); if (/getContext|canvas|Could not load/i.test(m)) return; consoleErrors.push('jsdomError: ' + m); });

const dom = new JSDOM(html, {
  runScripts: 'dangerously', pretendToBeVisual: true, virtualConsole: vc,
  beforeParse(window) {
    window.__uncaught = [];
    window.addEventListener('error', (e) => window.__uncaught.push((e && e.message) || String(e)));
    window.requestAnimationFrame = (cb) => { cb(0); return 0; };
  },
});
const { window } = dom; const { document } = window;
await new Promise((r) => setTimeout(r, 80));

// 1. erros
for (const e of (window.__sentinelErrors || [])) fail(`renderer "${e.name}" lançou: ${e.message}`);
for (const m of window.__uncaught) fail(`erro uncaught: ${m}`);
ok(Array.isArray(window.__sentinelErrors), 'helper safe() ausente');

// 2. i18n
ok(typeof window.I18N === 'object', 'I18N não definido');
ok(window.I18N && window.I18N.lang === 'en', `idioma default deveria ser 'en', é '${window.I18N && window.I18N.lang}'`);
const h1 = document.querySelector('.cover h1');
ok(h1 && /crypto exchanges/i.test(h1.textContent), `h1 não está em EN: "${h1 && h1.textContent}"`);

// 3. dados + painéis
const DATA = JSON.parse(document.getElementById('DATA').textContent);
const items = (DATA.incidents && DATA.incidents.items) || [];
const W = DATA.window || {};
const inWin = items.filter(i => i.in_window != null ? i.in_window !== false : (!W.from || (i.date >= W.from && i.date <= W.to))).length;
const nBub = document.querySelectorAll('#imap .bub').length;
ok(nBub === inWin && nBub > 0, `#imap: ${nBub} bolhas (esperado ${inWin})`);
ok(/^\d+\/ \d+$/.test(document.getElementById('big-n').textContent.trim()), `#big-n não pintado: "${document.getElementById('big-n').textContent}"`);
ok(document.querySelectorAll('#keys > div').length === 4, '#keys não tem 4 números');
ok(document.querySelectorAll('#status .st').length === 3, '#status não tem 3 tiles');
ok(document.querySelectorAll('#score tbody tr').length === 4, '#score não tem 4 linhas');
ok(document.querySelectorAll('#inc-table tbody tr').length === items.length, `#inc-table: linhas != ${items.length}`);
ok(document.getElementById('ring').children.length > 0, '#ring vazio');
ok(document.querySelectorAll('#ring-table tbody tr').length >= 1, '#ring-table sem linhas');
ok(document.getElementById('hc').children.length > 0, '#hc vazio');
ok(document.querySelectorAll('#hc-table tbody tr').length >= 1, '#hc-table sem linhas');
ok(document.getElementById('vol').children.length > 50, '#vol sem barras');
const nDet = document.querySelectorAll('#det .d').length;
ok(nDet === (DATA.indicators_doc || []).length && nDet > 0, `#det: ${nDet} cards (esperado ${(DATA.indicators_doc||[]).length})`);
ok(document.getElementById('hm').children.length > 100, '#hm (heatmap) vazio');
ok(document.querySelectorAll('#tl-table tbody tr').length > 10, '#tl-table sem linhas');
ok(document.querySelectorAll('#cal tbody tr').length >= 1, '#cal sem linhas');

// 4. toggle PT/EN
const before = (window.__sentinelErrors||[]).length + window.__uncaught.length;
try {
  window.I18N.setLang('pt');
  const h1pt = document.querySelector('.cover h1');
  ok(h1pt && /exchanges de cripto/i.test(h1pt.textContent), `h1 não foi pra PT: "${h1pt && h1pt.textContent}"`);
  ok(document.querySelectorAll('#imap .bub').length === inWin, 'mapa sumiu após toggle PT');
  ok(/detectado|sem sinal/.test(document.getElementById('imap-legend').textContent), 'legenda do mapa não traduziu');
  window.I18N.setLang('en');
  ok(/crypto exchanges/i.test(document.querySelector('.cover h1').textContent), 'volta pra EN não repintou h1');
} catch (e) { fail('setLang lançou: ' + e.message); }
ok((window.__sentinelErrors||[]).length + window.__uncaught.length === before, 'toggle de idioma gerou erro');

// 5. modo tech
try {
  window.setMode('tech');
  ok(document.body.classList.contains('mode-tech'), 'modo tech não ativou');
  ok(document.getElementById('method') && document.getElementById('method').classList.contains('tech-only'), '#method não é tech-only');
  window.setMode('exec');
} catch (e) { fail('setMode lançou: ' + e.message); }

// 6. console
for (const m of consoleErrors.filter((m) => m.includes('[sentinel]'))) fail('console: ' + m.slice(0, 200));

if (failures.length) { console.error(`\n✗ smoke test FALHOU (${failures.length}):`); for (const f of failures) console.error('  - ' + f); process.exit(1); }
console.log(`✓ smoke test OK — ${nBub} incidentes no mapa · ${nDet} detectores · lang=${window.I18N.lang} · 0 erros de renderer`);
process.exit(0);
