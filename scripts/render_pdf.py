"""
render_pdf.py — Renderiza sentinel-br.html para PDF via Chromium headless.
Aplica tema claro + tipografia de impressão para maximizar legibilidade.
"""
import asyncio, sys
from pathlib import Path
from playwright.async_api import async_playwright

SRC = Path("/sessions/upbeat-stoic-mccarthy/mnt/outputs/sentinel-br.html").resolve()
OUT = Path("/sessions/upbeat-stoic-mccarthy/mnt/outputs/sentinel-br.pdf").resolve()

# Tema claro de alta legibilidade (print-only)
PRINT_CSS = r"""
  @page { size: A4 landscape; margin: 12mm 10mm 14mm 10mm; }

  /* ---------- paleta clara ---------- */
  :root, html, body {
    --bg:#ffffff !important;
    --panel:#ffffff !important;
    --panel-2:#f7f8fa !important;
    --border:#c9ced9 !important;
    --text:#0b0d12 !important;
    --muted:#3a4253 !important;   /* menos cinza p/ ler no papel */
    --accent:#a0791d !important;
    --accent-2:#c94a1a !important;
    --ok:#1a7a3d !important;
    --med:#8a6a12 !important;
    --high:#a33a1a !important;
    --crit:#c42240 !important;
  }
  * { -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important; }
  html, body {
    background:#fff !important; color:#000 !important;
    font-size: 12.5px !important; line-height: 1.55 !important;
  }
  body { max-width: 100% !important; padding: 0 4mm !important; }

  /* tipografia maior e mais contrastada */
  h1, h2, h3, h4 { color:#0b0d12 !important; }
  h1 { font-size: 22px !important; }
  h2 { font-size: 16px !important; }
  h3 { font-size: 12px !important; color:#3a4253 !important; }
  p, li, td, th, span, div { color:#0b0d12 !important; }
  small, .muted, .sub { color:#3a4253 !important; }

  /* links em preto (não faz sentido azul/dourado em papel) */
  a { color:#0b0d12 !important; text-decoration: underline !important; }

  /* cartões e painéis com borda suave, sem sombra */
  .panel, .kpi, .sem-card, .lead, .heatmap-panel, .ring-panel, .tron-panel, .funding-panel {
    background:#ffffff !important;
    border:1px solid #c9ced9 !important;
    box-shadow:none !important;
    border-radius: 6px !important;
    padding: 14px 16px !important;
    margin-bottom: 14px !important;
  }
  .panel.highlight { border: 2px solid #c42240 !important; }

  /* header grande do produto */
  header.top {
    background:#fff !important;
    border-bottom:2px solid #0b0d12 !important;
  }
  .brand-mark { border:1px solid #0b0d12 !important; color:#0b0d12 !important; }
  .brand-name, .brand-sub { color:#0b0d12 !important; }
  .meta { color:#3a4253 !important; }

  /* manchete (lead) */
  .lead {
    display:block !important;
    background:#fafbfd !important;
    padding: 18px 22px !important;
  }
  .lead .headline {
    font-size: 22px !important;
    line-height: 1.3 !important;
    color:#0b0d12 !important;
  }
  .lead .headline b { color:#a33a1a !important; }
  .lead .sub { font-size: 12px !important; color:#3a4253 !important; max-width: 100% !important; }
  .lead .sub b { color:#0b0d12 !important; font-weight: 700 !important; }
  .lead .credit { display:none !important; }  /* 13/14 gigante não cabe em papel */

  /* tags */
  .tag { background:#f0f2f6 !important; color:#0b0d12 !important; border:1px solid #c9ced9 !important; }

  /* semáforo */
  .sem-card { padding: 14px 16px !important; }
  .sem-card::before { width: 4px !important; }
  .sem-card.green::before  { background:#1a7a3d !important; }
  .sem-card.yellow::before { background:#8a6a12 !important; }
  .sem-card.red::before    { background:#c42240 !important; }
  .sem-light { width: 34px !important; height: 34px !important; border-radius: 999px !important; display:grid !important; place-items:center !important; font-weight:700 !important; }
  .sem-light.green  { background:#e5f4ea !important; color:#1a7a3d !important; box-shadow: inset 0 0 0 1px #1a7a3d !important; }
  .sem-light.yellow { background:#fcf3db !important; color:#8a6a12 !important; box-shadow: inset 0 0 0 1px #8a6a12 !important; }
  .sem-light.red    { background:#fbe3e8 !important; color:#c42240 !important; box-shadow: inset 0 0 0 1px #c42240 !important; }
  .sem-label { color:#3a4253 !important; }
  .sem-value { color:#0b0d12 !important; font-size: 18px !important; }
  .sem-explain { color:#0b0d12 !important; font-size: 11.5px !important; }
  .sem-explain b { color:#0b0d12 !important; font-weight: 700 !important; }

  /* KPIs: números grandes, legenda clara */
  .kpi .l { color:#3a4253 !important; font-size: 10px !important; }
  .kpi .v { color:#0b0d12 !important; font-size: 20px !important; font-weight: 700 !important; }
  .kpi .s { color:#3a4253 !important; font-size: 10.5px !important; }
  .kpi.crit .v { color:#c42240 !important; }
  .kpi.high .v { color:#a33a1a !important; }
  .kpi.med  .v { color:#8a6a12 !important; }
  .kpi.ok   .v { color:#1a7a3d !important; }

  /* tabelas — essenciais para leitura */
  table { width:100% !important; border-collapse: collapse !important; font-size: 11px !important; }
  thead th {
    background:#f0f2f6 !important; color:#0b0d12 !important;
    border-bottom: 1.5px solid #0b0d12 !important;
    text-align:left !important; padding: 6px 8px !important;
    font-weight: 700 !important; text-transform: uppercase; letter-spacing: .4px;
  }
  tbody td { padding: 5px 8px !important; border-bottom: 1px solid #e5e7ef !important; color:#0b0d12 !important; }
  tbody tr:nth-child(2n) td { background: #fafbfd !important; }

  /* chips de severidade */
  .sev, .badge, .pill, [class*="sev-"] { border:1px solid #0b0d12 !important; }
  .sev.critical, .sev-critical, .crit { background:#fbe3e8 !important; color:#c42240 !important; border-color:#c42240 !important; }
  .sev.high,     .sev-high,     .high { background:#fdece4 !important; color:#a33a1a !important; border-color:#a33a1a !important; }
  .sev.medium,   .sev-medium,   .med  { background:#fcf3db !important; color:#8a6a12 !important; border-color:#8a6a12 !important; }
  .sev.low,      .sev-low,      .ok   { background:#e5f4ea !important; color:#1a7a3d !important; border-color:#1a7a3d !important; }

  /* heatmap */
  .heat-cell { outline: 1px solid #e5e7ef !important; }

  /* canvases — deixa Chart.js decidir dimensões (height explicito confunde) */
  canvas { max-width: 100% !important; }
  /* containers de chart: garante altura mínima para não colapsarem */
  .chart-box, .chart-wrap, [class*="chart-"], .panel > .grid2 > div, .panel canvas {
    min-height: 230px !important;
  }
  /* heatmap precisa de espaço vertical */
  .heatmap-grid, [id*="heatmap"] { min-height: 340px !important; }

  /* painéis pequenos: evita corte feio */
  .kpi, .sem-card, .lead, .semaforo {
     break-inside: avoid !important;
     page-break-inside: avoid !important;
  }
  /* tabelas longas PODEM quebrar — o que não pode é o cabeçalho sumir */
  thead { display: table-header-group !important; }
  tfoot { display: table-footer-group !important; }
  tr    { page-break-inside: avoid !important; }

  /* esconder animações e coisas que não fazem sentido no papel */
  .dot, .pulse, [class*="blink"] { animation: none !important; }
  *, *::before, *::after { animation: none !important; transition: none !important; }

  /* widgets de scroll */
  [style*="overflow"] { overflow: visible !important; }

  /* footer das notas pequenas */
  .footer, footer { color:#3a4253 !important; border-top: 1px solid #c9ced9 !important; padding-top: 8px !important; margin-top: 12px !important; }
"""

HEADER_TEMPLATE = """
  <div style="font:10px/1.3 system-ui, sans-serif; color:#333; width:100%; padding:0 12mm;">
    <span style="float:left"><b>Sentinel BR</b> — Fraud Intelligence para exchanges BR</span>
    <span style="float:right">100% dados públicos · relatório técnico</span>
  </div>
"""

FOOTER_TEMPLATE = """
  <div style="font:9.5px/1.3 system-ui, sans-serif; color:#555; width:100%; padding:0 12mm;">
    <span style="float:left">Victor H. Gomes — sentinel-br</span>
    <span style="float:right">pág <span class="pageNumber"></span> / <span class="totalPages"></span></span>
  </div>
"""

async def main():
    if not SRC.exists():
        print(f"ERRO: {SRC} não existe", file=sys.stderr); sys.exit(1)
    print(f"source: {SRC} ({SRC.stat().st_size/1024:.1f} KB)")

    async with async_playwright() as p:
        browser = await p.chromium.launch()
        # viewport mais estreito "força" charts e tabelas a reflow de forma mais compacta
        ctx = await browser.new_context(viewport={"width": 1180, "height": 820}, device_scale_factor=2)
        page = await ctx.new_page()
        page.on("pageerror", lambda e: print(f"[pageerror] {e}"))

        await page.goto(SRC.as_uri(), wait_until="networkidle", timeout=60_000)

        # dá tempo extra p/ Chart.js terminar + força redraw em print-like size
        await page.wait_for_timeout(2500)

        # SCROLL-THROUGH: força inicialização de charts com IntersectionObserver/lazy-load.
        # Percorre o documento em ~12 saltos, espera rendering, volta ao topo.
        await page.evaluate("""
          async () => {
            const scrollH = document.documentElement.scrollHeight;
            const steps = 14;
            for (let i = 0; i <= steps; i++) {
              window.scrollTo(0, (scrollH * i) / steps);
              await new Promise(r => setTimeout(r, 350));
            }
            window.scrollTo(0, 0);
            await new Promise(r => setTimeout(r, 400));
          }
        """)
        await page.wait_for_timeout(1500)

        # remove fundo escuro do body inline (algumas coisas forçam bg via JS)
        await page.evaluate("""
          () => {
            document.documentElement.style.background = '#fff';
            document.body.style.background = '#fff';
            document.body.style.color = '#000';
          }
        """)

        await page.add_style_tag(content=PRINT_CSS)

        # ajusta cores dos Chart.js p/ tema claro E força re-render completo em cada um
        await page.evaluate("""
          async () => {
            if (window.Chart && Chart.defaults) {
              Chart.defaults.color = '#0b0d12';
              Chart.defaults.borderColor = '#e5e7ef';
            }
            if (window.Chart && Chart.instances) {
              for (const c of Object.values(Chart.instances)) {
                try {
                  const opts = c.options || {};
                  opts.animation = false;
                  opts.responsive = true;
                  opts.maintainAspectRatio = false;
                  const scales = opts.scales || {};
                  Object.values(scales).forEach(s => {
                    if (s.ticks) s.ticks.color = '#0b0d12';
                    if (s.grid)  s.grid.color  = '#e5e7ef';
                    if (s.title) s.title.color = '#0b0d12';
                  });
                  if (opts.plugins && opts.plugins.legend && opts.plugins.legend.labels) {
                    opts.plugins.legend.labels.color = '#0b0d12';
                  }
                  // garantir que o canvas tem dimensões antes de redesenhar
                  const el = c.canvas;
                  const parent = el && el.parentElement;
                  if (parent && (parent.clientHeight < 100 || parent.clientWidth < 100)) {
                    parent.style.minHeight = '260px';
                    parent.style.minWidth  = '300px';
                  }
                  c.resize();
                  c.update('none');
                  c.render();
                  await new Promise(r => setTimeout(r, 60));
                } catch(e){ console.warn('chart err', e); }
              }
            }
            window.dispatchEvent(new Event('resize'));
          }
        """)

        # dá MUITO mais tempo p/ Chart.js terminar (24 charts na página)
        await page.wait_for_timeout(4000)
        await page.emulate_media(media="print")
        await page.wait_for_timeout(800)

        print("rendering PDF...")
        await page.pdf(
            path=str(OUT),
            format="A4",
            landscape=True,
            print_background=True,
            margin={"top":"18mm","bottom":"16mm","left":"10mm","right":"10mm"},
            display_header_footer=True,
            header_template=HEADER_TEMPLATE,
            footer_template=FOOTER_TEMPLATE,
            prefer_css_page_size=False,
            scale=0.92,  # encolhe ~8% p/ caber mais por página
        )

        await browser.close()

    sz = OUT.stat().st_size / 1024
    print(f"ok: {OUT}  ({sz:.1f} KB)")

if __name__ == "__main__":
    asyncio.run(main())
