"""aiohttp dashboard: GET / (page) and GET /api/state (JSON, polled by the page)."""
from aiohttp import web

PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>warehouse-4414 · order pipeline</title>
<style>
  :root { --bg:#0b0f0c; --fg:#c8f5c8; --dim:#6f9c6f; --line:#1f3a22; --box:#0f1712; --hi:#7dffb0; --warn:#ffd166; --err:#ff6b6b; }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--fg); font:13px/1.5 ui-monospace, "SF Mono", Menlo, Consolas, monospace; }
  .term { max-width:1180px; margin:0 auto; padding:18px 16px 36px; }
  h1 { font-size:15px; margin:0 0 4px; color:var(--hi); font-weight:700; letter-spacing:.02em; overflow-wrap:anywhere; }
  .sub { color:var(--dim); margin:0 0 16px; }
  .sub b { color:var(--fg); font-weight:400; }
  .rule { border:0; border-top:1px dashed var(--line); margin:14px 0; }
  .stats { display:grid; grid-template-columns:repeat(auto-fit, minmax(190px, 1fr)); gap:10px; }
  .stat { border:1px solid var(--line); background:var(--box); padding:10px 12px; min-width:0; }
  .stat .k { color:var(--dim); font-size:11px; text-transform:uppercase; }
  .stat .v { font-size:26px; color:var(--hi); font-variant-numeric:tabular-nums; overflow-wrap:anywhere; }
  .stat .v.warn { color:var(--warn); } .stat .v.err { color:var(--err); }
  .stat .n { color:var(--dim); font-size:11px; }
  .tiers { display:grid; grid-template-columns:repeat(auto-fit, minmax(170px, 1fr)); gap:10px; align-items:stretch; }
  .tier { border:1px solid var(--line); background:var(--box); padding:10px 12px; min-width:0; position:relative; }
  .tier .t { color:var(--warn); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .tier .t.dead { color:var(--err); } .tier .t.main { color:var(--hi); }
  .tier .c { font-size:22px; font-variant-numeric:tabular-nums; }
  .tier .d { color:var(--dim); font-size:11px; }
  .cols { display:grid; grid-template-columns:repeat(auto-fit, minmax(330px, 1fr)); gap:14px; }
  .panel { border:1px solid var(--line); background:var(--box); padding:10px 12px; min-width:0; }
  .panel h2 { font-size:12px; color:var(--dim); margin:0 0 8px; font-weight:400; text-transform:uppercase; }
  .panel h2::before { content:"# "; }
  .scroll { overflow:auto; max-height:300px; }
  table { width:100%; border-collapse:collapse; table-layout:fixed; min-width:340px; }
  table.wide { min-width:720px; }
  th, td { text-align:left; padding:4px 6px; border-bottom:1px solid var(--line); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
  th { color:var(--dim); font-weight:400; }
  .r { text-align:right; font-variant-numeric:tabular-nums; }
  .win { display:flex; gap:3px; align-items:flex-end; height:60px; margin-top:8px; }
  .win.tall { height:150px; }
  .win i { flex:1 1 0; min-width:0; background:var(--dim); display:block; }
  .win i.last { background:var(--hi); }
  pre.log { margin:0; white-space:pre; max-height:300px; overflow:auto; }
  .ok { color:var(--hi); } .retry { color:var(--warn); } .dead { color:var(--err); }
  .dim { color:var(--dim); }
  .blink { animation:b 1s steps(2) infinite; } @keyframes b { to { visibility:hidden; } }
</style>
</head>
<body>
<div class="term">
  <h1>$ warehouse-4414 --watch<span class="blink">_</span></h1>
  <p class="sub">Arivanan V. · EG/2021/4414 · aiokafka + avro + Redpanda schema registry · uptime <b id="up">0s</b> · <b id="link" class="dim">polling…</b></p>

  <div class="stats">
    <div class="stat"><div class="k">running average (all)</div><div class="v" id="avg">0.00</div><div class="n"><span id="count">0</span> orders stored</div></div>
    <div class="stat"><div class="k">average of last <span id="wsz">20</span></div><div class="v" id="wavg">0.00</div><div class="n">sliding window</div></div>
    <div class="stat"><div class="k">recovered by retry</div><div class="v warn" id="rec">0</div><div class="n">succeeded on a retry tier</div></div>
    <div class="stat"><div class="k">dead-lettered</div><div class="v err" id="dead">0</div><div class="n">orders.dead-letter</div></div>
  </div>

  <hr class="rule">
  <div class="tiers" id="tiers"></div>
  <hr class="rule">

  <div class="cols">
    <div class="panel">
      <h2>average by product</h2>
      <div class="scroll"><table>
        <colgroup><col style="width:44%"><col style="width:22%"><col style="width:34%"></colgroup>
        <thead><tr><th>product</th><th class="r">n</th><th class="r">avg price</th></tr></thead>
        <tbody id="prod"><tr><td colspan="3" class="dim">(none yet)</td></tr></tbody>
      </table></div>
    </div>
    <div class="panel">
      <h2>last prices (sliding window, newest right)</h2>
      <div class="win tall" id="win"></div>
      <div class="dim" id="winnote" style="margin-top:8px">(no prices yet)</div>
    </div>
  </div>
  <hr class="rule">
  <div class="panel">
    <h2>event stream</h2>
    <pre class="log" id="log">(waiting)</pre>
  </div>

  <hr class="rule">
  <div class="panel">
    <h2>orders.dead-letter (read back from the topic)</h2>
    <div class="scroll"><table class="wide">
      <colgroup><col style="width:6%"><col style="width:9%"><col style="width:13%"><col style="width:9%"><col style="width:9%"><col style="width:44%"><col style="width:10%"></colgroup>
      <thead><tr><th class="r">off</th><th>orderId</th><th>product</th><th class="r">price</th><th class="r">attempts</th><th>reason</th><th>at</th></tr></thead>
      <tbody id="dlq"><tr><td colspan="7" class="dim">(empty)</td></tr></tbody>
    </table></div>
  </div>
</div>
<script>
  const g = id => document.getElementById(id);
  const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
  const f2 = v => Number(v).toFixed(2);
  const pad = (s, n) => String(s).padEnd(n).slice(0, n);

  function paint(d) {
    g('up').textContent = d.uptime + 's';
    g('avg').textContent = f2(d.average); g('count').textContent = d.count;
    g('wavg').textContent = f2(d.window_average); g('wsz').textContent = d.window_size;
    g('rec').textContent = d.recovered; g('dead').textContent = d.dead;

    const tiers = [{topic:'orders', delay:0, hits:d.main_seen, main:true}, ...d.tiers, {topic:'orders.dead-letter', delay:null, hits:d.dead, dead:true}];
    g('tiers').innerHTML = tiers.map(t => `<div class="tier"><div class="t ${t.main?'main':t.dead?'dead':''}" title="${esc(t.topic)}">${esc(t.topic)}</div><div class="c">${t.hits}</div><div class="d">${t.main?'consumed from main topic':t.dead?'permanent failures':'retry after '+t.delay+'s'}</div></div>`).join('');

    const prods = Object.entries(d.products);
    g('prod').innerHTML = prods.length ? prods.map(([p,s]) => `<tr><td>${esc(p)}</td><td class="r">${s.n}</td><td class="r">${f2(s.avg)}</td></tr>`).join('') : '<tr><td colspan="3" class="dim">(none yet)</td></tr>';

    const max = Math.max(1, ...d.window);
    g('win').innerHTML = d.window.map((v,i) => `<i title="${f2(v)}" class="${i===d.window.length-1?'last':''}" style="height:${Math.max(4, v/max*100)}%"></i>`).join('');
    g('winnote').textContent = d.window.length ? `${d.window.length} prices · min ${f2(Math.min(...d.window))} · max ${f2(max)} · window avg ${f2(d.window_average)}` : '(no prices yet)';

    g('log').innerHTML = d.events.length ? d.events.map(e =>
      `<span class="dim">${e.t}</span> <span class="${e.kind}">${pad(e.kind.toUpperCase(),5)}</span> ${pad(esc(e.id),5)} ${pad(esc(e.product),12)} ${f2(e.price).padStart(8)}  ${esc(e.text)}`).join('\\n') : '(waiting)';

    g('dlq').innerHTML = d.dlq.length ? d.dlq.map(r =>
      `<tr><td class="r">${r.offset}</td><td>${esc(r.orderId)}</td><td>${esc(r.product)}</td><td class="r">${f2(r.price)}</td><td class="r">${esc(r.attempts)}</td><td title="${esc(r.reason)}">${esc(r.reason)}</td><td>${esc(r.failed_at)}</td></tr>`).join('') : '<tr><td colspan="7" class="dim">(empty)</td></tr>';
  }

  async function tick() {
    try {
      const r = await fetch('/api/state', {cache:'no-store'});
      paint(await r.json());
      g('link').textContent = 'live'; g('link').className = 'ok';
    } catch (e) { g('link').textContent = 'offline'; g('link').className = 'dead'; }
  }
  tick(); setInterval(tick, 1000);
</script>
</body>
</html>
"""


def make_app(metrics) -> web.Application:
    async def index(_):
        return web.Response(text=PAGE, content_type="text/html")

    async def state(_):
        return web.json_response(metrics.to_dict())

    app = web.Application()
    app.add_routes([web.get("/", index), web.get("/api/state", state)])
    return app
