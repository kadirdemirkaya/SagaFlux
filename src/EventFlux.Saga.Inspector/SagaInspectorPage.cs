namespace EventFlux.Saga.Inspector;

internal static class SagaInspectorPage
{
    public const string Html = """
        <!doctype html>
        <html lang="en">
        <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Saga Inspector</title>
        <style>
        body { font-family: system-ui, sans-serif; margin: 1.5rem; color: #1f2328; background: #fff; }
        input { width: 24rem; max-width: 100%; padding: .4rem; font-family: monospace; }
        table { border-collapse: collapse; margin-top: 1rem; width: 100%; }
        th, td { border-bottom: 1px solid #d0d7de; padding: .35rem .5rem; text-align: left; font-size: .9rem; }
        th { background: #f6f8fa; }
        .muted { color: #656d76; }
        @media (prefers-color-scheme: dark) {
          body { color: #e6edf3; background: #0d1117; }
          th { background: #161b22; }
          th, td { border-color: #30363d; }
          .muted { color: #8d96a0; }
        }
        </style>
        </head>
        <body>
        <h1>Saga Inspector</h1>
        <form id="lookup"><input id="id" placeholder="correlation id" autocomplete="off"> <button>Show timeline</button> <a href="#" id="recent">recent</a></form>
        <p id="status" class="muted"></p>
        <table><thead><tr><th>When</th><th>Saga</th><th>Instance</th><th>Event</th><th>From</th><th>To</th><th>Published</th><th>Compensated</th><th>Timeouts</th></tr></thead><tbody id="rows"></tbody></table>
        <script>
        const rows = document.getElementById('rows');
        const status = document.getElementById('status');
        function cell(text) { const td = document.createElement('td'); td.textContent = text; return td; }
        function render(records) {
          rows.replaceChildren();
          for (const r of records) {
            const tr = document.createElement('tr');
            const timeouts = [...r.scheduled.map(k => '+' + k), ...r.unscheduled.map(k => '-' + k)].join(', ');
            [new Date(r.occurredAt).toISOString(), r.sagaType, r.correlationId, r.eventType, r.fromState, r.toState,
             r.published.join(', '), r.compensated.join(', '), timeouts].forEach(v => tr.appendChild(cell(v)));
            rows.appendChild(tr);
          }
          status.textContent = records.length + ' record(s)';
        }
        async function load(path) {
          const res = await fetch(path);
          if (!res.ok) { rows.replaceChildren(); status.textContent = res.status === 404 ? 'No records for this instance.' : 'Request failed: ' + res.status; return; }
          render(await res.json());
        }
        const base = location.pathname.endsWith('/') ? location.pathname : location.pathname + '/';
        document.getElementById('lookup').addEventListener('submit', e => { e.preventDefault(); load(base + 'instances/' + encodeURIComponent(document.getElementById('id').value.trim())); });
        document.getElementById('recent').addEventListener('click', e => { e.preventDefault(); load(base + 'recent'); });
        load(base + 'recent');
        </script>
        </body>
        </html>
        """;
}
