/**
 * QuantumLane website client. Vanilla JS, no build, no dependencies.
 *
 * The single global QL namespace exposes a small set of fetch + render functions
 * so the HTML pages can wire them up with one-liners.
 */
(function () {
  'use strict';

  // The website talks to the API via the same origin (Caddy proxies /api/* and /v1/* to FastAPI).
  // For local development this works via the same compose network.
  const API_BASE = window.QL_API_BASE || '';

  function statusBadge(status) {
    const colors = {
      healthy: 'bg-emerald-100 text-emerald-800',
      lagging: 'bg-amber-100 text-amber-800',
      stale: 'bg-orange-100 text-orange-800',
      down: 'bg-rose-100 text-rose-800',
    };
    const cls = colors[status] || 'bg-slate-100 text-slate-700';
    return `<span class="inline-block px-2 py-0.5 rounded text-xs font-medium ${cls}">${status}</span>`;
  }

  function escapeHTML(s) {
    if (s === null || s === undefined) return '';
    return String(s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  // Timestamps render in Toronto time with the zone spelled out ("2026-09-10 11:50 EDT")
  // rather than the viewer's locale string, so two visitors see the same thing.
  const TORONTO_TIME = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Toronto',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', hour12: false,
    timeZoneName: 'short',
  });

  function formatTime(iso) {
    if (!iso) return 'never';
    try {
      const parts = {};
      for (const p of TORONTO_TIME.formatToParts(new Date(iso))) parts[p.type] = p.value;
      return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute} ${parts.timeZoneName}`;
    } catch {
      return iso;
    }
  }

  // 2889542 → "33 d 10 h". Two units is enough to read; the raw seconds stay as secondary text.
  function formatDuration(seconds) {
    if (seconds === null || seconds === undefined) return '—';
    const s = Math.max(0, Math.floor(seconds));
    if (s < 60) return `${s} s`;
    if (s < 3600) return `${Math.floor(s / 60)} m ${s % 60} s`;
    if (s < 86400) return `${Math.floor(s / 3600)} h ${Math.floor((s % 3600) / 60)} m`;
    return `${Math.floor(s / 86400)} d ${Math.floor((s % 86400) / 3600)} h`;
  }

  function lagCell(seconds) {
    if (seconds === null || seconds === undefined) return '—';
    return `<span class="font-medium text-slate-900">${formatDuration(seconds)}</span>
      <span class="text-xs text-slate-400 ml-1">${seconds.toLocaleString()} s</span>`;
  }

  // "Last record" distinguishes a feed that has never ingested from one whose last
  // rows have aged out of the 3-day hot tier — those used to both read "never".
  function lastRecordCell(item) {
    if (!item.last_record_at) return '<span class="text-slate-400">never (no records yet)</span>';
    const when = formatTime(item.last_record_at);
    if (item.last_record_source === 'watermark') {
      return `${when} <span class="block text-xs text-slate-400">hot tier since rotated</span>`;
    }
    return when;
  }

  // An incident banner is static HTML (the cause and link are human knowledge), but it
  // hides itself once every feed it names is healthy again, so it can't go stale-wrong.
  function updateIncidentBanner(payload) {
    const el = document.getElementById('incident-banner');
    if (!el) return;
    const feeds = (el.dataset.feeds || '').split(',').map(s => s.trim()).filter(Boolean);
    const byKey = Object.fromEntries((payload.data || []).map(i => [i.feed_key, i]));
    const resolved = feeds.length > 0 && feeds.every(k => byKey[k] && byKey[k].status === 'healthy');
    el.classList.toggle('hidden', resolved);
  }

  async function fetchJSON(path) {
    const res = await fetch(API_BASE + path, { headers: { Accept: 'application/json' } });
    if (!res.ok) throw new Error(`API ${path} returned ${res.status}`);
    return res.json();
  }

  async function fetchFreshness() {
    return fetchJSON('/v1/freshness');
  }

  function renderFreshnessSummary(payload) {
    updateIncidentBanner(payload);
    const el = document.getElementById('freshness-summary');
    if (!el) return;
    const items = payload.data || [];
    if (items.length === 0) {
      el.innerHTML = `<div class="bg-white rounded-lg border border-slate-200 p-6 col-span-3 text-slate-500">
        No data yet. Either ingestion hasn't started or the database is empty.
      </div>`;
      return;
    }
    el.innerHTML = items.map(item => `
      <div class="bg-white rounded-lg border border-slate-200 p-6">
        <div class="text-xs font-mono text-slate-500 mb-2">${escapeHTML(item.feed_key)}</div>
        <div class="mb-3">${statusBadge(item.status)}</div>
        <div class="text-sm text-slate-600">
          <div>Lag: ${lagCell(item.lag_seconds)}</div>
          <div>5-min count: <span class="font-medium text-slate-900">${item.record_count_5min.toLocaleString()}</span></div>
          <div class="text-xs text-slate-500 mt-1">Last record: ${lastRecordCell(item)}</div>
        </div>
      </div>
    `).join('');
  }

  function renderFreshnessTable(payload) {
    updateIncidentBanner(payload);
    const el = document.getElementById('freshness-table');
    if (!el) return;
    const items = payload.data || [];
    if (items.length === 0) {
      el.innerHTML = `<div class="p-6 text-slate-500">No freshness snapshots yet.</div>`;
      return;
    }
    const rows = items.map(item => `
      <tr class="border-t border-slate-100">
        <td class="px-6 py-4 font-mono text-xs">${escapeHTML(item.feed_key)}</td>
        <td class="px-6 py-4">${statusBadge(item.status)}</td>
        <td class="px-6 py-4 text-sm text-slate-600">${lagCell(item.lag_seconds)}</td>
        <td class="px-6 py-4 text-sm text-slate-600">${item.record_count_5min.toLocaleString()}</td>
        <td class="px-6 py-4 text-sm text-slate-600">${item.record_count_1h.toLocaleString()}</td>
        <td class="px-6 py-4 text-sm text-slate-500">${lastRecordCell(item)}</td>
      </tr>
    `).join('');
    el.innerHTML = `
      <table class="w-full">
        <thead class="bg-slate-50 text-xs font-semibold uppercase tracking-wider text-slate-500">
          <tr>
            <th class="px-6 py-3 text-left">Feed</th>
            <th class="px-6 py-3 text-left">Status</th>
            <th class="px-6 py-3 text-left">Lag</th>
            <th class="px-6 py-3 text-left">Last 5 min</th>
            <th class="px-6 py-3 text-left">Last hour</th>
            <th class="px-6 py-3 text-left">Last record</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    `;
  }

  function renderDailyStats(payload) {
    const el = document.getElementById('daily-stats');
    if (!el) return;
    const items = payload.data || [];
    if (items.length === 0) {
      el.textContent = 'No data yet.';
      return;
    }
    el.innerHTML = `<table class="w-full text-sm">
      <thead class="text-xs uppercase text-slate-500"><tr>
        <th class="text-left py-2">Day</th><th class="text-left py-2">Feed</th><th class="text-right py-2">Records</th>
      </tr></thead>
      <tbody>${items.map(s => `
        <tr class="border-t border-slate-100">
          <td class="py-2">${escapeHTML(s.day)}</td>
          <td class="py-2 font-mono text-xs">${escapeHTML(s.feed_key)}</td>
          <td class="py-2 text-right">${s.record_count.toLocaleString()}</td>
        </tr>`).join('')}</tbody>
    </table>`;
  }

  function renderVehiclesLatest(payload) {
    const el = document.getElementById('vehicles-latest');
    if (!el) return;
    const items = payload.data || [];
    if (items.length === 0) {
      el.textContent = 'No vehicles in the last 5 minutes.';
      return;
    }
    el.innerHTML = `<table class="w-full text-sm">
      <thead class="text-xs uppercase text-slate-500"><tr>
        <th class="text-left py-2">Vehicle</th><th class="text-left py-2">Route</th>
        <th class="text-right py-2">Lat</th><th class="text-right py-2">Lon</th>
        <th class="text-right py-2">Speed (m/s)</th>
      </tr></thead>
      <tbody>${items.map(v => `
        <tr class="border-t border-slate-100">
          <td class="py-2 font-mono text-xs">${escapeHTML(v.vehicle_id)}</td>
          <td class="py-2">${escapeHTML(v.route_id || '—')}</td>
          <td class="py-2 text-right">${v.latitude?.toFixed(4) || '—'}</td>
          <td class="py-2 text-right">${v.longitude?.toFixed(4) || '—'}</td>
          <td class="py-2 text-right">${v.speed_mps?.toFixed(1) || '—'}</td>
        </tr>`).join('')}</tbody>
    </table>`;
  }

  // Parameterized version of renderVehiclesLatest, so a second vehicles card
  // (e.g. route-filtered) can render into its own container.
  function renderVehiclesInto(containerId, payload) {
    const el = document.getElementById(containerId);
    if (!el) return;
    const items = payload.data || [];
    if (items.length === 0) {
      el.textContent = 'No vehicles in the last 5 minutes.';
      return;
    }
    el.innerHTML = `<table class="w-full text-sm">
      <thead class="text-xs uppercase text-slate-500"><tr>
        <th class="text-left py-2">Vehicle</th><th class="text-left py-2">Route</th>
        <th class="text-right py-2">Lat</th><th class="text-right py-2">Lon</th>
        <th class="text-right py-2">Speed (m/s)</th>
      </tr></thead>
      <tbody>${items.map(v => `
        <tr class="border-t border-slate-100">
          <td class="py-2 font-mono text-xs">${escapeHTML(v.vehicle_id)}</td>
          <td class="py-2">${escapeHTML(v.route_id || '—')}</td>
          <td class="py-2 text-right">${v.latitude?.toFixed(4) || '—'}</td>
          <td class="py-2 text-right">${v.longitude?.toFixed(4) || '—'}</td>
          <td class="py-2 text-right">${v.speed_mps?.toFixed(1) || '—'}</td>
        </tr>`).join('')}</tbody>
    </table>`;
  }

  function renderRecentRuns(payload) {
    const el = document.getElementById('recent-runs');
    if (!el) return;
    const items = payload.data || [];
    if (items.length === 0) {
      el.textContent = 'No runs yet.';
      return;
    }
    el.innerHTML = `<table class="w-full text-sm">
      <thead class="text-xs uppercase text-slate-500"><tr>
        <th class="text-left py-2">Asset</th><th class="text-left py-2">Status</th>
        <th class="text-right py-2">Records</th><th class="text-left py-2">Started</th>
      </tr></thead>
      <tbody>${items.map(r => `
        <tr class="border-t border-slate-100">
          <td class="py-2 font-mono text-xs">${escapeHTML(r.asset_key)}</td>
          <td class="py-2">${statusBadge(r.status === 'success' ? 'healthy' : (r.status === 'failure' ? 'down' : 'lagging'))}</td>
          <td class="py-2 text-right">${r.records_written?.toLocaleString() || '—'}</td>
          <td class="py-2 text-xs text-slate-500">${formatTime(r.started_at)}</td>
        </tr>`).join('')}</tbody>
    </table>`;
  }

  function renderError(err) {
    console.error(err);
    document.querySelectorAll('[id$="-table"], [id$="-summary"], #daily-stats, #vehicles-latest, #recent-runs')
      .forEach(el => {
        el.innerHTML = `<div class="p-4 text-sm text-rose-700 bg-rose-50 border border-rose-200 rounded">
          Failed to fetch data from the API. ${escapeHTML(err.message || 'Unknown error')}
        </div>`;
      });
  }

  // Scoped error renderer: writes the error into ONE container instead of
  // blanking every known container the way renderError does.
  function renderErrorInto(containerId, err) {
    console.error(err);
    const el = document.getElementById(containerId);
    if (!el) return;
    el.innerHTML = `<div class="p-4 text-sm text-rose-700 bg-rose-50 border border-rose-200 rounded">
      Failed to fetch data from the API. ${escapeHTML(err.message || 'Unknown error')}
    </div>`;
  }

  window.QL = {
    fetchJSON, fetchFreshness, formatTime, formatDuration,
    renderFreshnessSummary, renderFreshnessTable,
    renderDailyStats, renderVehiclesLatest, renderVehiclesInto, renderRecentRuns,
    renderError, renderErrorInto,
  };
})();