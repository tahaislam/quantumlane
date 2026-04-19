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

  function formatTime(iso) {
    if (!iso) return 'never';
    try {
      return new Date(iso).toLocaleString();
    } catch {
      return iso;
    }
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
          <div>Lag: <span class="font-medium text-slate-900">${item.lag_seconds === null ? '—' : item.lag_seconds + 's'}</span></div>
          <div>5-min count: <span class="font-medium text-slate-900">${item.record_count_5min.toLocaleString()}</span></div>
        </div>
      </div>
    `).join('');
  }

  function renderFreshnessTable(payload) {
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
        <td class="px-6 py-4 text-sm text-slate-600">${item.lag_seconds === null ? '—' : item.lag_seconds + ' s'}</td>
        <td class="px-6 py-4 text-sm text-slate-600">${item.record_count_5min.toLocaleString()}</td>
        <td class="px-6 py-4 text-sm text-slate-600">${item.record_count_1h.toLocaleString()}</td>
        <td class="px-6 py-4 text-sm text-slate-500">${formatTime(item.last_record_at)}</td>
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

  window.QL = {
    fetchJSON, fetchFreshness,
    renderFreshnessSummary, renderFreshnessTable,
    renderDailyStats, renderVehiclesLatest, renderRecentRuns,
    renderError,
  };
})();
