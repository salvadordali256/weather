/* Snow Trip Planner — vanilla JS, no build tools */

(function () {
    'use strict';

    // ─── State ───
    let stationData = null;
    let map = null;
    let markers = {};      // station_id → L.circleMarker
    let selectedId = null;

    // ─── WMO weather code → emoji ───
    const WMO_ICONS = {
        0: '☀️', 1: '🌤️', 2: '⛅', 3: '☁️',
        45: '🌫️', 48: '🌫️',
        51: '🌧️', 53: '🌧️', 55: '🌧️',
        56: '🌧️', 57: '🌧️',
        61: '🌧️', 63: '🌧️', 65: '🌧️',
        66: '🌧️', 67: '🌧️',
        71: '🌨️', 73: '❄️', 75: '❄️',
        77: '❄️',
        80: '🌦️', 81: '🌦️', 82: '🌦️',
        85: '🌨️', 86: '❄️',
        95: '⛈️', 96: '⛈️', 99: '⛈️',
    };

    function wmoIcon(code) {
        return WMO_ICONS[code] || '🌡️';
    }

    // ─── Score → color ───
    function scoreColor(score) {
        if (score >= 60) return '#3b82f6';
        if (score >= 30) return '#6366f1';
        return '#94a3b8';
    }

    function scoreBadgeClass(score) {
        if (score >= 60) return 'badge-high';
        if (score >= 30) return 'badge-moderate';
        return 'badge-low';
    }

    function scoreLabel(score) {
        if (score >= 60) return 'High';
        if (score >= 30) return 'Moderate';
        return 'Low';
    }

    // ─── Haversine distance (km) ───
    function haversine(lat1, lon1, lat2, lon2) {
        const R = 6371;
        const dLat = (lat2 - lat1) * Math.PI / 180;
        const dLon = (lon2 - lon1) * Math.PI / 180;
        const a = Math.sin(dLat / 2) ** 2 +
                  Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
                  Math.sin(dLon / 2) ** 2;
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

    // ─── Date helpers ───
    function dateStr(d) {
        return d.toISOString().slice(0, 10);
    }

    function shortDate(s) {
        const d = new Date(s + 'T00:00:00');
        return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    }

    function shortDay(s) {
        const d = new Date(s + 'T00:00:00');
        return d.toLocaleDateString('en-US', { weekday: 'short' });
    }

    // ─── Init ───
    async function init() {
        showLoading(true);
        setDefaultDates();
        initMap();
        await loadData();
        showLoading(false);
        setupSearch();
        setupDateListeners();
    }

    function showLoading(show) {
        let el = document.getElementById('loading-overlay');
        if (show && !el) {
            el = document.createElement('div');
            el.id = 'loading-overlay';
            el.className = 'loading-overlay';
            el.innerHTML = '<div class="spinner"></div><p>Loading station data...</p>';
            document.body.appendChild(el);
        } else if (!show && el) {
            el.remove();
        }
    }

    function setDefaultDates() {
        const arrive = new Date();
        arrive.setDate(arrive.getDate() + 1);
        const depart = new Date();
        depart.setDate(depart.getDate() + 4);
        document.getElementById('date-arrive').value = dateStr(arrive);
        document.getElementById('date-depart').value = dateStr(depart);
    }

    // ─── Map init ───
    function initMap() {
        map = L.map('map', {
            center: [45, 10],
            zoom: 3,
            zoomControl: true,
            attributionControl: true,
        });

        L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/">CARTO</a>',
            subdomains: 'abcd',
            maxZoom: 19,
        }).addTo(map);
    }

    // ─── Load data ───
    async function loadData() {
        try {
            const resp = await fetch('station_data.json');
            if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
            stationData = await resp.json();
            document.getElementById('station-count').textContent =
                `${stationData.station_count} stations`;
            addMarkers();
            buildRegionList();
        } catch (err) {
            console.error('Failed to load station data:', err);
            document.getElementById('station-count').textContent = 'Data unavailable';
        }
    }

    // ─── Markers ───
    function addMarkers() {
        const stations = stationData.stations;
        for (const [id, s] of Object.entries(stations)) {
            const color = scoreColor(s.snow_score);
            const marker = L.circleMarker([s.lat, s.lon], {
                radius: 7,
                fillColor: color,
                color: '#fff',
                weight: 1,
                opacity: 0.8,
                fillOpacity: 0.8,
            }).addTo(map);

            const total7 = s.forecast.snowfall_cm.slice(0, 7)
                .reduce((a, b) => a + b, 0).toFixed(1);

            marker.bindPopup(`
                <div class="popup-name">${s.name}</div>
                <div class="popup-region">${regionDisplayName(s.region)}</div>
                <div class="popup-snow">
                    ${wmoIcon(s.forecast.weather_code[0])} <strong>${total7} cm</strong> next 7 days
                </div>
                <div class="popup-snow">Score: <strong>${s.snow_score}</strong>/100</div>
                <div class="popup-link" data-id="${id}">View details</div>
            `);

            marker.on('popupopen', function () {
                const link = document.querySelector(`.popup-link[data-id="${id}"]`);
                if (link) link.addEventListener('click', () => selectStation(id));
            });

            markers[id] = marker;
        }
    }

    function regionDisplayName(key) {
        if (stationData && stationData.regions && stationData.regions[key]) {
            return stationData.regions[key].display_name;
        }
        return key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
    }

    // ─── Region list ───
    function buildRegionList() {
        const ul = document.getElementById('region-list');
        const regions = stationData.regions;
        ul.innerHTML = '';
        for (const [key, r] of Object.entries(regions)) {
            const li = document.createElement('li');
            li.innerHTML = `<span>${r.display_name}</span><span class="region-count">${r.stations.length}</span>`;
            li.addEventListener('click', () => zoomToRegion(key));
            ul.appendChild(li);
        }
    }

    function zoomToRegion(regionKey) {
        const ids = stationData.regions[regionKey].stations;
        const lats = [], lons = [];
        for (const id of ids) {
            const s = stationData.stations[id];
            if (s) { lats.push(s.lat); lons.push(s.lon); }
        }
        if (lats.length === 0) return;
        map.fitBounds([
            [Math.min(...lats) - 1, Math.min(...lons) - 2],
            [Math.max(...lats) + 1, Math.max(...lons) + 2],
        ]);
    }

    // ─── Search ───
    function setupSearch() {
        const input = document.getElementById('search-input');
        const results = document.getElementById('search-results');

        input.addEventListener('input', function () {
            const q = this.value.trim().toLowerCase();
            results.innerHTML = '';
            if (q.length < 2 || !stationData) return;

            const matches = [];
            for (const [id, s] of Object.entries(stationData.stations)) {
                const nameMatch = s.name.toLowerCase().includes(q);
                const regionMatch = regionDisplayName(s.region).toLowerCase().includes(q);
                if (nameMatch || regionMatch) {
                    matches.push({ id, s, nameMatch });
                }
                if (matches.length >= 10) break;
            }

            // Sort: name matches first
            matches.sort((a, b) => (b.nameMatch ? 1 : 0) - (a.nameMatch ? 1 : 0));

            for (const m of matches) {
                const li = document.createElement('li');
                li.innerHTML = `<span>${m.s.name}</span><span class="result-region">${regionDisplayName(m.s.region)}</span>`;
                li.addEventListener('click', () => {
                    selectStation(m.id);
                    input.value = m.s.name;
                    results.innerHTML = '';
                });
                results.appendChild(li);
            }
        });

        // Close results on outside click
        document.addEventListener('click', (e) => {
            if (!e.target.closest('.panel-section')) results.innerHTML = '';
        });
    }

    // ─── Date listeners ───
    function setupDateListeners() {
        document.getElementById('date-arrive').addEventListener('change', refreshDetail);
        document.getElementById('date-depart').addEventListener('change', refreshDetail);
    }

    function getDateRange() {
        const arrive = document.getElementById('date-arrive').value;
        const depart = document.getElementById('date-depart').value;
        return { arrive, depart };
    }

    function refreshDetail() {
        if (selectedId) renderDetail(selectedId);
    }

    // ─── Select station ───
    function selectStation(id) {
        selectedId = id;
        const s = stationData.stations[id];
        if (!s) return;

        // Pan map
        map.setView([s.lat, s.lon], 6);

        // Highlight marker
        for (const [mid, m] of Object.entries(markers)) {
            m.setStyle({
                weight: mid === id ? 3 : 1,
                color: mid === id ? '#fff' : '#fff',
                radius: mid === id ? 10 : 7,
            });
        }

        // Show detail, hide region list
        document.getElementById('station-detail').style.display = 'block';
        document.getElementById('region-list-section').style.display = 'none';

        renderDetail(id);
    }

    // ─── Render detail ───
    function renderDetail(id) {
        const s = stationData.stations[id];
        if (!s) return;

        document.getElementById('detail-name').textContent = s.name;
        document.getElementById('detail-region').textContent = regionDisplayName(s.region);

        const scoreEl = document.getElementById('detail-score');
        scoreEl.textContent = `Snow Score: ${s.snow_score}/100 — ${scoreLabel(s.snow_score)}`;
        scoreEl.className = 'snow-score-badge ' + scoreBadgeClass(s.snow_score);

        renderForecastStrip(s);
        renderHistoryCard(s);
        renderRecentObs(s);
        renderNearby(id, s);
    }

    function renderForecastStrip(s) {
        const strip = document.getElementById('forecast-strip');
        const { arrive, depart } = getDateRange();
        const fc = s.forecast;

        let html = '';
        for (let i = 0; i < fc.dates.length; i++) {
            const d = fc.dates[i];
            const inRange = d >= arrive && d <= depart;
            const snow = fc.snowfall_cm[i];
            const icon = wmoIcon(fc.weather_code[i]);
            const tMax = fc.temp_max_c[i] != null ? Math.round(fc.temp_max_c[i]) : '—';
            const tMin = fc.temp_min_c[i] != null ? Math.round(fc.temp_min_c[i]) : '—';

            html += `<div class="fc-day${inRange ? ' in-range' : ''}">
                <div class="fc-date">${shortDay(d)}<br>${shortDate(d)}</div>
                <div class="fc-icon">${icon}</div>
                <div class="fc-snow">${snow > 0 ? snow + ' cm' : '—'}</div>
                <div class="fc-temp">${tMax}/${tMin}&deg;</div>
            </div>`;
        }
        strip.innerHTML = html;
    }

    function renderHistoryCard(s) {
        const card = document.getElementById('history-card');
        const { arrive } = getDateRange();

        // Find week number for arrival date
        const arrDate = new Date(arrive + 'T00:00:00');
        const startOfYear = new Date(arrDate.getFullYear(), 0, 1);
        const weekNum = Math.floor((arrDate - startOfYear) / (7 * 24 * 60 * 60 * 1000));

        const week = s.climatology.weeks[String(weekNum)];
        if (!week) {
            card.innerHTML = '<p class="no-data">No historical data for this period.</p>';
            return;
        }

        card.innerHTML = `
            <div class="history-row"><span class="hist-label">Week</span><span class="hist-value">${week.week_label}</span></div>
            <div class="history-row"><span class="hist-label">Avg daily snowfall</span><span class="hist-value">${week.avg_daily_snowfall_mm} mm</span></div>
            <div class="history-row"><span class="hist-label">Snow day probability</span><span class="hist-value">${Math.round(week.snow_day_probability * 100)}%</span></div>
            <div class="history-row"><span class="hist-label">Avg high / low</span><span class="hist-value">${week.avg_temp_max_c != null ? week.avg_temp_max_c + '°' : '—'} / ${week.avg_temp_min_c != null ? week.avg_temp_min_c + '°' : '—'}</span></div>
            <div class="history-row"><span class="hist-label">Max recorded</span><span class="hist-value">${week.max_recorded_snowfall_mm} mm</span></div>
            <div class="history-row"><span class="hist-label">Years of data</span><span class="hist-value">${week.years_of_data}</span></div>
        `;
    }

    function renderRecentObs(s) {
        const el = document.getElementById('recent-obs');
        if (!s.recent_observations || s.recent_observations.length === 0) {
            el.innerHTML = '<p class="no-data">No recent observations.</p>';
            return;
        }

        let html = `<table>
            <thead><tr><th>Date</th><th>Snow</th><th>High/Low</th></tr></thead>
            <tbody>`;
        for (const obs of s.recent_observations.slice(0, 7)) {
            const snow = obs.snowfall_mm > 0 ? obs.snowfall_mm + ' mm' : '—';
            const hi = obs.temp_max_c != null ? Math.round(obs.temp_max_c) + '°' : '—';
            const lo = obs.temp_min_c != null ? Math.round(obs.temp_min_c) + '°' : '—';
            html += `<tr><td>${shortDate(obs.date)}</td><td>${snow}</td><td>${hi}/${lo}</td></tr>`;
        }
        html += '</tbody></table>';
        el.innerHTML = html;
    }

    function renderNearby(currentId, current) {
        const ul = document.getElementById('nearby-list');
        const distances = [];
        for (const [id, s] of Object.entries(stationData.stations)) {
            if (id === currentId) continue;
            const dist = haversine(current.lat, current.lon, s.lat, s.lon);
            distances.push({ id, s, dist });
        }
        distances.sort((a, b) => a.dist - b.dist);

        ul.innerHTML = '';
        for (const d of distances.slice(0, 5)) {
            const li = document.createElement('li');
            const distLabel = d.dist < 100
                ? Math.round(d.dist) + ' km'
                : Math.round(d.dist / 10) * 10 + ' km';
            li.innerHTML = `<span>${d.s.name}</span><span class="nearby-dist">${distLabel} — score ${d.s.snow_score}</span>`;
            li.addEventListener('click', () => selectStation(d.id));
            ul.appendChild(li);
        }
    }

    // ─── Boot ───
    document.addEventListener('DOMContentLoaded', init);
})();
