/* Snow Trip Planner — Futurecast-First Overhaul */

(function () {
    'use strict';

    // ─── XSS-safe text helper ───
    function esc(str) {
        var el = document.createElement('span');
        el.textContent = String(str == null ? '' : str);
        return el.innerHTML;
    }

    // ─── State ───
    var stationData = null;
    var map = null;
    var markers = {};
    var selectedId = null;
    var currentMonth = new Date().getMonth();
    var currentYear = new Date().getFullYear();
    var useImperial = false;
    var currentView = 'regions'; // 'regions' | 'heatmap'

    // ─── Resort aliases for search ───
    var RESORT_ALIASES = {
        'phelps_wi': ['phelps', 'nicolet'],
        'land_o_lakes_wi': ['land o lakes', 'land o\'lakes'],
        'eagle_river_wi': ['eagle river'],
        'vail_co': ['vail', 'vail mountain'],
        'aspen_co': ['aspen', 'snowmass', 'aspen snowmass'],
        'breckenridge_co': ['breck', 'breckenridge'],
        'steamboat_springs_co': ['steamboat', 'the boat'],
        'telluride_co': ['telluride', 't-ride'],
        'mammoth_mountain_ca': ['mammoth', 'mammoth mountain'],
        'lake_tahoe_ca': ['tahoe', 'heavenly', 'palisades', 'kirkwood', 'northstar', 'squaw'],
        'mount_shasta_ca': ['shasta', 'mt shasta'],
        'big_bear_ca': ['big bear', 'snow summit', 'bear mountain'],
        'mount_baker_wa': ['baker', 'mt baker'],
        'stevens_pass_wa': ['stevens', 'stevens pass'],
        'mount_hood_or': ['hood', 'timberline', 'mt hood meadows'],
        'niseko_japan': ['niseko', 'japow'],
        'hakuba_japan': ['hakuba', 'happo one'],
        'chamonix_france': ['chamonix', 'cham', 'mont blanc'],
        'zermatt_switzerland': ['zermatt', 'matterhorn'],
        'st_moritz_switzerland': ['st moritz', 'saint moritz'],
        'innsbruck_austria': ['innsbruck', 'nordkette'],
        'whistler_bc': ['whistler', 'blackcomb', 'whistler blackcomb'],
        'revelstoke_bc': ['revelstoke', 'revy'],
        'banff_ab': ['banff', 'sunshine village'],
        'lake_louise_ab': ['lake louise'],
        'queenstown_nz': ['queenstown', 'remarkables', 'coronet peak'],
    };

    // ─── WMO weather code \u2192 emoji ───
    var WMO_ICONS = {
        0: '\u2600\uFE0F', 1: '\uD83C\uDF24\uFE0F', 2: '\u26C5', 3: '\u2601\uFE0F',
        45: '\uD83C\uDF2B\uFE0F', 48: '\uD83C\uDF2B\uFE0F',
        51: '\uD83C\uDF27\uFE0F', 53: '\uD83C\uDF27\uFE0F', 55: '\uD83C\uDF27\uFE0F',
        56: '\uD83C\uDF27\uFE0F', 57: '\uD83C\uDF27\uFE0F',
        61: '\uD83C\uDF27\uFE0F', 63: '\uD83C\uDF27\uFE0F', 65: '\uD83C\uDF27\uFE0F',
        66: '\uD83C\uDF27\uFE0F', 67: '\uD83C\uDF27\uFE0F',
        71: '\uD83C\uDF28\uFE0F', 73: '\u2744\uFE0F', 75: '\u2744\uFE0F',
        77: '\u2744\uFE0F',
        80: '\uD83C\uDF26\uFE0F', 81: '\uD83C\uDF26\uFE0F', 82: '\uD83C\uDF26\uFE0F',
        85: '\uD83C\uDF28\uFE0F', 86: '\u2744\uFE0F',
        95: '\u26C8\uFE0F', 96: '\u26C8\uFE0F', 99: '\u26C8\uFE0F',
    };

    function wmoIcon(code) { return WMO_ICONS[code] || '\uD83C\uDF21\uFE0F'; }

    // ─── Futurecast color scale ───
    function fcColor(score) {
        if (score >= 80) return '#22c55e';
        if (score >= 60) return '#3b82f6';
        if (score >= 40) return '#6366f1';
        if (score >= 20) return '#f59e0b';
        return '#94a3b8';
    }

    function fcLabel(score) {
        if (score >= 80) return 'Excellent';
        if (score >= 60) return 'Good';
        if (score >= 40) return 'Moderate';
        if (score >= 20) return 'Fair';
        return 'Poor';
    }

    function scoreBadgeClass(score) {
        if (score >= 60) return 'badge-high';
        if (score >= 30) return 'badge-moderate';
        return 'badge-low';
    }

    // ─── Unit formatting ───
    function fmtSnow(cm) {
        if (cm == null || cm === 0) return '\u2014';
        if (useImperial) return (cm / 2.54).toFixed(1) + ' in';
        return cm.toFixed(1) + ' cm';
    }

    function fmtSnowMm(mm) {
        if (mm == null || mm === 0) return '\u2014';
        if (useImperial) return (mm / 25.4).toFixed(1) + ' in';
        return mm.toFixed(1) + ' mm';
    }

    function fmtTemp(c) {
        if (c == null) return '\u2014';
        if (useImperial) return Math.round(c * 9 / 5 + 32) + '\u00B0F';
        return Math.round(c) + '\u00B0C';
    }

    function fmtWind(kmh) {
        if (kmh == null) return '';
        if (useImperial) return Math.round(kmh * 0.621) + ' mph';
        return Math.round(kmh) + ' km/h';
    }

    function unitLabel() { return useImperial ? 'in' : 'cm'; }
    function unitLabelMm() { return useImperial ? 'in' : 'mm'; }

    // ─── Haversine distance ───
    function haversine(lat1, lon1, lat2, lon2) {
        var R = 6371, dLat = (lat2 - lat1) * Math.PI / 180, dLon = (lon2 - lon1) * Math.PI / 180;
        var a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon / 2) ** 2;
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

    // ─── Date helpers ───
    function dateStr(d) { return d.toISOString().slice(0, 10); }

    function shortDate(s) {
        var d = new Date(s + 'T00:00:00');
        return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    }

    function shortDay(s) {
        var d = new Date(s + 'T00:00:00');
        return d.toLocaleDateString('en-US', { weekday: 'short' });
    }

    var MONTH_NAMES = ['January','February','March','April','May','June',
                       'July','August','September','October','November','December'];
    var MONTH_SHORT = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

    // ─── ISO week from date ───
    function isoWeekNum(ds) {
        var d = new Date(ds + 'T00:00:00');
        var dayNum = d.getDay() || 7;
        d.setDate(d.getDate() + 4 - dayNum);
        var yearStart = new Date(d.getFullYear(), 0, 1);
        return Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
    }

    // ─── Weeks in a given month ───
    function monthToWeeks(month, year) {
        var weeks = [];
        var d = new Date(year, month, 1);
        var last = new Date(year, month + 1, 0);
        while (d <= last) {
            var wk = isoWeekNum(dateStr(d));
            if (weeks.indexOf(wk) === -1) weeks.push(wk);
            d.setDate(d.getDate() + 7);
        }
        return weeks;
    }

    // ─── Get station's futurecast score for given month ───
    function stationMonthScore(s, month, year) {
        var fc = s.futurecast || {};
        if (Object.keys(fc).length === 0) return s.snow_score || 0;
        var weeks = monthToWeeks(month, year);
        var total = 0, count = 0;
        for (var i = 0; i < weeks.length; i++) {
            var wd = fc[String(weeks[i])];
            if (wd) { total += wd.score; count++; }
        }
        return count > 0 ? Math.round(total / count) : 0;
    }

    // ─── Init ───
    async function init() {
        showLoading(true);
        initMap();
        await loadData();
        showLoading(false);
        setupSearch();
        setupMonthSelector();
        setupUnitToggle();
        setupCloseButton();
        setupCollapsibles();
        setupViewToggle();
        setupPlanTripButton();
        setDefaultDates();
        updateFuturecastView();
    }

    function showLoading(show) {
        var el = document.getElementById('loading-overlay');
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
        var arrive = new Date(); arrive.setDate(arrive.getDate() + 1);
        var depart = new Date(); depart.setDate(depart.getDate() + 4);
        var arriveEl = document.getElementById('date-arrive');
        var departEl = document.getElementById('date-depart');
        if (arriveEl) arriveEl.value = dateStr(arrive);
        if (departEl) departEl.value = dateStr(depart);
    }

    // ─── Map ───
    function initMap() {
        map = L.map('map', { center: [45, 10], zoom: 3, zoomControl: true, attributionControl: true });
        L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/">CARTO</a>',
            subdomains: 'abcd', maxZoom: 19,
        }).addTo(map);
    }

    // ─── Load data ───
    async function loadData() {
        try {
            var resp = await fetch('station_data.json');
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            stationData = await resp.json();
            document.getElementById('station-count').textContent = stationData.station_count + ' stations';
            if (stationData.generated_at_human) {
                document.getElementById('nav-updated').textContent = 'Updated ' + stationData.generated_at_human;
            }
            addMarkers();
            buildRegionList();
        } catch (err) {
            document.getElementById('station-count').textContent = 'Data unavailable';
        }
    }

    // ─── Markers (colored by futurecast) ───
    function addMarkers() {
        var stations = stationData.stations;
        for (var id in stations) {
            var s = stations[id];
            var score = stationMonthScore(s, currentMonth, currentYear);
            var color = fcColor(score);
            var marker = L.circleMarker([s.lat, s.lon], {
                radius: 7, fillColor: color, color: '#fff',
                weight: 1, opacity: 0.8, fillOpacity: 0.8,
            }).addTo(map);

            marker.bindPopup(buildPopup(id, s));
            (function (mid) {
                marker.on('popupopen', function () {
                    var link = document.querySelector('.popup-link[data-id="' + CSS.escape(mid) + '"]');
                    if (link) link.addEventListener('click', function () { selectStation(mid); });
                });
            })(id);

            markers[id] = marker;
        }
    }

    function buildPopup(id, s) {
        var score = stationMonthScore(s, currentMonth, currentYear);
        var total7 = s.forecast.snowfall_cm.slice(0, 7).reduce(function (a, b) { return a + b; }, 0);
        return '<div class="popup-name">' + esc(s.name) + '</div>' +
            '<div class="popup-region">' + esc(regionDisplayName(s.region)) + '</div>' +
            '<div class="popup-snow">Futurecast: <strong>' + score + '</strong>/100 \u2014 ' + fcLabel(score) + '</div>' +
            '<div class="popup-snow">' + wmoIcon(s.forecast.weather_code[0]) +
                ' <strong>' + fmtSnow(total7) + '</strong> next 7 days</div>' +
            '<div class="popup-link" data-id="' + esc(id) + '">View details</div>';
    }

    function updateMarkerColors() {
        if (!stationData) return;
        for (var id in stationData.stations) {
            if (!markers[id]) continue;
            var s = stationData.stations[id];
            var score = stationMonthScore(s, currentMonth, currentYear);
            markers[id].setStyle({ fillColor: fcColor(score) });
            markers[id].setPopupContent(buildPopup(id, s));
        }
    }

    function regionDisplayName(key) {
        if (stationData && stationData.regions && stationData.regions[key]) {
            return stationData.regions[key].display_name;
        }
        return key.replace(/_/g, ' ').replace(/\b\w/g, function (c) { return c.toUpperCase(); });
    }

    // ─── Region list ───
    function buildRegionList() {
        var ul = document.getElementById('region-list');
        var regions = stationData.regions;
        ul.innerHTML = '';
        for (var key in regions) {
            var r = regions[key];
            var li = document.createElement('li');
            li.innerHTML = '<span>' + esc(r.display_name) + '</span><span class="region-count">' + esc(r.stations.length) + '</span>';
            (function (k) { li.addEventListener('click', function () { zoomToRegion(k); }); })(key);
            ul.appendChild(li);
        }
    }

    function zoomToRegion(regionKey) {
        var ids = stationData.regions[regionKey].stations;
        var lats = [], lons = [];
        for (var i = 0; i < ids.length; i++) {
            var s = stationData.stations[ids[i]];
            if (s) { lats.push(s.lat); lons.push(s.lon); }
        }
        if (lats.length === 0) return;
        map.fitBounds([[Math.min.apply(null, lats) - 1, Math.min.apply(null, lons) - 2],
                        [Math.max.apply(null, lats) + 1, Math.max.apply(null, lons) + 2]]);
    }

    // ─── Search with aliases ───
    function setupSearch() {
        var input = document.getElementById('search-input');
        var results = document.getElementById('search-results');

        input.addEventListener('input', function () {
            var q = this.value.trim().toLowerCase();
            results.innerHTML = '';
            if (q.length < 2 || !stationData) return;

            var matches = [];
            for (var id in stationData.stations) {
                var s = stationData.stations[id];
                var nameMatch = s.name.toLowerCase().indexOf(q) >= 0;
                var regionMatch = regionDisplayName(s.region).toLowerCase().indexOf(q) >= 0;
                var aliases = RESORT_ALIASES[id] || [];
                var aliasMatch = false;
                for (var a = 0; a < aliases.length; a++) {
                    if (aliases[a].indexOf(q) >= 0) { aliasMatch = true; break; }
                }
                if (nameMatch || regionMatch || aliasMatch) {
                    matches.push({ id: id, s: s, nameMatch: nameMatch, aliasMatch: aliasMatch });
                }
            }

            matches.sort(function (a, b) {
                if (a.nameMatch !== b.nameMatch) return b.nameMatch ? 1 : -1;
                if (a.aliasMatch !== b.aliasMatch) return b.aliasMatch ? 1 : -1;
                return 0;
            });

            var shown = matches.slice(0, 25);
            for (var i = 0; i < shown.length; i++) {
                var m = shown[i];
                var li = document.createElement('li');
                li.innerHTML = '<span>' + esc(m.s.name) + '</span><span class="result-region">' + esc(regionDisplayName(m.s.region)) + '</span>';
                (function (mid, mname) {
                    li.addEventListener('click', function () {
                        selectStation(mid);
                        input.value = mname;
                        results.innerHTML = '';
                    });
                })(m.id, m.s.name);
                results.appendChild(li);
            }

            if (matches.length > 25) {
                var more = document.createElement('li');
                more.className = 'search-more';
                more.textContent = (matches.length - 25) + ' more results...';
                results.appendChild(more);
            }
        });

        document.addEventListener('click', function (e) {
            if (!e.target.closest('.panel-section')) results.innerHTML = '';
        });
    }

    // ─── Month selector ───
    function setupMonthSelector() {
        document.getElementById('month-prev').addEventListener('click', function () {
            currentMonth--;
            if (currentMonth < 0) { currentMonth = 11; currentYear--; }
            updateFuturecastView();
        });
        document.getElementById('month-next').addEventListener('click', function () {
            currentMonth++;
            if (currentMonth > 11) { currentMonth = 0; currentYear++; }
            updateFuturecastView();
        });
    }

    function updateFuturecastView() {
        document.getElementById('month-display').textContent = MONTH_NAMES[currentMonth] + ' ' + currentYear;

        // ENSO badge
        if (stationData && stationData.enso_phase) {
            var badge = document.getElementById('enso-badge');
            var labels = { 'la_nina': 'La Ni\u00F1a', 'el_nino': 'El Ni\u00F1o', 'neutral': 'Neutral' };
            badge.textContent = 'ENSO: ' + (labels[stationData.enso_phase] || 'Unknown');
            badge.className = 'enso-badge enso-' + stationData.enso_phase;
        }

        updateMarkerColors();

        if (currentView === 'heatmap') renderHeatmap();
        if (selectedId) renderStationFuturecast(selectedId);
    }

    // ─── Unit toggle ───
    function setupUnitToggle() {
        var btn = document.getElementById('unit-toggle');
        btn.addEventListener('click', function () {
            useImperial = !useImperial;
            btn.textContent = useImperial ? 'in' : 'cm';
            btn.classList.toggle('imperial', useImperial);
            if (selectedId) renderDetail(selectedId);
        });
    }

    // ─── View toggle (Regions / Heatmap) ───
    function setupViewToggle() {
        var btns = document.querySelectorAll('#view-toggle .view-btn');
        btns.forEach(function (btn) {
            btn.addEventListener('click', function () {
                btns.forEach(function (b) { b.classList.remove('active'); });
                btn.classList.add('active');
                currentView = btn.dataset.view;
                if (currentView === 'heatmap') {
                    document.getElementById('heatmap-section').style.display = 'block';
                    document.getElementById('region-list-section').style.display = 'none';
                    renderHeatmap();
                } else {
                    document.getElementById('heatmap-section').style.display = 'none';
                    if (!selectedId) document.getElementById('region-list-section').style.display = 'block';
                }
            });
        });
    }

    // ─── Collapsible sections ───
    function setupCollapsibles() {
        document.querySelectorAll('.collapsible-header').forEach(function (header) {
            header.addEventListener('click', function () {
                var body = header.nextElementSibling;
                var icon = header.querySelector('.collapse-icon');
                var isOpen = body.style.display !== 'none';
                body.style.display = isOpen ? 'none' : 'block';
                icon.textContent = isOpen ? '+' : '\u2212';
            });
        });
    }

    // ─── Plan Trip button ───
    function setupPlanTripButton() {
        var btn = document.getElementById('plan-trip-btn');
        if (!btn) return;
        btn.addEventListener('click', function () {
            validateDates();
            if (selectedId) renderForecastStrip(stationData.stations[selectedId]);
            btn.classList.add('btn-applied');
            setTimeout(function () { btn.classList.remove('btn-applied'); }, 1500);
        });
    }

    // ─── Close / deselect ───
    function setupCloseButton() {
        document.getElementById('detail-close').addEventListener('click', deselectStation);
    }

    function deselectStation() {
        selectedId = null;
        for (var mid in markers) {
            markers[mid].setStyle({ weight: 1, color: '#fff', radius: 7 });
        }
        document.getElementById('station-detail').style.display = 'none';
        if (currentView === 'heatmap') {
            document.getElementById('heatmap-section').style.display = 'block';
        } else {
            document.getElementById('region-list-section').style.display = 'block';
        }
    }

    // ─── Select station ───
    function selectStation(id) {
        selectedId = id;
        var s = stationData.stations[id];
        if (!s) return;

        map.setView([s.lat, s.lon], 6);

        for (var mid in markers) {
            markers[mid].setStyle({
                weight: mid === id ? 3 : 1,
                color: mid === id ? '#f59e0b' : '#fff',
                radius: mid === id ? 10 : 7,
            });
        }

        document.getElementById('station-detail').style.display = 'block';
        document.getElementById('region-list-section').style.display = 'none';
        document.getElementById('heatmap-section').style.display = 'none';

        renderDetail(id);
    }

    // ─── Render detail ───
    function renderDetail(id) {
        var s = stationData.stations[id];
        if (!s) return;

        document.getElementById('detail-name').textContent = s.name;
        document.getElementById('detail-region').textContent = regionDisplayName(s.region);

        var score = stationMonthScore(s, currentMonth, currentYear);
        var scoreEl = document.getElementById('detail-score');
        scoreEl.textContent = 'Futurecast: ' + score + '/100 \u2014 ' + fcLabel(score);
        scoreEl.className = 'snow-score-badge ' + scoreBadgeClass(score);

        renderStationFuturecast(id);
        renderForecastStrip(s);
        renderHistoryCard(s);
        renderRecentObs(s);
        renderNearby(id, s);
    }

    // ─── Futurecast weekly outlook ───
    function renderStationFuturecast(id) {
        var s = stationData.stations[id];
        var container = document.getElementById('station-futurecast');
        var fc = s.futurecast || {};

        if (Object.keys(fc).length === 0) {
            container.innerHTML = '<p class="no-data">No historical data available for futurecast. Data is being collected and will appear soon.</p>';
            return;
        }

        var html = '';
        for (var mOffset = 0; mOffset < 6; mOffset++) {
            var m = (currentMonth + mOffset) % 12;
            var y = currentYear + Math.floor((currentMonth + mOffset) / 12);
            var weeks = monthToWeeks(m, y);

            html += '<div class="fc-month-group">';
            html += '<div class="fc-month-label">' + esc(MONTH_SHORT[m]) + ' ' + esc(y) + '</div>';
            html += '<div class="fc-week-bars">';

            for (var i = 0; i < weeks.length; i++) {
                var wk = weeks[i];
                var wd = fc[String(wk)];
                var score = wd ? wd.score : 0;
                var label = wd ? wd.week_label : 'Week ' + wk;
                var snowMm = wd ? wd.avg_daily_snow_mm : 0;
                var weeklySnow = snowMm * 7;
                var color = fcColor(score);

                var tip = label + ' | Score: ' + score + '/100';
                if (snowMm > 0) tip += ' | Avg: ' + fmtSnowMm(weeklySnow) + '/week';

                html += '<div class="fc-week-bar" title="' + esc(tip) + '">' +
                    '<div class="fc-bar-fill" style="height:' + score + '%;background:' + color + '"></div>' +
                    '<div class="fc-bar-score">' + (score > 0 ? score : '') + '</div>' +
                '</div>';
            }
            html += '</div></div>';
        }
        container.innerHTML = html;
    }

    // ─── Heatmap view ───
    function renderHeatmap() {
        if (!stationData) return;
        var wrap = document.getElementById('heatmap-wrap');

        // Collect 6 months of weeks
        var allWeeks = [];
        var monthHeaders = [];
        for (var mOffset = 0; mOffset < 6; mOffset++) {
            var m = (currentMonth + mOffset) % 12;
            var y = currentYear + Math.floor((currentMonth + mOffset) / 12);
            var weeks = monthToWeeks(m, y);
            monthHeaders.push({ label: MONTH_SHORT[m], span: weeks.length });
            for (var w = 0; w < weeks.length; w++) {
                if (allWeeks.indexOf(weeks[w]) === -1) allWeeks.push(weeks[w]);
            }
        }

        // Rank stations by peak futurecast score
        var ranked = [];
        for (var id in stationData.stations) {
            var s = stationData.stations[id];
            var fc = s.futurecast || {};
            var peak = 0;
            for (var wi = 0; wi < allWeeks.length; wi++) {
                var wd = fc[String(allWeeks[wi])];
                if (wd && wd.score > peak) peak = wd.score;
            }
            ranked.push({ id: id, s: s, peak: peak });
        }
        ranked.sort(function (a, b) { return b.peak - a.peak; });
        ranked = ranked.slice(0, 20);

        var html = '<table class="heatmap-table"><thead>';
        // Month header row
        html += '<tr><th></th>';
        for (var mh = 0; mh < monthHeaders.length; mh++) {
            html += '<th colspan="' + monthHeaders[mh].span + '" class="hm-month">' + esc(monthHeaders[mh].label) + '</th>';
        }
        html += '</tr></thead><tbody>';

        for (var r = 0; r < ranked.length; r++) {
            var item = ranked[r];
            var fc2 = item.s.futurecast || {};
            html += '<tr class="heatmap-row" data-id="' + esc(item.id) + '">';
            html += '<td class="hm-name">' + esc(item.s.name.split(',')[0]) + '</td>';
            for (var c = 0; c < allWeeks.length; c++) {
                var wd2 = fc2[String(allWeeks[c])];
                var sc = wd2 ? wd2.score : 0;
                var op = sc > 0 ? (0.3 + sc / 100 * 0.7).toFixed(2) : '0.15';
                html += '<td class="hm-cell" style="background:' + fcColor(sc) + ';opacity:' + op +
                    '" title="Wk ' + allWeeks[c] + ': ' + sc + '">' + (sc > 0 ? sc : '') + '</td>';
            }
            html += '</tr>';
        }
        html += '</tbody></table>';

        if (ranked.length === 0 || ranked[0].peak === 0) {
            html = '<p class="no-data">No futurecast data available yet. Historical data is being collected nightly.</p>';
        }

        wrap.innerHTML = html;

        wrap.querySelectorAll('.heatmap-row').forEach(function (row) {
            row.addEventListener('click', function () { selectStation(row.dataset.id); });
        });
    }

    // ─── Forecast strip (16-day live weather) ───
    function renderForecastStrip(s) {
        var strip = document.getElementById('forecast-strip');
        if (!strip) return;
        var arriveEl = document.getElementById('date-arrive');
        var departEl = document.getElementById('date-depart');
        var arrive = arriveEl ? arriveEl.value : '';
        var depart = departEl ? departEl.value : '';
        var fc = s.forecast;

        var html = '';
        for (var i = 0; i < fc.dates.length; i++) {
            var d = fc.dates[i];
            var inRange = arrive && depart && d >= arrive && d <= depart;
            var snow = fc.snowfall_cm[i];
            var icon = wmoIcon(fc.weather_code[i]);
            var tMax = fc.temp_max_c[i];
            var tMin = fc.temp_min_c[i];
            var wind = fc.wind_speed_max_kmh ? fc.wind_speed_max_kmh[i] : null;

            html += '<div class="fc-day' + (inRange ? ' in-range' : '') + '">' +
                '<div class="fc-date">' + esc(shortDay(d)) + '<br>' + esc(shortDate(d)) + '</div>' +
                '<div class="fc-icon">' + icon + '</div>' +
                '<div class="fc-snow">' + (snow > 0 ? fmtSnow(snow) : '\u2014') + '</div>' +
                '<div class="fc-temp">' + fmtTemp(tMax) + '/' + fmtTemp(tMin) + '</div>' +
                (wind ? '<div class="fc-wind">' + esc(fmtWind(wind)) + '</div>' : '') +
            '</div>';
        }
        strip.innerHTML = html;
    }

    // ─── Date validation ───
    function validateDates() {
        var warn = document.getElementById('date-warning');
        if (!warn) return;
        var arrive = document.getElementById('date-arrive').value;
        var depart = document.getElementById('date-depart').value;
        if (!arrive || !depart) { warn.textContent = ''; return; }
        if (depart < arrive) { warn.textContent = 'Depart date is before arrive date.'; return; }
        if (stationData) {
            var first = Object.values(stationData.stations)[0];
            if (first && first.forecast.dates.length > 0) {
                var lastDate = first.forecast.dates[first.forecast.dates.length - 1];
                if (arrive > lastDate) {
                    warn.textContent = 'Dates are beyond the 16-day forecast window. Use Futurecast for longer planning.';
                    return;
                }
            }
        }
        warn.textContent = '';
    }

    // ─── Historical card ───
    function renderHistoryCard(s) {
        var card = document.getElementById('history-card');
        var arriveEl = document.getElementById('date-arrive');
        var arrive = arriveEl ? arriveEl.value : dateStr(new Date());
        var weekNum = isoWeekNum(arrive);
        var week = s.climatology.weeks[String(weekNum)];
        if (!week) {
            card.innerHTML = '<p class="no-data">No historical data for this period.</p>';
            return;
        }

        card.innerHTML =
            '<div class="history-row"><span class="hist-label">Week</span><span class="hist-value">' + esc(week.week_label) + '</span></div>' +
            '<div class="history-row"><span class="hist-label">Avg daily snowfall</span><span class="hist-value">' + fmtSnowMm(week.avg_daily_snowfall_mm) + '</span></div>' +
            '<div class="history-row"><span class="hist-label">Snow day probability</span><span class="hist-value">' + esc(Math.round(week.snow_day_probability * 100)) + '%</span></div>' +
            '<div class="history-row"><span class="hist-label">Avg high / low</span><span class="hist-value">' + fmtTemp(week.avg_temp_max_c) + ' / ' + fmtTemp(week.avg_temp_min_c) + '</span></div>' +
            '<div class="history-row"><span class="hist-label">Max recorded</span><span class="hist-value">' + fmtSnowMm(week.max_recorded_snowfall_mm) + '</span></div>' +
            '<div class="history-row"><span class="hist-label">Years of data</span><span class="hist-value">' + esc(week.years_of_data) + '</span></div>';
    }

    // ─── Recent observations ───
    function renderRecentObs(s) {
        var el = document.getElementById('recent-obs');
        if (!s.recent_observations || s.recent_observations.length === 0) {
            el.innerHTML = '<p class="no-data">No recent observations.</p>';
            return;
        }

        var html = '<table><thead><tr><th>Date</th><th>Snow</th><th>High/Low</th></tr></thead><tbody>';
        var obs = s.recent_observations.slice(0, 7);
        for (var i = 0; i < obs.length; i++) {
            var o = obs[i];
            html += '<tr><td>' + esc(shortDate(o.date)) + '</td>' +
                '<td>' + (o.snowfall_mm > 0 ? fmtSnowMm(o.snowfall_mm) : '\u2014') + '</td>' +
                '<td>' + fmtTemp(o.temp_max_c) + '/' + fmtTemp(o.temp_min_c) + '</td></tr>';
        }
        html += '</tbody></table>';
        el.innerHTML = html;
    }

    // ─── Nearby stations ───
    function renderNearby(currentId, current) {
        var ul = document.getElementById('nearby-list');
        var distances = [];
        for (var id in stationData.stations) {
            if (id === currentId) continue;
            var s = stationData.stations[id];
            distances.push({ id: id, s: s, dist: haversine(current.lat, current.lon, s.lat, s.lon) });
        }
        distances.sort(function (a, b) { return a.dist - b.dist; });

        ul.innerHTML = '';
        var top5 = distances.slice(0, 5);
        for (var i = 0; i < top5.length; i++) {
            var d = top5[i];
            var li = document.createElement('li');
            var distLabel = d.dist < 100 ? Math.round(d.dist) + ' km' : Math.round(d.dist / 10) * 10 + ' km';
            var nearScore = stationMonthScore(d.s, currentMonth, currentYear);
            li.innerHTML = '<span>' + esc(d.s.name) + '</span><span class="nearby-dist">' + esc(distLabel) + ' \u2014 score ' + esc(nearScore) + '</span>';
            (function (did) { li.addEventListener('click', function () { selectStation(did); }); })(d.id);
            ul.appendChild(li);
        }
    }

    // ─── Boot ───
    document.addEventListener('DOMContentLoaded', init);
})();
