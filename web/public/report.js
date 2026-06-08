(function () {
    'use strict';

    /* XSS-safe text helper */
    function esc(str) {
        var d = document.createElement('div');
        d.textContent = String(str == null ? '' : str);
        return d.innerHTML;
    }

    function scoreClass(score) {
        if (score >= 60) return 'high';
        if (score >= 30) return 'moderate';
        return 'low';
    }

    function alertBadge(level) {
        var cls = 'alert-' + level.toLowerCase();
        return '<span class="alert-badge ' + esc(cls) + '">' + esc(level) + '</span>';
    }

    function shortDate(dateStr) {
        if (!dateStr) return '-';
        var parts = dateStr.split('-');
        var months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
        return months[parseInt(parts[1], 10) - 1] + ' ' + parseInt(parts[2], 10);
    }

    function snowCell(cm) {
        if (cm >= 5) return '<span class="snow-highlight">' + esc(cm.toFixed(1)) + ' cm</span>';
        return esc(cm.toFixed(1)) + ' cm';
    }

    /* ─── Pipeline Status ─── */
    function renderPipeline(p) {
        var badge = document.getElementById('pipeline-badge');
        var stats = document.getElementById('pipeline-stats');

        if (p.stations_missing.length === 0) {
            badge.textContent = 'ALL OK';
            badge.className = 'status-badge status-ok';
        } else if (p.stations_missing.length <= 5) {
            badge.textContent = p.stations_missing.length + ' MISSING';
            badge.className = 'status-badge status-warn';
        } else {
            badge.textContent = p.stations_missing.length + ' MISSING';
            badge.className = 'status-badge status-error';
        }

        var freshness = p.data_freshness ? new Date(p.data_freshness) : null;
        var freshLabel = freshness ? freshness.toLocaleString() : 'Unknown';

        stats.innerHTML =
            '<div class="stat"><span class="stat-value">' + esc(p.station_count) + '</span><span class="stat-label">Total Stations</span></div>' +
            '<div class="stat"><span class="stat-value">' + esc(p.stations_with_forecast) + '</span><span class="stat-label">With Forecast</span></div>' +
            '<div class="stat"><span class="stat-value">' + esc(p.stations_missing.length) + '</span><span class="stat-label">Missing</span></div>' +
            '<div class="stat"><span class="stat-value" style="font-size:0.95rem">' + esc(freshLabel) + '</span><span class="stat-label">Data Freshness</span></div>';
    }

    /* ─── Score Distribution ─── */
    function renderDistribution(d) {
        var container = document.getElementById('dist-bar');
        var total = d.high + d.moderate + d.low;
        if (total === 0) return;

        var hPct = (d.high / total * 100).toFixed(1);
        var mPct = (d.moderate / total * 100).toFixed(1);
        var lPct = (d.low / total * 100).toFixed(1);

        container.innerHTML =
            '<div class="dist-bar">' +
                '<div class="dist-segment high" style="width:' + hPct + '%">' + (d.high > 0 ? d.high : '') + '</div>' +
                '<div class="dist-segment moderate" style="width:' + mPct + '%">' + (d.moderate > 0 ? d.moderate : '') + '</div>' +
                '<div class="dist-segment low" style="width:' + lPct + '%">' + (d.low > 0 ? d.low : '') + '</div>' +
            '</div>' +
            '<div class="dist-labels">' +
                '<span class="dist-label"><span class="dist-dot" style="background:#3b82f6"></span> High 60+ (' + d.high + ')</span>' +
                '<span class="dist-label"><span class="dist-dot" style="background:#6366f1"></span> Moderate 30-59 (' + d.moderate + ')</span>' +
                '<span class="dist-label"><span class="dist-dot" style="background:rgba(148,163,184,0.6)"></span> Low 0-29 (' + d.low + ')</span>' +
            '</div>';
    }

    /* ─── Top 10 Table ─── */
    function renderTop10(stations) {
        var tbody = document.getElementById('alerts-body');
        var rows = '';

        for (var i = 0; i < stations.length; i++) {
            var s = stations[i];
            var sc = scoreClass(s.snow_score);
            rows +=
                '<tr>' +
                '<td>' + (i + 1) + '</td>' +
                '<td><strong>' + esc(s.name) + '</strong></td>' +
                '<td>' + esc(s.region) + '</td>' +
                '<td class="score-cell score-' + sc + '">' + esc(s.snow_score) + '</td>' +
                '<td>' + snowCell(s.snowfall_7d_cm) + '</td>' +
                '<td>' + snowCell(s.snowfall_3d_cm) + '</td>' +
                '<td>' + esc(shortDate(s.peak_day)) + ' (' + esc(s.peak_day_cm.toFixed(1)) + ' cm)</td>' +
                '<td>' + esc(s.temp_range) + '</td>' +
                '<td>' + alertBadge(s.alert_level) + '</td>' +
                '</tr>';
        }

        tbody.innerHTML = rows;
    }

    /* ─── Active Alerts ─── */
    function renderActiveAlerts(alerts) {
        var countEl = document.getElementById('alert-count');
        var tbody = document.getElementById('active-alerts-body');

        var high = alerts.filter(function (a) { return a.alert_level === 'HIGH'; });
        var moderate = alerts.filter(function (a) { return a.alert_level === 'MODERATE'; });

        countEl.textContent = high.length + ' high, ' + moderate.length + ' moderate';

        var rows = '';
        for (var i = 0; i < alerts.length; i++) {
            var a = alerts[i];
            var sc = scoreClass(a.snow_score);
            rows +=
                '<tr>' +
                '<td>' + esc(a.name) + '</td>' +
                '<td>' + esc(a.region) + '</td>' +
                '<td class="score-cell score-' + sc + '">' + esc(a.snow_score) + '</td>' +
                '<td>' + snowCell(a.snowfall_7d_cm) + '</td>' +
                '<td>' + alertBadge(a.alert_level) + '</td>' +
                '</tr>';
        }

        tbody.innerHTML = rows;
    }

    /* ─── Regional Summary ─── */
    function renderRegions(regions) {
        var grid = document.getElementById('region-grid');
        var html = '';

        for (var i = 0; i < regions.length; i++) {
            var r = regions[i];
            var sc = scoreClass(r.max_score);
            var barColor = sc === 'high' ? '#3b82f6' : sc === 'moderate' ? '#6366f1' : '#64748b';
            var barWidth = Math.min(100, r.avg_score);

            html +=
                '<div class="region-card">' +
                    '<div class="region-name">' + esc(r.display_name) + '</div>' +
                    '<div class="region-stats">' +
                        '<span>' + esc(r.station_count) + ' stations</span>' +
                        '<span>Avg: <strong class="score-' + sc + '">' + esc(r.avg_score) + '</strong></span>' +
                        '<span>Max: <strong class="score-' + sc + '">' + esc(r.max_score) + '</strong></span>' +
                        '<span>7d: ' + esc(r.total_7d_cm.toFixed(1)) + ' cm</span>' +
                    '</div>' +
                    '<div class="region-top">Top: ' + esc(r.top_station) + '</div>' +
                    '<div class="region-bar"><div class="region-bar-fill" style="width:' + barWidth + '%;background:' + barColor + '"></div></div>' +
                '</div>';
        }

        grid.innerHTML = html;
    }

    /* ─── Wisconsin ─── */
    function renderWisconsin(wi) {
        var el = document.getElementById('wisconsin-content');
        if (!wi) {
            el.innerHTML = '<p style="color:#64748b;font-style:italic">No Wisconsin forecast data available</p>';
            return;
        }

        el.innerHTML =
            '<div class="wi-grid">' +
                '<div class="wi-stat"><span class="wi-stat-value">' + esc(wi.forecast_days) + '</span><span class="wi-stat-label">Forecast Days</span></div>' +
                '<div class="wi-stat"><span class="wi-stat-value">' + esc(wi.snow_days) + '</span><span class="wi-stat-label">Snow Days (&ge;30%)</span></div>' +
                '<div class="wi-stat"><span class="wi-stat-value">' + esc(wi.max_probability) + '%</span><span class="wi-stat-label">Max Probability</span></div>' +
                '<div class="wi-stat"><span class="wi-stat-value">' + esc(wi.max_probability_snowfall || 'N/A') + '</span><span class="wi-stat-label">Expected Snowfall</span></div>' +
            '</div>';
    }

    /* ─── Main Load ─── */
    async function loadReport() {
        try {
            var resp = await fetch('daily_report.json');
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            var data = await resp.json();

            document.getElementById('nav-updated').textContent = 'Updated: ' + (data.generated_at_human || '');

            renderPipeline(data.pipeline);
            renderDistribution(data.score_distribution);
            renderTop10(data.top_10_stations || []);
            renderActiveAlerts(data.snow_alerts || []);
            renderRegions(data.regional_summary || []);
            renderWisconsin(data.wisconsin_summary);
        } catch (err) {
            document.querySelector('.report-container').innerHTML =
                '<div class="error-msg">Unable to load report data. The daily report may not have been generated yet.</div>';
        }
    }

    loadReport();
    setInterval(loadReport, 30 * 60 * 1000);
})();
