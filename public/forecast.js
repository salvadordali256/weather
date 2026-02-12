/* Northern Wisconsin Snow Forecast — frontend logic */

(function () {
    'use strict';

    // HTML-escape to prevent XSS from JSON data
    function esc(str) {
        var el = document.createElement('span');
        el.textContent = String(str);
        return el.innerHTML;
    }

    async function loadForecast() {
        try {
            var response = await fetch('latest_forecast.json');
            var data = await response.json();
            renderForecast(data);
        } catch (error) {
            document.getElementById('forecast-grid').innerHTML =
                '<p class="error">Unable to load forecast data. Please try again later.</p>';
        }
    }

    function renderForecast(data) {
        // Update timestamp
        document.getElementById('updated').textContent =
            'Last updated: ' + data.generated_at_human;

        // Render forecast cards
        var grid = document.getElementById('forecast-grid');
        var validLevels = ['high', 'moderate', 'low', 'minimal'];
        grid.innerHTML = data.forecasts.map(function (forecast) {
            var level = String(forecast.alert_level).toLowerCase();
            if (validLevels.indexOf(level) === -1) level = 'minimal';
            return '<div class="forecast-card ' + level + '">' +
                '<div class="day">' + esc(forecast.day_of_week) + '</div>' +
                '<div class="date">' + esc(formatDate(forecast.date)) + '</div>' +
                '<div class="icon">' + esc(forecast.icon) + '</div>' +
                '<div class="probability">' + esc(forecast.probability) + '%</div>' +
                '<div class="event-type">' + esc(forecast.event_type) + '</div>' +
                '<div class="snowfall">' + esc(forecast.expected_snowfall) + '</div>' +
                '<div class="confidence">Confidence: ' + esc(forecast.confidence) + '</div>' +
            '</div>';
        }).join('');

        // Render summary
        var summaryEl = document.getElementById('summary');
        var pre = document.createElement('pre');
        pre.textContent = data.summary_text;
        summaryEl.innerHTML = '';
        summaryEl.appendChild(pre);

        // Render observations
        var obsList = document.getElementById('observations-list');
        if (data.recent_observations && data.recent_observations.length > 0) {
            obsList.innerHTML = '<table>' +
                '<thead><tr><th>Station</th><th>Date</th><th>Snowfall</th></tr></thead>' +
                '<tbody>' +
                data.recent_observations.slice(0, 10).map(function (obs) {
                    return '<tr>' +
                        '<td>' + esc(obs.station) + '</td>' +
                        '<td>' + esc(obs.date) + '</td>' +
                        '<td>' + esc(obs.snowfall_inches) + '" (' + esc(obs.snowfall_mm) + 'mm)</td>' +
                    '</tr>';
                }).join('') +
                '</tbody></table>';
        } else {
            obsList.innerHTML = '<p>No recent observations available.</p>';
        }
    }

    function formatDate(dateStr) {
        var date = new Date(dateStr);
        return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    }

    // Load forecast on page load
    loadForecast();

    // Refresh every 30 minutes
    setInterval(loadForecast, 30 * 60 * 1000);
})();
