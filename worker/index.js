/**
 * Snow Trip Planner API — Cloudflare Worker
 *
 * Routes:
 *   GET /api/stations          — station index (id, name, lat, lon, snow_score)
 *   GET /api/forecast/:id      — full station data from KV
 *   GET /api/search?q=chamonix — fuzzy name/region match
 *   GET /api/nearby?lat=&lon=&limit= — haversine nearest stations
 */

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', ...CORS_HEADERS },
  });
}

function haversine(lat1, lon1, lat2, lon2) {
  const R = 6371;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

async function getIndex(kv) {
  const raw = await kv.get('station_index', 'json');
  return raw || [];
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    // GET /api/stations
    if (path === '/api/stations') {
      const index = await getIndex(env.STATION_KV);
      return json({ stations: index, count: index.length });
    }

    // GET /api/forecast/:station_id
    const forecastMatch = path.match(/^\/api\/forecast\/([a-z0-9_]+)$/);
    if (forecastMatch) {
      const id = forecastMatch[1];
      const data = await env.STATION_KV.get(`station:${id}`, 'json');
      if (!data) return json({ error: 'Station not found' }, 404);
      return json(data);
    }

    // GET /api/search?q=...
    if (path === '/api/search') {
      const q = (url.searchParams.get('q') || '').toLowerCase().trim();
      if (q.length < 2) return json({ results: [] });

      const index = await getIndex(env.STATION_KV);
      const results = index
        .filter(
          (s) =>
            s.name.toLowerCase().includes(q) ||
            s.region.toLowerCase().includes(q)
        )
        .slice(0, 10);
      return json({ results });
    }

    // GET /api/nearby?lat=&lon=&limit=
    if (path === '/api/nearby') {
      const lat = parseFloat(url.searchParams.get('lat'));
      const lon = parseFloat(url.searchParams.get('lon'));
      const limit = parseInt(url.searchParams.get('limit') || '5', 10);
      if (isNaN(lat) || isNaN(lon)) {
        return json({ error: 'lat and lon required' }, 400);
      }

      const index = await getIndex(env.STATION_KV);
      const withDist = index
        .map((s) => ({ ...s, distance_km: Math.round(haversine(lat, lon, s.lat, s.lon)) }))
        .sort((a, b) => a.distance_km - b.distance_km)
        .slice(0, limit);
      return json({ stations: withDist });
    }

    return json({ error: 'Not found' }, 404);
  },
};
