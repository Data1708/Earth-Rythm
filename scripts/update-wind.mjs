#!/usr/bin/env node
// ---------------------------------------------------------------
// scripts/update-wind.mjs
//
// Fetches global wind data from Open-Meteo and writes a downsampled JSON
// wind grid for the frontend.
//
// Why Open-Meteo (not raw NOAA GFS)?
//   - No GRIB2 parsing (no wgrib2/eccodes/system tools)
//   - Free tier, no API key
//   - Aggregates multiple models (GFS, ECMWF IFS, ICON) for best quality
//
// Rate limit handling:
//   Open-Meteo's free tier limits to ~600 calls/min.  We use 5° resolution
//   (2,664 cells fetched in 27 batches of 100) and pause 1.2s between
//   batches — a calm pace that stays well under any limit.
//
// Output: public/data/wind.json (~30KB)
// ---------------------------------------------------------------

import { writeFile, mkdir } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUTPUT_PATH = resolve(__dirname, '..', 'public', 'data', 'wind.json');

// ---------------------------------------------------------------
// Grid configuration
//
// 5° resolution: 72 lons × 37 lats = 2,664 cells. Coarse but more than
// enough for visualization at globe scale (jet streams span ~10° of lat,
// trade wind belts span ~30°). The streamlines that render from this
// data don't reveal fine detail anyway.
// ---------------------------------------------------------------

const RES_DEG = 5.0;
const GRID_LONS = Math.round(360 / RES_DEG);    // 72
const GRID_LATS = Math.round(180 / RES_DEG) + 1; // 37 (-90 to +90 inclusive)
const BATCH_SIZE = 100;                          // Open-Meteo handles 100/call
const BATCH_PAUSE_MS = 1200;                     // calm pace, well under rate limits
const API_BASE = 'https://api.open-meteo.com/v1/forecast';

function buildSamplePoints() {
  const points = [];
  for (let j = 0; j < GRID_LATS; j++) {
    const lat = 90 - j * RES_DEG;
    for (let i = 0; i < GRID_LONS; i++) {
      const lon = -180 + i * RES_DEG;
      points.push({ lat, lon, i, j });
    }
  }
  return points;
}

async function fetchBatch(batch) {
  const lats = batch.map(p => p.lat.toFixed(2)).join(',');
  const lons = batch.map(p => p.lon.toFixed(2)).join(',');
  const params = new URLSearchParams({
    latitude: lats,
    longitude: lons,
    current: 'wind_speed_10m,wind_direction_10m',
    wind_speed_unit: 'ms',
    timezone: 'UTC'
  });
  const url = `${API_BASE}?${params.toString()}`;
  const res = await fetch(url, {
    headers: { 'User-Agent': 'a-living-earth-app/1.0 (wind update)' }
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`HTTP ${res.status}: ${body.slice(0, 200)}`);
  }
  const data = await res.json();
  return Array.isArray(data) ? data : [data];
}

// Speed + direction → U/V components (meteorological convention)
//   direction 0° = wind FROM north (blowing south)
//   direction 90° = wind FROM east (blowing west)
// We want the vector OF motion (where the wind is going):
//   U (east-west, +east) = -speed * sin(direction)
//   V (north-south, +north) = -speed * cos(direction)
function speedDirToUV(speed, direction) {
  const rad = direction * Math.PI / 180;
  return [-speed * Math.sin(rad), -speed * Math.cos(rad)];
}

async function main() {
  console.log(`\n[update-wind] ${new Date().toISOString()}\n`);
  console.log(`Grid: ${GRID_LONS}×${GRID_LATS} at ${RES_DEG}° (${GRID_LONS * GRID_LATS} cells)`);

  const points = buildSamplePoints();
  const totalBatches = Math.ceil(points.length / BATCH_SIZE);
  console.log(`Will fetch in ${totalBatches} batches of up to ${BATCH_SIZE} points`);
  console.log(`Pausing ${BATCH_PAUSE_MS}ms between batches\n`);

  const u = new Float32Array(GRID_LONS * GRID_LATS);
  const v = new Float32Array(GRID_LONS * GRID_LATS);

  let fetched = 0;
  let failed = 0;
  let runDate = null;

  for (let batchIdx = 0; batchIdx < totalBatches; batchIdx++) {
    const start = batchIdx * BATCH_SIZE;
    const batch = points.slice(start, start + BATCH_SIZE);

    let attempt = 0;
    let success = false;
    while (attempt < 3 && !success) {
      attempt++;
      try {
        const results = await fetchBatch(batch);
        if (results.length !== batch.length) {
          throw new Error(`Expected ${batch.length} results, got ${results.length}`);
        }
        for (let k = 0; k < batch.length; k++) {
          const pt = batch[k];
          const r = results[k];
          if (!r.current ||
              typeof r.current.wind_speed_10m !== 'number' ||
              typeof r.current.wind_direction_10m !== 'number') {
            continue;
          }
          const [uVal, vVal] = speedDirToUV(
            r.current.wind_speed_10m,
            r.current.wind_direction_10m
          );
          const idx = pt.j * GRID_LONS + pt.i;
          u[idx] = uVal;
          v[idx] = vVal;
          if (!runDate && r.current.time) runDate = r.current.time;
          fetched++;
        }
        success = true;
      } catch (err) {
        const isRateLimit = err.message.includes('429') ||
                            err.message.includes('rate') ||
                            err.message.includes('limit');
        const backoff = isRateLimit ? 60000 * attempt : 2000 * attempt;
        console.warn(`  Batch ${batchIdx + 1}/${totalBatches} attempt ${attempt} failed: ${err.message.slice(0, 120)}`);
        if (attempt < 3) {
          if (isRateLimit) {
            console.warn(`    Rate limited; waiting ${backoff / 1000}s before retry`);
          }
          await new Promise(r => setTimeout(r, backoff));
        }
      }
    }
    if (!success) failed += batch.length;

    // Progress every ~10 batches
    if (batchIdx % 5 === 0 || batchIdx === totalBatches - 1) {
      const pct = Math.round((batchIdx + 1) / totalBatches * 100);
      console.log(`  Batch ${batchIdx + 1}/${totalBatches} (${pct}%) — ${fetched} fetched, ${failed} failed`);
    }

    // Polite pause between batches to stay under rate limits
    if (batchIdx < totalBatches - 1) {
      await new Promise(r => setTimeout(r, BATCH_PAUSE_MS));
    }
  }

  const total = GRID_LONS * GRID_LATS;
  console.log(`\nResults:`);
  console.log(`  Fetched: ${fetched}/${total} (${(fetched/total*100).toFixed(1)}%)`);
  console.log(`  Failed:  ${failed}`);

  if (fetched < total * 0.85) {
    throw new Error(`Too few cells populated (${fetched}/${total}); aborting`);
  }

  let maxSpeed = 0, sumSpeed = 0;
  for (let k = 0; k < u.length; k++) {
    const sp = Math.sqrt(u[k]**2 + v[k]**2);
    if (sp > maxSpeed) maxSpeed = sp;
    sumSpeed += sp;
  }
  console.log(`  Max wind:  ${maxSpeed.toFixed(1)} m/s`);
  console.log(`  Mean wind: ${(sumSpeed/total).toFixed(1)} m/s`);

  const round1 = x => Math.round(x * 10) / 10;
  const json = {
    schemaVersion: 1,
    source: 'Open-Meteo',
    level: '10m above ground',
    run: {
      date: runDate ? runDate.slice(0, 10).replace(/-/g, '') : null,
      cycle: runDate ? runDate.slice(11, 13) + 'Z' : null,
      isoDate: runDate || null
    },
    grid: {
      lons: GRID_LONS,
      lats: GRID_LATS,
      resolutionDeg: RES_DEG,
      lonStart: -180,
      latStart: 90,
      latStep: -RES_DEG
    },
    generatedAt: new Date().toISOString(),
    u: Array.from(u, round1),
    v: Array.from(v, round1)
  };
  const out = JSON.stringify(json);
  console.log(`  JSON size: ${(out.length/1024).toFixed(1)} KB`);

  await mkdir(dirname(OUTPUT_PATH), { recursive: true });
  await writeFile(OUTPUT_PATH, out);
  console.log(`\n✓ Wrote ${OUTPUT_PATH}\n`);
}

main().catch(err => {
  console.error('\nUpdate failed:', err.message);
  process.exit(1);
});
