#!/usr/bin/env node
// ---------------------------------------------------------------
// scripts/update-volcanoes.mjs
//
// Fetches volcano data from multiple sources and produces a clean
// JSON file at public/data/volcanoes.json for the frontend to load.
//
// Sources, in priority order:
//   1. Smithsonian Weekly Volcanic Activity Report (RSS, XML)
//      — the freshness signal: which volcanoes are currently reported
//   2. USGS HANS API (JSON)
//      — authoritative real-time alert levels for US-monitored volcanoes
//   3. Baseline list (this file)
//      — hand-curated globally-active volcanoes with coordinates,
//        ensures global coverage even if RSS is sparse this week
//
// Run: node scripts/update-volcanoes.mjs
// ---------------------------------------------------------------

import { writeFile, mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUTPUT_PATH = resolve(__dirname, '..', 'public', 'data', 'volcanoes.json');

const SMITHSONIAN_RSS = 'https://volcano.si.edu/news/WeeklyVolcanoRSS.xml';
const USGS_HANS_ELEVATED = 'https://volcanoes.usgs.gov/hans-public/api/volcano/getElevatedVolcanoes';

// ---------------------------------------------------------------
// BASELINE: hand-curated list of currently-active volcanoes worldwide.
// This is the safety net — guarantees we always have global coverage
// even if the RSS feed has only a handful of entries this week.
// Update these entries every few months as global activity shifts.
// ---------------------------------------------------------------
const BASELINE = [
  // Indonesia — most active region
  { name: 'Merapi',          country: 'Indonesia',  lat: -7.54,   lon: 110.45,  level: 'watch'    },
  { name: 'Semeru',          country: 'Indonesia',  lat: -8.108,  lon: 112.92,  level: 'watch'    },
  { name: 'Lewotobi',        country: 'Indonesia',  lat: -8.53,   lon: 122.78,  level: 'warning'  },
  { name: 'Ibu',             country: 'Indonesia',  lat: 1.488,   lon: 127.63,  level: 'watch'    },
  { name: 'Marapi',          country: 'Indonesia',  lat: -0.381,  lon: 100.473, level: 'advisory' },
  { name: 'Krakatau',        country: 'Indonesia',  lat: -6.102,  lon: 105.423, level: 'advisory' },
  { name: 'Dukono',          country: 'Indonesia',  lat: 1.693,   lon: 127.894, level: 'advisory' },
  { name: 'Karangetang',     country: 'Indonesia',  lat: 2.781,   lon: 125.407, level: 'advisory' },
  // Japan
  { name: 'Sakurajima',      country: 'Japan',      lat: 31.583,  lon: 130.660, level: 'advisory' },
  { name: 'Suwanosejima',    country: 'Japan',      lat: 29.638,  lon: 129.714, level: 'advisory' },
  { name: 'Aira',            country: 'Japan',      lat: 31.59,   lon: 130.66,  level: 'advisory' },
  // Philippines
  { name: 'Taal',            country: 'Philippines', lat: 14.002, lon: 120.993, level: 'advisory' },
  { name: 'Kanlaon',         country: 'Philippines', lat: 10.412, lon: 123.132, level: 'watch'    },
  { name: 'Mayon',           country: 'Philippines', lat: 13.257, lon: 123.685, level: 'advisory' },
  // Russia / Kamchatka
  { name: 'Sheveluch',       country: 'Russia',     lat: 56.653,  lon: 161.36,  level: 'watch'    },
  { name: 'Klyuchevskoy',    country: 'Russia',     lat: 56.056,  lon: 160.642, level: 'advisory' },
  { name: 'Bezymianny',      country: 'Russia',     lat: 55.972,  lon: 160.595, level: 'advisory' },
  { name: 'Karymsky',        country: 'Russia',     lat: 54.05,   lon: 159.45,  level: 'advisory' },
  // Alaska
  { name: 'Great Sitkin',    country: 'USA',        lat: 52.076,  lon: -176.13, level: 'watch'    },
  { name: 'Shishaldin',      country: 'USA',        lat: 54.756,  lon: -163.97, level: 'advisory' },
  { name: 'Atka volcanic complex', country: 'USA',  lat: 52.378,  lon: -174.139,level: 'advisory' },
  // Hawaii
  { name: 'Kilauea',         country: 'USA',        lat: 19.421,  lon: -155.287,level: 'watch'    },
  // Central / South America
  { name: 'Popocatepetl',    country: 'Mexico',     lat: 19.023,  lon: -98.622, level: 'advisory' },
  { name: 'Fuego',           country: 'Guatemala',  lat: 14.473,  lon: -90.88,  level: 'advisory' },
  { name: 'Santa Maria',     country: 'Guatemala',  lat: 14.756,  lon: -91.552, level: 'advisory' },
  { name: 'Pacaya',          country: 'Guatemala',  lat: 14.382,  lon: -90.601, level: 'advisory' },
  { name: 'San Cristobal',   country: 'Nicaragua',  lat: 12.702,  lon: -87.004, level: 'advisory' },
  { name: 'Masaya',          country: 'Nicaragua',  lat: 11.985,  lon: -86.165, level: 'advisory' },
  { name: 'Poas',            country: 'Costa Rica', lat: 10.20,   lon: -84.233, level: 'watch'    },
  { name: 'Rincon de la Vieja', country: 'Costa Rica', lat: 10.83, lon: -85.324,level: 'advisory' },
  { name: 'Reventador',      country: 'Ecuador',    lat: -0.077,  lon: -77.656, level: 'advisory' },
  { name: 'Sangay',          country: 'Ecuador',    lat: -2.005,  lon: -78.341, level: 'advisory' },
  { name: 'Nevado del Ruiz', country: 'Colombia',   lat: 4.892,   lon: -75.323, level: 'advisory' },
  { name: 'Villarrica',      country: 'Chile',      lat: -39.42,  lon: -71.93,  level: 'advisory' },
  // Iceland
  { name: 'Reykjanes',       country: 'Iceland',    lat: 63.85,   lon: -22.5,   level: 'watch'    },
  // Italy / Europe
  { name: 'Etna',            country: 'Italy',      lat: 37.748,  lon: 14.999,  level: 'advisory' },
  { name: 'Stromboli',       country: 'Italy',      lat: 38.789,  lon: 15.213,  level: 'advisory' },
  // Africa
  { name: 'Erta Ale',        country: 'Ethiopia',   lat: 13.6,    lon: 40.67,   level: 'advisory' },
  { name: 'Nyiragongo',      country: 'D.R. Congo', lat: -1.52,   lon: 29.25,   level: 'advisory' },
  { name: 'Nyamuragira',     country: 'D.R. Congo', lat: -1.408,  lon: 29.2,    level: 'advisory' },
  // Pacific island arcs
  { name: 'Yasur',           country: 'Vanuatu',    lat: -19.532, lon: 169.447, level: 'advisory' },
  { name: 'Ambrym',          country: 'Vanuatu',    lat: -16.25,  lon: 168.12,  level: 'advisory' },
  { name: 'Tofua',           country: 'Tonga',      lat: -19.75,  lon: -175.07, level: 'advisory' },
  { name: 'Ahyi',            country: 'USA (N. Marianas)', lat: 20.42, lon: 145.03, level: 'advisory' },
  { name: 'Heard',           country: 'Australia',  lat: -53.106, lon: 73.513,  level: 'advisory' }
];

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------

function normalizeName(s) {
  // Lowercase, strip punctuation, collapse whitespace
  return String(s || '')
    .toLowerCase()
    .replace(/[^\p{Letter}\p{Number}\s]/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

// Map USGS color codes to our normalized levels
function levelFromUsgsColor(color) {
  const c = String(color || '').toUpperCase();
  if (c === 'RED')    return 'warning';
  if (c === 'ORANGE') return 'watch';
  if (c === 'YELLOW') return 'advisory';
  return 'advisory'; // default for any non-NORMAL state in the elevated list
}

// "Highest" alert wins when merging
const LEVEL_RANK = { warning: 3, watch: 2, advisory: 1 };
function higherLevel(a, b) {
  return (LEVEL_RANK[a] || 0) >= (LEVEL_RANK[b] || 0) ? a : b;
}

// ---------------------------------------------------------------
// Source 1: Smithsonian RSS — list of volcanoes in this week's report
// ---------------------------------------------------------------
async function fetchSmithsonianRSS() {
  console.log('Fetching Smithsonian RSS …');
  let xml;
  try {
    const res = await fetch(SMITHSONIAN_RSS, {
      headers: { 'User-Agent': 'a-living-earth-app/1.0 (build script)' }
    });
    if (!res.ok) throw new Error(`Status ${res.status}`);
    xml = await res.text();
  } catch (err) {
    console.warn('  ✗ Smithsonian RSS fetch failed:', err.message);
    return [];
  }

  // RSS items: <item><title>Volcano Name (Country)</title>...
  // We only care about the volcano names. Coordinates come from baseline.
  const items = [];
  const itemRegex = /<item>([\s\S]*?)<\/item>/g;
  const titleRegex = /<title>(?:<!\[CDATA\[)?([\s\S]*?)(?:\]\]>)?<\/title>/;
  let match;
  while ((match = itemRegex.exec(xml)) !== null) {
    const block = match[1];
    const t = block.match(titleRegex);
    if (!t) continue;
    const title = t[1].trim();
    // Title format: "Volcano Name (Country)" or just "Volcano Name"
    const m = title.match(/^(.+?)\s*\(([^)]+)\)\s*$/);
    if (m) {
      items.push({ name: m[1].trim(), country: m[2].trim() });
    } else {
      items.push({ name: title, country: '' });
    }
  }
  console.log(`  ✓ Parsed ${items.length} volcanoes from RSS`);
  return items;
}

// ---------------------------------------------------------------
// Source 2: USGS HANS — current elevated alert levels for US volcanoes
// ---------------------------------------------------------------
async function fetchUsgsElevated() {
  console.log('Fetching USGS HANS elevated volcanoes …');
  try {
    const res = await fetch(USGS_HANS_ELEVATED);
    if (!res.ok) throw new Error(`Status ${res.status}`);
    const data = await res.json();
    const items = data.map(v => ({
      name: v.volcano_name,
      level: levelFromUsgsColor(v.color_code),
      sentUtc: v.sent_utc
    }));
    console.log(`  ✓ Parsed ${items.length} elevated US volcanoes`);
    return items;
  } catch (err) {
    console.warn('  ✗ USGS HANS fetch failed:', err.message);
    return [];
  }
}

// ---------------------------------------------------------------
// Merge logic
// ---------------------------------------------------------------
function mergeData(rssItems, usgsItems, baseline) {
  // Start with baseline keyed by normalized name
  const byKey = new Map();
  for (const b of baseline) {
    byKey.set(normalizeName(b.name), { ...b, sources: ['baseline'] });
  }

  // RSS items confirm the volcano was reported this week.
  // If RSS mentions a volcano not in baseline, we still record it but
  // can't place it on the globe without coordinates — so we only attach
  // a "reportedThisWeek" flag to existing entries, and log unmatched.
  const unmatchedRss = [];
  for (const r of rssItems) {
    const key = normalizeName(r.name);
    const existing = byKey.get(key);
    if (existing) {
      existing.reportedThisWeek = true;
      existing.sources.push('smithsonian-rss');
    } else {
      // Try a fuzzy fallback: see if any baseline name contains the RSS name
      // or vice versa (handles "Sakurajima" vs "Aira" type aliases).
      let matched = false;
      for (const [bKey, bVal] of byKey.entries()) {
        if (bKey.includes(key) || key.includes(bKey)) {
          bVal.reportedThisWeek = true;
          bVal.sources.push('smithsonian-rss(fuzzy)');
          matched = true;
          break;
        }
      }
      if (!matched) unmatchedRss.push(r);
    }
  }
  if (unmatchedRss.length > 0) {
    console.log(`  · ${unmatchedRss.length} RSS entries unmatched (will not appear on globe):`);
    unmatchedRss.forEach(u => console.log(`      - ${u.name} (${u.country})`));
  }

  // USGS elevated alert levels override the baseline level for matching volcanoes
  for (const u of usgsItems) {
    const key = normalizeName(u.name);
    let target = byKey.get(key);
    if (!target) {
      // Fuzzy match
      for (const [bKey, bVal] of byKey.entries()) {
        if (bKey.includes(key) || key.includes(bKey)) {
          target = bVal;
          break;
        }
      }
    }
    if (target) {
      target.level = higherLevel(target.level, u.level);
      target.sources.push('usgs-hans');
    }
  }

  return Array.from(byKey.values());
}

// ---------------------------------------------------------------
// Main
// ---------------------------------------------------------------
async function main() {
  const startedAt = new Date();
  console.log(`\n[update-volcanoes] ${startedAt.toISOString()}\n`);

  const [rssItems, usgsItems] = await Promise.all([
    fetchSmithsonianRSS(),
    fetchUsgsElevated()
  ]);

  const merged = mergeData(rssItems, usgsItems, BASELINE);

  const reportedCount = merged.filter(v => v.reportedThisWeek).length;
  const usgsCount = merged.filter(v => v.sources.includes('usgs-hans')).length;

  console.log('\nMerge results:');
  console.log(`  Total volcanoes:          ${merged.length}`);
  console.log(`  In Smithsonian RSS:       ${reportedCount}`);
  console.log(`  Updated via USGS HANS:    ${usgsCount}`);
  console.log(`  Warning level:            ${merged.filter(v => v.level === 'warning').length}`);
  console.log(`  Watch level:              ${merged.filter(v => v.level === 'watch').length}`);
  console.log(`  Advisory level:           ${merged.filter(v => v.level === 'advisory').length}`);

  const output = {
    schemaVersion: 1,
    generatedAt: startedAt.toISOString(),
    sources: {
      smithsonianRSS: SMITHSONIAN_RSS,
      usgsHANS: USGS_HANS_ELEVATED
    },
    counts: {
      total: merged.length,
      reportedThisWeek: reportedCount,
      warning: merged.filter(v => v.level === 'warning').length,
      watch: merged.filter(v => v.level === 'watch').length,
      advisory: merged.filter(v => v.level === 'advisory').length
    },
    volcanoes: merged.map(v => ({
      name: v.name,
      country: v.country,
      lat: v.lat,
      lon: v.lon,
      level: v.level,
      reportedThisWeek: v.reportedThisWeek === true,
      sources: v.sources
    }))
  };

  await mkdir(dirname(OUTPUT_PATH), { recursive: true });
  await writeFile(OUTPUT_PATH, JSON.stringify(output, null, 2) + '\n');
  console.log(`\n✓ Wrote ${OUTPUT_PATH}\n`);
}

main().catch(err => {
  console.error('Update failed:', err);
  process.exit(1);
});
