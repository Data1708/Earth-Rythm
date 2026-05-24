#!/usr/bin/env python3
"""
update-productivity.py — Fetch the most recent monthly Net Primary
Productivity (NPP) composite from Oregon State University's Ocean
Productivity data server and convert it to a PNG for the Gaia frontend.

This script uses OSU's XYZ text format rather than their HDF4 format.
XYZ is plain gzipped ASCII (3 columns: lat, lon, value), so we need no
special HDF4 library and the script runs with only numpy + Pillow +
requests installed.

What it produces:
    public/data/productivity.png    — RGBA image with the color-mapped NPP field
    public/data/productivity.json   — metadata (date, units, source)

Dependencies:
    numpy, Pillow, requests
"""

from __future__ import annotations

import gzip
import json
import sys
from datetime import date, datetime
from pathlib import Path

import numpy as np
import requests
from PIL import Image


# URL patterns to try for the OSU XYZ monthly composite. OSU's directory
# layout has changed a few times; we try several known patterns and use
# whichever responds.
FILE_URL_PATTERNS = [
    "http://orca.science.oregonstate.edu/data/1x2/monthly/vgpm.r2022.m.chl.m.sst/xyz/vgpm.{year}{doy:03d}.all.xyz.gz",
    "http://orca.science.oregonstate.edu/data/1x2/monthly/vgpm.r2018.m.chl.m.sst/xyz/vgpm.{year}{doy:03d}.all.xyz.gz",
    "http://orca.science.oregonstate.edu/data/1x2/monthly/vgpm.m.chl.m.sst/xyz/vgpm.{year}{doy:03d}.all.xyz.gz",
    "http://sites.science.oregonstate.edu/ocean.productivity/data/1x2/monthly/vgpm.r2022.m.chl.m.sst/xyz/vgpm.{year}{doy:03d}.all.xyz.gz",
]

OUTPUT_PNG  = Path("public/data/productivity.png")
OUTPUT_JSON = Path("public/data/productivity.json")

GRID_H = 1080
GRID_W = 2160

MIN_LOG_NPP = 1.0
MAX_LOG_NPP = 3500.0


def find_target_dates():
    """Walk back from 2 months ago up to 18 months back."""
    candidates = []
    today = date.today()
    for months_back in range(2, 18):
        target_year = today.year
        target_month = today.month - months_back
        while target_month < 1:
            target_month += 12
            target_year -= 1
        first_of_month = date(target_year, target_month, 1)
        doy = first_of_month.timetuple().tm_yday
        candidates.append((target_year, doy, target_month))
    return candidates


def download_xyz(year, doy):
    for pattern in FILE_URL_PATTERNS:
        url = pattern.format(year=year, doy=doy)
        try:
            print(f"  trying {url}")
            r = requests.get(url, timeout=120, allow_redirects=True)
            if r.status_code != 200 or len(r.content) < 1000:
                print(f"    HTTP {r.status_code}, {len(r.content)} bytes")
                continue
            if r.content[:2] != b"\x1f\x8b":
                print(f"    not a gzip file")
                continue
            text = gzip.decompress(r.content).decode("ascii", errors="replace")
            print(f"  got {len(text)} bytes of XYZ")
            return text
        except (requests.RequestException, gzip.BadGzipFile, OSError) as e:
            print(f"    failed: {e}")
            continue
    return None


def parse_xyz(text):
    """Parse XYZ text into a (GRID_H, GRID_W) NPP grid."""
    print("  parsing XYZ to numpy grid...")
    values = np.fromstring(
        " ".join(line.split()[2] for line in text.split("\n") if line.strip()),
        sep=" ",
        dtype=np.float32,
    )
    n_expected = GRID_H * GRID_W
    if values.size != n_expected:
        print(f"  WARNING: got {values.size} values, expected {n_expected}")
        if values.size > n_expected:
            values = values[:n_expected]
        else:
            padded = np.full(n_expected, -9999.0, dtype=np.float32)
            padded[: values.size] = values
            values = padded
    return values.reshape(GRID_H, GRID_W)


def render_png(npp):
    h, w = npp.shape
    print(f"  rendering {w}x{h} image")
    valid = (npp > 0) & (npp < 100000)
    print(f"  valid pixels: {valid.sum()} / {h*w} ({100*valid.sum()/(h*w):.1f}%)")

    log_npp = np.zeros_like(npp)
    log_npp[valid] = np.log10(np.clip(npp[valid], MIN_LOG_NPP, MAX_LOG_NPP))
    log_min = np.log10(MIN_LOG_NPP)
    log_max = np.log10(MAX_LOG_NPP)
    normalized = np.clip((log_npp - log_min) / (log_max - log_min), 0.0, 1.0)

    def colormap(t):
        stops = np.array([
            [0.05, 0.04, 0.18],   # deep indigo
            [0.05, 0.30, 0.45],   # teal
            [0.20, 0.65, 0.50],   # lime-green
            [0.85, 0.85, 0.25],   # amber/gold
            [1.00, 0.55, 0.10],   # warm peak
        ])
        n = stops.shape[0] - 1
        seg = np.clip((t * n).astype(np.int32), 0, n - 1)
        local_t = (t * n - seg).astype(np.float32)
        a = stops[seg]
        b = stops[seg + 1]
        return a + (b - a) * local_t[..., None]

    rgb = colormap(normalized)
    rgb8 = (rgb * 255).clip(0, 255).astype(np.uint8)
    alpha = (valid * 255).astype(np.uint8)
    rgba = np.dstack([rgb8, alpha])
    return Image.fromarray(rgba, mode="RGBA")


def write_outputs(img, year, month, doy):
    OUTPUT_PNG.parent.mkdir(parents=True, exist_ok=True)
    img_resized = img.resize((2048, 1024), Image.LANCZOS)
    img_resized.save(OUTPUT_PNG, optimize=True)
    print(f"  wrote {OUTPUT_PNG} ({OUTPUT_PNG.stat().st_size} bytes)")

    metadata = {
        "source": "Oregon State University Ocean Productivity (VGPM, MODIS Aqua)",
        "url": "http://sites.science.oregonstate.edu/ocean.productivity/",
        "model": "VGPM (Behrenfeld & Falkowski 1997)",
        "units": "mg C / m^2 / day",
        "year": year,
        "month": month,
        "julianDay": doy,
        "dateLabel": f"{year}-{month:02d}",
        "logScale": True,
        "minNpp": MIN_LOG_NPP,
        "maxNpp": MAX_LOG_NPP,
        "generatedAt": datetime.utcnow().isoformat() + "Z",
    }
    OUTPUT_JSON.write_text(json.dumps(metadata, indent=2))
    print(f"  wrote {OUTPUT_JSON}")


def main():
    candidates = find_target_dates()
    print(f"Trying {len(candidates)} candidate months...")
    for (year, doy, month) in candidates:
        print(f"\n--- {year}-{month:02d} (DOY {doy}) ---")
        text = download_xyz(year, doy)
        if text is None:
            continue
        try:
            npp = parse_xyz(text)
        except Exception as e:
            print(f"  XYZ parse failed: {e}")
            continue
        img = render_png(npp)
        write_outputs(img, year, month, doy)
        print(f"\nOK Productivity layer updated to {year}-{month:02d}")
        return 0
    print("\nFAIL All candidates failed", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
