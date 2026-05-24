#!/usr/bin/env python3
"""
update-productivity.py — Fetch the most recent monthly Net Primary
Productivity (NPP) composite from Oregon State University's Ocean
Productivity data server and convert it to a PNG for the Gaia frontend.

Data source:
    http://orca.science.oregonstate.edu/1080.by.2160.monthly.hdf.vgpm.m.chl.m.sst.php

What it produces:
    public/data/productivity.png    — RGBA image with the color-mapped NPP field
    public/data/productivity.json   — metadata (date, units, source)

Run:
    python scripts/update-productivity.py

Dependencies:
    pyhdf (provides SD class for HDF4)
    numpy
    Pillow
    requests
"""

from __future__ import annotations

import gzip
import io
import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import requests
from PIL import Image
from pyhdf.SD import SD, SDC


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------

# OSU VGPM monthly composite, MODIS chlorophyll + MODIS SST inputs.
# The path uses gzipped HDF4 files at 1080x2160 (1/6 degree) resolution.
OSU_BASE_URL = (
    "http://orca.science.oregonstate.edu/data/1x2/monthly/"
    "vgpm.m.chl.m.sst.7z/1080.by.2160.monthly.hdf.vgpm.m.chl.m.sst.php"
)
# Actual file path pattern, hosted as gzipped HDF4 files.
# We try a few naming variants because OSU occasionally restructures
# directory layouts; the script picks whichever URL responds.
FILE_URL_PATTERNS = [
    "http://orca.science.oregonstate.edu/data/1x2/monthly/vgpm.r2022.m.chl.m.sst/hdf/vgpm.{year}{doy:03d}.hdf.gz",
    "http://orca.science.oregonstate.edu/data/1x2/monthly/vgpm.r2018.m.chl.m.sst/hdf/vgpm.{year}{doy:03d}.hdf.gz",
    "http://orca.science.oregonstate.edu/data/1x2/monthly/vgpm.m.chl.m.sst/hdf/vgpm.{year}{doy:03d}.hdf.gz",
    "http://sites.science.oregonstate.edu/ocean.productivity/data/1x2/monthly/vgpm.r2022.m.chl.m.sst/hdf/vgpm.{year}{doy:03d}.hdf.gz",
]

OUTPUT_PNG  = Path("public/data/productivity.png")
OUTPUT_JSON = Path("public/data/productivity.json")

# Productivity data range. NPP is in mg C / m² / day. Real values span
# roughly 0 to 4000+ with most ocean below 1000. Log scaling is the
# standard practice.
MIN_LOG_NPP = 1.0      # ≈ 1 mg C / m² / day — barely above noise
MAX_LOG_NPP = 3500.0   # ≈ saturation point for hyper-productive coasts


# ----------------------------------------------------------------------
# Pipeline steps
# ----------------------------------------------------------------------

def find_target_dates() -> list[tuple[int, int]]:
    """
    Build a list of (year, julian_doy) candidates to try.
    OSU monthly products use the first-day-of-month Julian day.
    Most recent month is published ~6 weeks after month end, so we
    walk backwards from 2 months ago until we find something live.
    """
    candidates = []
    today = date.today()
    # Start at month - 2 to clear the publishing latency
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


def download_hdf(year: int, doy: int) -> bytes | None:
    """Try each URL pattern; return decompressed HDF bytes on first success."""
    for pattern in FILE_URL_PATTERNS:
        url = pattern.format(year=year, doy=doy)
        try:
            print(f"  trying {url}")
            r = requests.get(url, timeout=60, allow_redirects=True)
            if r.status_code != 200 or len(r.content) < 1000:
                continue
            # Magic check: gzipped files start with \x1f\x8b
            if r.content[:2] != b"\x1f\x8b":
                continue
            decompressed = gzip.decompress(r.content)
            print(f"  got {len(decompressed)} bytes from {url}")
            return decompressed
        except (requests.RequestException, gzip.BadGzipFile, OSError) as e:
            print(f"    failed: {e}")
            continue
    return None


def parse_hdf(hdf_bytes: bytes) -> np.ndarray:
    """
    Write the bytes to a temp file (pyhdf needs a file path), parse,
    and return the NPP grid as a 2D numpy array of shape (1080, 2160).
    Values are mg C / m² / day; sentinel for no-data is -9999.
    """
    tmp_path = Path("/tmp/vgpm_tmp.hdf")
    tmp_path.write_bytes(hdf_bytes)
    try:
        hdf = SD(str(tmp_path), SDC.READ)
        # The dataset is typically named "npp" — but be defensive
        datasets = hdf.datasets()
        print(f"  HDF datasets: {list(datasets.keys())}")
        # Find the first dataset that looks like NPP (largest 2D dataset)
        target_name = None
        target_shape = (0, 0)
        for name, info in datasets.items():
            dims, dtype, nattrs, _ = info
            if len(dims) == 2 and dims[0] * dims[1] > target_shape[0] * target_shape[1]:
                target_shape = dims
                target_name = name
        if target_name is None:
            raise ValueError("No suitable 2D dataset found in HDF")
        print(f"  using dataset '{target_name}', shape {target_shape}")
        sds = hdf.select(target_name)
        data = sds.get()
        sds.endaccess()
        hdf.end()
        return np.asarray(data, dtype=np.float32)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def render_png(npp: np.ndarray) -> Image.Image:
    """
    Convert the NPP grid to an RGBA image:
      - alpha = 0 where data is missing (-9999) or land
      - alpha = 255 where data is valid
      - rgb   = perceptually-uniform colormap on log-scaled NPP
    """
    h, w = npp.shape
    print(f"  rendering {w}x{h} image")

    # Mask out invalid pixels
    valid = (npp > 0) & (npp < 100000)
    print(f"  valid pixels: {valid.sum()} / {h*w} ({100*valid.sum()/(h*w):.1f}%)")

    # Log scale the valid values
    log_npp = np.zeros_like(npp)
    log_npp[valid] = np.log10(np.clip(npp[valid], MIN_LOG_NPP, MAX_LOG_NPP))
    log_min = np.log10(MIN_LOG_NPP)
    log_max = np.log10(MAX_LOG_NPP)
    normalized = np.clip((log_npp - log_min) / (log_max - log_min), 0.0, 1.0)

    # ---- Colormap: oligotrophic → productive ----
    # Indigo (low) → teal → lime → amber/gold (high)
    # Hand-tuned to be distinct from SST (cold-blue-warm-red), vegetation
    # (forest greens), and other layers. Maintains "this is biological"
    # character while being readable.
    def colormap(t):
        # t: ndarray, range 0..1
        # 4-stop gradient in linear RGB
        stops = np.array([
            [0.05, 0.04, 0.18],   # deep indigo (oligotrophic open ocean)
            [0.05, 0.30, 0.45],   # teal
            [0.20, 0.65, 0.50],   # lime-green
            [0.85, 0.85, 0.25],   # bright amber/gold
            [1.00, 0.55, 0.10],   # warm peak — hyper-productive
        ])
        n = stops.shape[0] - 1  # number of segments
        # Map t to segment index
        seg = np.clip((t * n).astype(np.int32), 0, n - 1)
        local_t = (t * n - seg).astype(np.float32)
        a = stops[seg]
        b = stops[seg + 1]
        return a + (b - a) * local_t[..., None]

    rgb = colormap(normalized)
    # Convert to 8-bit
    rgb8 = (rgb * 255).clip(0, 255).astype(np.uint8)
    alpha = (valid * 255).astype(np.uint8)

    # Compose RGBA
    rgba = np.dstack([rgb8, alpha])

    # The OSU grid is north-up, lon = [-180, +180), lat = [+90, -90).
    # GIBS WMS convention (and our Three.js spheres) is the same north-up
    # equirectangular, so we don't need to flip. But OSU stores from
    # the top-left as (90°N, -180°). PIL writes top-down too. We're good.
    return Image.fromarray(rgba, mode="RGBA")


def write_outputs(img: Image.Image, year: int, month: int, doy: int):
    OUTPUT_PNG.parent.mkdir(parents=True, exist_ok=True)
    # Save downsampled to 2048x1024 to match the other layers' resolution
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
        "generatedAt": __import__("datetime").datetime.utcnow().isoformat() + "Z",
    }
    OUTPUT_JSON.write_text(json.dumps(metadata, indent=2))
    print(f"  wrote {OUTPUT_JSON}")


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def main() -> int:
    candidates = find_target_dates()
    print(f"Trying {len(candidates)} candidate months...")
    for (year, doy, month) in candidates:
        print(f"\n--- {year}-{month:02d} (DOY {doy}) ---")
        hdf_bytes = download_hdf(year, doy)
        if hdf_bytes is None:
            continue
        try:
            npp = parse_hdf(hdf_bytes)
        except Exception as e:
            print(f"  HDF parse failed: {e}")
            continue
        img = render_png(npp)
        write_outputs(img, year, month, doy)
        print(f"\n✓ Productivity layer updated to {year}-{month:02d}")
        return 0
    print("\n✗ All candidates failed", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
