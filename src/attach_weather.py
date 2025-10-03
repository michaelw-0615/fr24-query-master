"""Attach nearest-15-minute weather records for departure and arrival stations to flights.

Rules implemented:
- 'station' in weather equals flight ORIGIN/DEST (case-insensitive, stripped).
- weather 'valid' datetime is rounded to nearest 15 minutes; flight times are parsed and rounded to nearest 15 minutes.
- match on (station, rounded_datetime) and same date/time.
- attach weather columns with prefixes DEP_ and ARR_ for departure and arrival respectively.

Output: write CSV with added columns.
"""
from __future__ import annotations

import argparse
from datetime import timedelta
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd


def round_to_nearest_quarter(dt: pd.Timestamp) -> pd.Timestamp:
    if pd.isna(dt):
        return pd.NaT
    seconds = dt.hour * 3600 + dt.minute * 60 + dt.second
    nearest = int(round(seconds / (15 * 60))) * (15 * 60)
    base = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return base + timedelta(seconds=nearest)


def build_weather_map(weather_path: Path, chunksize: int = 200_000) -> Tuple[Dict[Tuple[str, pd.Timestamp], Dict], list]:
    """Read weather CSV in chunks and build a mapping (station, valid_15) -> weather row dict.

    Returns (mapping, weather_columns)
    """
    mapping: Dict[Tuple[str, pd.Timestamp], Dict] = {}
    cols = None
    it = pd.read_csv(weather_path, parse_dates=["valid"], infer_datetime_format=True, chunksize=chunksize)
    for chunk in it:
        # normalize station code
        if "station" not in chunk.columns:
            raise KeyError("weather file missing 'station' column")
        chunk["station"] = chunk["station"].astype(str).str.strip().str.upper()
        chunk["valid"] = pd.to_datetime(chunk["valid"], errors="coerce")
        chunk = chunk.dropna(subset=["valid"])
        chunk["valid_15"] = chunk["valid"].map(round_to_nearest_quarter)
        # group to one row per station+valid_15 (keep first)
        g = chunk.groupby(["station", "valid_15"], as_index=False).first()
        for _, r in g.iterrows():
            key = (r["station"], r["valid_15"])  # tuple
            # prepare dict excluding station, valid, valid_15
            rowdict = {c: r[c] for c in g.columns if c not in ("station", "valid", "valid_15")}
            mapping[key] = rowdict
            if cols is None:
                cols = list(rowdict.keys())
    return mapping, cols or []


def parse_time_str(s: str) -> Tuple[int, int]:
    """Parse strings like '0730', '730', '7:30', '0730.0' -> (hour, minute)."""
    if pd.isna(s):
        return None
    s = str(s).strip()
    if s == "":
        return None
    # try HHMM digits
    import re

    m = re.search(r"(\d{1,2}):?(\d{2})", s)
    if m:
        h = int(m.group(1))
        mnt = int(m.group(2))
        # treat 24:00 as 00:00
        if h == 24 and mnt == 0:
            return 0, 0
        # validate ranges
        if 0 <= h <= 23 and 0 <= mnt <= 59:
            return h, mnt
        return None
    # fallback: extract first number and split
    digits = re.search(r"(\d+)", s)
    if digits:
        d = digits.group(1)
        d = d.zfill(4)
        h = int(d[:2]); mnt = int(d[2:])
        if h == 24 and mnt == 0:
            return 0, 0
        if 0 <= h <= 23 and 0 <= mnt <= 59:
            return h, mnt
        return None
    return None


def attach_weather(flights_path: Path, weather_path: Path, out_path: Path):
    print("Building weather map (this may take a moment)...")
    weather_map, weather_cols = build_weather_map(weather_path)
    print(f"Loaded weather map with {len(weather_map):,} entries and columns: {weather_cols}")

    # read flights
    print("Reading flights...")
    # read without forcing FL_DATE parsing to handle multiple formats
    fdf = pd.read_csv(flights_path, dtype=str)

    # robustly parse FL_DATE into Timestamp in new column FL_DATE_TS
    def parse_flight_date(v):
        if pd.isna(v):
            return pd.NaT
        s = str(v).strip()
        # try normal parsing
        try:
            t = pd.to_datetime(s, errors="coerce", infer_datetime_format=True)
            if pd.notna(t):
                return t
        except Exception:
            pass
        # try numeric (milliseconds or nanoseconds)
        import re
        dig = re.sub(r"\D", "", s)
        if dig == "":
            return pd.NaT
        # try ns then ms then s
        for unit in ("ns", "ms", "s"):
            try:
                t = pd.to_datetime(int(dig), unit=unit, errors="coerce")
                if pd.notna(t):
                    return t
            except Exception:
                continue
        return pd.NaT

    fdf["FL_DATE_TS"] = fdf["FL_DATE"].map(parse_flight_date)

    # normalize station columns
    fdf["ORIGIN"] = fdf["ORIGIN"].astype(str).str.strip().str.upper()
    fdf["DEST"] = fdf["DEST"].astype(str).str.strip().str.upper()

    # compute departure and arrival datetime rounded
    def compute_rounded_dt(row, time_col):
        t = row.get(time_col)
        parsed = parse_time_str(t)
        if not parsed:
            return pd.NaT
        h, m = parsed
        base = row.get("FL_DATE_TS")
        if pd.isna(base):
            # fallback to YEAR/MONTH/day=1
            try:
                yr = int(row.get("YEAR"))
                mo = int(row.get("MONTH"))
                day = 1
                dt = pd.Timestamp(year=yr, month=mo, day=day, hour=h, minute=m)
            except Exception:
                return pd.NaT
        else:
            dt = base.replace(hour=h, minute=m, second=0, microsecond=0)
        return round_to_nearest_quarter(dt)

    # Add YEAR/MONTH if missing
    if "YEAR" not in fdf.columns or "MONTH" not in fdf.columns:
        fdf["YEAR"] = pd.to_datetime(fdf["FL_DATE"]).dt.year
        fdf["MONTH"] = pd.to_datetime(fdf["FL_DATE"]).dt.month

    print("Computing rounded datetimes and attaching weather...")
    dep_rounded = []
    arr_rounded = []
    for _, row in fdf.iterrows():
        dep = compute_rounded_dt(row, "DEP_TIME")
        arr = compute_rounded_dt(row, "ARR_TIME")
        dep_rounded.append(dep)
        arr_rounded.append(arr)

    fdf["DEP_ROUND"] = dep_rounded
    fdf["ARR_ROUND"] = arr_rounded

    # Prepare weather columns in output, prefixed
    for wc in weather_cols:
        fdf[f"DEP_{wc}"] = pd.NA
        fdf[f"ARR_{wc}"] = pd.NA

    # attach by lookup
    for i, row in fdf.iterrows():
        origin = row["ORIGIN"]
        dest = row["DEST"]
        dep_key = (origin, row["DEP_ROUND"]) if pd.notna(row["DEP_ROUND"]) else None
        arr_key = (dest, row["ARR_ROUND"]) if pd.notna(row["ARR_ROUND"]) else None

        if dep_key and dep_key in weather_map:
            w = weather_map[dep_key]
            for wc in weather_cols:
                fdf.at[i, f"DEP_{wc}"] = w.get(wc)
        if arr_key and arr_key in weather_map:
            w = weather_map[arr_key]
            for wc in weather_cols:
                fdf.at[i, f"ARR_{wc}"] = w.get(wc)

    # write output
    fdf.to_csv(out_path, index=False)
    print(f"Wrote {out_path} ({len(fdf):,} rows)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--flights", default="inputs/aa_flight_test_enriched_hubs.csv")
    ap.add_argument("--weather", default="inputs/All_Hubs_Weather_2023-01-01_to_2025-01-01.csv")
    ap.add_argument("--out", default="outputs/aa_flight_test_enriched_hubs_weather.csv")
    args = ap.parse_args()

    attach_weather(Path(args.flights), Path(args.weather), Path(args.out))


if __name__ == "__main__":
    main()
