"""Merge aa_flight_test.csv with the merged summary (aircraft type + description).

Behavior:
- Cleans simple header/data issues in `aa_flight_test.csv` (fixes broken `DEP_\nTIME` or header+data on same line).
- Parses FL_DATE to YEAR and MONTH.
- Left-joins aa_flight_test (all columns kept) with merged summary on ORIGIN, DEST, YEAR, MONTH.
- From merged summary only AIRCRAFT_TYPE and DESCRIPTION are added (if available).

Writes: outputs/aa_flight_test_enriched.csv
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd


def fix_aa_test(inpath: Path, outpath: Path) -> None:
    txt = inpath.read_text()
    changed = False

    # Fix broken header token 'DEP_\nTIME' -> 'DEP_TIME'
    if 'DEP_\nTIME' in txt:
        txt = txt.replace('DEP_\nTIME', 'DEP_TIME')
        changed = True

    # If header and first data are on the same line separated by spaces, insert newline after header
    # pattern: header ends with 'DIVERTED' then whitespace then a date like 2023/1/1
    m = re.search(r'(DIVERTED)\s+(\d{4}/\d{1,2}/\d{1,2})', txt)
    if m:
        txt = txt.replace(m.group(0), m.group(1) + '\n' + m.group(2), 1)
        changed = True

    if changed:
        outpath.write_text(txt)
    else:
        # copy original if no changes
        outpath.write_text(txt)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--aa_test", default="inputs/aa_flight_test.csv", help="Input aa_flight_test CSV")
    ap.add_argument("--merged", default="outputs/US_AA_10airports.csv", help="Merged summary with aircraft DESCRIPTION")
    ap.add_argument("--out", default="outputs/aa_flight_test_enriched.csv", help="Output enriched CSV")
    ap.add_argument("--aircraft-types", default="inputs/DOT_AIRCRAFT_TYPE.csv", help="DOT aircraft types CSV to recover Code -> Description mapping")
    ap.add_argument("--filter-hubs", action="store_true", help="Keep only flights where origin and dest are in the hub list")
    ap.add_argument("--hubs", default=','.join(["DFW","LGA","JFK","PHL","DCA","CLT","MIA","ORD","PHX","LAX"]),
                    help="Comma-separated list of hub IATA codes to keep when --filter-hubs is used")
    args = ap.parse_args()

    aa_in = Path(args.aa_test)
    merged_in = Path(args.merged)
    out = Path(args.out)

    # prepare fixed aa_test file
    fixed = Path(".tmp_aa_flight_test_fixed.csv")
    fix_aa_test(aa_in, fixed)

    # read aa test
    names = [
        "FL_DATE", "MKT_UNIQUE_CARRIER", "OP_CARRIER_FL_NUM", "ORIGIN", "DEST",
        "CRS_DEP_TIME", "DEP_TIME", "WHEELS_OFF", "CRS_ARR_TIME", "ARR_TIME", "WHEELS_ON",
        "CANCELLED", "DIVERTED",
    ]
    df = pd.read_csv(fixed, names=names, header=0, parse_dates=["FL_DATE"], infer_datetime_format=True)
    # Normalize time columns to 4-digit zero-padded strings like '0730' or set NA when missing
    time_cols = ["CRS_DEP_TIME", "DEP_TIME", "WHEELS_OFF", "CRS_ARR_TIME", "ARR_TIME", "WHEELS_ON"]
    for tc in time_cols:
        if tc in df.columns:
            # extract the first contiguous digit group (handles values like '726.0'), then zero-pad to 4
            s = df[tc].astype(str).fillna("")
            s = s.str.extract(r"(\d+)", expand=False)
            s = s.where(s.notna(), pd.NA)
            s = s.astype(str).str.zfill(4)
            df[tc] = s.reindex(df.index)

    # extract year/month
    df["YEAR"] = df["FL_DATE"].dt.year
    df["MONTH"] = df["FL_DATE"].dt.month

    # read merged summary
    if not merged_in.exists():
        raise FileNotFoundError(f"Merged summary not found: {merged_in}")
    mdf = pd.read_csv(merged_in, dtype=str)

    # ensure AIRCRAFT_TYPE and DESCRIPTION columns exist in mdf
    if "AIRCRAFT_TYPE" not in mdf.columns:
        mdf["AIRCRAFT_TYPE"] = pd.NA
    if "DESCRIPTION" not in mdf.columns:
        mdf["DESCRIPTION"] = pd.NA

    # coerce YEAR/MONTH types to numeric for join
    mdf["YEAR"] = pd.to_numeric(mdf["YEAR"], errors="coerce")
    mdf["MONTH"] = pd.to_numeric(mdf["MONTH"], errors="coerce")

    # dedupe mapping to one row per key
    key_cols = ["ORIGIN", "DEST", "YEAR", "MONTH"]
    mmap = mdf.loc[:, key_cols + ["AIRCRAFT_TYPE", "DESCRIPTION"]].drop_duplicates(subset=key_cols)

    # perform left merge keeping all aa_test columns and adding AIRCRAFT_TYPE and DESCRIPTION
    out_df = pd.merge(df, mmap, how="left", on=key_cols)

    # If user provided aircraft types CSV, build reverse mapping from Description -> Code
    at_path = Path(args.aircraft_types)
    if at_path.exists():
        at_df = pd.read_csv(at_path, dtype=str)
        # find cols
        code_col = at_df.columns[0]
        desc_col = at_df.columns[1] if len(at_df.columns) > 1 else None
        if desc_col is not None:
            # normalize description -> code
            def _norm_desc(s: str) -> str:
                if pd.isna(s):
                    return ""
                return " ".join(str(s).strip().upper().split())

            desc_to_code = { _norm_desc(r[desc_col]): str(r[code_col]).zfill(3) for _, r in at_df.iterrows() }

            # fill AIRCRAFT_TYPE from DESCRIPTION where missing
            if "AIRCRAFT_TYPE" not in out_df.columns:
                out_df["AIRCRAFT_TYPE"] = pd.NA

            # Vectorized fill: where AIRCRAFT_TYPE is missing, try to map from DESCRIPTION
            # normalize descriptions in out_df
            desc_series = out_df["DESCRIPTION"].fillna("").astype(str).map(lambda s: _norm_desc(s))
            mapped_codes = desc_series.map(desc_to_code).where(desc_series != "", pd.NA)

            # where AIRCRAFT_TYPE is NA or empty, replace with mapped_codes
            at_series = out_df.get("AIRCRAFT_TYPE")
            if at_series is None:
                out_df["AIRCRAFT_TYPE"] = mapped_codes
            else:
                # coerce to string then where na/empty
                mask_missing = at_series.isna() | (at_series.astype(str).str.strip() == "")
                out_df.loc[mask_missing, "AIRCRAFT_TYPE"] = mapped_codes[mask_missing]

    # write out
    # optional filter to hubs
    if args.filter_hubs:
        hubs_set = set([h.strip().upper() for h in args.hubs.split(',') if h.strip()])
        keep = out_df["ORIGIN"].str.upper().isin(hubs_set) & out_df["DEST"].str.upper().isin(hubs_set)
        out_df = out_df.loc[keep]

    out_df.to_csv(out, index=False)
    print(f"Wrote enriched file: {out} ({len(out_df):,} rows)")


if __name__ == "__main__":
    main()
