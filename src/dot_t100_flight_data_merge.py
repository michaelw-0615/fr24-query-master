import argparse
from pathlib import Path
from typing import List, Optional

import pandas as pd


# Default 10 airports
DEFAULT_AIRPORTS = ["DFW", "LGA", "JFK", "PHL", "DCA", "CLT", "MIA", "ORD", "PHX", "LAX"]


def _pick_column(columns, candidates):
    cols = {c.upper(): c for c in columns}
    for cand in candidates:
        if cand.upper() in cols:
            return cols[cand.upper()]
    return None


def merge_summaries(
    inputs: List[str],
    output: str,
    dedupe_on: Optional[List[str]] = None,
    chunksize: Optional[int] = 200_000,
    filter_carrier: Optional[str] = None,
    filter_airports: Optional[List[str]] = None,
    project_minimal: bool = False,
    aircraft_types_path: Optional[str] = None,
):
    """Merge multiple CSV files into one output file.

    - Streams files in chunks to avoid loading everything at once.
    - Aligns columns by taking the union of all columns (missing values filled with empty).
    - Optional deduplication on specified column list (keeps first occurrence).

    Args:
        inputs: list of input CSV paths.
        output: output CSV path.
        dedupe_on: list of column names to use for deduplication; if None no dedupe.
        chunksize: pandas read_csv chunksize; if None loads each file fully.
    """
    out_path = Path(output)
    # remove existing output to ensure clean write
    if out_path.exists():
        out_path.unlink()

    header_written = False
    columns_union: List[str] = []
    seen_keys = set() if dedupe_on else None

    # mapping for columns we care about (actual names in input)
    carrier_col = None
    origin_col = None
    dest_col = None
    year_col = None
    month_col = None
    airports_set = set(a.upper() for a in filter_airports) if filter_airports else None
    aircraft_col = None

    # load aircraft types mapping if provided
    aircraft_map = None
    if aircraft_types_path:
        at_path = Path(aircraft_types_path)
        if not at_path.exists():
            raise FileNotFoundError(f"Aircraft types file not found: {at_path}")
        at_df = pd.read_csv(at_path, dtype=str)
        # try common names
        code_col = _pick_column(at_df.columns, ["Code", "CODE", "code"]) or at_df.columns[0]
        desc_col = _pick_column(at_df.columns, ["Description", "DESCRIPTION", "description"]) or at_df.columns[1]
        # normalize code: keep digits and zero-pad to 3
        def _norm_code(s: str) -> str:
            if pd.isna(s):
                return ""
            s2 = ''.join(ch for ch in str(s) if ch.isdigit())
            return s2.zfill(3) if s2 else ""

        aircraft_map = { _norm_code(r[code_col]): (r[desc_col] if not pd.isna(r[desc_col]) else "") for _, r in at_df.iterrows() }

    for inpath in inputs:
        inpath = Path(inpath)
        if not inpath.exists():
            raise FileNotFoundError(f"Input not found: {inpath}")

        reader = (
            pd.read_csv(inpath, dtype=str, low_memory=False, chunksize=chunksize)
            if chunksize
            else pd.read_csv(inpath, dtype=str, low_memory=False)
        )

        # pandas returns an iterator for chunks; if no chunksize, wrap single df
        if not hasattr(reader, "__iter__"):
            reader = [reader]

        for chunk in reader:
            # ensure all columns are strings
            chunk = chunk.astype("string")

            # detect important columns on first non-empty chunk
            if (filter_carrier or project_minimal or dedupe_on) and carrier_col is None:
                cols = list(chunk.columns)
                carrier_col = _pick_column(cols, ["UNIQUE_CARRIER", "OP_UNIQUE_CARRIER", "MKT_UNIQUE_CARRIER", "REPORTING_AIRLINE", "CARRIER"])
                origin_col = _pick_column(cols, ["ORIGIN", "ORIGIN_AIRPORT", "ORIGIN_AIRPORT_ID"]) 
                dest_col = _pick_column(cols, ["DEST", "DEST_AIRPORT", "DEST_AIRPORT_ID"]) 
                year_col = _pick_column(cols, ["YEAR", "FLIGHT_YEAR"]) 
                month_col = _pick_column(cols, ["MONTH", "MONTH_NUM"]) 
                aircraft_col = _pick_column(cols, ["AIRCRAFT_TYPE", "AIRCRAFT_CONFIG", "AIRCRAFT_GROUP", "AIRCRAFT_TYPE_CODE"])

            # If projection/filtering requested, build reduced chunk early
            if filter_carrier or project_minimal:
                # ensure required columns exist
                if project_minimal:
                    needed = [carrier_col, origin_col, dest_col, year_col, month_col]
                    if any(n is None for n in needed):
                        raise KeyError(f"Missing one of required columns for projection/filtering: {needed}")
                    # select and rename to canonical names; include aircraft_col if available
                    select_cols = [carrier_col, origin_col, dest_col, year_col, month_col]
                    if aircraft_col and aircraft_col in chunk.columns:
                        select_cols.append(aircraft_col)
                    proj = chunk.loc[:, select_cols].copy()
                    # rename to canonical names; if aircraft_col included, map to AIRCRAFT_TYPE
                    new_names = ["UNIQUE_CARRIER", "ORIGIN", "DEST", "YEAR", "MONTH"]
                    if aircraft_col and aircraft_col in chunk.columns:
                        new_names.append("AIRCRAFT_TYPE")
                    proj.columns = new_names
                else:
                    proj = chunk

                # apply carrier & airports filter
                if filter_carrier:
                    carr = proj["UNIQUE_CARRIER" if project_minimal else carrier_col].fillna("").astype(str).str.strip().str.upper()
                    # if project_minimal, column already renamed
                    if not project_minimal:
                        orig = chunk[origin_col].fillna("").astype(str).str.strip().str.upper()
                        dst = chunk[dest_col].fillna("").astype(str).str.strip().str.upper()
                    else:
                        orig = proj["ORIGIN"].fillna("").astype(str).str.strip().str.upper()
                        dst = proj["DEST"].fillna("").astype(str).str.strip().str.upper()

                    mask = (carr == filter_carrier.upper())
                    if airports_set is not None:
                        mask = mask & orig.isin(airports_set) & dst.isin(airports_set)

                    proj = proj.loc[mask]

                # after filtering/projection, set chunk to proj for downstream processing
                chunk = proj

            # If aircraft mapping provided, map codes to DESCRIPTION and add column
            if aircraft_map is not None:
                # normalize aircraft code in chunk to digits zfill(3)
                def _norm_series_code(s: pd.Series) -> pd.Series:
                    return s.fillna("").astype(str).str.replace(r"[^0-9]", "", regex=True).str.zfill(3)

                # determine which column in chunk contains aircraft code
                possible_cols = []
                if aircraft_col:
                    possible_cols.append(aircraft_col)
                # common canonical names
                possible_cols.extend(["AIRCRAFT_TYPE", "AIRCRAFT_CONFIG", "AIRCRAFT_TYPE_CODE"])
                col_in_chunk = next((c for c in possible_cols if c in chunk.columns), None)
                if col_in_chunk:
                    codes = _norm_series_code(chunk[col_in_chunk])
                    # ensure canonical AIRCRAFT_TYPE column exists
                    if "AIRCRAFT_TYPE" not in chunk.columns:
                        chunk["AIRCRAFT_TYPE"] = chunk[col_in_chunk]
                    chunk["DESCRIPTION"] = codes.map(aircraft_map).fillna("")

            # update union of columns and add missing cols to chunk
            for c in chunk.columns:
                if c not in columns_union:
                    columns_union.append(c)
            for c in columns_union:
                if c not in chunk.columns:
                    chunk[c] = pd.NA
            # reorder columns to stable union order
            chunk = chunk.loc[:, columns_union]

            if seen_keys is not None:
                # verify dedupe columns exist
                missing = [k for k in dedupe_on if k not in chunk.columns]
                if missing:
                    raise KeyError(f"Dedup keys not found in data: {missing}")

                # build a joined key string per row (fast vectorized op)
                key_series = chunk[dedupe_on].fillna("").astype(str).agg("||".join, axis=1)
                keep_mask = ~key_series.isin(seen_keys)
                # update seen set with new keys
                new_keys = key_series[keep_mask].tolist()
                seen_keys.update(new_keys)
                chunk = chunk.loc[keep_mask]

            if chunk.empty:
                continue

            # If we projected earlier, ensure header uses canonical minimal columns when requested
            if project_minimal:
                # ensure columns order and names
                out_cols = ["UNIQUE_CARRIER", "ORIGIN", "DEST", "YEAR", "MONTH"]
                # include DESCRIPTION only if actually present in this chunk
                if "DESCRIPTION" in chunk.columns:
                    out_cols.append("DESCRIPTION")
                chunk = chunk.loc[:, out_cols]
                chunk.to_csv(out_path, mode="a", index=False, header=not header_written)
            else:
                chunk.to_csv(out_path, mode="a", index=False, header=not header_written)
            header_written = True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", nargs="+", help="Input CSV files to merge (order preserved)")
    ap.add_argument("--out", default="outputs/US_CARRIER_SUMMARY_MERGED.csv", help="Output CSV path")
    ap.add_argument("--dedupe", default=None,
                    help="Comma-separated column list to dedupe on (keep first occurrence). Example: FL_DATE,OP_UNIQUE_CARRIER")
    ap.add_argument("--chunksize", type=int, default=200_000, help="pandas read_csv chunksize (set 0 or empty to disable)")
    ap.add_argument("--filter-aa", action="store_true", help="Keep only AA flights between the default 10 airports")
    ap.add_argument("--airports", default=','.join(DEFAULT_AIRPORTS), help="Comma-separated airport IATA codes to keep when using --filter-aa")
    ap.add_argument("--project-minimal", action="store_true", help="After filtering, only keep columns UNIQUE_CARRIER, ORIGIN, DEST, YEAR, MONTH")
    ap.add_argument("--aircraft-types", dest="aircraft_types", default=None,
                    help="Path to DOT_AIRCRAFT_TYPE.csv to map aircraft codes to DESCRIPTION")
    args = ap.parse_args()

    dedupe_on = [c.strip() for c in args.dedupe.split(",")] if args.dedupe else None
    chunksize = args.chunksize if args.chunksize and args.chunksize > 0 else None
    filter_airports = [a.strip().upper() for a in args.airports.split(",")] if args.filter_aa else None
    filter_carrier = "AA" if args.filter_aa else None

    merge_summaries(
        args.inputs,
        args.out,
        dedupe_on=dedupe_on,
        chunksize=chunksize,
        filter_carrier=filter_carrier,
        filter_airports=filter_airports,
        project_minimal=args.project_minimal,
        aircraft_types_path=args.aircraft_types,
    )


if __name__ == "__main__":
    main()
