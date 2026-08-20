#!/usr/bin/env python3
"""Okanagan TPO screening pipeline - entry point.

Examples
--------
# from saved extracts (recommended first run: reproducible inputs)
python run_pipeline.py --licences data/licences.gpkg --tiles data/okan_tiles.gpkg \
    --out out/okanagan_tpo_screening.xlsx

# straight from BCGW Oracle over SQL (preferred; needs oracledb + credentials
# in BCGW_USER / BCGW_PASSWORD / BCGW_DSN)
python run_pipeline.py --oracle --out out/okanagan_tpo_screening.xlsx

# credential-free WFS fallback (needs: pip install bcdata geopandas). Note the
# WFS bbox filter cannot return null-geometry rows, so the QA ladder will not
# reconcile to the province-wide profile.
python run_pipeline.py --bcdata --out out/okanagan_tpo_screening.xlsx

Order of operations: extract -> CLEAN (status/shells/dedup assigns ROW_ID)
-> spatial join on the cleaned rows -> components/rates -> workbook.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from tpo import io_sources as io
from tpo import pipeline as core
from tpo.workbook import write_workbook


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", default="config", help="config CSV directory")
    p.add_argument("--licences", help="saved licence extract (gpkg/geojson/csv)")
    p.add_argument("--tiles", help="saved OKAN assessment-watershed tiles")
    p.add_argument("--oracle", action="store_true",
                   help="pull licences and tiles from BCGW Oracle via SQL "
                        "(needs BCGW_USER/BCGW_PASSWORD/BCGW_DSN)")
    p.add_argument("--bcdata", action="store_true",
                   help="pull licences and tiles live from BCGW via WFS "
                        "(note: cannot return null-geometry rows)")
    p.add_argument("--pad-km", type=float, default=5.0,
                   help="bbox padding around units for the licence pull")
    p.add_argument("--out", default="out/okanagan_tpo_screening.xlsx")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cfg = core.Config(args.config)

    # one Oracle connection serves both pulls
    con = io.oracle_connect() if args.oracle else None
    try:
        # -------------------------------------------------------------- tiles
        if args.oracle:
            tiles = io.fetch_tiles_oracle(cfg, con=con)
        elif args.bcdata:
            tiles = io.fetch_tiles_bcdata(cfg)
        elif args.tiles:
            tiles = io.load_tiles_file(args.tiles, cfg)
        else:
            raise SystemExit("provide --tiles FILE, --oracle, or --bcdata")

        # ----------------------------------------------------------- licences
        if args.oracle:
            # province-wide by design: the QA ladder starts at the documented
            # raw count and steps down in the core, not in SQL
            raw = io.fetch_licences_oracle(con=con)
        elif args.bcdata:
            pad = args.pad_km * 1000
            minx, miny, maxx, maxy = tiles.total_bounds
            raw = io.fetch_licences_bcdata((minx - pad, miny - pad,
                                            maxx + pad, maxy + pad))
        elif args.licences:
            raw = io.load_licences_file(args.licences)
        else:
            raise SystemExit("provide --licences FILE, --oracle, or --bcdata")
    finally:
        if con is not None:
            con.close()
    raw = pd.DataFrame(raw)

    # ------------------------------------------------- clean, join, then core
    # run cleaning first so ROW_ID exists, then the spatial join, then rerun
    # the core with the mapping (the core is cheap; cleanliness beats cleverness)
    ledger, exceptions = core.Ledger(), core.Exceptions()
    df = core.normalize_strings(raw)
    df = core.filter_current(df, ledger)
    df, _shells = core.split_shells(df, ledger)
    df = core.dedup_holders(df, ledger, exceptions)

    pod_units = io.assign_pods_to_units(df, tiles)

    # boundary columns ride along so they reach POD_Audit; allocate() only
    # reads unit_id and reselects its own output, so they are inert there
    carry = [c for c in ("ROW_ID", "unit_id", "ASSESSMENT_WATERSHED",
                         "BOUNDARY_DIST_M", "NEAR_BOUNDARY")
             if c in pod_units.columns]
    tables = core.run_core(raw, pod_units[carry], cfg)
    near = pod_units[pod_units["NEAR_BOUNDARY"].fillna(False)]
    tables["ledger"].loc[len(tables["ledger"])] = {
        "stage": "pods_near_unit_boundary", "rows": len(near),
        "annual_m3": None, "note": "within 50 m - review material quantities"}

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    if args.oracle:
        source = f"BCGW Oracle (SQL) - {io.LICENCE_LAYER}"
    elif args.bcdata:
        source = f"BCGW WFS via bcdata - {io.LICENCE_LAYER} (bbox-clipped)"
    else:
        source = f"saved extract - {args.licences}"
    extracted = (raw["EXTRACT_DATE"].iloc[0]
                 if "EXTRACT_DATE" in raw.columns and len(raw) else "")
    write_workbook(str(out), tables, extract_note=source,
                   extract_date=extracted)
    print(tables["ledger"].to_string(index=False))
    print(f"\nworkbook written: {out}")


if __name__ == "__main__":
    main()
