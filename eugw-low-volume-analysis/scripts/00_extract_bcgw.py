"""
00_extract_bcgw.py
==================
Pull the reference layers from BCGW into a local GeoPackage cache.

Run once, then re-run only when the source data is refreshed. Everything
downstream reads the cache, so the analysis can be re-run and debugged without
hitting Oracle each time.

*** NOT YET TESTED AGAINST BCGW. ***
Object and field names below are taken from the BC Data Catalogue but MUST be
verified before the first production run. At least one commonly-cited object
(WLS_STREAM_RESTRICTIONS_SP) no longer exists, so treat all names as
provisional until confirmed. Verify with:

    SELECT column_name, data_type FROM all_tab_columns
    WHERE owner = 'WHSE_WATER_MANAGEMENT'
      AND table_name = 'GW_AQUIFERS_CLASSIFICATION_SVW';

Design notes
------------
Oracle is used for EXTRACTION ONLY - no spatial operators. Parcels are the one
layer that must be filtered server-side (PMBC holds millions of rows and we
need ~760), so PIDs go in a batched IN clause. Everything else is small enough
to pull whole. All spatial logic happens locally in geopandas, where it is far
easier to debug and re-run.

Geometry is returned as WKB rather than WKT: more compact, faster to serialise,
and loads straight into shapely without a CLOB round-trip.

Credentials come from the environment, never from this file:
    export BCGW_USER=your_idir
    export BCGW_PASSWORD=your_password
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd
import yaml
from shapely import wkb

try:
    import oracledb
except ImportError:  # pragma: no cover
    raise SystemExit("oracledb not installed:  pip install oracledb")

ROOT = Path(__file__).resolve().parents[1]
with open(ROOT / "config.yaml") as f:
    CFG = yaml.safe_load(f)

BCGW = CFG["bcgw"]
CRS = CFG["project"]["crs"]


# --------------------------------------------------------------------------- #
# Connection
# --------------------------------------------------------------------------- #

def connect():
    """Open a BCGW connection using thin mode (no Oracle client install needed)."""
    user, pw = os.environ.get("BCGW_USER"), os.environ.get("BCGW_PASSWORD")
    if not user or not pw:
        raise SystemExit("Set BCGW_USER and BCGW_PASSWORD environment variables.")
    print(f"Connecting to {BCGW['dsn']} as {user} ...")
    return oracledb.connect(user=user, password=pw, dsn=BCGW["dsn"])


def query_spatial(conn, sql: str, params=None, geom_alias="GEOM_WKB") -> gpd.GeoDataFrame:
    """
    Run a query returning WKB geometry and build a GeoDataFrame.

    The geometry column must be aliased to `geom_alias` and wrapped in
    SDO_UTIL.TO_WKBGEOMETRY() in the SQL.
    """
    with conn.cursor() as cur:
        cur.execute(sql, params or {})
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        return gpd.GeoDataFrame(df, geometry=[], crs=CRS)

    # oracledb returns LOBs lazily; read them before shapely sees them
    df[geom_alias] = df[geom_alias].apply(
        lambda v: v.read() if hasattr(v, "read") else v
    )
    geom = df.pop(geom_alias).apply(lambda b: wkb.loads(bytes(b)) if b else None)
    return gpd.GeoDataFrame(df, geometry=geom, crs=CRS)


# --------------------------------------------------------------------------- #
# Extractions
# --------------------------------------------------------------------------- #

def extract_parcels(conn, pids: list[str]) -> gpd.GeoDataFrame:
    """
    Fetch parcel polygons for the supplied PIDs, batched around Oracle's
    1000-item IN-list limit.

    PIDs must already be 9-character zero-padded strings (see step 01).
    """
    cfg = BCGW["parcels"]
    batch = BCGW["batch_size"]
    frames = []

    for start in range(0, len(pids), batch):
        chunk = pids[start:start + batch]
        binds = {f"p{i}": v for i, v in enumerate(chunk)}
        placeholders = ", ".join(f":{k}" for k in binds)
        sql = f"""
            SELECT {cfg['pid_field']} AS PID,
                   PARCEL_STATUS,
                   OWNER_TYPE,
                   SDO_UTIL.TO_WKBGEOMETRY({cfg['geom_field']}) AS GEOM_WKB
            FROM   {cfg['table']}
            WHERE  {cfg['pid_field']} IN ({placeholders})
        """
        frames.append(query_spatial(conn, sql, binds))
        print(f"  parcels batch {start // batch + 1}: "
              f"{len(chunk)} requested, {len(frames[-1])} returned")

    gdf = pd.concat(frames, ignore_index=True) if frames else gpd.GeoDataFrame()
    gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs=CRS)

    missing = set(pids) - set(gdf["PID"].astype(str))
    if missing:
        print(f"  WARNING: {len(missing)} PIDs not found in PMBC "
              f"(retired or subdivided parcels). Logged to qa_missing_pids.csv")
        pd.DataFrame({"pid": sorted(missing)}).to_csv(
            ROOT / CFG["paths"]["interim_dir"] / "qa_missing_pids.csv", index=False)
    return gdf


def extract_aquifers(conn) -> gpd.GeoDataFrame:
    """All mapped aquifers province-wide (~1,100 features - small enough to pull whole)."""
    cfg = BCGW["aquifers"]
    sql = f"""
        SELECT {cfg['id_field']} AS AQUIFER_ID,
               {cfg['name_field']} AS AQUIFER_NAME,
               {cfg['material_field']} AS MATERIAL,
               {cfg['subtype_field']} AS SUBTYPE,
               PRODUCTIVITY,
               VULNERABILITY,
               DEMAND,
               SDO_UTIL.TO_WKBGEOMETRY({cfg['geom_field']}) AS GEOM_WKB
        FROM   {cfg['table']}
    """
    return query_spatial(conn, sql)


def extract_aquifer_notations(conn) -> pd.DataFrame:
    """
    Aquifer allocation notations. Attributes only - we join to the aquifer
    polygons by ID rather than spatially.
    """
    sql = f"SELECT * FROM {BCGW['aquifer_notations']['table']}"
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = [r for r in cur.fetchall()]
    # Drop the geometry column if present; we only need the attributes
    df = pd.DataFrame(rows, columns=cols)
    return df.drop(columns=[c for c in df.columns
                            if c in ("SHAPE", "GEOMETRY", "SE_ANNO_CAD_DATA")],
                   errors="ignore")


def extract_water_reserves(conn) -> pd.DataFrame:
    """
    Aquifers reserved under WSA s.39-41.

    There is no aquifer number field - it is embedded in the description text
    ("OIC 0347/2024 - Aquifer 203 - 2024/06/17"), so it is parsed out here.
    Fragile if the naming convention changes; verify the parse count against
    the record count on every refresh.
    """
    cfg = BCGW["water_reserves"]
    sql = f"SELECT * FROM {cfg['table']}"
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=cols)
    df = df.drop(columns=[c for c in df.columns
                          if c in ("SHAPE", "GEOMETRY", "SE_ANNO_CAD_DATA")],
                 errors="ignore")

    if cfg.get("active_only") and "OIC_INACTIVE_IND" in df.columns:
        df = df[df["OIC_INACTIVE_IND"].isna() | (df["OIC_INACTIVE_IND"] == "")]

    df["aquifer_id"] = (df[cfg["desc_field"]].astype(str)
                        .str.extract(cfg["aquifer_regex"], flags=re.I)[0])
    unparsed = df["aquifer_id"].isna().sum()
    if unparsed:
        print(f"  WARNING: {unparsed} reserve records - aquifer number not parsed. "
              f"Check the description format.")
    return df


def extract_tpo(conn) -> gpd.GeoDataFrame:
    """
    Temporary Protection Orders.

    Client confirmed the criterion applies to any watershed that has EXPERIENCED
    an order, so historical records are retained. Filtering by ORDER_STATUS
    happens in step 03 based on config.criteria.tpo_scope.
    """
    cfg = BCGW["temp_protection_orders"]
    sql = f"""
        SELECT WATERSHED_NAME, ORDER_NUMBER, LEGISLATION, ORDER_STATUS,
               ORDER_START_DATE, ORDER_END_DATE, NUM_EUGW_APPS_AFFCTD,
               SDO_UTIL.TO_WKBGEOMETRY({cfg['geom_field']}) AS GEOM_WKB
        FROM   {cfg['table']}
    """
    return query_spatial(conn, sql)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main():
    interim = ROOT / CFG["paths"]["interim_dir"]
    cache = ROOT / CFG["paths"]["bcgw_cache"]

    pid_file = interim / "application_pids.csv"
    if not pid_file.exists():
        raise SystemExit("Run 01_prepare_applications.py first.")

    pids = pd.read_csv(pid_file, dtype={"pid": str})
    pids = sorted(pids.loc[pids["pid"].notna(), "pid"].unique())
    print(f"{len(pids)} distinct PIDs to fetch\n")

    conn = connect()
    try:
        print("Extracting parcels ...")
        parcels = extract_parcels(conn, pids)
        parcels.to_file(cache, layer="parcels", driver="GPKG")

        print("Extracting aquifers ...")
        aquifers = extract_aquifers(conn)
        aquifers.to_file(cache, layer="aquifers", driver="GPKG")

        print("Extracting temporary protection orders ...")
        tpo = extract_tpo(conn)
        tpo.to_file(cache, layer="tpo", driver="GPKG")

        print("Extracting aquifer notations ...")
        extract_aquifer_notations(conn).to_csv(
            interim / "aquifer_notations.csv", index=False)

        print("Extracting water reserves ...")
        extract_water_reserves(conn).to_csv(
            interim / "water_reserves.csv", index=False)
    finally:
        conn.close()

    # Provenance. BCGW is live - the TPO layer gained a record between two
    # runs a few days apart - so the deliverable has to be able to say which
    # vintage of the source data it was built from.
    extracted_at = datetime.now().astimezone()
    json.dump(
        {
            "extracted_at": extracted_at.isoformat(timespec="seconds"),
            "extracted_at_display": extracted_at.strftime("%Y-%m-%d %H:%M %Z"),
            "dsn": BCGW["dsn"],
            "row_counts": {"parcels": len(parcels), "aquifers": len(aquifers),
                           "tpo": len(tpo)},
        },
        open(interim / "bcgw_extract_info.json", "w"), indent=2)

    print("\n" + "=" * 68)
    print("STEP 00  BCGW EXTRACTION")
    print("=" * 68)
    print(f"extracted  {extracted_at.strftime('%Y-%m-%d %H:%M %Z')}")
    print(f"parcels    {len(parcels):>6d}  ({len(pids)} PIDs requested)")
    print(f"aquifers   {len(aquifers):>6d}")
    print(f"tpo        {len(tpo):>6d}")
    print(f"\nCached to {cache}")
    print("=" * 68)


if __name__ == "__main__":
    main()
