"""Extraction + spatial join. Everything here needs geopandas and (for the
BCGW loaders) network access; imports are lazy so the pure-pandas core and
its tests run anywhere.

Three ways in:
  load_licences_file(path)      any file geopandas/pandas can read (gpkg,
                                geojson, csv with LATITUDE/LONGITUDE)
  fetch_licences_bcdata(...)    credential-free WFS via the bcdata package
  fetch_licences_oracle(...)    direct BCGW Oracle (oracledb thin)

Then:
  fetch_tiles_bcdata(cfg) / load_tiles_file(path, cfg)
  assign_pods_to_units(licences_gdf, tiles_gdf)  -> ROW_ID -> unit_id
"""
from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

import pandas as pd

LICENCE_LAYER = "WHSE_WATER_MANAGEMENT.WLS_WATER_RIGHTS_LICENCES_SV"
TILE_LAYER = "WHSE_BASEMAPPING.FWA_ASSESSMENT_WATERSHEDS_POLY"
# geometry column names differ between the two layers - verified against
# ALL_TAB_COLUMNS during profiling (a GEOMETRY guess on the licence layer is
# what raised ORA-00904 in the round-1 profiling run)
LICENCE_GEOM_COL = "SHAPE"
TILE_GEOM_COL = "GEOMETRY"
BC_ALBERS = "EPSG:3005"

LICENCE_COLUMNS = [
    "WLS_WRL_SYSID", "LICENCE_NUMBER", "FILE_NUMBER", "LICENCE_STATUS",
    "POD_NUMBER", "POD_SUBTYPE", "POD_STATUS", "PURPOSE_USE_CODE",
    "PURPOSE_USE", "QUANTITY", "QUANTITY_UNITS", "QUANTITY_FLAG",
    "QUANTITY_FLAG_DESCRIPTION", "HYDRAULIC_CONNECTIVITY", "SOURCE_NAME",
    "PRIORITY_DATE", "WELL_TAG_NUMBER", "PRIMARY_LICENSEE_NAME",
    # licensee mailing address (public water-rights register; this workbook is
    # for internal water-officer use, so contacts ship with the usage data)
    "ADDRESS_LINE_1", "ADDRESS_LINE_2", "ADDRESS_LINE_3", "ADDRESS_LINE_4",
    "POSTAL_CODE", "COUNTRY",
    "LATITUDE", "LONGITUDE",
]


def _gpd():
    try:
        import geopandas as gpd
        return gpd
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            "geopandas is required for extraction/spatial steps: "
            "pip install geopandas") from e


# --------------------------------------------------------------------------- #
# licences
# --------------------------------------------------------------------------- #
def load_licences_file(path: str | Path) -> pd.DataFrame:
    """Read a saved licence extract (gpkg/geojson/parquet/csv)."""
    path = Path(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    gpd = _gpd()
    return gpd.read_file(path)


def fetch_licences_bcdata(bounds: tuple[float, float, float, float]):
    """WFS pull clipped to a BC-Albers bounding box (e.g. the units' bbox
    padded a few km). Returns a GeoDataFrame in EPSG:3005."""
    import bcdata  # lazy; pip install bcdata
    # bcdata >=0.16 dropped the `crs` kwarg and returns the layer's native
    # projection (3005 for BCGW); reproject explicitly so this holds either way
    gdf = bcdata.get_data(LICENCE_LAYER, as_gdf=True,
                          bounds=list(bounds), bounds_crs=BC_ALBERS)
    gdf = gdf.to_crs(BC_ALBERS)
    gdf.columns = [c.upper() if c != "geometry" else c for c in gdf.columns]
    return gdf


def oracle_connect(user: str | None = None, password: str | None = None,
                   dsn: str | None = None):
    """Connect to BCGW with oracledb (thin mode - no Oracle client needed).

    Credentials fall back to BCGW_USER / BCGW_PASSWORD / BCGW_DSN.
    """
    import oracledb  # lazy
    # CLOBs (SDO_UTIL.TO_WKTGEOMETRY output) come back as str, not LOB handles
    oracledb.defaults.fetch_lobs = False
    creds = {"user": user or os.environ.get("BCGW_USER"),
             "password": password or os.environ.get("BCGW_PASSWORD"),
             "dsn": dsn or os.environ.get("BCGW_DSN")}
    missing = [k for k, v in creds.items() if not v]
    if missing:
        raise SystemExit(
            "missing BCGW credentials: "
            + ", ".join(f"BCGW_{k.upper()}" for k in missing)
            + "\nset them as environment variables, e.g. in PowerShell:\n"
              '  $env:BCGW_USER="..."; $env:BCGW_PASSWORD="..."; '
              '$env:BCGW_DSN="..."')
    return oracledb.connect(**creds)


def _query(con, sql: str, params: dict | None = None) -> pd.DataFrame:
    """Run SQL and return a DataFrame (cursor-based; pandas 3 does not take
    raw DBAPI connections)."""
    with con.cursor() as cur:
        cur.execute(sql, params or {})
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def fetch_licences_oracle(con=None, **creds) -> pd.DataFrame:
    """Province-wide attribute pull from BCGW Oracle.

    Deliberately unfiltered: the QA ladder in the workbook is meant to start
    at the documented province-wide raw count and step down through the
    status/shell/dedup stages (handover v2 section 6), so the filtering
    happens in the core, not in SQL. Geometry is rebuilt from LATITUDE /
    LONGITUDE rather than read from SHAPE, which keeps the ~10.4k null-SHAPE
    rows (mostly empty stubs) visible to the reconciliation.
    """
    cols = ", ".join(LICENCE_COLUMNS)
    sql = f"SELECT {cols} FROM {LICENCE_LAYER}"
    owns = con is None
    con = con or oracle_connect(**creds)
    try:
        df = _query(con, sql)
    finally:
        if owns:
            con.close()
    for c in ("QUANTITY", "LATITUDE", "LONGITUDE"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["EXTRACT_DATE"] = dt.datetime.now().isoformat(timespec="seconds")
    df["SOURCE_DATASET"] = LICENCE_LAYER
    return df


def fetch_tiles_oracle(cfg, con=None, groups=None, **creds):
    """Assessment-watershed tiles for the FWA group(s) the units need.

    Groups come from the watershed_group column of stream_units.csv (default
    OKAN); a cross-group unit (one whose watershed_group is not OKAN) adds its
    group so its tile is pulled too. Only bridged tiles survive the _merge_bridge join, so
    pulling a whole extra group and keeping one tile is harmless.

    Geometry crosses as WKT (SDO_UTIL.TO_WKTGEOMETRY) and is parsed by
    shapely - a few hundred polygons, so the text round-trip is cheap and
    avoids needing an Oracle spatial client.
    """
    gpd = _gpd()
    from shapely import wkt as shapely_wkt

    if groups is None:
        if "watershed_group" in cfg.stream_units.columns:
            groups = tuple(sorted(set(cfg.stream_units["watershed_group"].fillna("OKAN"))))
        else:
            groups = ("OKAN",)
    placeholders = ", ".join(f":g{i}" for i in range(len(groups)))
    params = {f"g{i}": g for i, g in enumerate(groups)}
    sql = f"""
        SELECT WATERSHED_FEATURE_ID, WATERSHED_GROUP_CODE, FWA_WATERSHED_CODE,
               LOCAL_WATERSHED_CODE, GNIS_NAME_1, AREA_HA,
               SDO_UTIL.TO_WKTGEOMETRY({TILE_GEOM_COL}) AS WKT
        FROM {TILE_LAYER}
        WHERE WATERSHED_GROUP_CODE IN ({placeholders})
    """
    owns = con is None
    con = con or oracle_connect(**creds)
    try:
        df = _query(con, sql, params)
    finally:
        if owns:
            con.close()
    if df.empty:
        raise ValueError(f"no tiles returned for watershed group(s) {groups!r}")
    geom = df.pop("WKT").map(shapely_wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry=geom.values, crs=BC_ALBERS)
    gdf.columns = [c.lower() if c != "geometry" else c for c in gdf.columns]
    return _merge_bridge(gdf, cfg)


def to_points(df: pd.DataFrame):
    """Build EPSG:3005 POD points from LATITUDE/LONGITUDE (rows without
    coordinates keep a null geometry and simply never join a unit)."""
    gpd = _gpd()
    if hasattr(df, "geometry") and getattr(df, "crs", None) is not None:
        return df.to_crs(BC_ALBERS)
    pts = gpd.GeoSeries.from_xy(df["LONGITUDE"], df["LATITUDE"], crs="EPSG:4326")
    return gpd.GeoDataFrame(df, geometry=pts).to_crs(BC_ALBERS)


# --------------------------------------------------------------------------- #
# tiles
# --------------------------------------------------------------------------- #
def fetch_tiles_bcdata(cfg):
    """The 71 bridged assessment-watershed tiles, merged with unit_id."""
    import bcdata
    gdf = bcdata.get_data(TILE_LAYER, as_gdf=True,
                          query="WATERSHED_GROUP_CODE='OKAN'")
    gdf = gdf.to_crs(BC_ALBERS)
    gdf.columns = [c.lower() if c != "geometry" else c for c in gdf.columns]
    return _merge_bridge(gdf, cfg)


def load_tiles_file(path: str | Path, cfg):
    gpd = _gpd()
    gdf = gpd.read_file(path).to_crs(BC_ALBERS)
    gdf.columns = [c.lower() if c != "geometry" else c for c in gdf.columns]
    return _merge_bridge(gdf, cfg)


def _merge_bridge(tiles, cfg):
    keep = tiles.merge(cfg.bridge[["watershed_feature_id", "unit_id"]],
                       on="watershed_feature_id", how="inner")
    missing = set(cfg.bridge["watershed_feature_id"]) - \
        set(keep["watershed_feature_id"])
    if missing:
        raise ValueError(f"bridge tiles missing from tile layer: {sorted(missing)}")
    return keep


# --------------------------------------------------------------------------- #
# spatial join
# --------------------------------------------------------------------------- #
def assign_pods_to_units(cleaned: pd.DataFrame, tiles,
                         boundary_flag_m: float = 50.0) -> pd.DataFrame:
    """ROW_ID -> unit_id via point-in-tile, plus a near-boundary review flag.

    `cleaned` is the post-dedup table from the core (carries ROW_ID); rows
    without coordinates get no unit and surface as 'outside_units'.
    """
    gpd = _gpd()
    pts = to_points(cleaned[["ROW_ID", "LATITUDE", "LONGITUDE"]].copy())
    # the tile's own name travels with the join: a unit is a whole drainage,
    # so a POD often sits in a named tributary (Winfield Creek) rather than
    # the stream the unit is named for (Vernon Creek). Without this the row
    # looks like it was filed under the wrong watershed.
    name_col = next((c for c in ("gnis_name_1", "GNIS_NAME_1")
                     if c in tiles.columns), None)
    keep = ["unit_id", "geometry"] + ([name_col] if name_col else [])
    hit = gpd.sjoin(pts, tiles[keep], how="left", predicate="within")
    out_cols = ["ROW_ID", "unit_id"] + ([name_col] if name_col else [])
    out = hit[out_cols].drop_duplicates("ROW_ID")
    if name_col:
        out = out.rename(columns={name_col: "ASSESSMENT_WATERSHED"})

    # near-boundary flag: distance to the assigned unit's dissolved boundary
    dissolved = tiles.dissolve("unit_id")["geometry"]
    merged = pts.merge(out, on="ROW_ID", how="left")
    dist = []
    for _, r in merged.iterrows():
        if pd.isna(r["unit_id"]) or r.geometry is None:
            dist.append(pd.NA)
        else:
            dist.append(r.geometry.distance(dissolved[r["unit_id"]].boundary))
    out = out.assign(BOUNDARY_DIST_M=pd.array(dist, dtype="Float64"))
    out["NEAR_BOUNDARY"] = out["BOUNDARY_DIST_M"] < boundary_flag_m
    return out
