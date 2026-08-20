"""
02_resolve_aquifers.py
======================
Determine which aquifer(s) each application sits in, by two independent routes.

This is the core of the analysis. The client spreadsheet has an `Aquifer`
column but it is empty on all 712 rows, so aquifer identity has to be derived.

Two passes
----------
Pass 1 - PARCEL. PID -> PMBC polygon -> intersect aquifers. Returns ALL
    intersecting aquifers with the share of parcel area in each. This is the
    client-directed method.

Pass 2 - WELL. Cleaned well coordinate -> point-in-polygon against aquifers.
    One aquifer per well. Run as an independent check, and as the only route
    for the ~60 applications that have coordinates but no PID.

Why both
--------
A parcel can straddle several aquifers; a well point sits in exactly one.
Parcel-only assignment therefore OVER-FLAGS: an application can be excluded
because its parcel clips a restricted aquifer even though the well itself is
elsewhere. Running both quantifies that cost instead of leaving it unknown.
The disagreement count is a deliverable in its own right.

Why full polygons, not centroids
--------------------------------
A centroid can fall outside an L-shaped or multipart parcel, and it discards
the multi-aquifer case entirely. Centroids are used only in step 04 for the
output point layer, and there via representative_point(), which is guaranteed
to fall inside the geometry.

Known limitation - vertical stacking
------------------------------------
This is a 2D analysis. Aquifers stack, so a well on a parcel overlying a
shallow sand-and-gravel aquifer may in fact be completed in bedrock beneath it.
`Well Depth` is present in the source but BC aquifer mapping does not generally
carry vertical extents to test against, so depth cannot resolve this. Where a
location overlies multiple aquifers the ambiguity is flagged, not resolved.

Output (data/interim/application_aquifers.csv)
----------------------------------------------
Long format: one row per application x aquifer x method. Deliberately NOT
collapsed to one row per application - that happens in step 04, so the
combination rule stays a configuration choice.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
with open(ROOT / "config.yaml") as f:
    CFG = yaml.safe_load(f)

P = CFG["paths"]
CRS = CFG["project"]["crs"]
SRC_CRS = CFG["project"]["source_crs"]
ASSIGN = CFG["aquifer_assignment"]


# --------------------------------------------------------------------------- #
# Pass 1 - parcel based
# --------------------------------------------------------------------------- #

def resolve_by_parcel(pids: pd.DataFrame, parcels: gpd.GeoDataFrame,
                      aquifers: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Intersect application parcels with aquifers.

    Returns one row per application x aquifer with `overlap_share`, the
    proportion of total parcel area for that application falling inside the
    aquifer. Keeping this as a proportion rather than a boolean means the
    inclusion threshold stays a config parameter.
    """
    linked = pids.merge(parcels, left_on="pid", right_on="PID", how="inner")
    if linked.empty:
        return pd.DataFrame(columns=["tracking_number", "aquifer_id",
                                     "method", "overlap_share"])

    gdf = gpd.GeoDataFrame(linked, geometry="geometry", crs=CRS)

    # Dissolve multi-parcel applications so overlap shares are per application
    app_geom = gdf.dissolve(by="tracking_number").reset_index()
    app_geom["parcel_area"] = app_geom.geometry.area

    inter = gpd.overlay(
        app_geom[["tracking_number", "parcel_area", "geometry"]],
        aquifers[["AQUIFER_ID", "geometry"]],
        how="intersection", keep_geom_type=False,
    )
    if inter.empty:
        return pd.DataFrame(columns=["tracking_number", "aquifer_id",
                                     "method", "overlap_share"])

    inter["overlap_area"] = inter.geometry.area
    inter["overlap_share"] = (inter["overlap_area"] / inter["parcel_area"]).clip(0, 1)

    out = (inter.groupby(["tracking_number", "AQUIFER_ID"], as_index=False)
                .agg(overlap_share=("overlap_share", "sum"))
                .rename(columns={"AQUIFER_ID": "aquifer_id"}))
    out = out[out["overlap_share"] >= ASSIGN["min_parcel_overlap"]]
    out["method"] = "parcel"
    return out


# --------------------------------------------------------------------------- #
# Pass 2 - well point based
# --------------------------------------------------------------------------- #

def resolve_by_well(wells: pd.DataFrame,
                    aquifers: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Point-in-polygon assignment for wells with usable coordinates.

    Only coordinates flagged `ok` or `sign_corrected` in step 01 are used;
    low-precision and out-of-BC values are excluded.
    """
    usable = wells[wells["coord_status"].isin(["ok", "sign_corrected"])].copy()
    if usable.empty:
        return pd.DataFrame(columns=["tracking_number", "aquifer_id",
                                     "method", "n_wells_in_aquifer"])

    pts = gpd.GeoDataFrame(
        usable,
        geometry=gpd.points_from_xy(usable["longitude"], usable["latitude"]),
        crs=SRC_CRS,
    ).to_crs(CRS)

    joined = gpd.sjoin(pts, aquifers[["AQUIFER_ID", "geometry"]],
                       how="left", predicate="within")

    out = (joined[joined["AQUIFER_ID"].notna()]
           .groupby(["tracking_number", "AQUIFER_ID"], as_index=False)
           .agg(n_wells_in_aquifer=("well_index", "count"))
           .rename(columns={"AQUIFER_ID": "aquifer_id"}))
    out["method"] = "well"
    return out, joined


# --------------------------------------------------------------------------- #
# Cross-check
# --------------------------------------------------------------------------- #

def check_well_in_parcel(well_pts: gpd.GeoDataFrame,
                         pids: pd.DataFrame,
                         parcels: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Does each well coordinate fall inside its own application's parcel?

    A free consistency check. Where both agree, confidence is high. Where they
    disagree, one of the two is wrong - bad coordinate, wrong PID, or a well
    genuinely sited off-parcel. Catches errors neither source reveals alone.
    """
    linked = pids.merge(parcels, left_on="pid", right_on="PID", how="inner")
    if linked.empty or well_pts.empty:
        return pd.DataFrame(columns=["tracking_number", "well_on_parcel"])

    app_parcels = (gpd.GeoDataFrame(linked, geometry="geometry", crs=CRS)
                   .dissolve(by="tracking_number").reset_index()
                   [["tracking_number", "geometry"]])

    merged = well_pts[["tracking_number", "well_index", "geometry"]].merge(
        app_parcels.rename(columns={"geometry": "parcel_geom"}),
        on="tracking_number", how="inner")
    if merged.empty:
        return pd.DataFrame(columns=["tracking_number", "well_on_parcel"])

    merged["well_on_parcel"] = [
        pg.contains(wg) if pg is not None and wg is not None else None
        for wg, pg in zip(merged["geometry"], merged["parcel_geom"])
    ]
    return (merged.groupby("tracking_number", as_index=False)
                  .agg(wells_checked=("well_on_parcel", "size"),
                       wells_on_parcel=("well_on_parcel", "sum")))


def reconcile(parcel_res: pd.DataFrame, well_res: pd.DataFrame) -> pd.DataFrame:
    """
    Combine both passes into one long table with an agreement flag per
    application: parcel_only, well_only, agree, disagree.
    """
    combined = pd.concat([parcel_res, well_res], ignore_index=True)
    if combined.empty:
        return combined

    p = parcel_res.groupby("tracking_number")["aquifer_id"].apply(set)
    w = well_res.groupby("tracking_number")["aquifer_id"].apply(set)

    flags = []
    for tn in set(p.index) | set(w.index):
        ps, ws = p.get(tn, set()), w.get(tn, set())
        if ps and not ws:
            f = "parcel_only"
        elif ws and not ps:
            f = "well_only"
        elif ps == ws:
            f = "agree"
        elif ws & ps:
            f = "partial_agree"
        else:
            f = "disagree"
        flags.append({"tracking_number": tn, "agreement": f,
                      "n_aquifers_parcel": len(ps), "n_aquifers_well": len(ws)})

    return combined.merge(pd.DataFrame(flags), on="tracking_number", how="left")


def apply_assignment_rule(result: pd.DataFrame) -> pd.DataFrame:
    """
    Mark which application x aquifer rows the client's rule actually selects.

    The PID is the primary location and the well is used only where the PID
    cannot answer (config.aquifer_assignment.primary_method, and the note
    beside it). Three cases, recorded in `assignment_basis`:

        parcel                    parcel resolves to exactly one aquifer.
                                  The well is ignored even if it disagrees.
        well_no_pid               no usable PID; the well is the only location.
        well_parcel_ambiguous     the parcel straddles several aquifers and the
                                  well says which one.
        parcel_ambiguous_no_well  the parcel straddles several and there is no
                                  usable well. Every parcel aquifer is kept -
                                  the ambiguity is carried, not guessed away.

    Both passes are left in the table; only `assigned` changes. Step 04
    collapses over the assigned rows, while `By_aquifer` still shows all of
    them so the choice is auditable.
    """
    if result.empty:
        result["assigned"] = []
        result["assignment_basis"] = []
        return result

    frames = []
    for _, g in result.groupby("tracking_number", sort=False):
        p_ids = set(g.loc[g["method"] == "parcel", "aquifer_id"])
        w_ids = set(g.loc[g["method"] == "well", "aquifer_id"])

        if not p_ids:
            basis, method, ids = "well_no_pid", "well", w_ids
        elif len(p_ids) == 1:
            basis, method, ids = "parcel", "parcel", p_ids
        elif w_ids:
            basis, method, ids = "well_parcel_ambiguous", "well", w_ids
        else:
            basis, method, ids = "parcel_ambiguous_no_well", "parcel", p_ids

        g = g.copy()
        g["assigned"] = (g["method"] == method) & g["aquifer_id"].isin(ids)
        g["assignment_basis"] = basis
        frames.append(g)

    return pd.concat(frames, ignore_index=True)


def _norm_id(s: pd.Series) -> pd.Series:
    """Aquifer IDs arrive from GeoPandas as floats; '161.0' must match '161'."""
    return s.astype(str).str.replace(r"\.0$", "", regex=True)


def attach_aquifer_attributes(result: pd.DataFrame,
                              aquifers: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Carry aquifer name, material and subtype onto the application x aquifer
    pairs.

    MATERIAL is not used to decide anything - an application is undetermined
    when more than one aquifer is assigned, whatever they are made of - but it
    is carried so the candidate aquifers can be listed as "32 (Sand and
    Gravel)" for the well-by-well review, and so the layering can be seen.
    """
    if result.empty:
        return result

    cols = ["AQUIFER_ID", "AQUIFER_NAME", "MATERIAL", "SUBTYPE"]
    attrs = aquifers[[c for c in cols if c in aquifers.columns]].copy()
    attrs["aquifer_id"] = _norm_id(attrs["AQUIFER_ID"])
    attrs = attrs.drop(columns=["AQUIFER_ID"]).drop_duplicates("aquifer_id")

    result["aquifer_id"] = _norm_id(result["aquifer_id"])
    return result.merge(attrs, on="aquifer_id", how="left")


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main():
    interim = ROOT / P["interim_dir"]
    cache = ROOT / P["bcgw_cache"]
    if not cache.exists():
        raise SystemExit("BCGW cache not found. Run 00_extract_bcgw.py first.")

    pids = pd.read_csv(interim / "application_pids.csv",
                       dtype={"pid": str, "tracking_number": str})
    pids = pids[pids["pid"].notna()]
    wells = pd.read_csv(interim / "application_wells.csv",
                        dtype={"tracking_number": str})

    parcels = gpd.read_file(cache, layer="parcels").to_crs(CRS)
    aquifers = gpd.read_file(cache, layer="aquifers").to_crs(CRS)
    parcels["PID"] = parcels["PID"].astype(str).str.zfill(9)

    parcel_res = resolve_by_parcel(pids, parcels, aquifers)
    well_res, well_pts = resolve_by_well(wells, aquifers)
    result = reconcile(parcel_res, well_res)
    result = attach_aquifer_attributes(result, aquifers)
    result = apply_assignment_rule(result)
    consistency = check_well_in_parcel(well_pts, pids, parcels)

    result.to_csv(interim / "application_aquifers.csv", index=False)
    consistency.to_csv(interim / "qa_well_in_parcel.csv", index=False)
    well_pts.drop(columns=["index_right"], errors="ignore").to_file(
        cache, layer="well_points", driver="GPKG")

    # ----------------------------- summary -------------------------------- #
    n_apps = pd.read_csv(interim / "applications.csv").shape[0]
    resolved = result["tracking_number"].nunique() if len(result) else 0

    print("=" * 68)
    print("STEP 02  RESOLVE AQUIFERS")
    print("=" * 68)
    print(f"Applications                   {n_apps}")
    print(f"  aquifer resolved             {resolved}  "
          f"({resolved / n_apps * 100:4.1f}%)")
    print(f"  UNRESOLVED                   {n_apps - resolved}")

    print(f"\nBy parcel   {parcel_res['tracking_number'].nunique():>5d} applications, "
          f"{len(parcel_res)} application-aquifer pairs")
    print(f"By well     {well_res['tracking_number'].nunique():>5d} applications, "
          f"{len(well_res)} application-aquifer pairs")

    if len(result):
        print("\nAgreement between methods")
        agree = result.drop_duplicates("tracking_number")["agreement"]
        for k, c in agree.value_counts().items():
            print(f"  {k:<28s} {c:>5d}  ({c / resolved * 100:4.1f}%)")

        multi = (result[result["method"] == "parcel"]
                 .groupby("tracking_number").size())
        print(f"\nMulti-aquifer applications (parcel): "
              f"{(multi > 1).sum()} of {len(multi)}")

        basis = (result.drop_duplicates("tracking_number")["assignment_basis"]
                 .value_counts())
        print("\nAssignment basis (PID primary; well only where PID cannot say)")
        for k, c in basis.items():
            print(f"  {k:<28s} {c:>5d}")
        print(f"  assigned pairs {int(result['assigned'].sum())} "
              f"of {len(result)} computed")

    if len(consistency):
        off = consistency[consistency["wells_on_parcel"]
                          < consistency["wells_checked"]]
        print(f"\nConsistency check: {len(off)} applications have at least one "
              f"well falling outside their own parcel")

    print(f"\nWritten to {interim / 'application_aquifers.csv'}")
    print("=" * 68)


if __name__ == "__main__":
    main()
