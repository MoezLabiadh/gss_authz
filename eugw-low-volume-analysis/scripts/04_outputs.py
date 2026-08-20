"""
04_outputs.py
=============
Collapse the long criteria table to one verdict per application and write the
two deliverables: a spreadsheet and a spatial layer.

This is the only step where the multi-aquifer collapse happens. Everything
upstream stays long so the combination rule remains a configuration choice
(criteria.collapse_rule) rather than something baked into the spatial work.

Collapse rule
-------------
"any"  - an application fails if ANY associated aquifer is restricted.
         Conservative, and the default. Combined with parcel-based assignment
         it will over-flag, because a parcel can clip a restricted aquifer the
         well is not in. The `method_disagreement` column quantifies that.
"all"  - fails only if ALL associated aquifers are restricted.

Outputs (data/outputs/)
-----------------------
EUGW_low_volume_screening.xlsx
    Summary        - funnel, counts, and the caveats. Read this first.
    Applications   - one row per application, pass/fail per criterion.
    By_aquifer     - the long table, for anyone who wants to see the working.
    Not_assessed   - the four criteria with no data source, and why.
    QA             - cleaning log and unresolved records.
EUGW_low_volume_screening.gpkg
    application_points - one point per application, all screening attributes.

Point geometry uses representative_point() rather than centroid(): a centroid
can fall outside an L-shaped or multipart parcel, a representative point
cannot. Wells are used where available, parcels otherwise.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyogrio
import yaml
from openpyxl.styles import Alignment, Border, Font, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo

ROOT = Path(__file__).resolve().parents[1]
with open(ROOT / "config.yaml") as f:
    CFG = yaml.safe_load(f)

P = CFG["paths"]
CRIT = CFG["criteria"]
CRS = CFG["project"]["crs"]
BCGW = CFG["bcgw"]
UNDET = CFG["aquifer_assignment"]["undetermined_aquifer"]


# --------------------------------------------------------------------------- #
# Workbook formatting
#
# Reproduces the formatting applied by hand to the 2026-08-12 review copy, so
# it survives a re-run instead of being redone each time.
# --------------------------------------------------------------------------- #

THIN = Side(style="thin")
BOX = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
TABLE_STYLE = "TableStyleMedium9"


def _autofit(ws, floor: int = 10, cap: int = 32) -> None:
    """
    Width from content, capped.

    The cap matters: legal_description and the aquifer list run to hundreds of
    characters, and a true autofit would make the sheet unusable.
    """
    for i, col in enumerate(ws.iter_cols(min_row=1, max_row=ws.max_row), 1):
        longest = max((len(str(c.value)) for c in col if c.value is not None),
                      default=0)
        ws.column_dimensions[get_column_letter(i)].width = \
            min(max(longest + 2, floor), cap)


def _style_header(ws, size: int = 11, white: bool = False) -> None:
    for c in ws[1]:
        c.font = Font(bold=True, size=size,
                      color="FFFFFFFF" if white else None)
        c.alignment = Alignment(horizontal="center", vertical="top",
                                wrap_text=True)
        c.border = Border(left=THIN, right=THIN, bottom=THIN)


def _style_text_sheet(ws, widths: dict, wrap: bool) -> None:
    """
    README and Summary: a boxed header, a spacer row, and an outline around
    each block of rows so the sections read as separate panels.
    """
    ws.sheet_view.showGridLines = False
    ws.insert_rows(2)
    for col, w in widths.items():
        ws.column_dimensions[col].width = w

    for c in ws[1]:
        c.font = Font(bold=True)
        c.border = BOX
        c.alignment = Alignment(horizontal="center", vertical="top",
                                wrap_text=wrap)

    align = Alignment(vertical="top", wrap_text=wrap)
    blank = [r for r in range(3, ws.max_row + 1)
             if all(ws.cell(r, c).value in (None, "")
                    for c in range(1, ws.max_column + 1))]

    start = None
    for r in range(3, ws.max_row + 2):
        empty = r in blank or r > ws.max_row
        if not empty and start is None:
            start = r
        elif empty and start is not None:
            for rr in range(start, r):
                for cc in range(1, ws.max_column + 1):
                    cell = ws.cell(rr, cc)
                    cell.alignment = align
                    cell.border = Border(
                        left=THIN if cc == 1 else None,
                        right=THIN if cc == ws.max_column else None,
                        top=THIN if rr == start else None,
                        bottom=THIN if rr == r - 1 else None)
            ws.cell(start, 1).font = Font(bold=True)   # section heading
            start = None


def _style_table_sheet(ws, name: str, bold_col: str | None = None,
                       freeze: str | None = None) -> None:
    """Applications and By_aquifer: a real Excel table, filterable and striped."""
    ws.sheet_view.showGridLines = False
    ref = f"A1:{get_column_letter(ws.max_column)}{ws.max_row}"
    table = Table(displayName=name, ref=ref)
    table.tableStyleInfo = TableStyleInfo(
        name=TABLE_STYLE, showRowStripes=True, showColumnStripes=False,
        showFirstColumn=False, showLastColumn=False)
    ws.add_table(table)

    ws.row_dimensions[1].height = 21
    _style_header(ws, size=12, white=True)

    if bold_col:
        headers = [c.value for c in ws[1]]
        if bold_col in headers:
            i = headers.index(bold_col) + 1
            for r in range(2, ws.max_row + 1):
                ws.cell(r, i).font = Font(bold=True)

    if freeze:
        ws.freeze_panes = freeze
    _autofit(ws)


def collapse(pairs: pd.DataFrame, rule: str) -> pd.DataFrame:
    """Reduce application x aquifer pairs to one row per application."""
    fail_cols = [c for c in pairs.columns if c.startswith("fail_")]
    agg = "max" if rule == "any" else "min"      # bools: max=any, min=all

    out = (pairs.groupby("tracking_number", as_index=False)
                .agg({**{c: agg for c in fail_cols},
                      "aquifer_id": lambda s: "; ".join(sorted(set(s.astype(str)))),
                      "agreement": "first",
                      "n_aquifers_parcel": "first",
                      "n_aquifers_well": "first"})
                .rename(columns={"aquifer_id": "aquifers"}))

    for c in fail_cols:
        out[c] = out[c].astype(bool)

    out["n_criteria_failed"] = out[fail_cols].sum(axis=1)
    out["qualifies"] = out["n_criteria_failed"] == 0
    out["method_disagreement"] = out["agreement"].isin(["disagree", "partial_agree"])
    return out


# Internal values -> what the client reads. The workbook should not carry the
# pipeline's vocabulary; these are the only place the two are tied together.
BASIS_LABELS = {
    "parcel": "Parcel ID",
    "well_parcel_ambiguous": "Well - parcel covered several aquifers",
    "well_no_pid": "Well - no parcel ID",
    "parcel_ambiguous_no_well": "Parcel ID - several aquifers, no well",
}
STATUS_LABELS = {
    True: "Qualifies",
    False: "Excluded",
    "not_assessable": "Not assessable",
}
DEPENDS = "Depends on aquifer"


def to_report(apps: pd.DataFrame) -> pd.DataFrame:
    """
    Presentation layer for the client workbook.

    Plain labels instead of internal values, working columns dropped, and the
    aquifer columns grouped so the assignment reads as one block. The analysis
    frames keep their own vocabulary; only what is published is renamed.
    """
    out = apps.copy()

    labels = dict(STATUS_LABELS)
    labels[UNDET["status_label"]] = "Further assessment needed"
    out["status"] = out["qualifies"].map(lambda v: labels.get(v, v))
    # A blank basis means no aquifer was matched at all - say so rather than
    # leaving the reader to interpret an empty cell.
    out["assignment_basis"] = (out["assignment_basis"]
                               .map(lambda v: BASIS_LABELS.get(v, v))
                               .fillna("No aquifer matched"))

    # Publish the aquifer list with materials rather than bare ids - it is the
    # same information the reader needs and avoids two near-duplicate columns.
    if "aquifer_list" in out.columns:
        out["aquifers"] = out["aquifer_list"].replace("", pd.NA)

    # A condition whose answer differs between the candidate aquifers has no
    # TRUE/FALSE answer for the application. Collapsing it with `any` reported
    # the restriction as fact when only one candidate aquifer carried it, which
    # contradicted the status on the same row.
    mixed_cols = [c for c in out.columns if c.startswith("mixed_fail_")]
    for m in mixed_cols:
        crit = m[len("mixed_"):]
        if crit in out.columns:
            out[crit] = out[crit].astype(object).mask(
                out[m].fillna(False).astype(bool), DEPENDS)

    if mixed_cols:
        # Nor can the failures be counted while any of them is undecided.
        any_mixed = out[mixed_cols].fillna(False).astype(bool).any(axis=1)
        out.loc[any_mixed, "n_criteria_failed"] = pd.NA
    out = out.drop(columns=mixed_cols, errors="ignore")

    # Two-method comparison is internal working, not a finding: the parcel ID
    # decides and these feed no result, so publishing them only prompts "why
    # did the well disagree?". `outcome_depends` is likewise internal - it
    # drives the status, which is already published. All stay in data/interim.
    out = out.drop(columns=["qualifies", "aquifer_list", "outcome_depends",
                            "agreement", "method_disagreement",
                            "verdict_depends", "n_aquifers_parcel",
                            "n_aquifers_well"], errors="ignore")

    # `status` is the answer the sheet exists to give, so it sits beside the
    # tracking number rather than at the far right where it was appended.
    # The aquifer story stays together: what it matched, how that was decided,
    # and whether it is determined at all.
    block = ["aquifers", "assignment_basis", "aquifer_undetermined"]
    rest = [c for c in out.columns if c not in block + ["status"]]
    anchor = rest.index("location_source") + 1 if "location_source" in rest \
        else len(rest)
    ordered = (rest[:anchor] + [c for c in block if c in out.columns]
               + rest[anchor:])
    ordered.insert(1, "status")
    return out[ordered]


def bcgw_extracted_at() -> str:
    """
    When the BCGW data behind this workbook was pulled.

    Step 00 records it. Falls back to the cache file's modification time for
    workbooks built before that was added, labelled so the two are not
    confused - a file timestamp survives a copy, an extraction time does not.
    """
    interim = ROOT / P["interim_dir"]
    info = interim / "bcgw_extract_info.json"
    if info.exists():
        with open(info) as f:
            return json.load(f)["extracted_at_display"]

    # Not the cache file: step 02 writes the well_points layer back into it,
    # so its timestamp is the last run of step 02, not the extraction. These
    # two are written by step 00 and by nothing else.
    for name in ("aquifer_notations.csv", "water_reserves.csv"):
        f = interim / name
        if f.exists():
            ts = datetime.fromtimestamp(f.stat().st_mtime).astimezone()
            print("  NOTE: BCGW extraction time not recorded; inferred from "
                  f"{name}. Re-run step 00 to record it exactly.")
            return ts.strftime("%Y-%m-%d %H:%M %Z")
    return "unknown - no BCGW extract found"


def build_readme(apps: pd.DataFrame, pairs: pd.DataFrame,
                 qa: pd.DataFrame) -> pd.DataFrame:
    """
    Plain-language guide, written as the first sheet.

    The workbook goes to readers who did not run the analysis and cannot be
    expected to infer the conventions. Two columns actively mislead without a
    guide: `qualifies` looks boolean but carries four values, and a `fail_`
    column reads like an error when it means the criterion excludes the
    application.

    Row counts come from the frames rather than being typed in, so the guide
    cannot drift out of step with the sheets it describes.
    """
    located = apps["locatable"].fillna(False).astype(bool)
    resolved = apps["aquifers"].notna()
    n_unlocated = int((~located).sum())
    n_unmapped = int((located & ~resolved).sum())
    extracted_at = bcgw_extracted_at()

    rows = [
        ("DATA USED", ""),
        ("Aquifer and parcel data", "Extracted from the BC Geographic "
                                    "Warehouse on " + extracted_at),
        ("Application list", f"{len(apps)} applications, as supplied."),
        ("", ""),

        ("SHEETS", ""),
        ("Summary", "How the applications break down at each step."),
        ("Applications", f"{len(apps):,} rows, one per application, with its "
                         f"result. The main sheet."),
        ("By_aquifer", f"{len(pairs):,} rows. An application appears once for "
                       f"each aquifer it was matched to. Use this to see "
                       f"which aquifer caused a result."),
        ("Not_assessed", "The four conditions that could not be checked, and "
                         "why."),
        ("QA", f"{len(qa):,} rows. Every record corrected or set aside while "
               f"preparing the data, with the reason."),
        ("", ""),

        ("RESULT  (column: status)", ""),
        ("Qualifies", "Meets all four conditions that could be checked."),
        ("Excluded", "One or more conditions apply. The four condition "
                     "columns show which."),
        ("Further assessment needed",
         "The application was matched to more than one aquifer AND those "
         "aquifers give different answers, so the result depends on which one "
         "the well actually draws from. The mapping cannot show that, so no "
         "result is given until the well is looked at."),
        ("Not assessable",
         f"No aquifer could be matched. {n_unlocated} applications have no "
         f"usable parcel ID and no usable well coordinate. A further "
         f"{n_unmapped} are well located but sit where no aquifer is mapped - "
         f"not all of BC is. Neither group is a pass."),
        ("", ""),

        ("HOW THE AQUIFER WAS DECIDED", "The parcel ID is used first. The "
                                        "well coordinate is used only where "
                                        "the parcel ID cannot answer."),
        ("aquifers", "The aquifer or aquifers the application was matched to. "
                     "The conditions were checked against these."),
        ("assignment_basis", "Which of the two locations decided it, and why."),
        ("   No aquifer matched",
         "Neither the parcel nor the well landed on a mapped aquifer, so no "
         "condition could be checked. Note this can happen even where the "
         "application has both a parcel ID and a well coordinate: the parcel "
         "ID may no longer exist in the parcel fabric (retired or subdivided, "
         "46 of them), and the well may sit outside any mapped aquifer. The "
         "two failures are unrelated and an application can hit both."),
        ("   Parcel ID", "The parcel sits on one aquifer. The well was not "
                         "used, even where it points elsewhere."),
        ("   Well - parcel covered several aquifers",
         "The parcel spans more than one aquifer, so it cannot say which. The "
         "well coordinate decides."),
        ("   Well - no parcel ID", "No usable parcel ID, so the well is the "
                                   "only location available."),
        ("   Parcel ID - several aquifers, no well",
         "The parcel spans several aquifers and there is no usable well to "
         "narrow it down. All of them are kept, so this application may be "
         "excluded by an aquifer it does not actually draw from."),
        ("", ""),

        ("aquifer_undetermined",
         "TRUE where the application was matched to more than one aquifer, so "
         "which one the well draws from is not known. This happens where "
         "aquifers are layered one above the other, and where a parcel spans "
         "neighbouring aquifers. What the aquifers are made of makes no "
         "difference - two sand and gravel aquifers can be layered just as a "
         "sand and gravel aquifer over bedrock can."),
        ("", ""),

        ("WHY SOME UNDETERMINED APPLICATIONS STILL HAVE A RESULT",
         "An undetermined aquifer does not always change the answer. "
         "Where every aquifer the application was matched to gives the "
         "same result, that result stands - knowing which one is correct "
         "would not move it. Only where the aquifers disagree is the "
         "result withheld as 'Further assessment needed'."),
        ("", "Example: an application matched to four aquifers that are all "
             "on the sensitive-stream list is Excluded, because it is "
             "excluded whichever of the four the well draws from. Looking at "
             "the well would not change the outcome."),
        ("", "So aquifer_undetermined = TRUE with a result of Qualifies or "
             "Excluded is not a contradiction. It means the aquifer is "
             "unknown but the answer is not."),
        ("", "The same applies to an Excluded application showing '" + DEPENDS
             + "' against some conditions. Each candidate aquifer breaks a "
               "condition, just not the same one, so the application is "
               "excluded either way while no single condition can be named."),
        ("", ""),

        ("CONDITIONS CHECKED", "Three possible values, not two."),
        ("   TRUE", "The condition applies. The application is excluded by "
                    "it."),
        ("   FALSE", "The condition does not apply."),
        ("   " + DEPENDS,
         "The application was matched to more than one aquifer and this "
         "condition applies to some of them but not others, so it has no "
         "single answer until the well is looked at. This is the column to "
         "read when deciding what a well review would settle - it names the "
         "exact condition in question."),
        ("   (blank)", "No aquifer was matched, so nothing could be checked."),
        ("fail_sensitive_stream",
         "The aquifer is on the list of aquifers connected to sensitive "
         "streams.  SOURCE: supplied by the client, sheet '"
         + str(P["sensitive_sheet"]).strip() + "' of the application "
         "workbook. Used as supplied, not re-derived."),
        ("fail_fully_recorded",
         "The aquifer carries a Fully Recorded notation - already fully "
         "allocated, no water available.  SOURCE: BCGW "
         + BCGW["aquifer_notations"]["table"] + ", notation codes "
         + " and ".join(BCGW["aquifer_notations"]["fully_recorded_codes"])
         + "."),
        ("fail_water_reserve",
         "The aquifer is reserved under Water Sustainability Act s.39-41.  "
         "SOURCE: BCGW " + BCGW["water_reserves"]["table"]
         + ". The aquifer number is not a field in that table - it is read "
           "out of the reserve description text."),
        ("fail_tpo_watershed",
         "The watershed has been subject to a s.88 temporary protection "
         "order.  SOURCE: BCGW " + BCGW["temp_protection_orders"]["table"]
         + ", matched by location rather than by watershed name. Counts any "
           "order ever made, not only those in effect, as directed. This is "
           "the only condition judged on the application rather than the "
           "aquifer, so it never reads '" + DEPENDS + "'."),
        ("n_criteria_failed",
         "How many of the four apply. Left blank where any condition reads "
         "'" + DEPENDS + "' - the count would depend on which aquifer is "
         "correct, so there is no honest number to give."),
        ("na_ ... (four columns)", "The four conditions with no data source "
                                   "available. Always 'not_assessed'. These "
                                   "are not passes. The Not_assessed sheet "
                                   "says why each one could not be checked."),
        ("", ""),

        ("LOCATION COLUMNS", ""),
        ("n_pids", "Valid parcel IDs found on the application."),
        ("n_wells_total / n_wells_usable", "Wells listed, and those with a "
                                           "coordinate good enough to use."),
        ("locatable", "TRUE if the application has either a usable parcel or "
                      "a usable well."),
        ("location_source", "Which was available: parcel, well, both, or "
                            "none."),
        ("", ""),

        ("'By_aquifer' ONLY", ""),
        ("assigned", "TRUE where this row is the one that decided the result. "
                     "The sheet shows every aquifer considered; filter on "
                     "assigned = TRUE to see only what counted."),
        ("aquifer_id", "Provincial aquifer number."),
        ("AQUIFER_NAME / MATERIAL / SUBTYPE", "Aquifer name, what it is made "
                                              "of, and its classification."),
        ("method", "Whether this row came from the parcel or the well."),
        ("overlap_share", "How much of the parcel falls inside this aquifer, "
                          "0 to 1. Parcel rows only."),
        ("n_wells_in_aquifer", "How many of the application's wells fall "
                               "inside this aquifer. Well rows only."),
        ("", ""),

        ("'QA' SHEET", ""),
        ("issue / detail / action",
         "What was found, on which application, and what was done. Note that "
         "'coordinate dropped' refers to one well coordinate being set aside, "
         "not the application - most of those applications are still located "
         "by their parcel."),
    ]
    return pd.DataFrame(rows, columns=["Item", "Meaning"])


def aquifer_ambiguity(pairs: pd.DataFrame) -> pd.DataFrame:
    """
    Applications whose aquifer is not determined, and whether it matters.

    More than one assigned aquifer means the mapping cannot say which one the
    well draws from. Two aquifers side by side, or four layered on top of each
    other, are equally undetermined - material is not the test.

    Whether that ambiguity changes anything is a separate question. If every
    candidate aquifer gives the same answer, the answer stands: knowing which
    one is correct would not move it. `outcome_depends` marks the applications
    where the candidates genuinely disagree, and drives the status.

    The watershed criterion is judged on the application, not the aquifer
    (criteria.application_level), so it stays decisive throughout and is
    excluded from the disagreement test.

    Returns one row per application with an aquifer:
        aquifer_undetermined  more than one aquifer assigned
        outcome_depends       the candidates differ on the result
        aquifer_list          the aquifers with their material
    """
    app_level = {f"fail_{c}" for c in CRIT.get("application_level", [])}
    aq_cols = [c for c in pairs.columns
               if c.startswith("fail_") and c not in app_level]

    p = pairs.copy()
    p["aq_fail"] = p[aq_cols].astype(bool).any(axis=1)

    g = p.groupby("tracking_number")
    n_aquifers = g["aquifer_id"].nunique()
    undetermined = n_aquifers > 1
    # Undetermined AND the candidates disagree: some fail, some do not.
    depends = undetermined & g["aq_fail"].any() & ~g["aq_fail"].all()

    # Per criterion, not just overall. An application can be excluded whichever
    # aquifer is correct while a particular condition still turns on the
    # choice - 36 do - and collapsing those to a bare TRUE asserts a finding
    # that only one of the candidate aquifers supports.
    mixed = {f"mixed_{c}": g[c].any() & ~g[c].all() for c in aq_cols}

    def describe(g: pd.DataFrame) -> str:
        """'197 (Sand and Gravel); 203 (Bedrock)'"""
        rows = g.drop_duplicates("aquifer_id").sort_values("aquifer_id")
        return "; ".join(f"{r.aquifer_id} ({r.MATERIAL})"
                         for r in rows.itertuples())

    listing = (p.groupby("tracking_number")[["aquifer_id", "MATERIAL"]]
                .apply(describe)
                .rename("aquifer_list"))

    return pd.DataFrame({"aquifer_undetermined": undetermined,
                         "outcome_depends": depends,
                         **mixed}).join(listing).reset_index()


def build_points(apps: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    One point per application. Well location preferred; parcel representative
    point where no usable coordinate exists.
    """
    cache = ROOT / P["bcgw_cache"]
    interim = ROOT / P["interim_dir"]
    frames = []

    try:
        wells = gpd.read_file(cache, layer="well_points").to_crs(CRS)
        w = (wells.sort_values("well_index")
                  .drop_duplicates("tracking_number")
                  [["tracking_number", "geometry"]])
        w["geom_source"] = "well"
        frames.append(w)
    except Exception:
        print("  WARNING: well_points unavailable")

    try:
        parcels = gpd.read_file(cache, layer="parcels").to_crs(CRS)
        parcels["PID"] = parcels["PID"].astype(str).str.zfill(9)
        pids = pd.read_csv(interim / "application_pids.csv",
                           dtype={"pid": str, "tracking_number": str})
        linked = pids.merge(parcels, left_on="pid", right_on="PID", how="inner")
        pg = (gpd.GeoDataFrame(linked, geometry="geometry", crs=CRS)
                 .dissolve(by="tracking_number").reset_index())
        pg["geometry"] = pg.geometry.representative_point()
        pg = pg[["tracking_number", "geometry"]]
        pg["geom_source"] = "parcel"
        frames.append(pg)
    except Exception:
        print("  WARNING: parcels unavailable")

    if not frames:
        return gpd.GeoDataFrame(apps, geometry=[], crs=CRS)

    pts = (pd.concat(frames, ignore_index=True)
             .drop_duplicates("tracking_number", keep="first"))
    merged = apps.merge(pts, on="tracking_number", how="left")
    return gpd.GeoDataFrame(merged, geometry="geometry", crs=CRS)


def _attach(gdf: gpd.GeoDataFrame, report: pd.DataFrame,
            skip: tuple = ()) -> gpd.GeoDataFrame:
    """Carry the screening result onto a geometry layer, without re-deriving it."""
    cols = [c for c in report.columns
            if c not in skip and c not in gdf.columns and c != "geometry"]
    return gdf.merge(report[["tracking_number"] + cols], on="tracking_number",
                     how="left")


def build_parcel_layer(report: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    One polygon per application x parcel.

    Not dissolved, unlike the assignment in step 02: the point of this layer is
    to show the parcels themselves. An application with several parcels appears
    several times, and the six parcels shared between applications appear once
    per application - the geometry is genuinely the same land.

    Only parcels found in the parcel fabric are here. Valid PIDs that no longer
    resolve (retired or subdivided) have no polygon to draw.
    """
    cache = ROOT / P["bcgw_cache"]
    interim = ROOT / P["interim_dir"]

    parcels = gpd.read_file(cache, layer="parcels").to_crs(CRS)
    parcels["PID"] = parcels["PID"].astype(str).str.zfill(9)

    pids = pd.read_csv(interim / "application_pids.csv",
                       dtype={"pid": str, "tracking_number": str})
    pids = pids[pids["pid_status"] != "invalid"]

    linked = pids.merge(parcels, left_on="pid", right_on="PID", how="inner")
    gdf = gpd.GeoDataFrame(linked, geometry="geometry", crs=CRS)
    gdf = gdf.drop(columns=["PID", "pid_index"], errors="ignore")
    gdf = gdf.rename(columns={"PARCEL_STATUS": "parcel_status",
                              "OWNER_TYPE": "owner_type"})
    return _attach(gdf, report)


def build_well_layer(report: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    One point per application x usable well.

    Every well with a coordinate good enough to use, not just the one that
    represents the application on the point layer. Coordinates that are
    missing, outside BC or truncated to whole degrees are left out - they have
    no defensible location - and are listed on the QA sheet instead.

    Carries the aquifer each well actually falls in, which the application-level
    layers cannot show, and whether the well sits on its own application's
    parcel.
    """
    cache = ROOT / P["bcgw_cache"]
    interim = ROOT / P["interim_dir"]

    wells = pd.read_csv(interim / "application_wells.csv",
                        dtype={"tracking_number": str})
    usable = wells[wells["coord_status"].isin(["ok", "sign_corrected"])].copy()
    gdf = gpd.GeoDataFrame(
        usable,
        geometry=gpd.points_from_xy(usable["longitude"], usable["latitude"]),
        crs=CFG["project"]["source_crs"]).to_crs(CRS)

    # Which aquifer(s) this individual well sits in. A well over layered
    # aquifers falls in more than one, so they are listed rather than counted.
    aq = gpd.read_file(cache, layer="aquifers").to_crs(CRS)
    aq["aquifer_id"] = aq["AQUIFER_ID"].astype(int).astype(str)
    hit = gpd.sjoin(gdf[["tracking_number", "well_index", "geometry"]],
                    aq[["aquifer_id", "MATERIAL", "geometry"]],
                    how="inner", predicate="within")
    listed = (hit.sort_values("aquifer_id")
                 .groupby(["tracking_number", "well_index"])
                 .apply(lambda g: "; ".join(f"{r.aquifer_id} ({r.MATERIAL})"
                                            for r in g.itertuples()),
                        include_groups=False)
                 .rename("well_in_aquifer").reset_index())
    gdf = gdf.merge(listed, on=["tracking_number", "well_index"], how="left")

    # Does the well fall on its own application's parcel? Where it does not,
    # one of the two locations is wrong - or the well is genuinely off-parcel.
    parcels = gpd.read_file(cache, layer="parcels").to_crs(CRS)
    parcels["PID"] = parcels["PID"].astype(str).str.zfill(9)
    pids = pd.read_csv(interim / "application_pids.csv",
                       dtype={"pid": str, "tracking_number": str})
    linked = pids.merge(parcels, left_on="pid", right_on="PID", how="inner")
    if not linked.empty:
        own = (gpd.GeoDataFrame(linked, geometry="geometry", crs=CRS)
               .dissolve(by="tracking_number")["geometry"])
        gdf["well_on_own_parcel"] = [
            own[tn].contains(geom) if tn in own.index else None
            for tn, geom in zip(gdf["tracking_number"], gdf.geometry)]

    return _attach(gdf, report, skip=("n_wells_total", "n_wells_usable"))


def build_aquifer_layer(report: pd.DataFrame,
                        pairs: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    The aquifers these applications were assigned to, with counts.

    Answers "which aquifers are absorbing this" rather than "where are the
    applications". Only aquifers actually assigned to at least one application
    are included - the province has 1,166 and the rest would be noise.

    An application assigned to several aquifers counts toward each, so
    n_applications sums to more than the number of applications.
    `n_sole_aquifer` is the subset for which this is the only candidate, and so
    the only count that can be added up without double counting.

    The three restriction flags are properties of the aquifer itself. The
    watershed criterion is deliberately absent: it belongs to the application,
    not the aquifer, and would differ between applications on the same aquifer.
    """
    cache = ROOT / P["bcgw_cache"]
    assigned = pairs[pairs["assigned"].astype(bool)].copy()
    assigned["aquifer_id"] = assigned["aquifer_id"].astype(str)

    a = assigned.merge(report[["tracking_number", "status"]],
                       on="tracking_number", how="left")
    per_app = assigned.groupby("tracking_number")["aquifer_id"].nunique()
    a["sole"] = a["tracking_number"].map(per_app).eq(1)

    g = a.groupby("aquifer_id")
    summary = pd.DataFrame({
        "n_applications": g["tracking_number"].nunique(),
        "n_sole_aquifer": g.apply(
            lambda x: x.loc[x["sole"], "tracking_number"].nunique(),
            include_groups=False),
        "sensitive_stream": g["fail_sensitive_stream"].max().astype(bool),
        "fully_recorded": g["fail_fully_recorded"].max().astype(bool),
        "water_reserve": g["fail_water_reserve"].max().astype(bool),
    })

    counts = (a.drop_duplicates(["aquifer_id", "tracking_number"])
               .pivot_table(index="aquifer_id", columns="status",
                            values="tracking_number", aggfunc="nunique")
               .fillna(0).astype(int))
    counts.columns = [f"n_{c.lower().replace(' ', '_')}" for c in counts.columns]
    summary = summary.join(counts).reset_index()

    aq = gpd.read_file(cache, layer="aquifers").to_crs(CRS)
    aq["aquifer_id"] = aq["AQUIFER_ID"].astype(int).astype(str)
    aq = aq.rename(columns={"AQUIFER_NAME": "aquifer_name",
                            "MATERIAL": "material", "SUBTYPE": "subtype",
                            "PRODUCTIVITY": "productivity",
                            "VULNERABILITY": "vulnerability",
                            "DEMAND": "demand"})
    keep = ["aquifer_id", "aquifer_name", "material", "subtype",
            "productivity", "vulnerability", "demand", "geometry"]
    aq = aq[[c for c in keep if c in aq.columns]]

    out = aq.merge(summary, on="aquifer_id", how="inner")
    return out.sort_values("n_applications", ascending=False)


def build_layer_guide(counts: dict, extracted_at: str) -> pd.DataFrame:
    """
    A guide to the layers, written into the GeoPackage as an aspatial table.

    The spreadsheet README describes the spreadsheet. Documentation for the map
    file has to travel inside the map file, because the two are sent
    separately and a GeoPackage that arrives on its own would otherwise carry
    no explanation of what its four layers mean or why their counts differ.
    """
    rows = [
        ("ABOUT", None,
         "EUGW low volume threshold screening. Aquifer and parcel data "
         f"extracted from the BC Geographic Warehouse on {extracted_at}. All "
         "layers are BC Albers (EPSG:3005), share tracking_number, and carry "
         "the screening result in `status`, so any of them can be coloured "
         "the same way. The accompanying spreadsheet explains every attribute "
         "column."),
        ("application_points", counts.get("application_points"),
         "One point per application: the well where there is one, otherwise a "
         "point inside the parcel. `geom_source` says which was used. Use "
         "this for a simple map of where the applications are."),
        ("application_parcels", counts.get("application_parcels"),
         "The parcels themselves, one shape per application per parcel. An "
         "application with several parcels appears several times, and a "
         "parcel shared between two applications appears once for each - the "
         "geometry is the same land."),
        ("application_wells", counts.get("application_wells"),
         "Every well with a usable coordinate, not only the one representing "
         "the application on the points layer. Carries the aquifer each "
         "individual well falls in, and whether it sits on its own "
         "application's parcel. `well_on_own_parcel` is empty where the "
         "application has no resolvable parcel to test against - that means "
         "not checked, not off-parcel."),
        ("aquifers_assigned", counts.get("aquifers_assigned"),
         "The aquifers these applications were matched to, with how many "
         "applications each carries, split by result. Aquifers with no "
         "applications are not included."),
        ("COUNTING", None,
         "No single layer reproduces the totals in the spreadsheet, and the "
         "gaps are real rather than errors. Parcels exist only where the "
         "parcel ID still resolves in the parcel fabric; wells only where the "
         "coordinate is usable; points only where either of those worked."),
        ("COUNTING", None,
         "On aquifers_assigned, an application matched to several aquifers is "
         "counted in every one of them, so n_applications adds up to more "
         "than the number of applications. n_sole_aquifer counts only "
         "applications for which that aquifer is the single candidate and is "
         "the field that can safely be summed."),
    ]
    return pd.DataFrame(rows, columns=["layer", "features", "description"])


def build_summary(apps: pd.DataFrame, base: pd.DataFrame) -> pd.DataFrame:
    """The funnel: how the population reduces at each criterion."""
    fail_cols = [c for c in apps.columns if c.startswith("fail_")]
    n = len(base)
    # `apps` is a left join onto the full population, so it always holds every
    # application - counting rows here measures nothing. Resolution is marked
    # by `aquifers` being populated.
    #
    # Both drop-offs are itemised. A funnel that steps 712 -> 704 -> 640
    # without saying where the 8 and the 64 went invites the reader to assume
    # something was silently discarded.
    located = apps["locatable"].fillna(False).astype(bool)
    resolved = apps["aquifers"].notna()

    rows = [
        ("Applications supplied by client", n),
        ("  locatable (parcel or well)", int(located.sum())),
        ("    not locatable - no usable PID and no usable well coordinate",
         int((~located).sum())),
        ("  aquifer resolved", int(resolved.sum())),
        ("    located, but not over any mapped aquifer",
         int((located & ~resolved).sum())),
        ("", ""),
        ("How many applications each restriction reaches", ""),
        ("  Counts every application whose assigned aquifer carries the", ""),
        ("  restriction. These overlap each other, and they are larger than", ""),
        ("  the Excluded total below, because applications held for well", ""),
        ("  review are counted here but are not excluded.", ""),
    ]
    for c in fail_cols:
        rows.append((f"  {c.replace('fail_', '').replace('_', ' ')}",
                     int(apps[c].sum())))
    # `qualifies` is deliberately three-valued: True, False, or the string
    # "not_assessable" where no aquifer could be resolved. Compare explicitly
    # rather than summing - a bare .sum() hits str + bool on the mixed column.
    qual = apps["qualifies"]
    rows += [
        ("", ""),
        ("RESULT", ""),
        ("Qualifies", int((qual == True).sum())),          # noqa: E712
        ("Excluded", int((qual == False).sum())),          # noqa: E712
        ("Further assessment needed - aquifer undetermined and it matters",
         int((qual == UNDET["status_label"]).sum())),
        ("Not assessable - no aquifer matched",
         int((qual == "not_assessable").sum())),
        ("", ""),
        ("NOTES", ""),
        ("  Four of the eight conditions could be checked. The other four", ""),
        ("  have no data source and are listed on the Not_assessed sheet.", ""),
        ("  Aquifer mapping is flat, so where a sand and gravel aquifer sits", ""),
        ("  on bedrock it cannot show which one a well draws from. Those", ""),
        ("  applications are held for well review rather than given a result.", ""),
        ("  Volumes are as supplied and were not recalculated.", ""),
    ]
    return pd.DataFrame(rows, columns=["Measure", "Count"])


def main():
    interim, outdir = ROOT / P["interim_dir"], ROOT / P["output_dir"]
    outdir.mkdir(parents=True, exist_ok=True)

    # Excel holds an exclusive lock on an open workbook. Check before doing
    # the work, rather than failing on the last step with a traceback and
    # leaving the spatial output from the previous run in place.
    for name in ("EUGW_low_volume_screening.xlsx",
                 "EUGW_low_volume_screening.gpkg"):
        target = outdir / name
        if target.exists():
            try:
                with open(target, "ab"):
                    pass
            except PermissionError:
                raise SystemExit(f"{target} is open in another program. "
                                 "Close it and re-run.")

    base = pd.read_csv(interim / "applications.csv", dtype={"tracking_number": str})
    pairs = pd.read_csv(interim / "application_criteria.csv",
                        dtype={"tracking_number": str})
    qa = pd.read_csv(interim / "qa_log.csv")

    # The client's rule (see config.aquifer_assignment) decides which rows
    # count. Both passes stay in `pairs` for By_aquifer and audit, but the
    # verdict is taken only over the assigned rows - otherwise a well point
    # could exclude an application whose PID resolves cleanly on its own.
    if "assigned" not in pairs.columns:
        raise SystemExit("pairs has no `assigned` column - re-run "
                         "02_resolve_aquifers.py, then 03_apply_criteria.py")
    assigned = pairs[pairs["assigned"].astype(bool)]

    collapsed = collapse(assigned, CRIT["collapse_rule"])
    apps = base.merge(collapsed, on="tracking_number", how="left")

    basis = (pairs.drop_duplicates("tracking_number")
                  [["tracking_number", "assignment_basis"]])
    apps = apps.merge(basis, on="tracking_number", how="left")

    amb = aquifer_ambiguity(assigned)
    apps = apps.merge(amb, on="tracking_number", how="left")
    flags = ["aquifer_undetermined", "outcome_depends"]
    flags += [c for c in apps.columns if c.startswith("mixed_fail_")]
    for c in flags:
        apps[c] = apps[c].fillna(False).astype(bool)
    apps["aquifer_list"] = apps["aquifer_list"].fillna("")

    # Applications with no aquifer resolved cannot be screened at all
    apps["qualifies"] = apps["qualifies"].astype("object")
    apps.loc[apps["aquifers"].isna(), "qualifies"] = "not_assessable"

    # An undetermined aquifer is only withheld from pass/fail where the
    # candidate aquifers actually disagree - if they all give the same answer,
    # not knowing which one is correct does not change it. Applied after the
    # not_assessable case, which takes precedence: an application with no
    # aquifer at all has nothing to be ambiguous about.
    needs_review = apps["outcome_depends"] \
        if UNDET["scope"] == "disagreeing" else apps["aquifer_undetermined"]
    apps.loc[needs_review & (apps["qualifies"] != "not_assessable"),
             "qualifies"] = UNDET["status_label"]

    for c in CFG["criteria"]["not_assessable"]:
        apps[f"na_{c}"] = "not_assessed"

    not_assessed = pd.DataFrame([
        ("wsa_s86_s87_order", "No order has ever been issued under s.86 or s.87"),
        ("wsa_s82_agricultural", "No spatial source identified; unclear if any "
                                 "regulation has been made"),
        ("waterworks_serviced", "Licence data has no service areas; criterion "
                                "also depends on what the system supplies"),
        ("storage_condition", "Depends on applicant works. Client already "
                              "removed dugout-only applications. Works field "
                              "populated on 48 of 712 rows"),
    ], columns=["Criterion", "Why not assessed"])

    # Built once and used for both deliverables, so the spreadsheet and the
    # map layer cannot drift apart.
    report = to_report(apps)

    xlsx = outdir / "EUGW_low_volume_screening.xlsx"
    with pd.ExcelWriter(xlsx, engine="openpyxl") as xl:
        build_readme(apps, pairs, qa).to_excel(xl, "README", index=False)
        build_summary(apps, base).to_excel(xl, "Summary", index=False)
        report.to_excel(xl, "Applications", index=False)
        pairs.drop(columns=["agreement", "n_aquifers_parcel",
                            "n_aquifers_well"],
                   errors="ignore").to_excel(xl, "By_aquifer", index=False)
        not_assessed.to_excel(xl, "Not_assessed", index=False)
        qa.to_excel(xl, "QA", index=False)

        _style_text_sheet(xl.sheets["README"], {"A": 34, "B": 100}, wrap=True)
        _style_text_sheet(xl.sheets["Summary"], {"A": 56, "B": 12}, wrap=False)
        _style_table_sheet(xl.sheets["Applications"], "Applications",
                           bold_col="status", freeze="C2")
        _style_table_sheet(xl.sheets["By_aquifer"], "ByAquifer")
        for name in ("Not_assessed", "QA"):
            ws = xl.sheets[name]
            ws.sheet_view.showGridLines = False
            _style_header(ws)
            _autofit(ws, cap=60)

    # The map layer carries the same labels as the workbook. Built from `apps`
    # it published `qualifies` with the pipeline's own values, so a reader
    # symbolising the points saw different vocabulary from the spreadsheet.
    pts = build_points(report)
    gpkg = outdir / "EUGW_low_volume_screening.gpkg"
    if gpkg.exists():
        gpkg.unlink()   # layers are rewritten, not appended to
    pts[pts.geometry.notna()].to_file(gpkg, layer="application_points",
                                      driver="GPKG")
    parcel_layer = build_parcel_layer(report)
    parcel_layer.to_file(gpkg, layer="application_parcels", driver="GPKG")
    well_layer = build_well_layer(report)
    well_layer.to_file(gpkg, layer="application_wells", driver="GPKG")
    aquifer_layer = build_aquifer_layer(report, pairs)
    aquifer_layer.to_file(gpkg, layer="aquifers_assigned", driver="GPKG")

    # An aspatial table, so the explanation cannot be separated from the file.
    pyogrio.write_dataframe(
        build_layer_guide(
            {"application_points": int(pts.geometry.notna().sum()),
             "application_parcels": len(parcel_layer),
             "application_wells": len(well_layer),
             "aquifers_assigned": len(aquifer_layer)},
            bcgw_extracted_at()),
        gpkg, layer="layer_guide", driver="GPKG", append=True)

    # ----------------------------- summary -------------------------------- #
    n = len(apps)
    q = int((apps["qualifies"] == True).sum())   # noqa: E712
    x = int((apps["qualifies"] == False).sum())  # noqa: E712
    na = int((apps["qualifies"] == "not_assessable").sum())
    fa = int((apps["qualifies"] == UNDET["status_label"]).sum())

    print("=" * 68)
    print("STEP 04  OUTPUTS")
    print("=" * 68)
    print(f"Applications                   {n}")
    print(f"  qualifying                   {q:>5d}  ({q / n * 100:4.1f}%)")
    print(f"  excluded                     {x:>5d}  ({x / n * 100:4.1f}%)")
    print(f"  not assessable               {na:>5d}  ({na / n * 100:4.1f}%)")
    print(f"  further assessment needed    {fa:>5d}  ({fa / n * 100:4.1f}%)")
    print(f"\nAquifer undetermined (>1 aquifer) "
          f"{int(apps['aquifer_undetermined'].sum())}"
          f"   scope: {UNDET['scope']}")
    print(f"  outcome depends on which        "
          f"{int(apps['outcome_depends'].sum())}")
    print(f"\nCollapse rule                  {CRIT['collapse_rule']}")
    print(f"Method disagreement            "
          f"{int(apps['method_disagreement'].fillna(False).sum())}")
    print(f"\nSpreadsheet  {xlsx}")
    print(f"Spatial      {gpkg}")
    print(f"  application_points   {int(pts.geometry.notna().sum()):>5d}  "
          f"one per application")
    print(f"  application_parcels  {len(parcel_layer):>5d}  "
          f"one per application x parcel")
    print(f"  application_wells    {len(well_layer):>5d}  "
          f"one per application x usable well")
    print(f"  aquifers_assigned    {len(aquifer_layer):>5d}  "
          f"aquifers carrying at least one application")
    print("=" * 68)


if __name__ == "__main__":
    main()
