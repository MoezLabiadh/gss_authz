"""
Claim area - Land Act dispositions and WSA authorizations
=========================================================
Lists Land Act dispositions (Tantalis) and Water Sustainability Act authorizations
(WLS) that fall within a claim area, and writes one Excel file for each
(a "Results" sheet plus a "Notes" sheet describing the selection rules).

    python claim_area_authorizations.py path/to/claim_area.gpkg

BCGW connection comes from BCGW_USER, BCGW_PASSWORD and BCGW_HOST
(BCGW_HOST is an Easy Connect string, e.g. bcgw.bcgov/idwprod1.bcgov).
Requires: oracledb (thin mode is fine), geopandas, shapely>=2, pandas, openpyxl
"""
import os
import sys
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import oracledb
import pandas as pd
import shapely
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.styles import Alignment
from shapely.geometry.polygon import orient

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------
AOI_GPKG = "Revised+MMFN+Claim+Area+(Sept+2026).gpkg"       
AOI_LAYER = None                    # None = first layer in the GeoPackage
OUT_DIR = Path("outputs")
START_DATE = pd.Timestamp("1950-01-01")
WRITE_QA_GPKG = True                # also write the selected dispositions to a GeoPackage for checking in a GIS

LICENCES_VIEW = "WHSE_WATER_MANAGEMENT.WLS_WATER_RIGHTS_LICENCES_SP"
APPLICATIONS_VIEW = "WHSE_WATER_MANAGEMENT.WLS_WATER_RIGHTS_APPLICTNS_SP"
# The public WLS_WATER_APPROVALS_SVW holds short-term use approvals only and no client names.
APPROVALS_VIEW = "WHSE_WATER_MANAGEMENT.WLS_WATER_APPROVALS_GOV_SVW"
LICENCE_PARCELS_VIEW = "WHSE_WATER_MANAGEMENT.WLS_LICENCE_WITH_PARCELS_ISP"   # licence -> PID link

# Row order in the WSA file; approvals keep their dataset APPROVAL_TYPE code, unknown codes go last
TYPE_ORDER = ["Licence", "Licence application", "STU", "A-CIAS", "N-CIAS"]

oracledb.defaults.fetch_lobs = False   # BLOBs (the WKB) come back as bytes

# ---------------------------------------------------------------------------
# SQL - the claim area is bound as :aoi (WKB, BC Albers)
# ---------------------------------------------------------------------------
# HITS = interest parcels touching the claim area (MATERIALIZE keeps Oracle on the
# spatial index). Every current parcel of those dispositions is then pulled, so
# Total Area and the % inside cover the whole disposition, not just the part that hits.
# Lookups and tenants are LEFT JOINs so a missing code or client never drops a disposition.
LAND_SQL = """
WITH HITS AS (
    SELECT /*+ MATERIALIZE */ SH.INTRID_SID
    FROM WHSE_TANTALIS.TA_INTEREST_PARCEL_SHAPES SH
    WHERE SDO_RELATE(SH.SHAPE, SDO_GEOMETRY(:aoi, 3005), 'mask=ANYINTERACT') = 'TRUE'
)
SELECT
    DT.DISPOSITION_TRANSACTION_SID      AS DTID,
    DS.FILE_CHR                         AS FILE_NBR,
    TY.TYPE_NME                         AS TENURE_TYPE,
    ST.SUBTYPE_NME                      AS TENURE_SUBTYPE,
    PU.PURPOSE_NME                      AS TENURE_PURPOSE,
    SP.SUBPURPOSE_NME                   AS TENURE_SUBPURPOSE,
    DT.LOCATION_DSC                     AS LOCATION,
    TT.ACTIVATION_CDE                   AS ACTIVATION,
    SG.STAGE_NME                        AS STAGE,
    TT.STATUS_NME                       AS STATUS,
    TS.EFFECTIVE_DAT                    AS STATUS_EFFECTIVE_DATE,
    DT.APPLICATION_TYPE_CDE             AS APP_TYPE,
    OU.UNIT_NAME                        AS UNIT_NAME,
    DT.RECEIVED_DAT                     AS RECEIVED_DATE,
    DT.COMMENCEMENT_DAT                 AS COMMENCEMENT_DATE,
    DT.EXPIRY_DAT                       AS EXPIRY_DATE,
    IP.INTRID_SID                       AS PARCEL_ID,
    IP.AREA_HA_NUM                      AS PARCEL_AREA_HA,
    COALESCE(PR.LEGAL_NAME, TRIM(PR.FIRST_NAME || ' ' || PR.LAST_NAME)) AS CLIENT_NAME,
    TE.PRIMARY_CONTACT_YRN              AS PRIMARY_CONTACT,
    -- arc densify is a no-op on straight-edged shapes and keeps arcs out of the WKB
    SDO_UTIL.TO_WKBGEOMETRY(SDO_GEOM.SDO_ARC_DENSIFY(SH.SHAPE, 0.005, 'arc_tolerance=0.05')) AS PARCEL_WKB
FROM WHSE_TANTALIS.TA_DISPOSITION_TRANSACTIONS DT
JOIN WHSE_TANTALIS.TA_DISPOSITIONS DS
  ON DS.DISPOSITION_SID = DT.DISPOSITION_SID
JOIN WHSE_TANTALIS.TA_INTEREST_PARCELS IP
  ON IP.DISPOSITION_TRANSACTION_SID = DT.DISPOSITION_TRANSACTION_SID
 AND IP.EXPIRY_DAT IS NULL
LEFT JOIN WHSE_TANTALIS.TA_INTEREST_PARCEL_SHAPES SH
  ON SH.INTRID_SID = IP.INTRID_SID
LEFT JOIN WHSE_TANTALIS.TA_DISP_TRANS_STATUSES TS
  ON TS.DISPOSITION_TRANSACTION_SID = DT.DISPOSITION_TRANSACTION_SID
 AND TS.EXPIRY_DAT IS NULL
LEFT JOIN WHSE_TANTALIS.TA_STAGES SG
  ON SG.CODE_CHR = TS.CODE_CHR_STAGE
LEFT JOIN WHSE_TANTALIS.TA_STATUS TT
  ON TT.CODE_CHR = TS.CODE_CHR_STATUS
LEFT JOIN WHSE_TANTALIS.TA_AVAILABLE_TYPES TY
  ON TY.TYPE_SID = DT.TYPE_SID
LEFT JOIN WHSE_TANTALIS.TA_AVAILABLE_SUBTYPES ST
  ON ST.SUBTYPE_SID = DT.SUBTYPE_SID AND ST.TYPE_SID = DT.TYPE_SID
LEFT JOIN WHSE_TANTALIS.TA_AVAILABLE_PURPOSES PU
  ON PU.PURPOSE_SID = DT.PURPOSE_SID
LEFT JOIN WHSE_TANTALIS.TA_AVAILABLE_SUBPURPOSES SP
  ON SP.SUBPURPOSE_SID = DT.SUBPURPOSE_SID AND SP.PURPOSE_SID = DT.PURPOSE_SID
LEFT JOIN WHSE_TANTALIS.TA_ORGANIZATION_UNITS OU
  ON OU.ORG_UNIT_SID = DT.ORG_UNIT_SID
LEFT JOIN WHSE_TANTALIS.TA_TENANTS TE
  ON TE.DISPOSITION_TRANSACTION_SID = DT.DISPOSITION_TRANSACTION_SID
 AND TE.SEPARATION_DAT IS NULL
LEFT JOIN WHSE_TANTALIS.TA_INTERESTED_PARTIES PR
  ON PR.INTERESTED_PARTY_SID = TE.INTERESTED_PARTY_SID
WHERE DT.DISPOSITION_TRANSACTION_SID IN (
    SELECT IP2.DISPOSITION_TRANSACTION_SID
    FROM WHSE_TANTALIS.TA_INTEREST_PARCELS IP2
    JOIN HITS ON HITS.INTRID_SID = IP2.INTRID_SID
    WHERE IP2.EXPIRY_DAT IS NULL
)
"""

# One row per point of diversion x licensee x purpose; PIDs joined on licence number.
LICENCES_SQL = f"""
WITH IN_CLAIM AS (
    SELECT /*+ MATERIALIZE */
        L.LICENCE_NUMBER, L.FILE_NUMBER, L.LICENCE_STATUS, L.LICENCE_STATUS_DATE,
        L.PRIORITY_DATE, L.EXPIRY_DATE, L.PURPOSE_USE, L.SOURCE_NAME, L.POD_NUMBER,
        L.POD_SUBTYPE, L.PERMIT_OVER_CROWN_LAND_NUMBER, L.PRIMARY_LICENSEE_NAME,
        L.DISTRICT_PRECINCT_NAME
    FROM {LICENCES_VIEW} L
    WHERE SDO_RELATE(L.SHAPE, SDO_GEOMETRY(:aoi, 3005), 'mask=ANYINTERACT') = 'TRUE'
)
SELECT IN_CLAIM.*, P.PID, P.PIN_SID
FROM IN_CLAIM
LEFT JOIN {LICENCE_PARCELS_VIEW} P
  ON P.LICENCE_NO = IN_CLAIM.LICENCE_NUMBER
"""

APPLICATIONS_SQL = f"""
SELECT A.APPLICATION_JOB_NUMBER, A.FILE_NUMBER, A.APPLICATION_STATUS, A.PURPOSE_USE,
       A.POD_NUMBER, A.PRIMARY_APPLICANT_NAME, A.DISTRICT_PRECINCT_NAME
FROM {APPLICATIONS_VIEW} A
WHERE SDO_RELATE(A.SHAPE, SDO_GEOMETRY(:aoi, 3005), 'mask=ANYINTERACT') = 'TRUE'
"""

APPROVALS_SQL = f"""
SELECT A.WATER_APPROVAL_ID, A.APPROVAL_FILE_NUMBER, A.APPROVAL_TYPE, A.APPROVAL_STATUS,
       A.APPROVAL_ISSUANCE_DATE, A.APPROVAL_EXPIRY_DATE, A.SOURCE, A.WORKS_DESCRIPTION,
       A.WATER_DISTRICT, A.PRECINCT, A.CLIENT_NAME
FROM {APPROVALS_VIEW} A
WHERE SDO_RELATE(A.SHAPE, SDO_GEOMETRY(:aoi, 3005), 'mask=ANYINTERACT') = 'TRUE'
"""

# ---------------------------------------------------------------------------
# Output columns (internal name -> client's column name, in the client's order;
# columns after "Client Name" / "Location" are extras)
# ---------------------------------------------------------------------------
LAND_COLUMNS = {
    "FILE_NBR": "File #",
    "DTID": "DID#",
    "TENURE_TYPE": "Type",
    "TENURE_SUBTYPE": "Subtype",
    "TENURE_PURPOSE": "Purpose",
    "TENURE_SUBPURPOSE": "Subpurpose",
    "LOCATION": "Location",
    "ACTIVATION": "Activation",
    "STAGE": "Stage",
    "STATUS": "Status",
    "STATUS_EFFECTIVE_DATE": "Status Effective Date",
    "RECEIVED_DATE": "Received",
    "APP_TYPE": "App Type",
    "TOTAL_AREA_HA": "Total Area (ha)",
    "UNIT_NAME": "Unit Name",
    "COMMENCEMENT_DATE": "Commencement",
    "EXPIRY_DATE": "Expiry",
    "CLIENT_NAME": "Client Name",
    "PCT_IN_CLAIM": "% Within Claim Area",
    "AREA_IN_CLAIM_HA": "Area Within Claim Area (ha)",
}

WSA_COLUMNS = [
    "File #", "Licence #", "Type", "Purpose", "Subpurpose", "PID", "PCL #", "Issue Date",
    "Source", "Client or Applicant", "Status", "Location",
    "Priority Date", "Status Date", "Expiry Date", "POD # in Claim Area", "Works Description",
    "Approval / Job #",
]

LAND_DATES = ["RECEIVED_DATE", "COMMENCEMENT_DATE", "STATUS_EFFECTIVE_DATE", "EXPIRY_DATE"]

DISPOSITION_FIELDS = [
    "FILE_NBR", "TENURE_TYPE", "TENURE_SUBTYPE", "TENURE_PURPOSE", "TENURE_SUBPURPOSE",
    "LOCATION", "ACTIVATION", "STAGE", "STATUS", "APP_TYPE", "UNIT_NAME", *LAND_DATES,
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def load_claim_area(path):
    """Claim area as one valid 2D MultiPolygon in BC Albers, ring order the way Oracle expects."""
    aoi = gpd.read_file(path, layer=AOI_LAYER)
    if aoi.crs is None:
        sys.exit(f"{path} has no CRS defined (expected EPSG:4326)")
    parts = shapely.make_valid(shapely.force_2d(aoi.to_crs(3005).geometry.to_numpy()))
    merged = shapely.union_all(parts)
    polygons = []
    for part in shapely.get_parts(merged):          # make_valid can leave stray lines or points
        if part.geom_type in ("Polygon", "MultiPolygon"):
            polygons.extend(shapely.get_parts(part))
    if not polygons:
        sys.exit(f"No polygons found in {path}")
    # Oracle wants exterior rings counter-clockwise and holes clockwise
    claim = shapely.MultiPolygon([orient(p, sign=1.0) for p in polygons])
    note = f"{path.name} - {len(aoi)} feature(s), {claim.area / 1e4:,.1f} ha"
    print(f"Claim area: {note}")
    return claim, note


def query(conn, sql, claim_wkb):
    """Run a query with the claim area bound as :aoi (a WKB BLOB) and return a DataFrame."""
    with conn.cursor() as cur:
        cur.arraysize = 5000
        cur.setinputsizes(aoi=oracledb.DB_TYPE_BLOB)
        cur.execute(sql, aoi=claim_wkb)
        columns = [d[0] for d in cur.description]
        df = pd.DataFrame(cur.fetchall(), columns=columns)
    print(f"  {len(df)} rows")
    return df


def join_unique(values):
    """'a; b' from the distinct non-blank values, in first-seen order."""
    seen = []
    for v in values:
        if pd.isna(v):
            continue
        text = str(v).strip()
        if text and text not in seen:
            seen.append(text)
    return "; ".join(seen) or None


def iso_dates(table, columns):
    """Dates as ISO text: sorts correctly, and Excel can't show the 1890s priority dates as real dates."""
    for c in columns:
        table[c] = pd.to_datetime(table[c], errors="coerce").dt.strftime("%Y-%m-%d")
    return table


def split_purpose(purpose_use):
    """'03B - Irrigation: Private ' -> ('Irrigation', 'Private')."""
    text = purpose_use.astype("string").str.strip().str.replace(r"^\w+\s+-\s+", "", regex=True)
    parts = text.str.split(":", n=1, expand=True).reindex(columns=[0, 1]).astype("string")
    return parts[0].str.strip(), parts[1].str.strip()


def clean_name(names):
    """Drop the trailing eLicensing client number: 'Chutter Ranch Ltd. (28447)' -> 'Chutter Ranch Ltd.'."""
    return names.astype("string").str.replace(r"\s*\(\d+\)\s*$", "", regex=True).str.strip()


def water_source(source_name, pod_subtype):
    """Groundwater PODs (PWD / PG) carry an aquifer number or name as their source - say so."""
    src = source_name.astype("string").str.strip()
    numeric = src.str.fullmatch(r"\d+", na=False)
    groundwater = ("Groundwater - aquifer " + src).where(numeric, "Groundwater - " + src)
    return src.where(~pod_subtype.isin(["PWD", "PG"]), groundwater)


def format_pid(pid, pin):
    """'010540687' -> '010-540-687'; the PIN is shown where a parcel has no PID."""
    digits = pid.astype("string").str.strip().str.zfill(9)
    formatted = digits.str[:3] + "-" + digits.str[3:6] + "-" + digits.str[6:]
    return formatted.fillna("PIN " + pin.astype("string"))


# ---------------------------------------------------------------------------
# Land Act
# ---------------------------------------------------------------------------
def disposition_shapes(parcels):
    """Union of each disposition's mapped parcels, as a GeoSeries indexed by DTID (BC Albers)."""
    mapped = parcels.dropna(subset=["PARCEL_WKB"])
    geoms = shapely.make_valid(shapely.from_wkb(mapped["PARCEL_WKB"].to_numpy()))
    gdf = gpd.GeoDataFrame({"DTID": mapped["DTID"].to_numpy()}, geometry=geoms, crs=3005)
    return gdf.dissolve(by="DTID").geometry


def land_table(raw, claim):
    """One row per disposition transaction (DID#) from the parcel x tenant rows of LAND_SQL."""
    if raw.empty:
        return pd.DataFrame(columns=list(LAND_COLUMNS)), None
    raw = raw.copy()
    for c in LAND_DATES:
        raw[c] = pd.to_datetime(raw[c], errors="coerce")

    land = raw.drop_duplicates("DTID").set_index("DTID")[DISPOSITION_FIELDS].copy()
    # current tenants, primary contact first
    land["CLIENT_NAME"] = (raw.sort_values("PRIMARY_CONTACT", ascending=False)
                              .groupby("DTID")["CLIENT_NAME"].agg(join_unique))
    parcels = raw.drop_duplicates(["DTID", "PARCEL_ID"])
    land["TOTAL_AREA_HA"] = parcels.groupby("DTID")["PARCEL_AREA_HA"].sum(min_count=1)

    shapes = disposition_shapes(parcels)
    land["GIS_AREA_M2"] = shapes.area
    land["AREA_IN_CLAIM_M2"] = shapes.intersection(claim).area
    land["PCT_IN_CLAIM"] = 100 * land["AREA_IN_CLAIM_M2"] / land["GIS_AREA_M2"].where(land["GIS_AREA_M2"] > 0)

    # Drop a disposition only when nothing on it reaches START_DATE: every date on record
    # (received, commencement, status effective, expiry) is earlier. The dates go in the output,
    # so the client can apply "issued since" or "in effect since" themselves. Undated records are kept.
    latest = land[LAND_DATES].max(axis=1)
    before_start = latest < START_DATE
    print(f"  {len(land)} dispositions touch or overlap the claim area; dropped {before_start.sum()} "
          f"with every date before {START_DATE:%Y-%m-%d}; kept {latest.isna().sum()} with no dates")
    land = land[~before_start].reset_index()
    land = land.sort_values(["FILE_NBR", "COMMENCEMENT_DATE", "DTID"], na_position="last")
    print(land["TENURE_TYPE"].value_counts(dropna=False).to_string())

    land["TOTAL_AREA_HA"] = land["TOTAL_AREA_HA"].astype(float).round(4)
    land["AREA_IN_CLAIM_HA"] = (land["AREA_IN_CLAIM_M2"] / 1e4).round(4)
    land["PCT_IN_CLAIM"] = land["PCT_IN_CLAIM"].round(2)
    land = iso_dates(land, LAND_DATES)
    return land, shapes


def write_qa_gpkg(path, land, shapes):
    """Selected dispositions with their overlap, for a quick look in ArcGIS Pro / QGIS."""
    fields = ["DTID", "FILE_NBR", "TENURE_TYPE", "TENURE_SUBTYPE", "STATUS",
              "COMMENCEMENT_DATE", "PCT_IN_CLAIM", "AREA_IN_CLAIM_HA"]
    qa = gpd.GeoDataFrame(land[fields], geometry=shapes.reindex(land["DTID"]).to_numpy(), crs=3005)
    qa.to_file(path, layer="land_act_dispositions", driver="GPKG")
    print(f"  QA layer -> {path}")


# ---------------------------------------------------------------------------
# Water Sustainability Act
# ---------------------------------------------------------------------------
def licences_table(raw):
    """One row per licence and purpose."""
    if raw.empty:
        return pd.DataFrame(columns=WSA_COLUMNS)
    df = raw.copy()
    for c in ("LICENCE_STATUS_DATE", "PRIORITY_DATE", "EXPIRY_DATE"):
        df[c] = pd.to_datetime(df[c], errors="coerce")
    # BCGW has no licence issue date, and the priority date can predate issuance by a century,
    # so only licences that had already ended before START_DATE are dropped.
    ended_before_start = ((df["LICENCE_STATUS"] != "Current")
                          & (df["LICENCE_STATUS_DATE"] < START_DATE)
                          & (df["PRIORITY_DATE"] < START_DATE))
    df = df[~ended_before_start].copy()
    print(f"  dropped {ended_before_start.sum()} rows of licences that ended before {START_DATE:%Y}")

    df["PURPOSE"], df["SUBPURPOSE"] = split_purpose(df["PURPOSE_USE"])
    df["CLIENT"] = clean_name(df["PRIMARY_LICENSEE_NAME"])
    df["SOURCE"] = water_source(df["SOURCE_NAME"], df["POD_SUBTYPE"])
    df["PID_OR_PIN"] = format_pid(df["PID"], df["PIN_SID"])

    keys = ["FILE_NUMBER", "LICENCE_NUMBER", "PURPOSE", "SUBPURPOSE"]
    out = df.groupby(keys, dropna=False, sort=False).agg(**{
        "PID": ("PID_OR_PIN", join_unique),
        "PCL #": ("PERMIT_OVER_CROWN_LAND_NUMBER", join_unique),
        "Source": ("SOURCE", join_unique),
        "Client or Applicant": ("CLIENT", join_unique),
        "Status": ("LICENCE_STATUS", "first"),
        "Location": ("DISTRICT_PRECINCT_NAME", join_unique),
        "Priority Date": ("PRIORITY_DATE", "first"),
        "Status Date": ("LICENCE_STATUS_DATE", "first"),
        "Expiry Date": ("EXPIRY_DATE", "first"),
        "POD # in Claim Area": ("POD_NUMBER", join_unique),
    }).reset_index()
    out["Type"] = "Licence"
    out = out.rename(columns={"FILE_NUMBER": "File #", "LICENCE_NUMBER": "Licence #",
                              "PURPOSE": "Purpose", "SUBPURPOSE": "Subpurpose"})
    return out.reindex(columns=WSA_COLUMNS)


def applications_table(raw):
    """One row per licence application and purpose."""
    if raw.empty:
        return pd.DataFrame(columns=WSA_COLUMNS)
    df = raw.copy()
    df["PURPOSE"], df["SUBPURPOSE"] = split_purpose(df["PURPOSE_USE"])
    df["CLIENT"] = clean_name(df["PRIMARY_APPLICANT_NAME"])

    keys = ["FILE_NUMBER", "APPLICATION_JOB_NUMBER", "PURPOSE", "SUBPURPOSE"]
    out = df.groupby(keys, dropna=False, sort=False).agg(**{
        "Client or Applicant": ("CLIENT", join_unique),
        "Status": ("APPLICATION_STATUS", "first"),
        "Location": ("DISTRICT_PRECINCT_NAME", join_unique),
        "POD # in Claim Area": ("POD_NUMBER", join_unique),
    }).reset_index()
    out["Type"] = "Licence application"
    out = out.rename(columns={"FILE_NUMBER": "File #", "APPLICATION_JOB_NUMBER": "Approval / Job #",
                              "PURPOSE": "Purpose", "SUBPURPOSE": "Subpurpose"})
    return out.reindex(columns=WSA_COLUMNS)


def approvals_table(raw):
    """One row per approval."""
    if raw.empty:
        return pd.DataFrame(columns=WSA_COLUMNS)
    df = raw.copy()
    df["CLIENT"] = clean_name(df["CLIENT_NAME"])
    # PRECINCT already includes the district ("20E - New Westminster / Coquitlam"): drop the code
    precinct = df["PRECINCT"].astype("string").str.replace(r"^\w+\s+-\s+", "", regex=True).str.strip()
    df["LOCATION"] = precinct.fillna(df["WATER_DISTRICT"].astype("string"))

    out = df.groupby("WATER_APPROVAL_ID", sort=False).agg(**{
        "File #": ("APPROVAL_FILE_NUMBER", "first"),
        "Type": ("APPROVAL_TYPE", "first"),
        "Issue Date": ("APPROVAL_ISSUANCE_DATE", "first"),
        "Source": ("SOURCE", join_unique),
        "Client or Applicant": ("CLIENT", join_unique),
        "Status": ("APPROVAL_STATUS", "first"),
        "Location": ("LOCATION", join_unique),
        "Expiry Date": ("APPROVAL_EXPIRY_DATE", "first"),
        "Works Description": ("WORKS_DESCRIPTION", join_unique),
    }).reset_index().rename(columns={"WATER_APPROVAL_ID": "Approval / Job #"})
    return out.reindex(columns=WSA_COLUMNS)


def wsa_table(*parts):
    """Stack licences, applications and approvals, in the client's Type order."""
    wsa = pd.concat(parts, ignore_index=True)[WSA_COLUMNS]
    wsa["_order"] = wsa["Type"].map({t: i for i, t in enumerate(TYPE_ORDER)}).fillna(len(TYPE_ORDER))
    wsa = wsa.sort_values(["_order", "File #", "Licence #"], na_position="last").drop(columns="_order")
    return iso_dates(wsa, ["Issue Date", "Priority Date", "Status Date", "Expiry Date"])


# ---------------------------------------------------------------------------
# Excel
# ---------------------------------------------------------------------------
def write_excel(path, table, notes):
    table = table.copy()
    for col in table.columns:   # openpyxl refuses control characters, which do turn up in free-text fields
        if not pd.api.types.is_numeric_dtype(table[col]):
            table[col] = table[col].map(lambda v: ILLEGAL_CHARACTERS_RE.sub("", v) if isinstance(v, str) else v)

    with pd.ExcelWriter(path, engine="openpyxl") as xl:
        table.to_excel(xl, sheet_name="Results", index=False)
        pd.DataFrame(list(notes.items()), columns=["Item", "Note"]).to_excel(xl, sheet_name="Notes", index=False)

        results = xl.sheets["Results"]
        results.freeze_panes = "A2"
        results.auto_filter.ref = results.dimensions
        for cells in results.iter_cols(max_row=min(results.max_row, 500)):
            width = max((len(str(c.value)) for c in cells if c.value is not None), default=8)
            results.column_dimensions[cells[0].column_letter].width = min(width + 2, 60)

        sheet = xl.sheets["Notes"]
        sheet.column_dimensions["A"].width = 24
        sheet.column_dimensions["B"].width = 110
        for cell in sheet["B"]:
            cell.alignment = Alignment(wrap_text=True, vertical="top")
    print(f"  {len(table)} rows -> {path}")


def land_notes(claim_note, run_date):
    return {
        "Claim area": claim_note,
        "Source": f"Tantalis (WHSE_TANTALIS) through the BC Geographic Warehouse, extracted {run_date:%Y-%m-%d %H:%M}",
        "Selection": "Disposition transactions with at least one current interest parcel intersecting the claim area",
        "One row per": "Disposition transaction (DID#). A file that was replaced or amended appears once per DID#, in date order",
        "Date rule": (f"Listed unless every date on record (received, commencement, status effective, expiry) is before "
                      f"{START_DATE:%Y-%m-%d}; undated records are kept. Commencement shows what was issued since "
                      f"{START_DATE:%Y}; Status Effective Date and Expiry show what was still in effect after it"),
        "Status Effective Date": ("Date the current status took effect, e.g. the expiry or cancellation date of a "
                                  "disposition no longer in good standing"),
        "Total Area (ha)": "Area recorded in Tantalis (sum of the disposition's current interest parcels)",
        "% Within Claim Area": ("Share of the disposition's mapped parcels (GIS area, BC Albers) inside the claim area. "
                                "Dispositions that only touch the claim boundary are listed with 0%"),
        "Client Name": "Current tenants; several tenants are separated by ';'",
        "Limitation": "Dispositions with no mapped interest parcel in Tantalis cannot be located spatially and are not listed",
    }


def wsa_notes(claim_note, run_date):
    return {
        "Claim area": claim_note,
        "Source": (f"BC Geographic Warehouse: {LICENCES_VIEW}, {APPLICATIONS_VIEW}, {APPROVALS_VIEW}, "
                   f"{LICENCE_PARCELS_VIEW}; extracted {run_date:%Y-%m-%d %H:%M}"),
        "Selection": "Points of diversion (licences, applications) and approval points located in the claim area",
        "One row per": "Licence or application and purpose; approval",
        "Type": ("Licence and Licence application rows come from the licence and application datasets. Approval rows "
                 "keep the dataset's APPROVAL_TYPE code: STU = s.10 use approval (short term use of water); "
                 "A-CIAS = s.11 change approval and N-CIAS = s.11 notification (changes in and about a stream)"),
        "Issue Date": ("Approvals only - the warehouse holds no licence issue date. For N-CIAS notifications it is the "
                       "date work may begin: 45 days after acceptance, or the proposed start date if later"),
        "Date rule": (f"Licences that had already ended (cancelled, abandoned, expired) before {START_DATE:%Y} are "
                      "excluded. Priority Date is a licence's date of precedence and can be much earlier than its issuance"),
        "PID": "Parcels linked to the licence in Land Parcels with Water Licences; the PIN is shown where a parcel has no PID",
        "Limitation": "Points of diversion with no recorded location cannot be located spatially and are not listed",
    }


# ---------------------------------------------------------------------------
def main():
    claim_path = Path(sys.argv[1] if len(sys.argv) > 1 else AOI_GPKG)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    run_date = datetime.now()
    stamp = run_date.strftime("%Y%m%d")

    claim, claim_note = load_claim_area(claim_path)
    claim_wkb = shapely.to_wkb(claim)

    with oracledb.connect(user=os.environ["BCGW_USER"], password=os.environ["BCGW_PASSWORD"],
                          dsn=os.environ["BCGW_HOST"]) as conn:
        print("Land Act dispositions (Tantalis)")
        land, shapes = land_table(query(conn, LAND_SQL, claim_wkb), claim)
        land_xlsx = OUT_DIR / f"LandAct_Dispositions_{stamp}.xlsx"
        write_excel(land_xlsx, land.rename(columns=LAND_COLUMNS)[list(LAND_COLUMNS.values())],
                    land_notes(claim_note, run_date))
        if WRITE_QA_GPKG and not land.empty:
            write_qa_gpkg(OUT_DIR / f"LandAct_Dispositions_{stamp}_QA.gpkg", land, shapes)

        print("WSA licences")
        licences = licences_table(query(conn, LICENCES_SQL, claim_wkb))
        print("WSA licence applications")
        applications = applications_table(query(conn, APPLICATIONS_SQL, claim_wkb))
        print("WSA approvals")
        approvals = approvals_table(query(conn, APPROVALS_SQL, claim_wkb))

    wsa = wsa_table(licences, applications, approvals)
    print(wsa["Type"].value_counts().to_string())
    write_excel(OUT_DIR / f"WSA_Authorizations_{stamp}.xlsx", wsa, wsa_notes(claim_note, run_date))


if __name__ == "__main__":
    main()
