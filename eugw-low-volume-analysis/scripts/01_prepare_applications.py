"""
01_prepare_applications.py
==========================
Clean the client-supplied EUGW low-volume application spreadsheet and split it
into normalised tables ready for spatial work.

Inputs
------
EUGW_Volumes_In_Progress_Low_Volume.xlsx, sheet "EUGW Low Volume"
    One row per application (vFCBC Tracking Number is unique). Multi-well and
    multi-parcel applications carry semicolon-delimited values in the well and
    PID columns.

Outputs (data/interim/)
-----------------------
applications.csv        One row per application: core attributes + QA flags.
application_pids.csv    Long. One row per application x PID.
application_wells.csv   Long. One row per application x well.
qa_log.csv              Every record dropped or altered, with a reason.

Why long tables
---------------
Roughly half of these applications touch more than one parcel, well or
aquifer. Collapsing to one row per application at this stage would bake in a
combination rule that the client may still change. Collapse happens in step 04.

Key cleaning decisions (see METHODOLOGY.md section 4)
-----------------------------------------------------
1. PIDs arrive hyphenated ("018-266-433"); PMBC stores them as a 9-character
   zero-padded string. Hyphens stripped, left-padded, non-conforming flagged.
2. Around 40% of longitudes are positive - the negative sign was lost on
   export. Flipped where doing so places the point inside BC. Sign is checked
   per coordinate, not per row: some multi-well rows mix both.
3. Delimited lists are split WITHOUT dropping empty tokens. Latitude,
   longitude and well tag lists are positionally aligned and use empty slots
   as placeholders ("104617; " means well 1 has a tag, well 2 does not).
   Dropping empties silently mis-pairs tags to locations.
4. Coordinates with no decimal component (lat 48, lon -123) are truncated
   placeholders, not locations. Retained but excluded from spatial work.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import yaml

# --------------------------------------------------------------------------- #
# Setup
# --------------------------------------------------------------------------- #

ROOT = Path(__file__).resolve().parents[1]

with open(ROOT / "config.yaml") as f:
    CFG = yaml.safe_load(f)

P = CFG["paths"]
CLEAN = CFG["cleaning"]
BBOX = CLEAN["bc_bbox"]

qa_rows: list[dict] = []


def qa(tracking, issue, detail="", action=""):
    """Record a data-quality observation for the audit log."""
    qa_rows.append(
        {"tracking_number": tracking, "issue": issue,
         "detail": str(detail)[:200], "action": action}
    )


# --------------------------------------------------------------------------- #
# Parsing helpers
# --------------------------------------------------------------------------- #

def split_keep_empty(value) -> list[str]:
    """
    Split a delimited cell on ';' or ',' preserving empty positions.

    Positional alignment matters: the well tag, latitude and longitude lists
    use empty slots to mark wells with no recorded tag. Returns [] for blanks
    so single-valued and empty cells behave predictably.
    """
    if pd.isna(value):
        return []
    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return []
    return [part.strip() for part in re.split(r"[;,]", text)]


def clean_pid(raw: str) -> tuple[str | None, str]:
    """
    Normalise a PID to the 9-digit form used by PMBC_PARCEL_FABRIC_POLY_SVW.

    Returns (pid, status) where status is one of:
        ok            - 9 digits, usable
        padded        - fewer than 9 digits, left-padded with zeros
        invalid       - no digits, all zeros, or more than 9 digits
    """
    digits = re.sub(r"\D", "", str(raw))
    if not digits or int(digits) == 0:
        return None, "invalid"
    if len(digits) > 9:
        return None, "invalid"
    if len(digits) < 9:
        return digits.zfill(9), "padded"
    return digits, "ok"


def clean_coordinate(lat_raw: str, lon_raw: str) -> dict:
    """
    Validate and correct a single lat/lon pair.

    Applies the longitude sign correction only where the flip actually moves
    the point into BC - a blind flip would corrupt already-correct values.

    Returns a dict with the corrected values and a status:
        ok                  - inside BC, usable
        sign_corrected      - longitude sign flipped, now inside BC
        low_precision       - integer-only values, excluded from spatial work
        outside_bc          - outside the BC bounding box even after flipping
        unparseable / missing
    """
    out = {"latitude": None, "longitude": None,
           "coord_status": "missing", "sign_corrected": False}

    if lat_raw.strip() == "" or lon_raw.strip() == "":
        return out

    try:
        lat, lon = float(lat_raw), float(lon_raw)
    except ValueError:
        out["coord_status"] = "unparseable"
        return out

    out["latitude"], out["longitude"] = lat, lon

    def in_bc(la, lo):
        return (BBOX["min_lat"] <= la <= BBOX["max_lat"]
                and BBOX["min_lon"] <= lo <= BBOX["max_lon"])

    # Longitude sign correction, applied per coordinate
    if CLEAN["fix_longitude_sign"] and not in_bc(lat, lon) and in_bc(lat, -lon):
        out["longitude"] = -lon
        out["sign_corrected"] = True
        lon = -lon

    if not in_bc(lat, lon):
        out["coord_status"] = "outside_bc"
        return out

    # Truncated placeholders such as (48, -123) are not real locations
    if CLEAN["flag_integer_coordinates"] and lat == int(lat) and lon == int(lon):
        out["coord_status"] = "low_precision"
        return out

    out["coord_status"] = "sign_corrected" if out["sign_corrected"] else "ok"
    return out


def clean_wtn(raw: str) -> str | None:
    """Normalise a well tag number. Handles float-formatted values ('86267.0')."""
    text = str(raw).strip()
    if text in ("", "nan", "None"):
        return None
    try:
        return str(int(float(text)))
    except ValueError:
        return None


# --------------------------------------------------------------------------- #
# Load
# --------------------------------------------------------------------------- #

def load_applications() -> pd.DataFrame:
    """Read the client spreadsheet and normalise whitespace in column names."""
    df = pd.read_excel(
        ROOT / P["applications_xlsx"],
        sheet_name=P["applications_sheet"],
        header=P["applications_header_row"],
    )
    df.columns = [re.sub(r"\s+", " ", str(c)).strip() for c in df.columns]

    # Spreadsheet exports frequently carry trailing blank rows
    before = len(df)
    df = df[df["vFCBC Tracking Number"].notna()].copy()
    if before != len(df):
        qa(None, "blank_rows_removed", f"{before - len(df)} trailing blank rows",
           "dropped")

    df["tracking_number"] = df["vFCBC Tracking Number"].astype(int).astype(str)

    dupes = df["tracking_number"].duplicated().sum()
    if dupes:
        qa(None, "duplicate_tracking_numbers", dupes, "retained - review")

    return df


# --------------------------------------------------------------------------- #
# Build normalised tables
# --------------------------------------------------------------------------- #

def build_pid_table(df: pd.DataFrame) -> pd.DataFrame:
    """Explode the Land PID column into one row per application x parcel."""
    rows = []
    for _, r in df.iterrows():
        tn = r["tracking_number"]
        tokens = [t for t in split_keep_empty(r.get("Land PID")) if t]

        if not tokens:
            qa(tn, "no_pid", r.get("Land Details", ""), "well coords only")
            continue

        for i, tok in enumerate(tokens):
            pid, status = clean_pid(tok)
            if status == "invalid":
                qa(tn, "invalid_pid", tok, "excluded")
            elif status == "padded":
                qa(tn, "pid_zero_padded", f"{tok} -> {pid}", "retained")
            rows.append({"tracking_number": tn, "pid_index": i,
                         "pid_raw": tok, "pid": pid, "pid_status": status})

    return pd.DataFrame(rows)


def build_well_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Explode well columns into one row per application x well.

    Latitude, longitude and well tag lists are positionally aligned. Where the
    lists are different lengths the row is flagged and the shorter list is
    padded, so wells are never silently mis-paired.
    """
    rows = []
    for _, r in df.iterrows():
        tn = r["tracking_number"]
        lats = split_keep_empty(r.get("Well Latitude"))
        lons = split_keep_empty(r.get("Well Longitude"))
        wtns = split_keep_empty(r.get("Well Tag Number"))

        n = max(len(lats), len(lons), len(wtns))
        if n == 0:
            qa(tn, "no_well_data", "", "parcel only")
            continue

        if len({len(x) for x in (lats, lons, wtns) if x} - {0}) > 1:
            qa(tn, "well_list_length_mismatch",
               f"lat={len(lats)} lon={len(lons)} wtn={len(wtns)}",
               "padded - verify pairing")

        pad = lambda lst: lst + [""] * (n - len(lst))  # noqa: E731
        lats, lons, wtns = pad(lats), pad(lons), pad(wtns)

        for i in range(n):
            coord = clean_coordinate(lats[i], lons[i])
            if coord["coord_status"] in ("outside_bc", "unparseable"):
                qa(tn, f"coord_{coord['coord_status']}",
                   f"well {i}: {lats[i]}, {lons[i]}", "excluded from spatial")
            elif coord["coord_status"] == "low_precision":
                qa(tn, "coord_low_precision", f"well {i}: {lats[i]}, {lons[i]}",
                   "excluded from spatial")

            rows.append({
                "tracking_number": tn,
                "well_index": i,
                "well_tag_number": clean_wtn(wtns[i]),
                **coord,
            })

    return pd.DataFrame(rows)


def build_application_table(df: pd.DataFrame, pids, wells) -> pd.DataFrame:
    """One row per application: attributes carried forward plus QA counts."""
    keep = {
        "tracking_number": "tracking_number",
        "File Status": "file_status",
        "Region": "region",
        "District/Precinct": "district",
        "Watershed": "watershed",
        "Aquifer": "aquifer_client",       # empty in source - we derive it
        "App Purpose Name": "purpose",
        "# of Days in Season": "days_in_season",
        "Application Quantity": "quantity",
        "Application Quantity Units": "quantity_units",
        "Converted Quantity per Day (m3)": "cmd_client",
        "Well Depth": "well_depth",
        "Land Details": "land_details",
        "Legal Description of Land": "legal_description",
        "Works": "works",
        "Work Comments": "work_comments",
    }
    app = df[[c for c in keep if c in df.columns]].rename(columns=keep).copy()

    usable_pids = (pids[pids["pid_status"] != "invalid"]
                   .groupby("tracking_number").size().rename("n_pids"))
    usable_wells = (wells[wells["coord_status"].isin(["ok", "sign_corrected"])]
                    .groupby("tracking_number").size().rename("n_wells_usable"))
    all_wells = wells.groupby("tracking_number").size().rename("n_wells_total")

    app = (app.merge(usable_pids, on="tracking_number", how="left")
              .merge(usable_wells, on="tracking_number", how="left")
              .merge(all_wells, on="tracking_number", how="left")
              .fillna({"n_pids": 0, "n_wells_usable": 0, "n_wells_total": 0}))

    for c in ("n_pids", "n_wells_usable", "n_wells_total"):
        app[c] = app[c].astype(int)

    # Can this application be located at all?
    app["locatable"] = (app["n_pids"] > 0) | (app["n_wells_usable"] > 0)
    app["location_source"] = app.apply(
        lambda r: "both" if r["n_pids"] > 0 and r["n_wells_usable"] > 0
        else "parcel" if r["n_pids"] > 0
        else "well" if r["n_wells_usable"] > 0
        else "none", axis=1)
    return app


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main():
    out = ROOT / P["interim_dir"]
    out.mkdir(parents=True, exist_ok=True)

    df = load_applications()
    pids = build_pid_table(df)
    wells = build_well_table(df)
    app = build_application_table(df, pids, wells)
    qa_df = pd.DataFrame(qa_rows)

    app.to_csv(out / "applications.csv", index=False)
    pids.to_csv(out / "application_pids.csv", index=False)
    wells.to_csv(out / "application_wells.csv", index=False)
    qa_df.to_csv(out / "qa_log.csv", index=False)

    # ----------------------------- summary -------------------------------- #
    n = len(app)
    print("=" * 68)
    print("STEP 01  PREPARE APPLICATIONS")
    print("=" * 68)
    print(f"Applications read              {n}")
    print(f"  unique tracking numbers      {app['tracking_number'].nunique()}")

    print("\nRegion")
    for reg, c in app["region"].value_counts().items():
        print(f"  {reg:<28s} {c:>5d}")

    print(f"\nParcels (PIDs)                 {len(pids)} tokens")
    for s, c in pids["pid_status"].value_counts().items():
        print(f"  {s:<28s} {c:>5d}")

    print(f"\nWells                          {len(wells)} records")
    for s, c in wells["coord_status"].value_counts().items():
        print(f"  {s:<28s} {c:>5d}")
    flipped = int(wells["sign_corrected"].sum())
    print(f"  longitude sign corrected     {flipped:>5d}")

    print("\nLocatability")
    for s, c in app["location_source"].value_counts().items():
        print(f"  {s:<28s} {c:>5d}  ({c / n * 100:4.1f}%)")
    unlocatable = int((~app["locatable"]).sum())
    print(f"\n  NOT LOCATABLE                {unlocatable:>5d}  "
          f"({unlocatable / n * 100:4.1f}%)")

    print(f"\nQA log entries                 {len(qa_df)}")
    if len(qa_df):
        for issue, c in qa_df["issue"].value_counts().items():
            print(f"  {issue:<28s} {c:>5d}")

    print(f"\nWritten to {out}")
    print("=" * 68)


if __name__ == "__main__":
    main()
