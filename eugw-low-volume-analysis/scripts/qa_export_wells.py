"""
QA export - well points with raw and cleaned coordinates side by side.

Not part of the 00-04 pipeline. This exists so the coordinate cleaning in
step 01 can be checked visually on a map before anything downstream is
trusted. Safe to re-run at any time; it reads step 01's output and the source
spreadsheet, and writes one CSV.

Output: data/interim/qa_wells_verification.csv
    One row per well (not per application). Load in QGIS/ArcGIS as an XY
    layer using longitude / latitude (WGS84, EPSG:4326).

    lat_raw / lon_raw     exactly as they appear in the spreadsheet cell
    latitude / longitude  after cleaning; what the spatial work actually uses
    sign_corrected        True where the longitude sign was flipped
    coord_status          ok | sign_corrected | low_precision | outside_bc
                          | unparseable | missing
    use_in_spatial        True only for ok and sign_corrected - the rows step
                          02 will actually resolve to an aquifer

Rows where use_in_spatial is False are kept deliberately: the point of this
export is to see what is being excluded, not to hide it.

    uv run scripts/qa_export_wells.py
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

# Reuse step 01's own parser rather than reimplementing the split. The lists
# are positionally aligned, so a second implementation could drift and pair
# the wrong latitude with the wrong longitude.
_spec = importlib.util.spec_from_file_location(
    "prep", ROOT / "scripts" / "01_prepare_applications.py")
prep = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(prep)


def raw_coordinates() -> pd.DataFrame:
    """The untouched latitude/longitude cells, exploded to one row per well."""
    df = prep.load_applications()
    rows = []
    for _, r in df.iterrows():
        lats = prep.split_keep_empty(r.get("Well Latitude"))
        lons = prep.split_keep_empty(r.get("Well Longitude"))
        wtns = prep.split_keep_empty(r.get("Well Tag Number"))

        n = max(len(lats), len(lons), len(wtns))
        pad = lambda lst: lst + [""] * (n - len(lst))  # noqa: E731
        lats, lons = pad(lats), pad(lons)

        for i in range(n):
            rows.append({"tracking_number": r["tracking_number"],
                         "well_index": i,
                         "lat_raw": lats[i],
                         "lon_raw": lons[i]})
    return pd.DataFrame(rows)


def main():
    interim = ROOT / "data" / "interim"
    wells = pd.read_csv(interim / "application_wells.csv",
                        dtype={"tracking_number": str})
    apps = pd.read_csv(interim / "applications.csv",
                       dtype={"tracking_number": str})

    out = wells.merge(raw_coordinates(), on=["tracking_number", "well_index"],
                      how="left")

    out["use_in_spatial"] = out["coord_status"].isin(["ok", "sign_corrected"])

    context = ["tracking_number", "region", "district", "watershed", "purpose",
               "quantity", "quantity_units", "cmd_client", "well_depth",
               "n_pids", "n_wells_total", "locatable", "location_source",
               "land_details"]
    out = out.merge(apps[[c for c in context if c in apps.columns]],
                    on="tracking_number", how="left")

    order = ["tracking_number", "well_index", "well_tag_number",
             "lat_raw", "lon_raw", "latitude", "longitude",
             "sign_corrected", "coord_status", "use_in_spatial"]
    out = out[order + [c for c in out.columns if c not in order]]

    dest = interim / "qa_wells_verification.csv"
    out.to_csv(dest, index=False)

    print("=" * 68)
    print("QA EXPORT  WELL COORDINATES")
    print("=" * 68)
    print(f"Wells written                  {len(out)}")
    print(f"  usable for spatial work      {int(out['use_in_spatial'].sum())}")
    print(f"  excluded                     {int((~out['use_in_spatial']).sum())}")
    print("\nBy coordinate status")
    for status, n in out["coord_status"].value_counts().items():
        print(f"  {status:28} {n}")
    print(f"\nLongitude sign flipped         {int(out['sign_corrected'].sum())}")
    print(f"\nWritten to {dest}")
    print("Load as XY layer, EPSG:4326, using longitude / latitude.")
    print("=" * 68)


if __name__ == "__main__":
    main()
