"""
03_apply_criteria.py
====================
Flag each application-aquifer pair against the WSA low-volume exemption
criteria.

Criteria as confirmed by the client (2026-08)
---------------------------------------------
ASSESSABLE
  1. Aquifer hydraulically connected to a sensitive stream
     Source: client list of 76 aquifers ("Aquifers Overlapping Sensitive").
  2. Aquifer carrying a Fully Recorded notation
     Source: WLS_WATER_NOTATION_AQUIFERS_SP, codes FR and FR-EXC.
  3. Aquifer reserved under WSA s.39-41
     Source: WLS_WATER_RESERVES_AQUIFERS_SP. 18 aquifers, single OIC 0347/2024.
     Note 14 of the 18 are already on the client's sensitive list, so this
     criterion adds only four aquifers in practice.
  4. Watershed subject to a WSA s.88 temporary protection order
     Source: WLS_TEMP_PROT_ORDER_SV. Client confirmed the criterion applies to
     any watershed that has EXPERIENCED an order, not only current ones
     (config: criteria.tpo_scope = "ever").

NOT ASSESSABLE - carried as "not_assessed" columns so the gap is explicit in
the deliverable rather than silently absent
  5. WSA s.86 / s.87 orders - no order has ever been issued under either
     section, so there is nothing to test against.
  6. WSA s.82 dedicated agricultural water - no spatial source identified.
  7. Already serviced by a waterworks licensee - licence data gives purveyors
     and diversion points but no service areas, and the criterion turns on
     whether the system supplies the intended non-domestic use. Operational
     check, not a spatial one.
  8. Storage condition - depends on applicant works, not mappable. The client
     has already partly applied this by removing dugout-only applications.
     `Works` is populated on only 48 of 712 rows, but `Work Comments` on 487,
     which may support a text screen if the client wants one.

Output (data/interim/application_criteria.csv)
-----------------------------------------------
Still long - one row per application x aquifer x method, with a boolean per
criterion. Collapsing to one verdict per application happens in step 04.
"""

from __future__ import annotations

import re
from pathlib import Path

import geopandas as gpd
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
with open(ROOT / "config.yaml") as f:
    CFG = yaml.safe_load(f)

P = CFG["paths"]
CRIT = CFG["criteria"]
CRS = CFG["project"]["crs"]


# --------------------------------------------------------------------------- #
# Criterion source lists
# --------------------------------------------------------------------------- #

def load_sensitive_aquifers() -> set[str]:
    """Client-supplied list of aquifers connected to sensitive streams."""
    df = pd.read_excel(ROOT / P["applications_xlsx"],
                       sheet_name=P["sensitive_sheet"])
    col = next(c for c in df.columns if "Aquifer Number" in str(c))
    return set(df[col].dropna().astype(int).astype(str))


def load_fully_recorded_aquifers() -> set[str]:
    """
    Aquifers with a Fully Recorded notation.

    A single aquifer can carry several notations, so every notation-description
    field found is checked - filtering on one alone misses aquifers where FR is
    recorded elsewhere.
    """
    path = ROOT / P["interim_dir"] / "aquifer_notations.csv"
    if not path.exists():
        print("  WARNING: aquifer_notations.csv missing - criterion skipped")
        return set()

    df = pd.read_csv(path)
    codes = {c.strip().upper()
             for c in CFG["bcgw"]["aquifer_notations"]["fully_recorded_codes"]}

    id_col = next((c for c in df.columns if "AQUIFER" in c.upper()
                   and ("ID" in c.upper() or "NUMBER" in c.upper())), None)
    note_cols = [c for c in df.columns if "NOTATION_DESC" in c.upper()]
    if not id_col or not note_cols:
        print(f"  WARNING: expected columns not found in notations "
              f"(id={id_col}, notation={note_cols}) - criterion skipped")
        return set()

    # The notation code is embedded in free text, e.g.
    #   "143 - FR-EXC - 2023/10/10"
    # Split on space-hyphen-space so the internal hyphen of FR-EXC survives,
    # then match a segment exactly. Exact segments keep FR and FR-EXC
    # distinguishable; a substring test for "FR" would also hit FR-EXC and
    # make fully_recorded_codes unable to exclude it.
    sep = re.compile(r"\s+-\s+")

    def has_code(value: str) -> bool:
        return any(part.strip().upper() in codes for part in sep.split(value))

    mask = pd.Series(False, index=df.index)
    for c in note_cols:
        mask |= df[c].fillna("").astype(str).apply(has_code)
    return set(df.loc[mask, id_col].dropna().astype(int).astype(str))


def load_reserve_aquifers() -> set[str]:
    """Aquifers reserved under WSA s.39-41."""
    path = ROOT / P["interim_dir"] / "water_reserves.csv"
    if not path.exists():
        print("  WARNING: water_reserves.csv missing - criterion skipped")
        return set()
    df = pd.read_csv(path)
    if "aquifer_id" not in df.columns:
        cfg = CFG["bcgw"]["water_reserves"]
        df["aquifer_id"] = (df[cfg["desc_field"]].astype(str)
                            .str.extract(cfg["aquifer_regex"], flags=re.I)[0])
    return set(df["aquifer_id"].dropna().astype(int).astype(str))


def tpo_applications(applications: pd.DataFrame) -> set[str]:
    """
    Applications in a watershed subject to a temporary protection order.

    Spatial intersection is preferred over matching the client's `Watershed`
    text field: watershed names are ambiguous (there is a "Salmon" in both the
    Interior and on the coast) and the client field is blank on 72 rows.
    Falls back to name matching only if the TPO layer is unavailable.
    """
    cache = ROOT / P["bcgw_cache"]
    scope = CRIT["tpo_scope"]

    try:
        tpo = gpd.read_file(cache, layer="tpo").to_crs(CRS)
    except Exception:
        print("  WARNING: TPO layer unavailable - falling back to name match")
        return _tpo_by_name(applications, scope)

    if scope == "current":
        tpo = tpo[tpo["ORDER_STATUS"].astype(str).str.lower() == "current"]

    try:
        wells = gpd.read_file(cache, layer="well_points").to_crs(CRS)
    except Exception:
        print("  WARNING: well_points layer missing - TPO by name")
        return _tpo_by_name(applications, scope)

    hit = gpd.sjoin(wells, tpo[["WATERSHED_NAME", "geometry"]],
                    how="inner", predicate="within")
    return set(hit["tracking_number"].astype(str))


def _tpo_by_name(applications: pd.DataFrame, scope: str) -> set[str]:
    """Fallback: substring match on the client's Watershed field."""
    names = ["Koksilah", "Tsolum"] if scope == "ever" else []
    if not names:
        return set()
    w = applications["watershed"].astype(str)
    mask = pd.Series(False, index=applications.index)
    for n in names:
        mask |= w.str.contains(n, case=False, na=False)
    return set(applications.loc[mask, "tracking_number"].astype(str))


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main():
    interim = ROOT / P["interim_dir"]
    apps = pd.read_csv(interim / "applications.csv", dtype={"tracking_number": str})
    pairs = pd.read_csv(interim / "application_aquifers.csv",
                        dtype={"tracking_number": str})
    pairs["aquifer_id"] = pairs["aquifer_id"].astype(str).str.replace(r"\.0$", "",
                                                                     regex=True)

    print("Loading criterion source lists ...")
    sensitive = load_sensitive_aquifers()
    fully_rec = load_fully_recorded_aquifers()
    reserves = load_reserve_aquifers()
    tpo_apps = tpo_applications(apps)

    print(f"  sensitive-connected aquifers   {len(sensitive)}")
    print(f"  fully recorded aquifers        {len(fully_rec)}")
    print(f"  water reserve aquifers         {len(reserves)}")
    print(f"  applications in TPO watersheds {len(tpo_apps)}")

    # --- assessable criteria (aquifer level) ------------------------------- #
    pairs["fail_sensitive_stream"] = pairs["aquifer_id"].isin(sensitive)
    pairs["fail_fully_recorded"] = pairs["aquifer_id"].isin(fully_rec)
    pairs["fail_water_reserve"] = pairs["aquifer_id"].isin(reserves)

    # --- assessable criteria (application level) --------------------------- #
    pairs["fail_tpo_watershed"] = pairs["tracking_number"].isin(tpo_apps)

    # --- not assessable ---------------------------------------------------- #
    for c in CFG["criteria"]["not_assessable"]:
        pairs[f"na_{c}"] = "not_assessed"

    pairs.to_csv(interim / "application_criteria.csv", index=False)

    # ----------------------------- summary -------------------------------- #
    fail_cols = [c for c in pairs.columns if c.startswith("fail_")]
    print("\n" + "=" * 68)
    print("STEP 03  APPLY CRITERIA")
    print("=" * 68)
    print(f"Application-aquifer pairs      {len(pairs)}")
    print(f"Distinct applications          {pairs['tracking_number'].nunique()}")

    print("\nApplications failing each criterion (any aquifer, any method)")
    for c in fail_cols:
        n = pairs.loc[pairs[c], "tracking_number"].nunique()
        print(f"  {c.replace('fail_', ''):<28s} {n:>5d}")

    print("\nNot assessed (no data source)")
    for c in CFG["criteria"]["not_assessable"]:
        print(f"  {c}")
    print("=" * 68)


if __name__ == "__main__":
    main()
