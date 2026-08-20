"""Okanagan TPO screening pipeline - pure-pandas core.

Implements the locked processing spec (handover v2 section 6):
  0. string normalization (trailing-space unit twins)
  1. LICENCE_STATUS = 'Current'
  2. empty-shell removal (null POD_NUMBER + null QUANTITY)
  3. rights-holder dedup on the six-field key
  4. quantity-flag correction into authorization COMPONENTS
       T take | M once per licence-purpose | D/P/NULL per-POD sum
  5. unit normalization -> annual m3 with CONVERSION_METHOD lineage
  6. storage held aside at the PURPOSE level
  7. GW buckets: connected = 'Likely'; everything else GW = unknown
  8. allocation: component -> unit via its PODs (M-flag may straddle units)
  9. monthly m3/s: irrigation = client split; everything else flat

All interpretive knobs live in config CSVs, not in code.
"""
from __future__ import annotations

import calendar
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

SECONDS_YEAR = 31_536_000  # 365 d x 86,400 s (documented convention)
MONTH_ABBR = [calendar.month_abbr[m].upper() for m in range(1, 13)]

# Season used for the Summary sheet's headline average. Apr-Sep matches the
# irrigation season in the client's own monthly split, so the summary window
# and the demand curve it summarises agree. Column labels derive from
# SEASON_LABEL, so changing the window is a two-line edit here.
SEASON_MONTHS = [4, 5, 6, 7, 8, 9]
SEASON_LABEL = "APR_SEP"

# Licences named for eLicensing spot checks (handover v2 s8). Their
# exceptions stay in the workbook wherever they sit, in or out of area.
# 504805 / 503692 added 2026-07-21: the two extreme m3/sec "04A Land Improve"
# licences that drive Mill Creek (20 and 8.41 m3/sec) - likely source
# unit-coding errors, priority spot checks.
SPOT_CHECK_LICENCES = {"C132258", "506510", "509065", "504805", "503692"}

SIX_KEY = ["LICENCE_NUMBER", "POD_NUMBER", "PURPOSE_USE_CODE",
           "QUANTITY", "QUANTITY_UNITS", "QUANTITY_FLAG"]
THREE_KEY = ["LICENCE_NUMBER", "POD_NUMBER", "PURPOSE_USE_CODE"]

BUCKET_COLUMNS = {
    "sw": "LICENSED_SW_CURTAILMENT_POTENTIAL_M3S",
    "gw_connected": "LICENSED_GW_CONNECTED_M3S",
    "gw_unknown": "LICENSED_GW_CONNECTIVITY_UNKNOWN_M3S",
}


# --------------------------------------------------------------------------- #
# config
# --------------------------------------------------------------------------- #
class Config:
    """Loads and validates the editable CSVs that drive the analysis."""

    def __init__(self, config_dir: str | Path):
        d = Path(config_dir)
        self.stream_units = pd.read_csv(d / "stream_units.csv")
        self.bridge = pd.read_csv(d / "unit_tile_bridge.csv")
        self.purpose_treatment = pd.read_csv(d / "purpose_treatment.csv")
        self.unit_conversions = pd.read_csv(d / "unit_conversions.csv")
        self.irrigation = pd.read_csv(d / "irrigation_monthly.csv")
        self._validate()

    def _validate(self) -> None:
        pct = float(self.irrigation["pct"].sum())
        if abs(pct - 0.999) > 1e-9:
            raise ValueError(
                f"irrigation percentages sum to {pct}; client-specified values sum to 0.999")
        per_tile = self.bridge.groupby("watershed_feature_id")["unit_id"].nunique()
        if (per_tile > 1).any():
            bad = per_tile[per_tile > 1].index.tolist()
            raise ValueError(f"tiles assigned to more than one unit: {bad}")
        if self.stream_units["unit_id"].duplicated().any():
            raise ValueError("duplicate unit_id in stream_units.csv")

    def treatment_map(self) -> dict:
        return dict(zip(self.purpose_treatment["purpose_use_code"],
                        self.purpose_treatment["treatment"]))


# --------------------------------------------------------------------------- #
# QA ledger + exceptions
# --------------------------------------------------------------------------- #
@dataclass
class Ledger:
    rows: list = field(default_factory=list)

    def record(self, stage: str, n_rows: int, note: str = "",
               annual_m3: float | None = None) -> None:
        self.rows.append({"stage": stage, "rows": n_rows,
                          "annual_m3": annual_m3, "note": note})

    def frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows)


class Exceptions:
    def __init__(self) -> None:
        self._rows: list[dict] = []

    def add(self, kind: str, licence, purpose, detail: str) -> None:
        self._rows.append({"kind": kind, "licence_number": licence,
                           "purpose_use_code": purpose, "detail": detail})

    def frame(self) -> pd.DataFrame:
        cols = ["kind", "licence_number", "purpose_use_code", "detail"]
        return pd.DataFrame(self._rows, columns=cols)


# --------------------------------------------------------------------------- #
# cleaning stages
# --------------------------------------------------------------------------- #
def normalize_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace on every string column; blank -> NA.

    Profiling showed every QUANTITY_UNITS value has a trailing-space twin.
    """
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_string_dtype(out[col]) or out[col].dtype == object:
            s = out[col].astype("string").str.strip()
            out[col] = s.mask(s == "", pd.NA)
    return out


def filter_current(df: pd.DataFrame, ledger: Ledger) -> pd.DataFrame:
    kept = df[df["LICENCE_STATUS"] == "Current"].copy()
    ledger.record("status_filter_current", len(kept),
                  f"removed {len(df) - len(kept)} non-Current rows")
    return kept


def split_shells(df: pd.DataFrame, ledger: Ledger) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Stub records: no POD number AND no quantity (9,544 province-wide)."""
    is_shell = df["POD_NUMBER"].isna() & df["QUANTITY"].isna()
    shells = df[is_shell].copy()
    kept = df[~is_shell].copy()
    ledger.record("shell_records_removed", len(kept),
                  f"{len(shells)} null-POD/null-quantity stubs held aside")
    return kept, shells


def dedup_holders(df: pd.DataFrame, ledger: Ledger,
                  exceptions: Exceptions) -> pd.DataFrame:
    """Collapse rights-holder replication on the six-field key.

    The 3-field key (licence/POD/purpose) is NOT safe: profiling found 17
    groups carrying multiple distinct quantity/unit/flag combos.  Those
    combos survive here as distinct records and are exception-listed.
    """
    out = (df.sort_values(SIX_KEY, na_position="last")
             .groupby(SIX_KEY, dropna=False, as_index=False, sort=False)
             .agg(**{c: (c, "first") for c in df.columns if c not in SIX_KEY},
                  N_HOLDER_ROWS=("POD_SUBTYPE", "size")))
    # restore original column order + the new count
    out = out[[*df.columns, "N_HOLDER_ROWS"]]

    combo_counts = out.groupby(THREE_KEY, dropna=False).size()
    for key, n in combo_counts[combo_counts > 1].items():
        exceptions.add("multiple_combos_same_licence_pod_purpose",
                       key[0], key[2],
                       f"POD {key[1]}: {n} distinct qty/unit/flag combos retained as "
                       f"separate authorization components")
    ledger.record("dedup_six_field_key", len(out),
                  f"collapsed {len(df) - len(out)} holder-replicated rows")
    out["ROW_ID"] = range(len(out))
    return out


# --------------------------------------------------------------------------- #
# quantity-flag correction -> authorization components
# --------------------------------------------------------------------------- #
def build_components(df: pd.DataFrame, exceptions: Exceptions,
                     ledger: Ledger) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Turn cleaned POD rows into countable authorization components.

    Returns (components, comp_pod) where comp_pod maps COMP_ID -> ROW_ID for
    every POD a component could divert from (the spatial bridge).
    Validated rules (in-band QUANTITY_FLAG_DESCRIPTION + legacy help page):
      T  total, one POD          -> take as-is (stray multi-POD T -> treat as M)
      M  max repeated per POD    -> count ONCE per licence-purpose
      D/P per-POD known amounts  -> one component per POD row (they sum)
      NULL                        -> per-POD (volumetrically trivial: 78 rows)
    """
    work = df.copy()
    work["FLAG_N"] = work["QUANTITY_FLAG"].fillna("NONE")
    # PURPOSE_USE is the readable label for PURPOSE_USE_CODE; minimal extracts
    # and the synthetic fixtures carry only the code, so fall back to it
    if "PURPOSE_USE" not in work.columns:
        work["PURPOSE_USE"] = work["PURPOSE_USE_CODE"]
    gcols = ["LICENCE_NUMBER", "PURPOSE_USE_CODE", "FLAG_N"]

    stats = (work.groupby(gcols, dropna=False)
                 .agg(N_PODS=("POD_NUMBER", "nunique"),
                      N_QTY=("QUANTITY", "nunique"),
                      N_ROWS=("ROW_ID", "size")).reset_index())
    stats["COLLAPSE"] = (stats["FLAG_N"] == "M") | \
                        ((stats["FLAG_N"] == "T") & (stats["N_PODS"] > 1))
    work = work.merge(stats, on=gcols, how="left")

    comp_rows, bridge_rows = [], []

    def classify(sub: pd.DataFrame) -> tuple[str, str, str]:
        """(source_type, bucket, note) from a component's member POD rows."""
        subtypes = set(sub["POD_SUBTYPE"].dropna())
        note = ""
        if subtypes <= {"POD"}:
            return "SW", "sw", note
        if subtypes <= {"PWD", "PG"}:
            conn = set(sub["HYDRAULIC_CONNECTIVITY"].fillna("Unknown"))
            if conn == {"Likely"}:
                return "GW", "gw_connected", note
            if "Likely" in conn:
                note = "mixed connectivity across PODs -> conservatively Unknown"
            return "GW", "gw_unknown", note
        return "SW", "sw", "mixed SW/GW subtypes in one component"

    next_id = 0
    # collapsed components: one per licence-purpose-flag group
    for key, sub in work[work["COLLAPSE"]].groupby(gcols, dropna=False):
        licence, purpose, flag = key
        qty = sub["QUANTITY"].max()
        if sub["QUANTITY"].nunique() > 1:
            exceptions.add("m_flag_quantity_mismatch", licence, purpose,
                           f"{sub['QUANTITY'].nunique()} distinct quantities in an "
                           f"'{flag}' group; max ({qty}) used")
        if flag == "T":
            exceptions.add("t_flag_multiple_pods", licence, purpose,
                           f"T flag with {sub['POD_NUMBER'].nunique()} PODs; "
                           "counted once (treated as M)")
        units = sub.loc[sub["QUANTITY"].idxmax(), "QUANTITY_UNITS"]
        if sub["QUANTITY_UNITS"].nunique() > 1:
            exceptions.add("mixed_units_in_component", licence, purpose,
                           "differing QUANTITY_UNITS inside one collapsed group; "
                           "units of the max-quantity row used")
        src, bucket, note = classify(sub)
        if note:
            exceptions.add("component_classification", licence, purpose, note)
        comp_rows.append({"COMP_ID": next_id, "LICENCE_NUMBER": licence,
                          "PURPOSE_USE_CODE": purpose,
                          "PURPOSE_USE": sub["PURPOSE_USE"].iloc[0],
                          "QUANTITY_FLAG": flag,
                          "QUANTITY": qty, "QUANTITY_UNITS": units,
                          "N_PODS": int(sub["POD_NUMBER"].nunique()),
                          "SOURCE_TYPE": src, "BUCKET": bucket})
        bridge_rows.extend({"COMP_ID": next_id, "ROW_ID": rid}
                           for rid in sub["ROW_ID"])
        next_id += 1

    # per-POD components: one per remaining cleaned row (T single, D, P, NONE)
    for _, row in work[~work["COLLAPSE"]].iterrows():
        sub = row.to_frame().T
        src, bucket, note = classify(sub)
        if note:
            exceptions.add("component_classification", row["LICENCE_NUMBER"],
                           row["PURPOSE_USE_CODE"], note)
        comp_rows.append({"COMP_ID": next_id,
                          "LICENCE_NUMBER": row["LICENCE_NUMBER"],
                          "PURPOSE_USE_CODE": row["PURPOSE_USE_CODE"],
                          "PURPOSE_USE": row["PURPOSE_USE"],
                          "QUANTITY_FLAG": row["FLAG_N"],
                          "QUANTITY": row["QUANTITY"],
                          "QUANTITY_UNITS": row["QUANTITY_UNITS"],
                          "N_PODS": 1, "SOURCE_TYPE": src, "BUCKET": bucket})
        bridge_rows.append({"COMP_ID": next_id, "ROW_ID": row["ROW_ID"]})
        next_id += 1

    components = pd.DataFrame(comp_rows)
    comp_pod = pd.DataFrame(bridge_rows)
    ledger.record("authorization_components", len(components),
                  "after quantity-flag correction")
    return components, comp_pod


# --------------------------------------------------------------------------- #
# unit conversion + treatment
# --------------------------------------------------------------------------- #
def annualize(components: pd.DataFrame, cfg: Config,
              exceptions: Exceptions) -> pd.DataFrame:
    conv = cfg.unit_conversions.rename(columns={"quantity_units": "QUANTITY_UNITS"})
    out = components.merge(conv, on="QUANTITY_UNITS", how="left")
    unknown = out["conversion_method"].isna() & out["QUANTITY_UNITS"].notna()
    for _, r in out[unknown].iterrows():
        exceptions.add("unknown_unit", r["LICENCE_NUMBER"], r["PURPOSE_USE_CODE"],
                       f"unit '{r['QUANTITY_UNITS']}' not in unit_conversions.csv")
    out["quantifiable"] = out["quantifiable"].fillna(False).astype(bool)
    out["ANNUAL_M3"] = out["QUANTITY"] * out["factor_to_m3yr"]
    out.loc[~out["quantifiable"], "ANNUAL_M3"] = pd.NA
    out = out.rename(columns={"conversion_method": "CONVERSION_METHOD"})
    # instantaneous-rate flag (m3/sec): a licensed PEAK RATE, not a volume,
    # annualized here as continuous year-round flow. A few such licences can
    # dominate the headline (documented caveat), so the flag is carried through
    # to power a parallel 'excluding rate licences' Summary view. Config-driven
    # via the instantaneous_rate column in unit_conversions.csv.
    if "instantaneous_rate" in out.columns:
        out["INSTANTANEOUS_RATE"] = out["instantaneous_rate"].fillna(False).astype(bool)
    else:
        out["INSTANTANEOUS_RATE"] = False
    tmap = cfg.treatment_map()
    out["TREATMENT"] = out["PURPOSE_USE_CODE"].map(tmap).fillna("flat")
    drop = ["factor_to_m3yr"] + (["instantaneous_rate"]
                                 if "instantaneous_rate" in out.columns else [])
    return out.drop(columns=drop)


# --------------------------------------------------------------------------- #
# allocation (component -> unit through its PODs)
# --------------------------------------------------------------------------- #
def allocate(components: pd.DataFrame, comp_pod: pd.DataFrame,
             pod_units: pd.DataFrame, ledger: Ledger) -> pd.DataFrame:
    """pod_units: columns ROW_ID, unit_id (NA when the POD hits no unit).

    A component with PODs in one unit is assigned there; in several units it
    goes to the multi-watershed bucket (M-flag ceilings only, by construction,
    since D/P/T/NONE components have exactly one POD); in none -> outside.
    """
    m = comp_pod.merge(pod_units, on="ROW_ID", how="left")
    agg = (m.groupby("COMP_ID")["unit_id"]
             .agg(units=lambda s: sorted(set(s.dropna())))
             .reset_index())
    agg["n_units"] = agg["units"].str.len()
    agg["UNIT_ID"] = agg.apply(
        lambda r: r["units"][0] if r["n_units"] == 1 else pd.NA, axis=1)
    agg["ALLOCATION_STATUS"] = agg["n_units"].map(
        lambda n: "assigned" if n == 1 else
                  ("outside_units" if n == 0 else "multi_watershed"))
    agg["TOUCHED_UNITS"] = agg["units"].map("; ".join)
    out = components.merge(
        agg[["COMP_ID", "UNIT_ID", "ALLOCATION_STATUS", "TOUCHED_UNITS"]],
        on="COMP_ID", how="left")
    for status, n in out["ALLOCATION_STATUS"].value_counts().items():
        ledger.record(f"allocation_{status}", int(n))
    return out


# --------------------------------------------------------------------------- #
# monthly rates + aggregation
# --------------------------------------------------------------------------- #
def usable_components(components: pd.DataFrame) -> pd.DataFrame:
    """The components that actually contribute to the reported rates.

    Single source of truth for 'used in the calculation': assigned to exactly
    one unit, quantifiable, not storage, with a converted annual volume.
    POD_Audit is built from the same filter so the audit sheet can never
    drift from the numbers it is meant to explain.
    """
    return components[(components["ALLOCATION_STATUS"] == "assigned")
                      & components["quantifiable"]
                      & (components["TREATMENT"] != "storage")
                      & components["ANNUAL_M3"].notna()]


def monthly_rates(components: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """Long table of monthly m3/s per assigned, quantifiable, non-storage
    component.  Irrigation = client split; everything else flat."""
    usable = usable_components(components).copy()
    months = cfg.irrigation.copy()
    x = usable.merge(months, how="cross")
    flat = x["TREATMENT"] != "irrigation"
    x["RATE_M3S"] = 0.0
    x.loc[flat, "RATE_M3S"] = x.loc[flat, "ANNUAL_M3"] / SECONDS_YEAR
    irr = ~flat
    x.loc[irr, "RATE_M3S"] = (x.loc[irr, "ANNUAL_M3"] * x.loc[irr, "pct"]
                              / (x.loc[irr, "days"] * 86_400))
    return x[["COMP_ID", "UNIT_ID", "BUCKET", "PURPOSE_USE_CODE", "PURPOSE_USE",
              "TREATMENT", "INSTANTANEOUS_RATE", "month", "RATE_M3S"]]


def _mean_over_months(rates: pd.DataFrame, months: list[int],
                      keys: list[str]) -> pd.Series:
    """Mean m3/s across `months` for each combination of `keys`.

    Months with no rows count as zero (irrigation is zero Oct-Mar), so the
    window is reindexed before averaging - otherwise a dry month would be
    skipped rather than averaged in, overstating the mean.
    """
    r = rates[rates["month"].isin(months)]
    per_month = (r.groupby([*keys, "month"])["RATE_M3S"].sum()
                  .unstack("month")
                  .reindex(columns=months, fill_value=0.0)
                  .fillna(0.0))
    return per_month.mean(axis=1)


def _bucket_month_mean(rates: pd.DataFrame, months: list[int],
                       keys: list[str]) -> pd.DataFrame:
    """As _mean_over_months, with BUCKET pivoted out into columns.

    Columns are reindexed to BUCKET_COLUMNS order so surface water leads and
    every bucket is present even when nothing landed in it.
    """
    return (_mean_over_months(rates, months, [*keys, "BUCKET"])
            .unstack("BUCKET")
            .reindex(columns=list(BUCKET_COLUMNS))
            .fillna(0.0))


def aggregate(components: pd.DataFrame, rates: pd.DataFrame,
              cfg: Config) -> dict[str, pd.DataFrame]:
    """Build the output tables: long, master (wide), storage, multi-watershed,
    plus the client-facing summary / purpose / groundwater roll-ups."""
    long = (rates.groupby(["UNIT_ID", "month", "BUCKET", "PURPOSE_USE_CODE",
                           "PURPOSE_USE"], as_index=False)
                 .agg(RATE_M3S=("RATE_M3S", "sum"),
                      N_COMPONENTS=("COMP_ID", "nunique")))

    # master wide: unit x (bucket x month)
    wide = (rates.groupby(["UNIT_ID", "BUCKET", "month"])["RATE_M3S"].sum()
                 .unstack(["BUCKET", "month"]))
    wide = wide.reindex(columns=pd.MultiIndex.from_product(
        [list(BUCKET_COLUMNS), range(1, 13)]), fill_value=0.0).fillna(0.0)
    wide.columns = [f"{BUCKET_COLUMNS[b]}_{MONTH_ABBR[m - 1]}"
                    for b, m in wide.columns]
    master = (cfg.stream_units[["unit_id", "unit_name", "client_label",
                                "named_watershed_area_ha"]]
              .merge(wide, left_on="unit_id", right_index=True, how="left")
              .fillna(0.0))

    # licence counts per unit (assigned components only)
    counts = (components[components["ALLOCATION_STATUS"] == "assigned"]
              .groupby("UNIT_ID")
              .agg(N_LICENCES=("LICENCE_NUMBER", "nunique"),
                   N_COMPONENTS=("COMP_ID", "nunique")).reset_index())
    master = master.merge(counts, left_on="unit_id", right_on="UNIT_ID",
                          how="left").drop(columns=["UNIT_ID"], errors="ignore")

    # storage identified per unit (annual m3; NOT in totals)
    stor = components[(components["TREATMENT"] == "storage")
                      & (components["ALLOCATION_STATUS"] == "assigned")]
    stor_by_unit = (stor.groupby("UNIT_ID")["ANNUAL_M3"].sum()
                        .rename("STORAGE_IDENTIFIED_ANNUAL_M3").reset_index())
    master = master.merge(stor_by_unit, left_on="unit_id", right_on="UNIT_ID",
                          how="left").drop(columns=["UNIT_ID"], errors="ignore")

    # multi-watershed companion: NON-ADDITIVE annual-average m3/s that COULD
    # apply to each touched unit
    mw = components[components["ALLOCATION_STATUS"] == "multi_watershed"].copy()
    comp_rows = []
    for _, r in mw.iterrows():
        if pd.isna(r["ANNUAL_M3"]):
            continue
        for u in str(r["TOUCHED_UNITS"]).split("; "):
            comp_rows.append({"unit_id": u,
                              "m3s": r["ANNUAL_M3"] / SECONDS_YEAR})
    mw_by_unit = (pd.DataFrame(comp_rows).groupby("unit_id")["m3s"].sum()
                    .rename("MULTI_WATERSHED_ASSOCIATED_M3S_NONADDITIVE")
                    .reset_index()
                  if comp_rows else
                  pd.DataFrame(columns=["unit_id",
                                        "MULTI_WATERSHED_ASSOCIATED_M3S_NONADDITIVE"]))
    master = master.merge(mw_by_unit, on="unit_id", how="left")
    for c in ("STORAGE_IDENTIFIED_ANNUAL_M3",
              "MULTI_WATERSHED_ASSOCIATED_M3S_NONADDITIVE"):
        master[c] = master[c].fillna(0.0)

    month_names = dict(zip(range(1, 13), MONTH_ABBR))
    long["MONTH"] = long["month"].map(month_names)

    # ---------------------------------------------------------------- summary
    # One row per unit: the Apr-Sep irrigation season alongside the annual
    # mean, so watersheds can be ranked without reading the 44-column Master.
    season = _bucket_month_mean(rates, SEASON_MONTHS, ["UNIT_ID"])
    annual = _bucket_month_mean(rates, list(range(1, 13)), ["UNIT_ID"])
    for frame, tag in ((season, SEASON_LABEL), (annual, "ANNUAL")):
        frame.columns = [f"{BUCKET_COLUMNS[b]}_{tag}_MEAN" for b in frame.columns]
    summary = (cfg.stream_units[["unit_id", "unit_name", "client_label",
                                 "named_watershed_area_ha"]]
               .merge(season, left_on="unit_id", right_index=True, how="left")
               .merge(annual, left_on="unit_id", right_index=True, how="left")
               .fillna(0.0))
    sw_col = f"{BUCKET_COLUMNS['sw']}_{SEASON_LABEL}_MEAN"
    gwc_col = f"{BUCKET_COLUMNS['gw_connected']}_{SEASON_LABEL}_MEAN"
    # SW + hydraulically-connected GW: the defensible headline. GW 'Unknown'
    # is the client's safety check, not a confirmed connection, so it stays
    # in its own column rather than being folded into a total.
    headline = f"SW_PLUS_GW_CONNECTED_{SEASON_LABEL}_M3S"
    summary[headline] = summary[sw_col] + summary[gwc_col]
    # same figure on Master, so the monthly sheet also carries the combined
    # season number without needing twelve more columns
    master = master.merge(summary[["unit_id", headline]], on="unit_id",
                          how="left")
    master[headline] = master[headline].fillna(0.0)
    summary["RANK_BY_SW_PLUS_GW_CONNECTED"] = (
        summary[headline].rank(ascending=False, method="min").astype(int))
    for col in ("N_LICENCES", "N_COMPONENTS", "STORAGE_IDENTIFIED_ANNUAL_M3"):
        summary = summary.merge(master[["unit_id", col]], on="unit_id",
                                how="left")
    for col in ("N_LICENCES", "N_COMPONENTS"):  # counts, not measurements
        summary[col] = summary[col].fillna(0).astype(int)

    # ------------------------------------ excluding instantaneous-rate view
    # m3/sec licences are licensed peak rates annualized as continuous flow; a
    # handful can swamp the headline (e.g. two 04A licences drive Mill Creek).
    # Report the headline with them set ASIDE, alongside the as-licensed
    # ceiling, so prioritization is not dictated by a few rate authorizations.
    # Nothing is dropped: the full figure stays, the difference is shown, and
    # every rate licence is itemized on the Exceptions sheet.
    rates_excl = rates[~rates["INSTANTANEOUS_RATE"].fillna(False)]
    season_excl = _bucket_month_mean(rates_excl, SEASON_MONTHS, ["UNIT_ID"])
    col_excl = f"{headline}_EXCL_RATE"
    headline_excl = (season_excl["sw"] + season_excl["gw_connected"]).rename(col_excl)
    hx = headline_excl.reset_index().rename(columns={"UNIT_ID": "unit_id"})
    summary = summary.merge(hx, on="unit_id", how="left")
    summary[col_excl] = summary[col_excl].fillna(0.0)
    summary["RATE_LICENCE_APR_SEP_M3S"] = summary[headline] - summary[col_excl]
    summary["RANK_EXCL_RATE_LICENCES"] = (
        summary[col_excl].rank(ascending=False, method="min").astype(int))

    # Default order is the excl-rate rank (the defensible prioritization); the
    # as-licensed rank stays as a column for the ceiling view.
    summary = summary.sort_values(["RANK_EXCL_RATE_LICENCES",
                                   "RANK_BY_SW_PLUS_GW_CONNECTED"])
    front = ["RANK_EXCL_RATE_LICENCES", "RANK_BY_SW_PLUS_GW_CONNECTED",
             "unit_id", "unit_name", "client_label", "named_watershed_area_ha",
             headline, col_excl, "RATE_LICENCE_APR_SEP_M3S"]
    front = [c for c in front if c in summary.columns]
    summary = summary[front + [c for c in summary.columns if c not in front]]

    # -------------------------------------------------------- purpose / GW
    # Deliverable 2.3: purpose breakdown, ungrouped (client-confirmed).
    pb_keys = ["UNIT_ID", "PURPOSE_USE_CODE", "PURPOSE_USE"]
    pb_season = _bucket_month_mean(rates, SEASON_MONTHS, pb_keys)
    pb_annual = _bucket_month_mean(rates, list(range(1, 13)), pb_keys)
    pb_season.columns = [f"{BUCKET_COLUMNS[b]}_{SEASON_LABEL}_MEAN"
                         for b in pb_season.columns]
    pb_annual.columns = [f"{BUCKET_COLUMNS[b]}_ANNUAL_MEAN"
                         for b in pb_annual.columns]
    purpose = (pb_season.join(pb_annual, how="outer").fillna(0.0)
                        .reset_index()
                        .sort_values(["UNIT_ID", sw_col],
                                     ascending=[True, False]))

    # Deliverable 2.4: groundwater supporting sheet, by connectivity bucket.
    gw_keys = ["UNIT_ID", "BUCKET", "PURPOSE_USE_CODE", "PURPOSE_USE"]
    gw_rates = rates[rates["BUCKET"].isin(["gw_connected", "gw_unknown"])]
    gw_season_col = f"{SEASON_LABEL}_MEAN_M3S"
    if len(gw_rates):
        gw_sheet = pd.DataFrame({
            gw_season_col: _mean_over_months(gw_rates, SEASON_MONTHS, gw_keys),
            "ANNUAL_MEAN_M3S": _mean_over_months(gw_rates, list(range(1, 13)),
                                                 gw_keys),
        }).reset_index().sort_values(["UNIT_ID", "BUCKET", gw_season_col],
                                     ascending=[True, True, False])
    else:
        gw_sheet = pd.DataFrame(columns=[*gw_keys, gw_season_col,
                                         "ANNUAL_MEAN_M3S"])

    # --------------------------------------------- watershed unit definitions
    # Surfaced so the client can verify what each unit actually is: the FWA
    # code prefix, any carve-out, and the tile-area vs named-watershed-area
    # cross-check that demonstrates the selection is right. Also puts the
    # provisional Vernon definition and the Bear Creek / Shatford notes in
    # front of them rather than leaving them in a config CSV.
    # "tile" is GIS shorthand for an assessment-watershed polygon; the
    # client-facing columns say assessment watershed instead
    tiles_by_unit = (cfg.bridge.groupby("unit_id")
                     .agg(N_ASSESSMENT_WATERSHEDS=("watershed_feature_id",
                                                   "nunique"),
                          SELECTED_AREA_HA=("area_ha", "sum")).reset_index())
    units = cfg.stream_units.merge(tiles_by_unit, on="unit_id", how="left")
    units["SELECTED_AREA_HA"] = units["SELECTED_AREA_HA"].round(1)
    units["AREA_DIFF_HA"] = (units["SELECTED_AREA_HA"]
                             - units["named_watershed_area_ha"]).round(1)
    # ASSESSMENT_WATERSHEDS_INCLUDED: the distinct named assessment watersheds
    # (tiles) in each unit, the unit's own stream first - transparency for the
    # client on what a drainage is built from. This is a tile-resolution list,
    # NOT an exhaustive creek inventory: a named tributary below assessment-
    # watershed resolution (e.g. Esperon Creek) is counted within the assessment
    # watershed that contains it (Terrace Creek, in the Lambly unit) and is not
    # listed here. A long creek spans several assessment watersheds of the same
    # name, so this list is shorter than the count.
    if "gnis_name" in cfg.bridge.columns:
        own = dict(zip(cfg.stream_units["unit_id"],
                       cfg.stream_units["fwa_gnis_name"]))
        inc = {}
        for uid, g in cfg.bridge.groupby("unit_id"):
            nm = list(pd.Series(g["gnis_name"]).dropna().unique())
            head = own.get(uid)
            ordered = ([head] if head in nm else []) + sorted(
                n for n in nm if n != head)
            inc[uid] = " + ".join(ordered)
        units["ASSESSMENT_WATERSHEDS_INCLUDED"] = units["unit_id"].map(inc)
    unit_cols = ["unit_id", "client_label", "unit_name", "fwa_gnis_name",
                 "aw_include_prefix", "aw_exclude_prefix",
                 "N_ASSESSMENT_WATERSHEDS", "ASSESSMENT_WATERSHEDS_INCLUDED",
                 "SELECTED_AREA_HA", "named_watershed_area_ha", "AREA_DIFF_HA",
                 "notes"]
    watershed_units = units[[c for c in unit_cols if c in units.columns]]

    return {"master": master,
            "summary": summary,
            "watershed_units": watershed_units,
            "purpose_breakdown": purpose,
            "gw_connectivity": gw_sheet,
            # MONTH_NUM (1-12) kept alongside the MONTH label so the sheet
            # sorts chronologically; renamed from the internal lowercase
            # "month" because Excel tables treat "month"/"MONTH" as a duplicate
            # column name (case-insensitive) and flag the table for repair.
            "monthly_long": long[["UNIT_ID", "month", "MONTH", "BUCKET",
                                  "PURPOSE_USE_CODE", "PURPOSE_USE", "RATE_M3S",
                                  "N_COMPONENTS"]].rename(
                                      columns={"month": "MONTH_NUM"}),
            "storage": stor,
            "multi_watershed": mw}


# --------------------------------------------------------------------------- #
# orchestration
# --------------------------------------------------------------------------- #
def run_core(raw: pd.DataFrame, pod_units: pd.DataFrame, cfg: Config):
    """Everything except extraction and the spatial join.

    raw:       licence rows as extracted (any status, holders un-deduped)
    pod_units: ROW_ID -> unit_id mapping produced by the spatial join
               (tpo.io_sources.assign_pods_to_units) or a mock in tests.
               NOTE: pod_units is keyed on ROW_ID, which is assigned during
               dedup - callers doing a real spatial join should join
               geometry AFTER clean-up (see run_pipeline.py).
    """
    ledger, exceptions = Ledger(), Exceptions()
    ledger.record("raw_rows", len(raw))
    df = normalize_strings(raw)
    df = filter_current(df, ledger)
    df, shells = split_shells(df, ledger)
    df = dedup_holders(df, ledger, exceptions)
    components, comp_pod = build_components(df, exceptions, ledger)
    components = annualize(components, cfg, exceptions)
    components = allocate(components, comp_pod, pod_units, ledger)
    rates = monthly_rates(components, cfg)
    tables = aggregate(components, rates, cfg)

    # POD audit: exactly the PODs behind the reported numbers, nothing else.
    # The extract is province-wide by design (the QA ladder needs it), but
    # the audit sheet answers one question - which PODs produced this unit's
    # figure - so it is joined through the component bridge to the same
    # usable_components() set that monthly_rates uses. Everything dropped is
    # still counted in the stages above and in the ledger line below.
    used = usable_components(components)
    comp_cols = ["COMP_ID", "UNIT_ID", "BUCKET", "TREATMENT", "ANNUAL_M3",
                 "N_PODS", "CONVERSION_METHOD"]
    audit = (df.merge(comp_pod, on="ROW_ID", how="inner")
               .merge(used[comp_cols], on="COMP_ID", how="inner")
               .merge(pod_units, on="ROW_ID", how="left"))
    # An M-flag authorization can list PODs outside the unit it was assigned
    # to (the quantity is counted once, wherever it may be diverted). Those
    # PODs contribute nothing here and would read as a spatial error, so the
    # audit keeps only PODs whose own location is inside their unit.
    off_unit = audit["unit_id"].isna() | (audit["unit_id"] != audit["UNIT_ID"])
    ledger.record("pod_audit_rows_off_unit_excluded", int(off_unit.sum()),
                  "PODs of assigned components that sit outside that unit - "
                  "the quantity is counted once and stays with the unit")
    audit = audit[~off_unit].drop(columns=["unit_id"])
    # An M-flag component is counted ONCE even though it lists several PODs,
    # so ANNUAL_M3 repeats down those rows and must not be summed here.
    audit["COMPONENT_SHARED_ACROSS_PODS"] = audit["N_PODS"] > 1
    lead = [c for c in ("UNIT_ID", "ASSESSMENT_WATERSHED", "COMP_ID",
                        "LICENCE_NUMBER", "POD_NUMBER") if c in audit.columns]
    audit = audit[lead + [c for c in audit.columns if c not in lead]]
    audit = audit.sort_values(lead)
    ledger.record("pod_audit_rows", len(audit),
                  "PODs behind the reported figures - the only rows in "
                  "POD_Audit (assigned, quantifiable, non-storage)")

    # how many PODs stand behind each unit's figure (ties Summary to POD_Audit)
    per_unit = audit.groupby("UNIT_ID")["ROW_ID"].nunique().rename("N_PODS_USED")
    tables["summary"] = tables["summary"].merge(
        per_unit, left_on="unit_id", right_index=True, how="left")
    tables["summary"]["N_PODS_USED"] = (tables["summary"]["N_PODS_USED"]
                                        .fillna(0).astype(int))
    # N_COMPONENTS_USED: countable authorizations actually behind the m3/s
    # (assigned, quantifiable, non-storage). Distinct from N_COMPONENTS, which
    # also counts storage and unquantifiable components assigned to the unit -
    # so a reader is not left guessing which count feeds the reported flow.
    comp_used = used.groupby("UNIT_ID")["COMP_ID"].nunique().rename(
        "N_COMPONENTS_USED")
    tables["summary"] = tables["summary"].merge(
        comp_used, left_on="unit_id", right_index=True, how="left")
    tables["summary"]["N_COMPONENTS_USED"] = (
        tables["summary"]["N_COMPONENTS_USED"].fillna(0).astype(int))

    # Near-boundary PODs are not a separate sheet - they ride along as the
    # NEAR_BOUNDARY / BOUNDARY_DIST_M columns on POD_Audit (filter there) and
    # are counted in the QA ledger. Kept lean per the back-of-envelope brief.

    # Components sheet: the authorizations in the study area. The extract is
    # province-wide so the ladder reconciles, but a licence in the Kootenays
    # belongs in the ledger count, not in a sheet the client reads. Storage
    # and unquantifiable components stay - they are inside the units and are
    # referenced by the Storage sheet and N_COMPONENTS.
    in_scope = components["ALLOCATION_STATUS"].isin(["assigned",
                                                     "multi_watershed"])
    ledger.record("components_outside_study_area", int((~in_scope).sum()),
                  "components with no POD in any unit - counted here, "
                  "omitted from the Components sheet")
    scoped = components[in_scope]

    # Licensee contact (name + mailing address) folded onto Components, next to
    # the usage, so water officers see who holds each authorization without a
    # separate sheet. Public water-rights register data; internal workbook. One
    # holder per licence (the deduped 'first'); address lines collapse to one.
    addr_parts = ["ADDRESS_LINE_1", "ADDRESS_LINE_2", "ADDRESS_LINE_3",
                  "ADDRESS_LINE_4", "POSTAL_CODE", "COUNTRY"]
    have_addr = [c for c in addr_parts if c in df.columns]
    if "PRIMARY_LICENSEE_NAME" in df.columns or have_addr:
        first = df.sort_values("ROW_ID").groupby("LICENCE_NUMBER").first()
        contact = pd.DataFrame(index=first.index)
        if "PRIMARY_LICENSEE_NAME" in df.columns:
            contact["PRIMARY_LICENSEE_NAME"] = first["PRIMARY_LICENSEE_NAME"]
        if have_addr:
            def _join_address(row) -> str:
                parts, acc = [], ""
                for x in row:
                    if pd.isna(x):
                        continue
                    s = str(x).strip()
                    # skip blanks and pieces already present (POSTAL_CODE is
                    # usually repeated inside ADDRESS_LINE_4)
                    if not s or s in acc:
                        continue
                    parts.append(s)
                    acc += " " + s
                return ", ".join(parts)
            contact["LICENSEE_ADDRESS"] = first[have_addr].apply(_join_address,
                                                                 axis=1)
        scoped = scoped.merge(contact.reset_index(), on="LICENCE_NUMBER",
                              how="left")

    # Instantaneous-rate (m3/sec) licences: itemize every in-scope one so water
    # staff can see exactly what the Summary 'EXCL_RATE' view sets aside. These
    # are licensed peak rates annualized as continuous flow, not consumptive
    # volumes; a few dominate the as-licensed headline.
    rate_comps = components[components["INSTANTANEOUS_RATE"].fillna(False)
                            & components["ALLOCATION_STATUS"].isin(
                                ["assigned", "multi_watershed"])]
    for _, r in rate_comps.sort_values("ANNUAL_M3", ascending=False).iterrows():
        flat = (r["ANNUAL_M3"] / SECONDS_YEAR
                if pd.notna(r["ANNUAL_M3"]) else float("nan"))
        exceptions.add(
            "instantaneous_rate_licence", r["LICENCE_NUMBER"],
            r["PURPOSE_USE_CODE"],
            f"{r['QUANTITY']} m3/sec licensed peak rate annualized as continuous "
            f"flow = {r['ANNUAL_M3'] / 1e6:.1f} Mm3/yr ({flat:.3f} m3/s flat), "
            f"unit {r['UNIT_ID']} [{r['BUCKET']}]. Rate ceiling, not consumptive "
            f"volume; set aside in the Summary *_EXCL_RATE columns.")

    # Two-tap M-flag components: assigned wholly to one unit yet listing PODs
    # that fall outside ALL units. allocate() drops the null-unit PODs before
    # counting, so these count as one-unit and the full quantity is credited to
    # the in-unit POD - a conservative screening ceiling that may overstate,
    # since the amount could lawfully be diverted at the out-of-unit POD instead
    # (handover 6.8). Itemize each so the assumption is auditable on Exceptions,
    # not merely described in the README.
    # Scope to the flow-contributing set (usable_components: assigned,
    # quantifiable, non-storage) - the components actually behind the reported
    # m3/s, matching the handover 6.8 volume metric. A two-tap on a storage or
    # unquantifiable component does not move a reported flow figure.
    cp_u = comp_pod.merge(pod_units[["ROW_ID", "unit_id"]], on="ROW_ID",
                          how="left")
    flow_comps = used
    two_tap_ids = (set(cp_u.loc[cp_u["unit_id"].isna(), "COMP_ID"])
                   & set(flow_comps["COMP_ID"]))
    alook = flow_comps.set_index("COMP_ID")
    for cid in sorted(two_tap_ids):
        pods = cp_u[cp_u["COMP_ID"] == cid]
        au = alook.loc[cid, "UNIT_ID"]
        n_in = int((pods["unit_id"] == au).sum())
        n_off = int(pods["unit_id"].isna().sum())
        ann = alook.loc[cid, "ANNUAL_M3"]
        ann_txt = f"{ann / 1e6:.2f} Mm3/yr" if pd.notna(ann) else "unquantified"
        exceptions.add(
            "two_tap_partial_out_of_area", alook.loc[cid, "LICENCE_NUMBER"],
            alook.loc[cid, "PURPOSE_USE_CODE"],
            f"M-flag component COMP_ID {cid}: {n_in} POD(s) in {au}, {n_off} "
            f"POD(s) outside all units; full {ann_txt} credited conservatively "
            f"to {au} (could lawfully be diverted at an out-of-unit POD - may "
            f"overstate {au}).")

    # Exceptions: same principle. Keep anything touching an in-scope licence,
    # plus the licences named for eLicensing spot checks (handover v2 s8),
    # which must stay visible wherever they sit.
    exc = exceptions.frame()
    if len(exc) and "licence_number" in exc.columns:
        keep = set(scoped["LICENCE_NUMBER"].astype("string").dropna())
        keep |= SPOT_CHECK_LICENCES
        exc = exc[exc["licence_number"].astype("string").isin(keep)]

    tables.update({"cleaned_pods": audit, "shells": shells,
                   "components": scoped, "comp_pod": comp_pod,
                   "components_all": components,
                   "exceptions": exc, "ledger": ledger.frame()})
    return tables
