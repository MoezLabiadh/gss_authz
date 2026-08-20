#!/usr/bin/env python3
"""Independent QA of a delivered workbook.

Recomputes the reported figures from the Components sheet + the config CSVs -
NOT by reading the pipeline's own roll-ups back - then cross-checks every
sheet against every other. Any FAIL is a real inconsistency between sheets or
against a from-scratch recompute.

Usage:  uv run python qa_check.py out/okanagan_tpo_screening.xlsx
Exit code is the number of failed checks (0 = all pass).

Tolerance is TOL = 5e-5: sheets are written .round(6), and summing several
rounded rows (e.g. Purpose_Breakdown -> Summary) accumulates to ~1e-6. A real
error shows up far larger than this.
"""
import sys
from pathlib import Path

import pandas as pd

SY = 31_536_000
SEASON = [4, 5, 6, 7, 8, 9]
TOL = 5e-5
BUCKET = {"sw": "LICENSED_SW_CURTAILMENT_POTENTIAL_M3S",
          "gw_connected": "LICENSED_GW_CONNECTED_M3S",
          "gw_unknown": "LICENSED_GW_CONNECTIVITY_UNKNOWN_M3S"}

workbook = sys.argv[1] if len(sys.argv) > 1 else \
    "out/okanagan_tpo_screening.xlsx"
here = Path(__file__).parent

xl = pd.ExcelFile(workbook)
S = xl.parse("Summary"); M = xl.parse("Master"); C = xl.parse("Components")
P = xl.parse("Purpose_Breakdown"); L = xl.parse("Monthly_Long")
A = xl.parse("POD_Audit"); G = xl.parse("GW_Connectivity")
ST = xl.parse("Storage"); Q = xl.parse("QA_Ledger")
irr = pd.read_csv(here / "config/irrigation_monthly.csv")
conv = pd.read_csv(here / "config/unit_conversions.csv")

results = []
def check(name, ok, detail=""):
    results.append(ok)
    print(("PASS " if ok else "FAIL ") + name + (f"  {detail}" if detail else ""))

# 1 -------------------------------------------------- QA ladder arithmetic
q = dict(zip(Q.stage, Q.rows))
check("ladder: allocation splits sum to components",
      q["allocation_assigned"] + q["allocation_outside_units"]
      + q["allocation_multi_watershed"] == q["authorization_components"])

# 2 --------------------------- independent recompute of the season figures
used = C[(C.ALLOCATION_STATUS == "assigned") & C.quantifiable
         & (C.TREATMENT != "storage") & C.ANNUAL_M3.notna()]
rows = []
for _, r in used.iterrows():
    for _, mo in irr.iterrows():
        rate = (r.ANNUAL_M3 * mo.pct / (mo.days * 86400)
                if r.TREATMENT == "irrigation" else r.ANNUAL_M3 / SY)
        rows.append((r.UNIT_ID, r.BUCKET, int(mo.month), rate))
mine = pd.DataFrame(rows, columns=["UNIT_ID", "BUCKET", "month", "RATE"])
season = (mine[mine.month.isin(SEASON)]
          .groupby(["UNIT_ID", "BUCKET", "month"])["RATE"].sum()
          .unstack("month").reindex(columns=SEASON, fill_value=0.0).fillna(0.0)
          .mean(axis=1).unstack("BUCKET").fillna(0.0))
worst = 0.0
for b, col in BUCKET.items():
    got = S.set_index("unit_id")[f"{col}_APR_SEP_MEAN"]
    exp = season[b].reindex(got.index).fillna(0.0) if b in season else got * 0
    worst = max(worst, (got - exp).abs().max())
check("Summary Apr-Sep means recomputed from Components", worst < TOL,
      f"max diff {worst:.1e}")

# 3 ------------------------------------------ Summary headline = SW + GW-c
h = "SW_PLUS_GW_CONNECTED_APR_SEP_M3S"
d = (S[h] - S[f"{BUCKET['sw']}_APR_SEP_MEAN"]
     - S[f"{BUCKET['gw_connected']}_APR_SEP_MEAN"]).abs().max()
check("Summary headline = SW + GW-connected", d < TOL, f"max diff {d:.1e}")

# 4 --------------------------- Master monthly mean == Summary season mean
for b, col in BUCKET.items():
    mcols = [f"{col}_{m}" for m in ["APR", "MAY", "JUN", "JUL", "AUG", "SEP"]]
    exp = M.set_index("unit_id")[mcols].mean(axis=1)
    got = S.set_index("unit_id")[f"{col}_APR_SEP_MEAN"]
    d = (got - exp.reindex(got.index)).abs().max()
    check(f"Master Apr-Sep monthly mean == Summary [{b}]", d < TOL,
          f"max diff {d:.1e}")

# 5 -------------------------------- Purpose_Breakdown sums to Summary
for b, col in BUCKET.items():
    exp = P.groupby("UNIT_ID")[f"{col}_APR_SEP_MEAN"].sum()
    got = S.set_index("unit_id")[f"{col}_APR_SEP_MEAN"]
    d = (got - exp.reindex(got.index).fillna(0.0)).abs().max()
    check(f"Purpose_Breakdown sums to Summary [{b}]", d < TOL,
          f"max diff {d:.1e}")

# 6 ----------------------------------------- Monthly_Long pivots to Master
piv = L.groupby(["UNIT_ID", "BUCKET", "MONTH"])["RATE_M3S"].sum()
d = max((abs(v - M.set_index("unit_id").loc[u, f"{BUCKET[b]}_{mo}"])
         for (u, b, mo), v in piv.items() if f"{BUCKET[b]}_{mo}" in M.columns),
        default=0.0)
check("Monthly_Long pivots to Master", d < TOL, f"max diff {d:.1e}")

# 7 ------------------------------------------- unit conversion, all rows
cm = conv.set_index("quantity_units")
bad = sum(abs(r.QUANTITY * cm.loc[r.QUANTITY_UNITS, "factor_to_m3yr"]
              - r.ANNUAL_M3) > 1e-6
          for _, r in C[C.quantifiable & C.ANNUAL_M3.notna()].iterrows())
check("QUANTITY x factor == ANNUAL_M3 (all components)", bad == 0,
      f"{bad} mismatches")

# 8 ------------------------------------ storage excluded from reported rates
check("Storage sheet is storage-only and assigned",
      (ST.TREATMENT == "storage").all()
      and (ST.ALLOCATION_STATUS == "assigned").all())

# 9 --------------------------------------- POD_Audit ties to Summary/ledger
pods = A.groupby("UNIT_ID").ROW_ID.nunique()
got = S.set_index("unit_id")["N_PODS_USED"]
check("Summary N_PODS_USED == POD_Audit rows per unit",
      (got - pods.reindex(got.index).fillna(0)).abs().max() == 0)
check("POD_Audit row count == ledger pod_audit_rows",
      len(A) == q["pod_audit_rows"], f"{len(A)} vs {q['pod_audit_rows']}")
check("POD_Audit components all present in Components",
      set(A.COMP_ID) <= set(C.COMP_ID))
check("POD_Audit has no storage rows", not (A.TREATMENT == "storage").any())

# 10 --------------------------- GW_Connectivity ties to Summary GW buckets
for b in ("gw_connected", "gw_unknown"):
    exp = G[G.BUCKET == b].groupby("UNIT_ID")["APR_SEP_MEAN_M3S"].sum()
    got = S.set_index("unit_id")[f"{BUCKET[b]}_APR_SEP_MEAN"]
    d = (got - exp.reindex(got.index).fillna(0.0)).abs().max()
    check(f"GW_Connectivity sums to Summary [{b}]", d < TOL, f"max diff {d:.1e}")

n_fail = sum(1 for ok in results if not ok)
print("=" * 60)
print(f"{len(results) - n_fail}/{len(results)} checks passed"
      + ("" if not n_fail else f"  --  {n_fail} FAILED"))
sys.exit(n_fail)
