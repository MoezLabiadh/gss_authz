"""Assemble the deliverable .xlsx from the pipeline's output tables.

Values-only workbook (a generated analytical report, not a recalculating
model). Sheet set per handover v2 section 2, ordered so the client-facing
roll-ups come first and the audit/QA material sits behind them.
"""
from __future__ import annotations

import datetime as dt

import pandas as pd
from openpyxl.styles import Alignment, Font
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo

HEADER_FONT = Font(name="Arial", bold=True, size=10)
BODY_FONT = Font(name="Arial", size=10)
TITLE_FONT = Font(name="Arial", bold=True, size=11)

# Every data sheet is written as a native Excel table so users can sort/filter.
TABLE_STYLE = "TableStyleMedium9"  # blue, banded rows

# Licensee name + address ride on the Components sheet (next to the usage), so
# these raw fields are dropped from POD_Audit to avoid repeating personal data.
# Public water-rights register; this workbook is for internal water-officer use.
POD_AUDIT_DROP = ["PRIMARY_LICENSEE_NAME", "ADDRESS_LINE_1", "ADDRESS_LINE_2",
                  "ADDRESS_LINE_3", "ADDRESS_LINE_4", "COUNTRY", "POSTAL_CODE",
                  "LICENSEE_ADDRESS",
                  # Esri/Oracle internals with no analytical meaning
                  "SE_ANNO_CAD_DATA", "geometry", "OBJECTID"]

# ---------------------------------------------------------------------------
# README content
# ---------------------------------------------------------------------------
SHEET_GUIDE = [
    ("Summary",
     "START HERE. One row per watershed, ranked. Shows the Apr-Sep "
     "(irrigation season) and full-year average licensed quantities in "
     "m3/s, split into surface water, groundwater with likely hydraulic "
     "connection, and groundwater of unknown connection. Two rankings are "
     "given: RANK_EXCL_RATE_LICENCES (the default row order, recommended for "
     "prioritization) sets aside instantaneous-rate m3/sec licences; "
     "RANK_BY_SW_PLUS_GW_CONNECTED is the full as-licensed ceiling including "
     "them. See 'Instantaneous-rate (m3/sec) licences' in the caveats below "
     "for why the two can differ sharply."),
    ("Watershed_Units",
     "What each watershed unit actually is: the FWA watershed "
     "code prefix used to select it, anything excluded from it, how many "
     "assessment watersheds it is built from, and a check that those add up "
     "to the published area of the named watershed. Review this to confirm "
     "the units match what was intended."),
    ("Master",
     "The same watersheds with every month shown separately (Jan-Dec) for "
     "each of the three categories. Use this when you need a specific "
     "month; use Summary to compare watersheds."),
    ("Purpose_Breakdown",
     "Licensed quantity split by what the water is used for (irrigation, "
     "domestic, etc.), per watershed. This is the sheet for refining "
     "estimates - e.g. excluding a purpose you judge non-curtailable."),
    ("GW_Connectivity",
     "Groundwater only, split by connectivity bucket and purpose. Supports "
     "the safety check on the 'Unknown' category."),
    ("Monthly_Long",
     "Every watershed x month x category x purpose combination as one row "
     "each. The pivot-friendly source behind Summary and Master."),
    ("Storage",
     "Storage-purpose authorizations. Identified and reported here, and "
     "EXCLUDED from all totals elsewhere, per client instruction."),
    ("Components",
     "One row per countable authorization in the listed watersheds - the unit "
     "of accounting after the quantity-flag rules are applied. Shows how "
     "each licensed quantity was converted to m3/year, which watershed it "
     "landed in, and the licensee name and mailing address for that licence. "
     "Sum ANNUAL_M3 here, not on POD_Audit. Licences outside the listed "
     "watersheds are counted in QA_Ledger but not listed here."),
    ("POD_Audit",
     "THE PODs BEHIND THE NUMBERS. One row per point of diversion that "
     "actually contributes to a unit's reported figure - sorted by unit, "
     "with the licence, the purpose, the converted annual volume and the "
     "component it belongs to. Nothing else is in this sheet: PODs outside "
     "the units, storage purposes, unquantifiable licences and "
     "multi-watershed components are all excluded, because none of them "
     "feed the reported totals. Filter on UNIT_ID to see exactly which "
     "PODs produced that watershed's number; the NEAR_BOUNDARY / "
     "BOUNDARY_DIST_M columns flag PODs close to a unit edge for review."),
    ("Shells_Excluded",
     "Stub records with no POD number and no quantity. Carried here so row "
     "counts reconcile; they contribute no volume."),
    ("Exceptions",
     "Records needing human review - conflicting quantities, mixed units, "
     "unquantifiable licences. Each row says what was assumed."),
    ("QA_Ledger",
     "The reconciliation ladder: how many rows entered, what each cleaning "
     "stage removed, and how the final allocation splits. Read top to "
     "bottom to confirm nothing was lost silently."),
]

GLOSSARY = [
    ("unit_id / UNIT_ID",
     "Short code for a watershed analysis unit (e.g. 'mission'). One per "
     "named stream on the client's list. Covers the whole drainage of that "
     "stream, tributaries included."),
    ("ASSESSMENT_WATERSHED",
     "The named watershed a point of diversion actually sits in - often a "
     "tributary (e.g. Winfield Creek) of the unit it counts towards (e.g. "
     "Vernon Creek). Shown so a point is not mistaken for being filed under "
     "the wrong stream."),
    ("client_label",
     "The watershed name as written on the client's stream list."),
    ("COMP_ID",
     "Authorization component ID - an internal row number for one countable "
     "licensed quantity. Because one licence can authorize several purposes, "
     "and one purpose can span several PODs, a licence is not the unit of "
     "accounting; the component is. Use it to join Components to "
     "Monthly_Long. It has no meaning outside this workbook."),
    ("ROW_ID",
     "Internal row number for a cleaned POD record; joins POD_Audit to the "
     "other sheets. No meaning outside this workbook."),
    ("POD",
     "Point of diversion - the physical location water is taken from. One "
     "licence may have many."),
    ("BUCKET",
     "Which category a quantity falls in: 'sw' surface water, "
     "'gw_connected' groundwater likely connected to surface water, "
     "'gw_unknown' groundwater of unknown connection."),
    ("LICENSED_SW_CURTAILMENT_POTENTIAL_M3S",
     "Surface water: licensed quantity potentially associated with "
     "curtailment, as a flow rate in cubic metres per second."),
    ("LICENSED_GW_CONNECTED_M3S",
     "As above for groundwater where HYDRAULIC_CONNECTIVITY = 'Likely'."),
    ("LICENSED_GW_CONNECTIVITY_UNKNOWN_M3S",
     "As above for groundwater where connectivity is 'Unknown'. Reported "
     "separately as a safety check - not included in the SW+GW headline."),
    ("SW_PLUS_GW_CONNECTED_APR_SEP_M3S",
     "Surface water plus likely-connected groundwater, averaged over April "
     "to September - the full as-licensed ceiling. Repeated on "
     "Master as the single season figure alongside the monthly columns. "
     "Groundwater of unknown connectivity is deliberately excluded. It is a "
     "seasonal average, so it does not correspond to any one month's "
     "column."),
    ("SW_PLUS_GW_CONNECTED_APR_SEP_M3S_EXCL_RATE",
     "The same figure with instantaneous-rate (m3/sec) licences set aside - "
     "the recommended prioritization basis (RANK_EXCL_RATE_LICENCES orders "
     "the sheet by it). These m3/sec licences are licensed peak rates "
     "annualized as continuous flow and can dominate the headline; here they "
     "are removed so the ranking reflects volume-based demand."),
    ("RATE_LICENCE_APR_SEP_M3S",
     "How much of the as-licensed headline comes from instantaneous-rate "
     "(m3/sec) licences = SW_PLUS_GW_CONNECTED_APR_SEP_M3S minus its "
     "_EXCL_RATE version. Large values (e.g. Mill Creek) flag a watershed "
     "whose figure is driven by a few rate authorizations, not consumptive "
     "use. Every such licence is itemized on the Exceptions sheet."),
    ("RANK_EXCL_RATE_LICENCES / RANK_BY_SW_PLUS_GW_CONNECTED",
     "Two rankings: the first sets aside m3/sec rate licences (default order, "
     "recommended); the second is the full as-licensed ceiling. A large gap "
     "between a watershed's two ranks (e.g. Mill Creek) means its headline is "
     "rate-licence driven."),
    ("APR_SEP_MEAN / ANNUAL_MEAN",
     "Average m3/s across Apr-Sep, or across all twelve months. Months with "
     "no authorized diversion count as zero in the average."),
    ("aw_include_prefix / aw_exclude_prefix",
     "The FWA watershed-code prefix used to select the assessment "
     "watersheds making up a unit, and any prefix carved back out of it "
     "(Vernon Creek excludes Coldstream Creek so the two do not "
     "double-count)."),
    ("N_ASSESSMENT_WATERSHEDS",
     "How many FWA assessment watersheds make up the unit. These are the "
     "standard building-block watersheds the province publishes; a unit is "
     "assembled from whole ones, never part of one."),
    ("ASSESSMENT_WATERSHEDS_INCLUDED",
     "The named FWA assessment watersheds that make up the unit, its own "
     "stream first. These are the province's standard building-block "
     "watersheds (see N_ASSESSMENT_WATERSHEDS) - this is NOT an exhaustive "
     "list of every creek in the drainage. A smaller named tributary below "
     "assessment-watershed resolution is counted within the assessment "
     "watershed that contains it and is not listed separately - e.g. Esperon "
     "Creek is part of Terrace Creek in the Lambly unit. A long creek spans "
     "several assessment watersheds that all carry its name, so this list is "
     "usually shorter than N_ASSESSMENT_WATERSHEDS, and a few small headwater "
     "watersheds are unnamed and not listed."),
    ("SELECTED_AREA_HA vs named_watershed_area_ha",
     "Total area of the assessment watersheds selected for the unit, "
     "against the published area of the FWA named watershed. AREA_DIFF_HA "
     "should be at or near zero - it is the check that the right ones were "
     "picked."),
    ("QUANTITY_FLAG",
     "The source layer's own code for how a quantity relates to its PODs. "
     "See 'Quantity flags' below."),
    ("ALLOCATION_STATUS",
     "'assigned' = fell inside one watershed unit; 'outside_units' = fell "
     "outside all of them; 'multi_watershed' = M-flag component whose PODs "
     "span more than one unit, held aside as non-additive."),
    ("MULTI_WATERSHED_ASSOCIATED_M3S_NONADDITIVE",
     "Quantity that could be taken in any one of several watersheds. Shown "
     "against each watershed it touches, so these figures MUST NOT be "
     "summed across rows."),
    ("TREATMENT",
     "How the purpose is handled monthly: 'irrigation' uses the client's "
     "seasonal split; 'storage' is excluded from totals; 'flat' spreads the "
     "annual volume evenly."),
    ("CONVERSION_METHOD",
     "How the licensed quantity was converted to m3/year."),
    ("quantifiable",
     "FALSE where the licence authorizes an unmeasurable amount (e.g. "
     "'Total Flow'). Such records are counted but carry no volume."),
    ("N_PODS / N_COMPONENTS / N_LICENCES",
     "Counts of diversion points, countable authorizations, and distinct "
     "licences respectively."),
    ("N_PODS_USED / N_COMPONENTS_USED",
     "On Summary: how many points of diversion, and how many countable "
     "authorizations, stand behind that watershed's reported m3/s (assigned, "
     "quantifiable, non-storage). N_PODS_USED matches the row count for that "
     "UNIT_ID on POD_Audit. Use these two for 'what feeds the number'. By "
     "contrast N_COMPONENTS (below) counts EVERY component assigned to the "
     "unit including storage and unquantifiable ones, so it is larger; those "
     "extra ones are on the Storage and Exceptions sheets."),
    ("COMPONENT_SHARED_ACROSS_PODS",
     "TRUE where one authorization is shared across several PODs (an "
     "'M' flag - the full amount could be taken at any one of them). The "
     "component is counted ONCE, but appears on each of its POD rows, so "
     "ANNUAL_M3 repeats. DO NOT sum ANNUAL_M3 down POD_Audit - use the "
     "Components sheet, where each authorization appears exactly once."),
]

METHOD_ROWS = [
    ("What these numbers are",
     "Screening-level licensed quantities potentially associated with "
     "curtailment. They are NOT estimates of actual diversion or guaranteed "
     "streamflow response. Licensed quantities are a ceiling and overstate "
     "actual use; groundwater response is lagged and attenuated relative to "
     "surface water; no return flows, conveyance losses, or operations are "
     "modelled."),
    ("Level of assessment",
     "Back-of-the-envelope, as requested. That applies to the hydrologic "
     "interpretation - not to the accounting: dedup, unit conversion, flag "
     "handling and spatial attribution are done exactly."),
    ("Instantaneous-rate (m3/sec) licences - READ BEFORE RANKING",
     "Most licences are quantified as a volume (m3/year or m3/day) - how much "
     "water is taken. A minority are quantified as an instantaneous flow RATE "
     "(m3/sec) - a cap on peak diversion, not an annual volume. To compare "
     "them, each m3/sec licence is annualized as if that peak rate ran "
     "continuously all year (20 m3/sec becomes 631 million m3/yr). That is a "
     "true licensed ceiling but far above any realistic use, and such works "
     "often run intermittently or return most water to the stream (e.g. "
     "power). Across the study area 39 licences are m3/sec and together they "
     "are the majority of the raw surface-water headline. They are NOT "
     "dropped: the full figure is kept (SW_PLUS_GW_CONNECTED_APR_SEP_M3S and "
     "RANK_BY_SW_PLUS_GW_CONNECTED), and a parallel figure with them set "
     "aside is given (…_EXCL_RATE and RANK_EXCL_RATE_LICENCES, the default "
     "order). Use the EXCL_RATE ranking to prioritize; use the difference "
     "(RATE_LICENCE_APR_SEP_M3S) to see which watersheds are rate-driven. "
     "Example: Mill Creek is first on the as-licensed ceiling (~30 m3/s, two "
     "'Land Improvement' licences of 20 and 8.4 m3/sec) but fourth once those "
     "are set aside. Every m3/sec licence is listed on Exceptions; the two "
     "extreme Mill Creek ones are flagged for eLicensing verification as "
     "possible source unit-coding errors."),
    ("Why April-September",
     "The Summary sheet leads with the Apr-Sep mean because that is the "
     "irrigation season in the client-supplied monthly split, so the "
     "summary window matches the demand curve it summarises. Full-year "
     "means and every individual month are also provided. Note this is an "
     "average across the season: it smooths out the July peak (23.6% of "
     "annual irrigation volume), so it understates the single worst month."),
    ("A unit is a whole drainage, not one creek",
     "Each unit covers the named stream AND everything that "
     "drains into it. Selecting 'Mission Creek' takes in Klo, "
     "Priest, Hydraulic, Joe Rich, Pearson and the rest of its tributaries "
     "(Belgo, also on the list, is carved out onto its own row - see below). "
     "So water taken from a tributary IS counted, under the name of the "
     "stream it drains into. The ASSESSMENT_WATERSHED column on POD_Audit "
     "shows the tributary a point of diversion actually sits in; UNIT_ID "
     "shows which unit it counts towards."),
    ("Which Okanagan watersheds are NOT included",
     "The 24 units cover 103 of the 152 assessment watersheds in the "
     "Okanagan (OKAN) group. Licences in the remaining "
     "Okanagan watersheds are excluded because those streams were not on the "
     "list provided - they include the Okanagan River mainstem, Okanagan "
     "Lake, and Deep, Peachland, Park Rill and Naswhito Creeks. A few list "
     "items were resolved with you (2026-07-22): Middle Vernon Creek is not "
     "included at your request - it is a local management label, not a "
     "Freshwater Atlas watershed, and the Vernon Creek unit already covers "
     "that area; Dennis Creek and Two-Forty (240) Creek both sit "
     "inside Penticton Creek with no separate assessment watershed, so their "
     "water is already counted within Penticton; Greata Creek (matched from "
     "'Greta') is confirmed and included; and Vance Creek has been removed at "
     "your request, as the only Freshwater Atlas 'Vance Creek' drains to the "
     "Shuswap, not the Okanagan. Adding or changing a "
     "stream is a configuration change, not a re-analysis."),
    ("Some listed streams flow into other listed streams",
     "Where the list includes both a stream and something that drains into "
     "it, the upstream drainage sits wholly INSIDE the downstream one, so "
     "reporting each at full extent would count the upstream licences twice. "
     "The rule applied: the downstream unit EXCLUDES any listed upstream "
     "stream, which is carved out onto its own row, so rows can be added "
     "together without double-counting. Four such carves are in this "
     "workbook: Coldstream, B.X. and Clark Creeks are carved out of Vernon "
     "Creek; Belgo Creek is carved out of Mission Creek. The one exception "
     "is Shingle/Shatford, listed as a single stream and so kept as one unit "
     "(see below)."),
    ("Vernon Creek EXCLUDES Coldstream, B.X. and Clark",
     "All four were listed as separate streams, so all four are reported "
     "separately. Vernon Creek's full drainage is about 75,246 ha; Coldstream "
     "(20,605 ha), B.X. (13,199 ha) and Clark (2,794 ha) are carved out, "
     "leaving Vernon reported as about 38,839 ha. No water is lost: every "
     "carved-out licence is counted on its own row, and the rows can be "
     "added back together to get the whole Vernon system. Vernon-minus-"
     "Coldstream is a working definition pending your confirmation. (Note: "
     "the client also listed 'Middle Vernon Creek'; the Freshwater Atlas does "
     "not define it as a named watershed, and the client confirmed keeping "
     "Vernon Creek only, so it is not a separate unit - this Vernon Creek "
     "unit covers that area.)"),
    ("Shingle Creek INCLUDES Shatford Creek",
     "These were listed as a single stream ('Shingle/Shatford'), so they "
     "are reported as one unit of about 29,860 ha, within which Shatford "
     "accounts for about 14,021 ha. Shatford's licences are counted inside "
     "the Shingle total rather than on a row of their own. This is the "
     "opposite treatment to Vernon/Coldstream, and it follows directly "
     "from how the two were listed. If you want Shatford reported "
     "separately it can be split out - again a configuration change, not a "
     "re-analysis."),
    ("Stream name note",
     "'Bear Creek' is a local alias; the FWA/GNIS name is Lambly Creek, and "
     "there is no separate Bear Creek watershed in the Okanagan group. "
     "Reported as Lambly Creek. See the Watershed_Units sheet for the "
     "definition, component watersheds and area check behind every unit."),
    ("Licences with several points of diversion",
     "Some licences authorize one quantity that may be taken at any of "
     "several points ('M' flag). The quantity is counted once. Where those "
     "points span more than one unit the amount is held aside "
     "as non-additive rather than assigned. Where some points fall outside "
     "every unit entirely, the amount is still credited to the unit "
     "that does contain a point - a deliberate screening-level assumption "
     "that may overstate that unit, since the water could lawfully be taken "
     "elsewhere. It affects a small number of licences, each listed "
     "individually on the Exceptions sheet (kind 'two_tap_partial_out_of_area', "
     "with the in-unit and out-of-unit POD counts and the volume assigned)."),
    ("Watershed units",
     "FWA assessment watersheds selected by FWA watershed-code prefix "
     "per named stream (client choice, 2026-07); their aggregate area matches "
     "the FWA named-watershed drainages to within about 1 ha (see the "
     "Watershed_Units sheet AREA_DIFF_HA - the one larger difference, B.X. "
     "Creek, is documented there and reflects a named-vs-assessment boundary "
     "divergence, not a selection error); units are non-overlapping. "
     "Nesting rule: a downstream unit excludes any listed upstream unit "
     "(Vernon Creek excludes Coldstream, B.X. and Clark; Mission Creek "
     "excludes Belgo - working definitions)."),
    ("Status filter", "LICENCE_STATUS = 'Current' at the extract date."),
    ("Dedup (the multi-holder correction)",
     "The source layer repeats a licence once per rights holder, so a "
     "1,000 m3/yr licence with three holders appears three times. Rows are "
     "collapsed on licence + POD + purpose + quantity + units + flag, which "
     "removes holder replication only; genuinely distinct quantity combos "
     "are retained and exception-listed."),
    ("Quantity flags",
     "Per the layer's own QUANTITY_FLAG_DESCRIPTION: T total (one POD); "
     "M maximum for the purpose repeated at each POD (counted once); "
     "D/P per-POD known quantities (summed); null flags treated per-POD. "
     "Empty stub records (null POD number and quantity) are excluded and "
     "counted in QA."),
    ("Units",
     "m3/year taken as licensed; m3/day x365 assumes year-round "
     "authorization; m3/sec annualized assumes continuous diversion - these "
     "instantaneous-rate licences span several purposes (land improvement, "
     "power, conservation) and can dominate the headline, so see the "
     "'Instantaneous-rate (m3/sec) licences' caveat and the EXCL_RATE Summary "
     "columns; 'Total Flow' licences authorize the entire source and cannot "
     "be quantified - counted and listed separately."),
    ("Irrigation months",
     "Client-specified split of annual licensed volume: Apr 10.4 / May 17.1 "
     "/ Jun 20.1 / Jul 23.6 / Aug 19.0 / Sep 9.7 % (sums to 99.9% as "
     "provided); Oct-Mar zero; all other purposes annually averaged. This is "
     "a temporal allocation of licensed volume, not estimated actual use."),
    ("Groundwater buckets",
     "Connected = HYDRAULIC_CONNECTIVITY 'Likely'. Unknown reported "
     "separately for the client's safety check. The field's domain contains "
     "no 'not likely' value, so the excluded bucket is empty by "
     "construction."),
    ("Storage",
     "Storage-purpose authorizations reported separately and excluded from "
     "totals per client instruction; exclusion is at the purpose level "
     "(many licences mix storage and non-storage purposes)."),
    ("Multi-watershed licences",
     "M-flag components whose PODs fall in more than one unit are held in "
     "an unallocated bucket; the per-unit companion column is NON-ADDITIVE "
     "(the same volume could apply to any touched unit)."),
    ("Purposes are ungrouped",
     "Purpose codes are reported exactly as the source layer records them, "
     "with no roll-up into categories - grouping is a policy judgement for "
     "water staff, not for the analysis."),
    ("Licensee contact details",
     "Licensee name and mailing address are on the Components sheet, beside "
     "each authorization, so water officers can see who holds it. Public "
     "water-rights register data; this workbook is for internal ministry use."),
]


def _write_df(writer, name: str, df: pd.DataFrame) -> None:
    df.to_excel(writer, sheet_name=name, index=False)
    ws = writer.book[name]
    for cell in ws[1]:
        cell.font = HEADER_FONT
    for row in ws.iter_rows(min_row=2):
        for cell in row:
            cell.font = BODY_FONT
    ws.freeze_panes = "A2"
    for i, col in enumerate(df.columns, start=1):
        content = 0
        if len(df):
            n = df[col].astype("string").str.len().max()
            content = int(n) if pd.notna(n) else 0
        width = max(len(str(col)) + 2, min(48, content + 2), 12)
        ws.column_dimensions[get_column_letter(i)].width = width
    # wrap the data in a native Excel table (sort/filter + banded rows)
    if df.shape[1] and len(df):
        ref = f"A1:{get_column_letter(df.shape[1])}{len(df) + 1}"
        tbl = Table(displayName=f"tbl_{name}", ref=ref)
        tbl.tableStyleInfo = TableStyleInfo(
            name=TABLE_STYLE, showRowStripes=True, showColumnStripes=False,
            showFirstColumn=False, showLastColumn=False)
        ws.add_table(tbl)


def _write_readme(writer, sections: list[tuple[str, list[tuple[str, str]]]],
                  header: list[tuple[str, str]]) -> None:
    """README as titled blocks rather than one flat table, so it reads."""
    rows: list[tuple[str, str]] = []
    for item, detail in header:
        rows.append((item, detail))
    for title, entries in sections:
        rows.append(("", ""))
        rows.append((title.upper(), ""))
        rows.extend(entries)
    df = pd.DataFrame(rows, columns=["item", "detail"])
    df.to_excel(writer, sheet_name="README", index=False)
    ws = writer.book["README"]
    for cell in ws[1]:
        cell.font = HEADER_FONT
    for row in ws.iter_rows(min_row=2):
        label, detail = row[0], row[1]
        label.font = TITLE_FONT if (label.value and not detail.value) else BODY_FONT
        detail.font = BODY_FONT
        detail.alignment = Alignment(wrap_text=True, vertical="top")
        label.alignment = Alignment(vertical="top")
    ws.column_dimensions["A"].width = 42
    ws.column_dimensions["B"].width = 110
    ws.freeze_panes = "A2"


def _clean_pod_audit(pods: pd.DataFrame) -> pd.DataFrame:
    """POD_Audit without the licensee contact fields (now on Components) or
    Esri/Oracle internals."""
    return pods.drop(columns=[c for c in POD_AUDIT_DROP if c in pods.columns])


def write_workbook(path: str, tables: dict, extract_note: str = "",
                   extract_date: str = "") -> str:
    pods = _clean_pod_audit(tables["cleaned_pods"])
    header = [
        ("Workbook", "Okanagan TPO screening - licensed curtailment potential"),
        ("Generated", dt.date.today().isoformat()),
        ("Data source", extract_note or "see run log"),
        ("Extract date", extract_date or "see run log"),
        ("Units", "cubic metres per second (m3/s) unless a column says "
                  "otherwise; storage is annual m3"),
    ]
    sections = [("How to read this workbook - sheet by sheet", SHEET_GUIDE),
                ("Column glossary", GLOSSARY),
                ("Method, assumptions and caveats", METHOD_ROWS)]

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        _write_readme(writer, sections, header)
        # client-facing roll-ups first
        _write_df(writer, "Summary", tables["summary"].round(6))
        _write_df(writer, "Watershed_Units", tables["watershed_units"])
        _write_df(writer, "Master", tables["master"].round(6))
        _write_df(writer, "Purpose_Breakdown",
                  tables["purpose_breakdown"].round(6))
        _write_df(writer, "GW_Connectivity",
                  tables["gw_connectivity"].round(6))
        _write_df(writer, "Monthly_Long", tables["monthly_long"].round(6))
        _write_df(writer, "Storage", tables["storage"])
        # supporting detail / audit
        _write_df(writer, "Components", tables["components"])
        _write_df(writer, "POD_Audit", pods)
        _write_df(writer, "Shells_Excluded", tables["shells"])
        _write_df(writer, "Exceptions", tables["exceptions"])
        _write_df(writer, "QA_Ledger", tables["ledger"])
    return path
