"""
Total System Withdrawals Raster
================================
Combines EUGW well points and licensed groundwater withdrawals,
assigns aquifer IDs, calculates withdrawal density (CMD/km2) per aquifer,
and exports to raster.

Outputs:
  - hmn_out_combined_wells: combined well points (EUGW + licences)
  - hmn_out_aquifers_withdrawal_density: all aquifers with density fields
  - hmn_out_aquifers_density_sand_gravel: Sand and Gravel aquifers only
  - hmn_out_aquifers_density_bedrock: Bedrock aquifers only
  - hmn_out_density_sand_gravel_30m: raster (Sand and Gravel)
  - hmn_out_density_bedrock_30m: raster (Bedrock)
  - aquifer_withdrawal_density.xlsx: attribute table for charting

Deduplication:
  - Parse EUGW Well_Tag_Number (semicolon-separated) into individual WTNs
  - Look up each WTN in the Water Rights licences
  - If matched, subtract the licensed CMD volume from the EUGW qty_cmd
  - If EUGW qty_cmd <= 0 after subtraction, drop (fully licensed)
  - If EUGW qty_cmd > 0, keep with reduced volume

Aquifer assignment priority:
  LICENCES:
    1. SOURCE_NAME column - if short (<=5 chars) and numeric, use as aquifer ID
    2. Fallback: spatial join to aquifer polygons

  EUGW:
    1. Look up Well_Tag_Number in GWELLS (hmn_gwells) to get AQUIFER_ID
       (if multiple WTNs, use first match)
    2. Fallback: parse AQUIFER_IDS attribute (comma-separated),
       resolve overlaps (Sand and Gravel over Bedrock)

Input GDB: W:\srm\gss\sandbox\mlabiadh\workspace\20260303_eugw_consultation_support\data_test.gdb
Layers:    EUGW_Well_Points, hmn_aquifers, hmn_groundwater_licences, hmn_gwells
"""

import arcpy
import os
import pandas as pd

# ============================================================
# CONFIGURATION
# ============================================================
GDB = r"\\spatialfiles.bcgov\work\srm\gss\projects\gr_2026_227_eugw_consultation_support\work\data.gdb"
GDB_FOLDER = os.path.dirname(GDB)

EUGW_FC = os.path.join(GDB, "hmn_eugw_centroids")
LICENCES_FC = os.path.join(GDB, "hmn_groundwater_licences")
AQUIFERS_FC = os.path.join(GDB, "hmn_aquifers")
GWELLS_FC = os.path.join(GDB, "hmn_gwells")

OUTPUT_COMBINED = os.path.join(GDB, "hmn_out_combined_wells")
OUTPUT_AQUIFERS = os.path.join(GDB, "hmn_out_aquifers_withdrawal_density")
OUTPUT_AQ_SAND = os.path.join(GDB, "hmn_out_aquifers_density_sand_gravel")
OUTPUT_AQ_ROCK = os.path.join(GDB, "hmn_out_aquifers_density_bedrock")
OUTPUT_RAS_SAND = os.path.join(GDB, "hmn_out_density_sand_gravel_30m")
OUTPUT_RAS_ROCK = os.path.join(GDB, "hmn_out_density_bedrock_30m")
OUTPUT_XLSX = os.path.join(GDB_FOLDER, "aquifer_withdrawal_density.xlsx")

CELL_SIZE = 30

arcpy.env.overwriteOutput = True
arcpy.env.workspace = GDB


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def normalize_wtn(value):
    """Normalize a Well Tag Number to integer string."""
    if value is None:
        return ""
    try:
        return str(int(float(value)))
    except (ValueError, TypeError):
        return ""


def parse_wtns(wtn_str):
    """Parse semicolon-separated well tag numbers into normalized strings."""
    if wtn_str is None:
        return []
    raw = str(wtn_str).replace(',', ';')
    wtns = []
    for t in raw.split(';'):
        n = normalize_wtn(t.strip())
        if n:
            wtns.append(n)
    return wtns


def parse_aquifer_ids(aq_str):
    """Parse comma-separated aquifer IDs into list of integers."""
    if aq_str is None:
        return []
    ids = []
    for part in str(aq_str).replace(';', ',').split(','):
        part = part.strip()
        if part:
            try:
                ids.append(int(float(part)))
            except (ValueError, TypeError):
                continue
    return ids


def convert_licence_to_cmd(qty, unit):
    """Convert licence quantity to CMD."""
    if qty is None or unit is None:
        return None
    unit_lower = str(unit).strip().lower()
    if unit_lower in ("m3/day", "cmd"):
        return float(qty)
    elif unit_lower in ("m3/year", "cmy"):
        return float(qty) / 365.0
    else:
        return None


def parse_source_name_aquifer(source_name):
    """
    Try to extract aquifer ID from SOURCE_NAME.
    Returns int aquifer ID if short and numeric, else None.
    """
    if source_name is None:
        return None
    s = str(source_name).strip()
    if len(s) > 5:
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


def fc_to_dataframe(fc, fields):
    """Read a feature class into a pandas DataFrame (attributes only)."""
    existing = [f.name for f in arcpy.ListFields(fc)]
    valid_fields = [f for f in fields if f in existing]
    rows = []
    with arcpy.da.SearchCursor(fc, valid_fields) as cur:
        for row in cur:
            rows.append(dict(zip(valid_fields, row)))
    return pd.DataFrame(rows)


print("=" * 60)
print("TOTAL SYSTEM WITHDRAWALS RASTER")
print("=" * 60)

# ============================================================
# STEP 1: Build lookups
# ============================================================
print("\n--- Step 1: Building lookups ---")

# 1a. Licence volume lookup: WTN -> total licensed CMD
print("  Building licence volume lookup (WTN -> CMD)...")
licence_cmd_by_wtn = {}
licence_total = 0
licence_skipped = 0

with arcpy.da.SearchCursor(LICENCES_FC,
                            ["WELL_TAG_NUMBER", "QUANTITY", "QUANTITY_UNITS"]) as cur:
    for row in cur:
        wtn = normalize_wtn(row[0])
        if not wtn:
            licence_skipped += 1
            continue
        cmd = convert_licence_to_cmd(row[1], row[2])
        if cmd is None:
            licence_skipped += 1
            continue
        licence_cmd_by_wtn[wtn] = licence_cmd_by_wtn.get(wtn, 0) + cmd
        licence_total += 1

print(f"    Licence records processed: {licence_total}")
print(f"    Licence records skipped: {licence_skipped}")
print(f"    Unique WTNs with volumes: {len(licence_cmd_by_wtn)}")

# 1b. GWELLS lookup: WTN -> AQUIFER_ID
print("  Building GWELLS lookup (WTN -> AQUIFER_ID)...")
gwells_aq = {}
with arcpy.da.SearchCursor(GWELLS_FC, ["WELL_TAG_NUMBER", "AQUIFER_ID"]) as cur:
    for row in cur:
        wtn = normalize_wtn(row[0])
        if wtn and row[1] is not None:
            try:
                gwells_aq[wtn] = int(float(row[1]))
            except (ValueError, TypeError):
                continue

print(f"    GWELLS WTN->aquifer entries: {len(gwells_aq)}")

# 1c. Aquifer material lookup: AQUIFER_ID -> MATERIAL
print("  Building aquifer material lookup...")
aq_material = {}
with arcpy.da.SearchCursor(AQUIFERS_FC, ["AQUIFER_ID", "MATERIAL"]) as cur:
    for row in cur:
        if row[0] is not None:
            aq_material[int(row[0])] = str(row[1]).strip() if row[1] else ""

print(f"    Aquifers in lookup: {len(aq_material)}")

# ============================================================
# STEP 2: Process EUGW - subtract licensed volumes, assign aquifer
# ============================================================
print("\n--- Step 2: Processing EUGW - volume subtraction & aquifer assignment ---")

eugw_total = int(arcpy.GetCount_management(EUGW_FC)[0])
sr = arcpy.Describe(EUGW_FC).spatialReference

arcpy.CreateFeatureclass_management("in_memory", "eugw_adjusted",
                                    "POINT", spatial_reference=sr)
arcpy.AddField_management("in_memory/eugw_adjusted", "well_tag", "TEXT", field_length=255)
arcpy.AddField_management("in_memory/eugw_adjusted", "quantity_cmd", "DOUBLE")
arcpy.AddField_management("in_memory/eugw_adjusted", "resolved_aq_id", "LONG")
arcpy.AddField_management("in_memory/eugw_adjusted", "aq_source", "TEXT", field_length=50)
arcpy.AddField_management("in_memory/eugw_adjusted", "source", "TEXT", field_length=20)
arcpy.AddField_management("in_memory/eugw_adjusted", "dedup_flag", "TEXT", field_length=100)

eugw_kept = 0
eugw_reduced = 0
eugw_dropped = 0
eugw_no_match = 0
eugw_aq_gwells = 0
eugw_aq_attribute = 0
eugw_aq_none = 0

with arcpy.da.InsertCursor("in_memory/eugw_adjusted",
                            ["SHAPE@", "well_tag", "quantity_cmd", "resolved_aq_id",
                             "aq_source", "source", "dedup_flag"]) as ins:
    with arcpy.da.SearchCursor(EUGW_FC,
                                ["SHAPE@", "Well_Tag_Number", "qty_cmd", "AQUIFER_IDS"]) as cur:
        for row in cur:
            shape, wtn_raw, qty_cmd, aq_ids_raw = row

            if shape is None:
                continue

            # --- Volume subtraction ---
            wtns = parse_wtns(wtn_raw)
            licensed_vol = 0
            matched_wtns = []
            for wtn in wtns:
                if wtn in licence_cmd_by_wtn:
                    licensed_vol += licence_cmd_by_wtn[wtn]
                    matched_wtns.append(wtn)

            original_cmd = qty_cmd if qty_cmd is not None else 0

            if not matched_wtns:
                adjusted_cmd = original_cmd
                flag = "no_licence_match"
                eugw_no_match += 1
            else:
                adjusted_cmd = original_cmd - licensed_vol
                if adjusted_cmd <= 0:
                    eugw_dropped += 1
                    continue
                else:
                    flag = f"reduced_by_{','.join(matched_wtns)}"
                    eugw_reduced += 1

            # --- Aquifer assignment ---
            resolved_aq = None
            aq_src = ""

            # Priority 1: GWELLS lookup
            for wtn in wtns:
                if wtn in gwells_aq:
                    resolved_aq = gwells_aq[wtn]
                    aq_src = f"gwells_wtn_{wtn}"
                    eugw_aq_gwells += 1
                    break

            # Priority 2: AQUIFER_IDS attribute
            if resolved_aq is None:
                aq_ids = parse_aquifer_ids(aq_ids_raw)
                if len(aq_ids) == 1:
                    resolved_aq = aq_ids[0]
                    aq_src = "eugw_attribute_single"
                    eugw_aq_attribute += 1
                elif len(aq_ids) > 1:
                    shallow = [a for a in aq_ids
                               if "sand" in aq_material.get(a, "").lower()]
                    if shallow:
                        resolved_aq = shallow[0]
                        aq_src = f"eugw_attribute_resolved_shallow_{len(aq_ids)}_ids"
                    else:
                        resolved_aq = aq_ids[0]
                        aq_src = f"eugw_attribute_resolved_first_{len(aq_ids)}_ids"
                    eugw_aq_attribute += 1

            if resolved_aq is None:
                eugw_aq_none += 1

            wtn_str = '; '.join(wtns) if wtns else ""
            ins.insertRow([shape, wtn_str, adjusted_cmd, resolved_aq,
                           aq_src, "EUGW", flag])
            eugw_kept += 1

print(f"  EUGW total: {eugw_total}")
print(f"  EUGW no licence match (kept full): {eugw_no_match}")
print(f"  EUGW reduced (partial licence): {eugw_reduced}")
print(f"  EUGW dropped (fully licensed): {eugw_dropped}")
print(f"  EUGW kept: {eugw_kept}")
print(f"  Aquifer from GWELLS: {eugw_aq_gwells}")
print(f"  Aquifer from EUGW attribute: {eugw_aq_attribute}")
print(f"  No aquifer assigned: {eugw_aq_none}")

# ============================================================
# STEP 3: Process licences - assign aquifer
# ============================================================
print("\n--- Step 3: Processing licences - aquifer assignment ---")

arcpy.CreateFeatureclass_management("in_memory", "licences_cmd",
                                    "POINT",
                                    spatial_reference=arcpy.Describe(LICENCES_FC).spatialReference)
arcpy.AddField_management("in_memory/licences_cmd", "well_tag", "TEXT", field_length=255)
arcpy.AddField_management("in_memory/licences_cmd", "quantity_cmd", "DOUBLE")
arcpy.AddField_management("in_memory/licences_cmd", "resolved_aq_id", "LONG")
arcpy.AddField_management("in_memory/licences_cmd", "aq_source", "TEXT", field_length=50)
arcpy.AddField_management("in_memory/licences_cmd", "source", "TEXT", field_length=20)
arcpy.AddField_management("in_memory/licences_cmd", "dedup_flag", "TEXT", field_length=100)

lic_converted = 0
lic_skipped = 0
lic_aq_source_name = 0
lic_needs_sj = 0

licence_rows = []
with arcpy.da.SearchCursor(LICENCES_FC,
                            ["SHAPE@", "WELL_TAG_NUMBER", "QUANTITY",
                             "QUANTITY_UNITS", "SOURCE_NAME"]) as cur:
    for row in cur:
        shape, wtn, qty, unit, src_name = row
        cmd = convert_licence_to_cmd(qty, unit)
        if cmd is None:
            lic_skipped += 1
            continue

        wtn_str = normalize_wtn(wtn)
        aq_from_src = parse_source_name_aquifer(src_name)

        licence_rows.append({
            'shape': shape,
            'well_tag': wtn_str,
            'quantity_cmd': cmd,
            'resolved_aq_id': aq_from_src,
            'aq_source': 'licence_source_name' if aq_from_src else '',
            'source': 'LICENCE',
            'dedup_flag': ''
        })

        if aq_from_src:
            lic_aq_source_name += 1
        else:
            lic_needs_sj += 1

        lic_converted += 1

print(f"  Licences converted: {lic_converted}")
print(f"  Licences skipped: {lic_skipped}")
print(f"  Aquifer from SOURCE_NAME: {lic_aq_source_name}")
print(f"  Needing spatial join: {lic_needs_sj}")

with arcpy.da.InsertCursor("in_memory/licences_cmd",
                            ["SHAPE@", "well_tag", "quantity_cmd", "resolved_aq_id",
                             "aq_source", "source", "dedup_flag"]) as ins:
    for lr in licence_rows:
        ins.insertRow([lr['shape'], lr['well_tag'], lr['quantity_cmd'],
                       lr['resolved_aq_id'], lr['aq_source'], lr['source'],
                       lr['dedup_flag']])

# Spatial join for licences without aquifer ID
if lic_needs_sj > 0:
    print(f"  Running spatial join for {lic_needs_sj} licences...")

    arcpy.management.MakeFeatureLayer(
        "in_memory/licences_cmd", "lic_no_aq",
        "resolved_aq_id IS NULL"
    )

    sj_output = os.path.join(GDB, "temp_licence_aq_join")
    arcpy.analysis.SpatialJoin(
        "lic_no_aq", AQUIFERS_FC, sj_output,
        join_operation="JOIN_ONE_TO_MANY",
        join_type="KEEP_ALL",
        match_option="WITHIN"
    )

    sj_records = {}
    with arcpy.da.SearchCursor(sj_output,
                                ["TARGET_FID", "AQUIFER_ID", "MATERIAL"]) as cur:
        for row in cur:
            target_fid, aq_id, material = row
            if target_fid not in sj_records:
                sj_records[target_fid] = []
            if aq_id is not None:
                sj_records[target_fid].append(
                    (int(aq_id), str(material).lower() if material else ""))

    licence_sj_aq = {}  # target_fid -> (aquifer_id, aq_source_flag)
    for target_fid, entries in sj_records.items():
        if not entries:
            continue
        if len(entries) == 1:
            licence_sj_aq[target_fid] = (entries[0][0], "licence_spatial_join_single")
        else:
            shallow = [e for e in entries if "sand" in e[1]]
            if shallow:
                licence_sj_aq[target_fid] = (shallow[0][0], f"licence_spatial_join_resolved_shallow_{len(entries)}_ids")
            else:
                licence_sj_aq[target_fid] = (entries[0][0], f"licence_spatial_join_resolved_first_{len(entries)}_ids")

    lic_aq_sj_assigned = 0
    oid_lic = arcpy.Describe("in_memory/licences_cmd").OIDFieldName
    with arcpy.da.UpdateCursor("in_memory/licences_cmd",
                                [oid_lic, "resolved_aq_id", "aq_source"],
                                "resolved_aq_id IS NULL") as cur:
        for row in cur:
            oid = row[0]
            if oid in licence_sj_aq:
                row[1] = licence_sj_aq[oid][0]
                row[2] = licence_sj_aq[oid][1]
                lic_aq_sj_assigned += 1
                cur.updateRow(row)

    arcpy.management.Delete(sj_output)
    print(f"  Aquifer from spatial join: {lic_aq_sj_assigned}")

# ============================================================
# STEP 4: Combine into single layer
# ============================================================
print("\n--- Step 4: Combining adjusted EUGW + Licences ---")

arcpy.management.Merge(["in_memory/eugw_adjusted", "in_memory/licences_cmd"],
                        OUTPUT_COMBINED)
total_combined = int(arcpy.GetCount_management(OUTPUT_COMBINED)[0])
print(f"  Combined wells: {total_combined}")

aq_assigned = 0
aq_missing = 0
with arcpy.da.SearchCursor(OUTPUT_COMBINED, ["resolved_aq_id"]) as cur:
    for row in cur:
        if row[0] is not None:
            aq_assigned += 1
        else:
            aq_missing += 1
print(f"  With aquifer: {aq_assigned}")
print(f"  Without aquifer: {aq_missing}")

# ============================================================
# STEP 5: Summarize total withdrawal per aquifer
# ============================================================
print("\n--- Step 5: Calculating total withdrawal per aquifer ---")

summary_table = os.path.join(GDB, "aquifer_withdrawal_summary")
arcpy.analysis.Statistics(OUTPUT_COMBINED, summary_table,
                          [["quantity_cmd", "SUM"]],
                          case_field="resolved_aq_id")

aq_totals = {}
with arcpy.da.SearchCursor(summary_table,
                            ["resolved_aq_id", "SUM_quantity_cmd"]) as cur:
    for row in cur:
        if row[0] is not None:
            aq_totals[int(row[0])] = row[1] if row[1] else 0

print(f"  Aquifers with withdrawals: {len(aq_totals)}")

# ============================================================
# STEP 6: Join to aquifer polygons and calculate density
# ============================================================
print("\n--- Step 6: Calculating withdrawal density (CMD/km2) ---")

arcpy.management.CopyFeatures(AQUIFERS_FC, OUTPUT_AQUIFERS)

arcpy.AddField_management(OUTPUT_AQUIFERS, "total_cmd", "DOUBLE")
arcpy.AddField_management(OUTPUT_AQUIFERS, "area_km2", "DOUBLE")
arcpy.AddField_management(OUTPUT_AQUIFERS, "density_cmd_km2", "DOUBLE")

with arcpy.da.UpdateCursor(OUTPUT_AQUIFERS,
                            ["AQUIFER_ID", "SHAPE@AREA", "total_cmd",
                             "area_km2", "density_cmd_km2"]) as cur:
    for row in cur:
        aq_id = int(row[0]) if row[0] is not None else None
        area_m2 = row[1]
        area_km2 = area_m2 / 1_000_000.0

        total = aq_totals.get(aq_id, 0)
        density = total / area_km2 if area_km2 > 0 else 0

        row[2] = total
        row[3] = round(area_km2, 4)
        row[4] = round(density, 4)
        cur.updateRow(row)

print(f"  Top 10 aquifers by density:")
densities = []
with arcpy.da.SearchCursor(OUTPUT_AQUIFERS,
                            ["AQUIFER_ID", "density_cmd_km2", "total_cmd"]) as cur:
    for row in cur:
        if row[1] and row[1] > 0:
            densities.append(row)
densities.sort(key=lambda x: -x[1])
for aq_id, dens, total in densities[:10]:
    print(f"    Aquifer {aq_id}: {dens:.2f} CMD/km2  (total: {total:.2f} CMD)")

# ============================================================
# STEP 7: Split by material type
# ============================================================
print("\n--- Step 7: Splitting aquifers by material type ---")

# Sand and Gravel
arcpy.management.MakeFeatureLayer(OUTPUT_AQUIFERS, "sand_layer",
                                   "MATERIAL LIKE '%Sand%' OR MATERIAL LIKE '%Gravel%'")
sand_count = int(arcpy.GetCount_management("sand_layer")[0])
arcpy.management.CopyFeatures("sand_layer", OUTPUT_AQ_SAND)
print(f"  Sand and Gravel aquifers: {sand_count}")

# Bedrock
arcpy.management.MakeFeatureLayer(OUTPUT_AQUIFERS, "rock_layer",
                                   "MATERIAL LIKE '%Bedrock%'")
rock_count = int(arcpy.GetCount_management("rock_layer")[0])
arcpy.management.CopyFeatures("rock_layer", OUTPUT_AQ_ROCK)
print(f"  Bedrock aquifers: {rock_count}")

# Check for unclassified
other_count = int(arcpy.GetCount_management(OUTPUT_AQUIFERS)[0]) - sand_count - rock_count
if other_count > 0:
    print(f"  Other/unclassified material: {other_count}")

# ============================================================
# STEP 8: Convert to rasters (one per material type)
# ============================================================
print(f"\n--- Step 8: Converting to rasters ({CELL_SIZE}m) ---")

# Sand and Gravel raster
if sand_count > 0:
    arcpy.conversion.PolygonToRaster(
        OUTPUT_AQ_SAND,
        "density_cmd_km2",
        OUTPUT_RAS_SAND,
        cell_assignment="MAXIMUM_COMBINED_AREA",
        priority_field="density_cmd_km2",
        cellsize=CELL_SIZE
    )
    print(f"  Sand & Gravel raster: {OUTPUT_RAS_SAND}")
else:
    print("  No Sand & Gravel aquifers - skipping raster")

# Bedrock raster
if rock_count > 0:
    arcpy.conversion.PolygonToRaster(
        OUTPUT_AQ_ROCK,
        "density_cmd_km2",
        OUTPUT_RAS_ROCK,
        cell_assignment="MAXIMUM_COMBINED_AREA",
        priority_field="density_cmd_km2",
        cellsize=CELL_SIZE
    )
    print(f"  Bedrock raster: {OUTPUT_RAS_ROCK}")
else:
    print("  No Bedrock aquifers - skipping raster")

# ============================================================
# STEP 9: Export attribute table to Excel
# ============================================================
print(f"\n--- Step 9: Exporting aquifer attributes to Excel ---")

export_fields = ["AQUIFER_ID", "MATERIAL", "total_cmd", "area_km2", "density_cmd_km2"]

# Add any other useful fields from the aquifer dataset
all_fields = [f.name for f in arcpy.ListFields(OUTPUT_AQUIFERS)]
extra_fields = [f for f in all_fields
                if f not in export_fields
                and f.upper() not in ('OBJECTID', 'SHAPE', 'SHAPE_LENGTH',
                                       'SHAPE_AREA', 'GLOBALID')]
export_fields_full = export_fields + extra_fields

df = fc_to_dataframe(OUTPUT_AQUIFERS, export_fields_full)

# Sort by density descending
df = df.sort_values('density_cmd_km2', ascending=False).reset_index(drop=True)

# Round numeric columns
for col in ['total_cmd', 'area_km2', 'density_cmd_km2']:
    if col in df.columns:
        df[col] = df[col].round(4)

df.to_excel(OUTPUT_XLSX, index=False, sheet_name='Aquifer Withdrawal Density')
print(f"  Excel exported: {OUTPUT_XLSX}")
print(f"  Rows: {len(df)}")

# ============================================================
# Cleanup
# ============================================================
arcpy.management.Delete("in_memory/eugw_adjusted")
arcpy.management.Delete("in_memory/licences_cmd")
arcpy.management.Delete(summary_table)

print(f"\n{'=' * 60}")
print("DONE")
print(f"{'=' * 60}")
print(f"  Combined wells:          {OUTPUT_COMBINED}")
print(f"  All aquifer densities:   {OUTPUT_AQUIFERS}")
print(f"  Sand & Gravel FC:        {OUTPUT_AQ_SAND}")
print(f"  Bedrock FC:              {OUTPUT_AQ_ROCK}")
print(f"  Sand & Gravel raster:    {OUTPUT_RAS_SAND}")
print(f"  Bedrock raster:          {OUTPUT_RAS_ROCK}")
print(f"  Excel:                   {OUTPUT_XLSX}")
