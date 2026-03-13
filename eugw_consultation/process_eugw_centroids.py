"""
EUGW Parcel Centroid Extraction Script
=======================================
Alternative to process_eugw_to_wells.py - uses parcel polygon centroids
instead of extracting individual well coordinates.

Simpler approach: one point per parcel, centroid geometry.

Input:  EUGW GDB layer (polygon)
        W:\srm\wml\Workarea\Authorizations\Water\EUGW\deliverables\EUGW_Master_Data_20250902.gdb
        Layer: EUGW_Master_Spatial

Output: Point shapefile with centroids, classified purposes, and standardized quantities (CMD)

Purpose Classification (same as process_eugw_to_wells.py):
  - Single-purpose: keep original purpose
  - Multi-purpose with Commercial: "Multi-purpose (includes Commercial)"
  - Multi-purpose without Commercial: "Multi-purpose (no Commercial)"

Quantity Handling:
  - All quantities converted to CMD (cubic meters per day)
  - Multi-quantity parcels: summed to single total per parcel
  - Units: cmd (as-is), cmy (÷365), cms (×86400), Sel/kW (NaN)
  - qty_cmd_log: log10(qty_cmd + 1) for kernel density weighting (reduces skew)
"""

import arcpy
import os
import sys
import math

# ============================================================
# CONFIGURATION
# ============================================================

INPUT_GDB = r"W:\srm\wml\Workarea\Authorizations\Water\EUGW\deliverables\EUGW_Master_Data_20250902.gdb"
INPUT_LAYER = "EUGW_Master_Spatial"
OUTPUT_GDB = r"W:\srm\gss\projects\gr_2026_227_eugw_consultation_support\work\data.gdb"
OUTPUT_FC = "EUGW_Parcel_Centroids"

# Full paths
INPUT_FC = os.path.join(INPUT_GDB, INPUT_LAYER)
OUTPUT_PATH = os.path.join(OUTPUT_GDB, OUTPUT_FC)


# ============================================================
# UNIT CONVERSION
# ============================================================

def convert_single_qty_to_cmd(qty_str, unit_str):
    """Convert a single quantity value to CMD."""
    try:
        qty = float(qty_str)
    except (ValueError, TypeError):
        return None

    unit = str(unit_str).strip().lower()
    if unit == 'cmd':
        return qty
    elif unit == 'cmy':
        return qty / 365.0
    elif unit == 'cms':
        return qty * 86400.0
    elif unit in ('sel', 'kw'):
        return None
    else:
        return None


def parse_and_sum_quantities(qty_str, unit_str):
    """
    Parse potentially comma-separated quantities and units.
    Returns (total_cmd, quantity_flag).
    """
    if qty_str is None or unit_str is None:
        return (None, 'no_quantity')

    qty_str = str(qty_str).strip()
    unit_str = str(unit_str).strip()

    if qty_str in ('', 'None', 'nan'):
        return (None, 'no_quantity')

    qtys = [q.strip() for q in qty_str.split(',')]
    units = [u.strip() for u in unit_str.split(',')]

    while len(units) < len(qtys):
        units.append(units[-1])

    cmd_values = []
    has_unconvertible = False
    for q, u in zip(qtys, units):
        val = convert_single_qty_to_cmd(q, u)
        if val is not None:
            cmd_values.append(val)
        else:
            has_unconvertible = True

    if not cmd_values:
        return (None, 'unconvertible_units')

    total = sum(cmd_values)

    if len(qtys) == 1:
        flag = 'direct'
    else:
        flag = 'summed'
    if has_unconvertible:
        flag += '_partial_unconvertible'

    return (total, flag)


# ============================================================
# PURPOSE CLASSIFICATION
# ============================================================

def classify_purpose(purpose_str):
    """Classify purpose for symbology."""
    if purpose_str is None or str(purpose_str).strip() in ('', 'None', 'nan'):
        return 'Unknown'

    purposes = [p.strip() for p in str(purpose_str).split(',')]

    if len(purposes) == 1:
        return purposes[0]

    has_commercial = any('commercial' in p.lower() for p in purposes)
    if has_commercial:
        return 'Multi-purpose (includes Commercial)'
    else:
        return 'Multi-purpose (no Commercial)'


# ============================================================
# MAIN PROCESSING
# ============================================================

def main():
    arcpy.env.overwriteOutput = True

    # Validate input
    if not arcpy.Exists(INPUT_FC):
        print(f"ERROR: Input not found: {INPUT_FC}")
        sys.exit(1)

    print(f"Reading: {INPUT_FC}")
    input_count = int(arcpy.GetCount_management(INPUT_FC)[0])
    print(f"Input features: {input_count}")

    # Get input spatial reference
    sr = arcpy.Describe(INPUT_FC).spatialReference
    print(f"Spatial reference: {sr.name}")

    # Get all field names from input (exclude shape fields)
    input_fields = []
    skip_types = ['Geometry', 'OID', 'Blob', 'Raster']
    for f in arcpy.ListFields(INPUT_FC):
        if f.type not in skip_types and f.name.upper() not in ('SHAPE', 'SHAPE_LENGTH', 'SHAPE_AREA'):
            input_fields.append(f)

    # -------------------------------------------------------
    # Create output feature class (point)
    # -------------------------------------------------------
    print(f"\nCreating output: {OUTPUT_PATH}")
    if arcpy.Exists(OUTPUT_PATH):
        arcpy.Delete_management(OUTPUT_PATH)

    arcpy.CreateFeatureclass_management(
        os.path.dirname(OUTPUT_PATH),
        os.path.basename(OUTPUT_PATH),
        "POINT",
        spatial_reference=sr
    )

    # Add new processed fields
    new_fields = [
        ('cls_purpose', 'TEXT', 255),
        ('qty_cmd', 'DOUBLE', None),
        ('qty_cmd_log', 'DOUBLE', None),
        ('qty_flag', 'TEXT', 50),
    ]
    for fname, ftype, flength in new_fields:
        if flength:
            arcpy.AddField_management(OUTPUT_PATH, fname, ftype, field_length=flength)
        else:
            arcpy.AddField_management(OUTPUT_PATH, fname, ftype)

    # Add all original fields to output
    original_field_names = []
    for f in input_fields:
        out_name = f.name
        # Truncate field names > 64 chars for shapefile compatibility
        if len(out_name) > 64:
            out_name = out_name[:64]
        try:
            if f.type == 'String':
                arcpy.AddField_management(OUTPUT_PATH, out_name, 'TEXT', field_length=f.length)
            elif f.type == 'Double':
                arcpy.AddField_management(OUTPUT_PATH, out_name, 'DOUBLE')
            elif f.type in ('Integer', 'SmallInteger'):
                arcpy.AddField_management(OUTPUT_PATH, out_name, 'LONG')
            elif f.type == 'Date':
                arcpy.AddField_management(OUTPUT_PATH, out_name, 'DATE')
            elif f.type == 'Single':
                arcpy.AddField_management(OUTPUT_PATH, out_name, 'FLOAT')
            else:
                arcpy.AddField_management(OUTPUT_PATH, out_name, 'TEXT', field_length=255)
            original_field_names.append((f.name, out_name))
        except Exception as e:
            print(f"  Warning: Could not add field '{out_name}': {e}")

    # -------------------------------------------------------
    # Read input and write centroids
    # -------------------------------------------------------
    # Build cursor field list for input
    read_fields = ['SHAPE@'] + [f.name for f in input_fields]

    # Build cursor field list for output
    write_fields = ['SHAPE@', 'cls_purpose', 'qty_cmd', 'qty_cmd_log', 'qty_flag']
    write_fields += [out_name for _, out_name in original_field_names]

    # Find indices for key fields in read cursor
    field_names_list = [f.name for f in input_fields]

    def get_val(row_values, field_name):
        """Get value from row by field name."""
        try:
            idx = field_names_list.index(field_name) + 1  # +1 for SHAPE@
            return row_values[idx]
        except (ValueError, IndexError):
            return None

    print("\nProcessing...")
    counts = {
        'total': 0,
        'success': 0,
        'null_geometry': 0,
    }

    with arcpy.da.InsertCursor(OUTPUT_PATH, write_fields) as insert_cur:
        with arcpy.da.SearchCursor(INPUT_FC, read_fields) as search_cur:
            for row in search_cur:
                counts['total'] += 1
                shape = row[0]

                # Get centroid
                if shape is None or shape.area == 0:
                    counts['null_geometry'] += 1
                    continue

                centroid = shape.centroid
                point_geom = arcpy.PointGeometry(centroid, sr)

                # Classify purpose
                purpose_val = get_val(row, 'App_Purpose_Name')
                cls_purpose = classify_purpose(purpose_val)

                # Parse and sum quantities
                qty_val = get_val(row, 'Quantity')
                unit_val = get_val(row, 'Quantity_Units')
                qty_cmd, qty_flag = parse_and_sum_quantities(qty_val, unit_val)

                # Log10 transform for kernel density weighting
                if qty_cmd is not None and qty_cmd > 0:
                    qty_cmd_log = math.log10(qty_cmd + 1)
                else:
                    qty_cmd_log = None

                # Build output row
                out_row = [point_geom, cls_purpose, qty_cmd, qty_cmd_log, qty_flag]

                # Copy original field values
                for orig_name, out_name in original_field_names:
                    val = get_val(row, orig_name)
                    out_row.append(val)

                insert_cur.insertRow(out_row)
                counts['success'] += 1

                if counts['total'] % 500 == 0:
                    print(f"  Processed {counts['total']} features...")

    # -------------------------------------------------------
    # Summary
    # -------------------------------------------------------
    output_count = int(arcpy.GetCount_management(OUTPUT_PATH)[0])

    print(f"\n{'='*60}")
    print(f"PROCESSING SUMMARY")
    print(f"{'='*60}")
    print(f"Input polygons:     {counts['total']}")
    print(f"Output centroids:   {output_count}")
    print(f"Null geometry:      {counts['null_geometry']}")

    # Quick stats on output
    print(f"\n--- Purpose Classification ---")
    purpose_counts = {}
    with arcpy.da.SearchCursor(OUTPUT_PATH, ['cls_purpose']) as cur:
        for r in cur:
            p = r[0] if r[0] else 'Unknown'
            purpose_counts[p] = purpose_counts.get(p, 0) + 1
    for p, c in sorted(purpose_counts.items(), key=lambda x: -x[1]):
        print(f"  {p}: {c}")

    print(f"\n--- Quantity Flag ---")
    flag_counts = {}
    with arcpy.da.SearchCursor(OUTPUT_PATH, ['qty_flag']) as cur:
        for r in cur:
            f = r[0] if r[0] else 'unknown'
            flag_counts[f] = flag_counts.get(f, 0) + 1
    for f, c in sorted(flag_counts.items(), key=lambda x: -x[1]):
        print(f"  {f}: {c}")

    print(f"\n--- Quantity Stats (CMD) ---")
    qty_values = []
    null_count = 0
    with arcpy.da.SearchCursor(OUTPUT_PATH, ['qty_cmd']) as cur:
        for r in cur:
            if r[0] is not None:
                qty_values.append(r[0])
            else:
                null_count += 1
    if qty_values:
        print(f"  Records with valid qty: {len(qty_values)}")
        print(f"  Records with NULL qty:  {null_count}")
        print(f"  Min:    {min(qty_values):.4f}")
        print(f"  Max:    {max(qty_values):.4f}")
        print(f"  Mean:   {sum(qty_values)/len(qty_values):.4f}")
        sorted_vals = sorted(qty_values)
        median = sorted_vals[len(sorted_vals)//2]
        print(f"  Median: {median:.4f}")

    print(f"\n--- Quantity Stats (Log10) ---")
    log_values = []
    with arcpy.da.SearchCursor(OUTPUT_PATH, ['qty_cmd_log']) as cur:
        for r in cur:
            if r[0] is not None:
                log_values.append(r[0])
    if log_values:
        print(f"  Records with valid log: {len(log_values)}")
        print(f"  Min:    {min(log_values):.4f}")
        print(f"  Max:    {max(log_values):.4f}")
        print(f"  Mean:   {sum(log_values)/len(log_values):.4f}")

    print(f"\nOutput saved to: {OUTPUT_PATH}")


if __name__ == '__main__':
    main()