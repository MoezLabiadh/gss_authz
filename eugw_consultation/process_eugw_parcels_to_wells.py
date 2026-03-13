"""
EUGW Parcel-to-Well Point Processing Script
============================================
Processes the EUGW polygon/parcel dataset into individual well points
with standardized quantities (CMD - cubic meters per day) and 
classified purposes for mapping.

Input:  EUGW parcel-level Excel file (polygon export)
Output: Well-level point CSV ready for GIS import

Purpose Classification:
  - Single-purpose applications: keep original purpose
  - Multi-purpose, single-well: 
      "Multi-purpose (includes Commercial)" or 
      "Multi-purpose (no Commercial)"
  - Multi-purpose, multi-well (matching count): 1:1 assignment
  - Multi-purpose, multi-well (non-matching): treated as multi-purpose single-well

Quantity Handling:
  - All quantities converted to CMD (cubic meters per day)
  - Single quantity + multiple wells: split equally
  - Multiple quantities matching well count: assign 1:1
  - Multiple quantities not matching: sum total per well
  - Units: cmd (as-is), cmy (÷365), cms (×86400), Sel/kW (flagged, no conversion)

Coordinate Handling:
  - Rows with no valid coordinates are KEPT with NaN lat/lon
  - coord_status field indicates: 'valid', 'no_coordinates', 'invalid_coordinates'
  - These can later be recovered via GWELLS lookup using Well_Tag_Number or dropped
"""

import pandas as pd
import numpy as np
import sys
import os


# ============================================================
# UNIT CONVERSION
# ============================================================

def convert_single_qty_to_cmd(qty_str, unit_str):
    """Convert a single quantity value to CMD based on its unit."""
    try:
        qty = float(qty_str)
    except (ValueError, TypeError):
        return np.nan

    unit = str(unit_str).strip().lower()
    if unit == 'cmd':
        return qty
    elif unit == 'cmy':
        return qty / 365.0
    elif unit == 'cms':
        return qty * 86400.0
    elif unit in ('sel', 'kw'):
        return np.nan
    else:
        return np.nan


def parse_and_convert_quantities(qty_str, unit_str):
    """
    Parse potentially comma-separated quantities and units.
    Returns list of (quantity_cmd, original_unit) tuples.
    """
    if pd.isna(qty_str) or pd.isna(unit_str):
        return []

    qtys = [q.strip() for q in str(qty_str).split(',')]
    units = [u.strip() for u in str(unit_str).split(',')]

    while len(units) < len(qtys):
        units.append(units[-1])

    results = []
    for q, u in zip(qtys, units):
        cmd_val = convert_single_qty_to_cmd(q, u)
        results.append((cmd_val, u))

    return results


# ============================================================
# COORDINATE PARSING
# ============================================================

def parse_coordinates(lat_str, lon_str):
    """
    Parse potentially comma-separated lat/lon strings.
    Returns list of (lat, lon) tuples, filtering out invalid values.
    """
    if pd.isna(lat_str) or pd.isna(lon_str):
        return []

    lats = [l.strip() for l in str(lat_str).split(',')]
    lons = [l.strip() for l in str(lon_str).split(',')]

    coords = []
    for lat, lon in zip(lats, lons):
        if lat in ('None', '', 'nan', 'NaN') or lon in ('None', '', 'nan', 'NaN'):
            continue
        try:
            lat_f = float(lat)
            lon_f = float(lon)
            if 48 < lat_f < 60 and -140 < lon_f < -114:
                if lon_f > 0:
                    lon_f = -lon_f
                coords.append((lat_f, lon_f))
        except (ValueError, TypeError):
            continue

    return coords


def parse_well_tags(tag_str):
    """Parse semicolon or comma separated well tag numbers."""
    if pd.isna(tag_str):
        return []
    tags_str = str(tag_str).replace(',', ';')
    tags = [t.strip() for t in tags_str.split(';') if t.strip() and t.strip() != '0']
    return tags


# ============================================================
# PURPOSE CLASSIFICATION
# ============================================================

def classify_purpose(purpose_str):
    """
    Classify purpose for symbology.
    Returns (classified_purpose, is_multi, original_purposes_list)
    """
    if pd.isna(purpose_str):
        return ('Unknown', False, [])

    purposes = [p.strip() for p in str(purpose_str).split(',')]

    if len(purposes) == 1:
        return (purposes[0], False, purposes)

    has_commercial = any('commercial' in p.lower() for p in purposes)
    if has_commercial:
        return ('Multi-purpose (includes Commercial)', True, purposes)
    else:
        return ('Multi-purpose (no Commercial)', True, purposes)


# ============================================================
# OUTPUT ROW HELPER
# ============================================================

def make_output_row(base_attrs, lat, lon, well_tag, quantity_cmd,
                    classified_purpose, quantity_flag, coord_status):
    """Create a single output row dict with all fields."""
    out = base_attrs.copy()
    out['pt_latitude'] = lat
    out['pt_longitude'] = lon
    out['pt_well_tag'] = well_tag
    out['quantity_cmd'] = quantity_cmd
    out['classified_purpose'] = classified_purpose
    out['quantity_flag'] = quantity_flag
    out['coord_status'] = coord_status
    return out


# ============================================================
# QUANTITY PROCESSING HELPER
# ============================================================

def get_quantity_info(qty_results):
    """
    Returns (cmd_values, total_cmd, has_valid, all_unconvertible)
    """
    if not qty_results:
        return [], np.nan, False, False
    cmd_values = [q for q, u in qty_results]
    valid = [q for q in cmd_values if not np.isnan(q)]
    all_unconvertible = len(qty_results) > 0 and len(valid) == 0
    has_valid = len(valid) > 0
    total_cmd = sum(valid) if valid else np.nan
    return cmd_values, total_cmd, has_valid, all_unconvertible


# ============================================================
# MAIN PROCESSING
# ============================================================

def process_eugw(input_file, output_file):
    print(f"Reading: {input_file}")
    df = pd.read_excel(input_file)
    print(f"Input rows: {len(df)}")

    output_rows = []
    counts = {
        'no_coords': 0,
        'invalid_coords': 0,
        'no_quantity': 0,
        'unconvertible_units': 0,
    }

    for idx, row in df.iterrows():
        coords = parse_coordinates(row['Well_Latitude'], row['Well_Longitude'])
        well_tags = parse_well_tags(row.get('Well_Tag_Number', np.nan))
        qty_results = parse_and_convert_quantities(row['Quantity'], row['Quantity_Units'])
        classified_purpose, is_multi, purpose_list = classify_purpose(row['App_Purpose_Name'])
        base_attrs = row.to_dict()
        cmd_values, total_cmd, has_valid_qty, all_unconvertible = get_quantity_info(qty_results)

        # Determine quantity flag for missing/unconvertible
        if not qty_results:
            qty_flag_missing = 'no_quantity'
            counts['no_quantity'] += 1
        elif all_unconvertible:
            qty_flag_missing = 'unconvertible_units'
            counts['unconvertible_units'] += 1
        else:
            qty_flag_missing = None

        # ==============================================================
        # NO VALID COORDINATES - keep row with NaN geometry
        # ==============================================================
        if not coords:
            if pd.isna(row['Well_Latitude']) or pd.isna(row['Well_Longitude']):
                coord_status = 'no_coordinates'
                counts['no_coords'] += 1
            else:
                coord_status = 'invalid_coordinates'
                counts['invalid_coords'] += 1

            qty_cmd = total_cmd if has_valid_qty else np.nan
            qty_flag = qty_flag_missing if qty_flag_missing else 'direct'

            output_rows.append(make_output_row(
                base_attrs, np.nan, np.nan,
                well_tags[0] if well_tags else '',
                qty_cmd, classified_purpose, qty_flag, coord_status
            ))
            continue

        # ==============================================================
        # HAS COORDINATES - process normally
        # ==============================================================
        num_wells = len(coords)
        num_qtys = len(cmd_values)
        num_purposes = len(purpose_list)

        # --- No valid quantity: create points with NaN qty ---
        if qty_flag_missing:
            for i, (lat, lon) in enumerate(coords):
                output_rows.append(make_output_row(
                    base_attrs, lat, lon,
                    well_tags[i] if i < len(well_tags) else '',
                    np.nan, classified_purpose, qty_flag_missing, 'valid'
                ))
            continue

        # ==========================================================
        # CASE 1: Single purpose
        # ==========================================================
        if not is_multi:
            if num_wells == 1:
                output_rows.append(make_output_row(
                    base_attrs, coords[0][0], coords[0][1],
                    well_tags[0] if well_tags else '',
                    total_cmd, classified_purpose, 'direct', 'valid'
                ))

            elif num_qtys == num_wells:
                for i, (lat, lon) in enumerate(coords):
                    q = cmd_values[i] if not np.isnan(cmd_values[i]) else np.nan
                    output_rows.append(make_output_row(
                        base_attrs, lat, lon,
                        well_tags[i] if i < len(well_tags) else '',
                        q, classified_purpose, 'matched_1to1', 'valid'
                    ))

            else:
                split_qty = total_cmd / num_wells
                for i, (lat, lon) in enumerate(coords):
                    output_rows.append(make_output_row(
                        base_attrs, lat, lon,
                        well_tags[i] if i < len(well_tags) else '',
                        split_qty, classified_purpose, 'split_equal', 'valid'
                    ))

        # ==========================================================
        # CASE 2: Multi-purpose
        # ==========================================================
        else:
            if num_wells == 1:
                output_rows.append(make_output_row(
                    base_attrs, coords[0][0], coords[0][1],
                    well_tags[0] if well_tags else '',
                    total_cmd, classified_purpose, 'multi_purpose_summed', 'valid'
                ))

            elif num_purposes == num_wells:
                for i, (lat, lon) in enumerate(coords):
                    if i < len(cmd_values) and not np.isnan(cmd_values[i]):
                        q = cmd_values[i]
                        qflag = 'multi_purpose_matched'
                    else:
                        q = total_cmd / num_wells
                        qflag = 'multi_purpose_split'
                    purpose_i = purpose_list[i] if i < len(purpose_list) else classified_purpose
                    output_rows.append(make_output_row(
                        base_attrs, lat, lon,
                        well_tags[i] if i < len(well_tags) else '',
                        q, purpose_i, qflag, 'valid'
                    ))

            else:
                split_qty = total_cmd / num_wells
                for i, (lat, lon) in enumerate(coords):
                    output_rows.append(make_output_row(
                        base_attrs, lat, lon,
                        well_tags[i] if i < len(well_tags) else '',
                        split_qty, classified_purpose,
                        'multi_purpose_nonmatch_split', 'valid'
                    ))

    # ==========================================================
    # BUILD OUTPUT
    # ==========================================================
    out_df = pd.DataFrame(output_rows)

    new_cols = ['pt_latitude', 'pt_longitude', 'pt_well_tag',
                'classified_purpose', 'quantity_cmd', 'quantity_flag', 'coord_status']
    original_cols = [c for c in out_df.columns if c not in new_cols]
    out_df = out_df[new_cols + original_cols]

    out_df.to_csv(output_file, index=False)

    # ==========================================================
    # SUMMARY
    # ==========================================================
    print(f"\n{'='*60}")
    print(f"PROCESSING SUMMARY")
    print(f"{'='*60}")
    print(f"Input parcels:            {len(df)}")
    print(f"Output rows:              {len(out_df)}")
    print(f"\n--- Coordinate Status ---")
    print(out_df['coord_status'].value_counts().to_string())
    print(f"\n  no_coordinates:    {counts['no_coords']} (NaN lat/lon, Well_Latitude was empty)")
    print(f"  invalid_coords:    {counts['invalid_coords']} (lat/lon present but failed validation)")
    print(f"\n--- Purpose Classification ---")
    print(out_df['classified_purpose'].value_counts().to_string())
    print(f"\n--- Quantity Flag ---")
    print(out_df['quantity_flag'].value_counts().to_string())
    print(f"\n--- Quantity Stats (CMD) ---")
    valid_qty = out_df['quantity_cmd'].dropna()

    print(f"\nOutput saved to: {output_file}")


if __name__ == '__main__':
    input_file = r'\\spatialfiles.bcgov\work\srm\gss\projects\gr_2026_227_eugw_consultation_support\work\existing_use_groundwater_polys_exported_from_gdb.xlsx'
    output_file = r'\\spatialfiles.bcgov\work\srm\gss\projects\gr_2026_227_eugw_consultation_support\work\existing_use_groundwater_extracted_well_points.csv'
    process_eugw(input_file, output_file)