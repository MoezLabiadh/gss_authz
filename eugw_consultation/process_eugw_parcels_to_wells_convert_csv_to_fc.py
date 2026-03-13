"""
Convert EUGW well points CSV to feature class.
Only includes rows where coord_status = 'valid'.
"""

import arcpy
import os

# --- CONFIGURATION ---
INPUT_CSV = r"\\spatialfiles.bcgov\srm\gss\projects\gr_2026_227_eugw_consultation_support\work\existing_use_groundwater_extracted_well_points.csv"
OUTPUT_GDB = r"\\spatialfiles.bcgov\work\srm\gss\projects\gr_2026_227_eugw_consultation_support\work\data.gdb"
OUTPUT_FC = "EUGW_Well_Points"

SR_WGS84 = arcpy.SpatialReference(4326)
SR_BCALBERS = arcpy.SpatialReference(3005)

arcpy.env.overwriteOutput = True

# Make XY Event Layer from CSV (WGS84)
arcpy.management.MakeXYEventLayer(INPUT_CSV, "pt_longitude", "pt_latitude", "temp_layer", SR_WGS84)

# Copy to in-memory FC to get OIDs
arcpy.management.CopyFeatures("temp_layer", "in_memory/temp_fc")

# Make feature layer with where clause to filter valid coords only
arcpy.management.MakeFeatureLayer("in_memory/temp_fc", "valid_layer", "coord_status = 'valid'")

# Project to BC Albers and export
arcpy.management.Project("valid_layer", os.path.join(OUTPUT_GDB, OUTPUT_FC), SR_BCALBERS)

# Cleanup
arcpy.management.Delete("in_memory/temp_fc")

count = arcpy.GetCount_management(os.path.join(OUTPUT_GDB, OUTPUT_FC))[0]
print(f"Done. {count} features exported to {OUTPUT_FC} (BC Albers EPSG:3005)")