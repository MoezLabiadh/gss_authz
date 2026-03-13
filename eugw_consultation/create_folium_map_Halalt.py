#-------------------------------------------------------------------------------
# Name:        EUGW Consultation Interactive Map
#
# Purpose:     Generates an HTML map for EUGW consultation support
#              - EUGW wells: colour-coded by purpose, sized by volume (log10)
#              - Kernel density heatmap raster overlay (natural breaks)
#              - WMS base layers: aquifers, water licensing watersheds
#
# Input(s):    (1) EUGW wells centroid feature class (GDB)
#              (2) Kernel density heatmap raster (GeoTIFF)
#
# Output(s):   HTML interactive map
#
# Author:      
#
# Created:     2026-03-04
#-------------------------------------------------------------------------------

import warnings
warnings.simplefilter(action='ignore')

import os
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely import wkb
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
import jenkspy
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import folium
from folium.plugins import MiniMap, GroupedLayerControl, MarkerCluster
from branca.element import Template, MacroElement, Element
from PIL import Image
import base64
import io
import datetime as dt
import timeit


# ============================================================
# CONFIGURATION
# ============================================================
EUGW_FC = r"\\spatialfiles.bcgov\work\srm\gss\projects\gr_2026_227_eugw_consultation_support\work\data.gdb\hmn_eugw_centroids"
AOI_FC = r"\\spatialfiles.bcgov\work\srm\gss\projects\gr_2026_227_eugw_consultation_support\work\data.gdb\AOI_halalt_hmn_core"
HEATMAP_WEIGHTED_TIF = r"\\spatialfiles.bcgov\work\srm\gss\projects\gr_2026_227_eugw_consultation_support\work\hmn_weighted_qtylog_epanechnikov_30sqm_3000m.tif"
HEATMAP_UNWEIGHTED_TIF = r"\\spatialfiles.bcgov\work\srm\gss\projects\gr_2026_227_eugw_consultation_support\work\hmn_unweighted_epanechnikov_30sqm_3000m.tif"
OUTPUT_HTML = r"\\spatialfiles.bcgov\work\srm\gss\projects\gr_2026_227_eugw_consultation_support\delivrables\Halalt delivrables\halalt_eugw_consultation_map.html"

# WMS URLs
WMS_AQUIFERS = 'https://openmaps.gov.bc.ca/geo/pub/WHSE_WATER_MANAGEMENT.GW_AQUIFERS_CLASSIFICATION_SVW/ows?service=WMS'
WATERSHEDS_FC = r"\\spatialfiles.bcgov\work\srm\gss\projects\gr_2026_227_eugw_consultation_support\work\data.gdb\hmn_water_licencing_watersheds"
DENSITY_SAND_FC = r"\\spatialfiles.bcgov\work\srm\gss\projects\gr_2026_227_eugw_consultation_support\work\data.gdb\hmn_out_aquifers_density_sand_gravel"
DENSITY_ROCK_FC = r"\\spatialfiles.bcgov\work\srm\gss\projects\gr_2026_227_eugw_consultation_support\work\data.gdb\hmn_out_aquifers_density_bedrock"

# Purpose colour scheme - each purpose gets a distinct colour
PURPOSE_COLOURS = {
    'Commercial Enterprise': '#e41a1c',
    'Multi-purpose (includes Commercial)': '#ff7f00',
    'Irrigation': '#4daf4a',
    'Waterworks Local Provider': '#377eb8',
    'Livestock and Animal': '#a65628',
    'Camps & Public Facilities': '#984ea3',
    'Multi-purpose (no Commercial)': '#f781bf',
    'Waterworks - Others': '#17becf',
    'Processing and Manufacturing': '#bcbd22',
    'Miscellaneous Industrial': '#aec7e8',
    'Greenhouse and Nursery': '#98df8a',
    'Fish Hatchery': '#1f77b4',
    'Vehicle and Equipment': '#ffbb78',
    'Conservation - Stored Water': '#c49c94',
    'Land Improvement - General': '#dbdb8d',
    'Pond and Aquaculture': '#9edae5',
    'Aquifer Storage - Non-Power': '#c5b0d5',
    'Heat Exchangers - Industrial & Commercial': '#ff9896',
    'Conservation - Use of Water': '#66c2a5',
    'Waterworks - Water Delivery': '#2ca02c',
    'Waste Management': '#8c564b',
    'Water Sales': '#e377c2',
    'Fresh Water Bottling': '#7f7f7f',
    'Cooling': '#b5cf6b',
    'Swimming Pool': '#cedb9c',
    'Heat Exchangers - Residential': '#e7969c',
    'Lawn': '#de9ed6',
    'Fairway and Garden': '#ad494a',
    'Conservation - Construct Works': '#6b6ecf',
    'Power - Residential': '#b5cf6b',
    'Unknown': '#c7c7c7',
}
DEFAULT_COLOUR = '#636363'

# Heatmap settings
HEATMAP_CMAP = 'YlOrRd'
HEATMAP_OPACITY = 0.6
NUM_CLASSES = 10

# Withdrawal density symbology
DENSITY_CMAP = 'cool'
DENSITY_CLASSES = 5
DENSITY_OPACITY = 0.65

# Dot size range (pixels)
MIN_RADIUS = 4
MAX_RADIUS = 20


# ============================================================
# DATA READING
# ============================================================

def esri_to_gdf(aoi):
    """Returns a GeoDataFrame from an ESRI shp or featureclass (gdb)"""
    if '.shp' in aoi:
        gdf = gpd.read_file(aoi)
    elif '.gdb' in aoi:
        l = aoi.split('.gdb')
        gdb = l[0] + '.gdb'
        fc = os.path.basename(aoi)
        gdf = gpd.read_file(filename=gdb, layer=fc)
    else:
        raise Exception('Format not recognized. Please provide a shp or featureclass (gdb)!')
    return gdf


def flatten_to_2d(gdf):
    """Flattens 3D geometries to 2D"""
    for i, row in gdf.iterrows():
        geom = row.geometry
        if geom is not None and geom.has_z:
            geom_2d = wkb.loads(wkb.dumps(geom, output_dimension=2))
            gdf.at[i, 'geometry'] = geom_2d
    return gdf


def reproject_to_wgs84(gdf):
    """Reprojects a gdf to WGS84"""
    if gdf.crs != 'epsg:4326':
        gdf = gdf.to_crs('epsg:4326')
    return gdf


def prepare_geo_data(aoi):
    """Runs data preparation functions"""
    gdf = esri_to_gdf(aoi)
    gdf = flatten_to_2d(gdf)
    gdf = reproject_to_wgs84(gdf)
    return gdf


# ============================================================
# HEATMAP RASTER PROCESSING
# ============================================================

def process_heatmap_raster(tif_path, num_classes=10):
    """
    Read heatmap GeoTIFF, reproject to WGS84, classify with natural breaks,
    render to RGBA PNG.
    Returns (png_base64, bounds_wgs84, breaks, cmap).
    """
    print('...Reading heatmap raster')
    with rasterio.open(tif_path) as src:
        src_data = src.read(1)
        src_crs = src.crs
        src_transform = src.transform
        src_nodata = src.nodata
        src_height, src_width = src.shape
        src_bounds = src.bounds

    # --- Reproject raster to EPSG:4326 ---
    print('...Reprojecting raster to WGS84')
    dst_crs = 'EPSG:4326'

    dst_transform, dst_width, dst_height = calculate_default_transform(
        src_crs, dst_crs, src_width, src_height,
        left=src_bounds.left, bottom=src_bounds.bottom,
        right=src_bounds.right, top=src_bounds.top
    )

    dst_data = np.empty((dst_height, dst_width), dtype=src_data.dtype)

    reproject(
        source=src_data,
        destination=dst_data,
        src_transform=src_transform,
        src_crs=src_crs,
        dst_transform=dst_transform,
        dst_crs=dst_crs,
        resampling=Resampling.bilinear,
        src_nodata=src_nodata,
        dst_nodata=src_nodata
    )

    # Calculate WGS84 bounds from the reprojected transform
    left = dst_transform.c
    top = dst_transform.f
    right = left + dst_transform.a * dst_width
    bottom = top + dst_transform.e * dst_height
    bounds_wgs84 = (left, bottom, right, top)

    # Mask nodata and zeros
    data = dst_data
    if src_nodata is not None:
        mask = (data == src_nodata) | (data <= 0)
    else:
        mask = (data <= 0) | np.isnan(data)

    valid_data = data[~mask]
    print(f'   Reprojected shape: {data.shape}')
    print(f'   Valid pixels: {len(valid_data)}')
    print(f'   Value range: {valid_data.min():.4f} - {valid_data.max():.4f}')

    # Natural breaks (Jenks) - sample if too many pixels
    if len(valid_data) > 50000:
        sample = np.random.choice(valid_data, 50000, replace=False)
    else:
        sample = valid_data

    breaks = jenkspy.jenks_breaks(sample.tolist(), n_classes=num_classes)
    print(f'   Natural breaks ({num_classes} classes): {[f"{b:.2f}" for b in breaks]}')

    # Create colour map and norm
    cmap = plt.get_cmap(HEATMAP_CMAP, num_classes)
    norm = mcolors.BoundaryNorm(breaks, cmap.N)

    # Render to RGBA (vectorized)
    print('...Rendering raster to RGBA')
    mapped = cmap(norm(data))
    rgba = (mapped * 255).astype(np.uint8)

    # Apply transparency
    alpha = np.where(mask, 0, int(HEATMAP_OPACITY * 255)).astype(np.uint8)
    rgba[:, :, 3] = alpha

    # Encode to PNG
    img = Image.fromarray(rgba, 'RGBA')
    buffer = io.BytesIO()
    img.save(buffer, format='PNG')
    png_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')

    print(f'   Bounds (WGS84): {[f"{b:.4f}" for b in bounds_wgs84]}')

    return png_base64, bounds_wgs84, breaks, cmap


# ============================================================
# MAP BUILDING
# ============================================================

def get_scaled_radius(log_val, log_min, log_max):
    """Scale a log value to a circle radius between MIN_RADIUS and MAX_RADIUS"""
    if log_val is None or np.isnan(log_val) or log_max == log_min:
        return MIN_RADIUS
    scaled = (log_val - log_min) / (log_max - log_min)
    return MIN_RADIUS + scaled * (MAX_RADIUS - MIN_RADIUS)


def build_popup_html(row, popup_fields):
    """Build popup HTML table for a well point"""
    rows_html = ""
    for col, label in popup_fields:
        val = row.get(col, '')
        if val is None or (isinstance(val, float) and np.isnan(val)):
            val = '–'
        elif isinstance(val, float):
            val = f"{val:.4f}" if abs(val) < 100 else f"{val:.2f}"
        rows_html += f"<tr><td style='font-weight:bold;padding:2px 6px;'>{label}</td>" \
                     f"<td style='padding:2px 6px;'>{val}</td></tr>"

    return f"<table style='font-size:11px;font-family:Arial;'>{rows_html}</table>"


def assign_colours(gdf):
    """
    Assign a unique colour to each purpose. Uses PURPOSE_COLOURS dict first,
    then generates distinct colours for any remaining purposes.
    """
    purposes_in_data = sorted(gdf['cls_purpose'].unique())

    # Collect purposes not in the predefined dict
    unmapped = [p for p in purposes_in_data if p not in PURPOSE_COLOURS]

    if unmapped:
        extra_cmap = plt.get_cmap('tab20', len(unmapped))
        for i, purpose in enumerate(unmapped):
            PURPOSE_COLOURS[purpose] = mcolors.to_hex(extra_cmap(i))

    # Verify no duplicate hex values for purposes actually in data
    active = {p: PURPOSE_COLOURS.get(p, DEFAULT_COLOUR) for p in purposes_in_data}
    used_colours = set()

    fallback_cmap = plt.get_cmap('Set3', 12)
    fallback_idx = 0

    for purpose in purposes_in_data:
        colour = active[purpose]
        if colour in used_colours:
            while fallback_idx < 12:
                new_colour = mcolors.to_hex(fallback_cmap(fallback_idx))
                fallback_idx += 1
                if new_colour not in used_colours:
                    colour = new_colour
                    PURPOSE_COLOURS[purpose] = colour
                    break
        used_colours.add(colour)
        active[purpose] = colour

    gdf['color'] = gdf['cls_purpose'].map(active).fillna(DEFAULT_COLOUR)

    return gdf, active


def create_toggle_all_js(layer_names):
    """
    Creates raw JS/CSS to inject a 'Toggle All EUGW' checkbox.
    Uses layer names to find FeatureGroups via the Leaflet layer control DOM.
    """
    # Build a JS array of purpose names to match against layer control labels
    names_js = ', '.join(['"' + n + '"' for n in layer_names])

    js_code = """
    <script>
    (function() {
        var purposeNames = [NAMES_PLACEHOLDER];
        
        var checkExist = setInterval(function() {
            var cb = document.getElementById('eugw-toggle-cb');
            if (!cb) return;
            
            // Find the grouped layer control container
            var glcContainer = document.querySelector('.leaflet-control-layers-group-selector');
            if (!glcContainer) {
                // Try alternative: look for any layer control with our purpose names
                var allLabels = document.querySelectorAll('.leaflet-control-layers label');
                if (allLabels.length === 0) return;
            }
            
            clearInterval(checkExist);
            
            // Function to find all checkboxes belonging to EUGW purpose layers
            function getEugwCheckboxes() {
                var checkboxes = [];
                var allLabels = document.querySelectorAll('.leaflet-control-layers label');
                allLabels.forEach(function(label) {
                    var span = label.querySelector('span');
                    var input = label.querySelector('input[type="checkbox"]');
                    if (!span || !input) return;
                    var labelText = span.textContent.trim();
                    for (var i = 0; i < purposeNames.length; i++) {
                        if (labelText === purposeNames[i]) {
                            checkboxes.push(input);
                            break;
                        }
                    }
                });
                return checkboxes;
            }
            
            cb.addEventListener('change', function() {
                var visible = cb.checked;
                var eugwCheckboxes = getEugwCheckboxes();
                
                eugwCheckboxes.forEach(function(input) {
                    if (input.checked !== visible) {
                        input.click();
                    }
                });
            });
            
        }, 300);
    })();
    </script>
    """.replace('NAMES_PLACEHOLDER', names_js)

    return js_code


def create_html_map(gdf_eugw, gdf_aoi, gdf_ws, gdf_dens_sand, gdf_dens_rock,
                    png_b64_weighted, raster_bounds_weighted, breaks_weighted, cmap_weighted,
                    png_b64_unweighted, raster_bounds_unweighted):
    """Creates the HTML map"""

    # --- Map extent from points ---
    xmin, ymin, xmax, ymax = gdf_eugw['geometry'].total_bounds
    m = folium.Map(tiles=None)
    m.fit_bounds([[ymin, xmin], [ymax, xmax]])

    # --- MiniMap and Fullscreen ---
    MiniMap(toggle_display=True).add_to(m)
    folium.plugins.Fullscreen(
        position="topright",
        title="Expand me",
        title_cancel="Exit me",
        force_separate_button=True
    ).add_to(m)

    # --- Basemaps ---
    basemap_positron = folium.TileLayer(
        tiles='CartoDB positron',
        name='CartoDB Light',
        overlay=False,
        control=True,
        show=True
    )
    basemap_positron.add_to(m)

    basemap_osm = folium.TileLayer(
        tiles='OpenStreetMap',
        name='OpenStreetMap',
        overlay=False,
        control=True,
        show=False
    )
    basemap_osm.add_to(m)

    satellite_url = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'
    basemap_satellite = folium.TileLayer(
        tiles=satellite_url,
        name='Imagery Basemap',
        attr='Tiles &copy; Esri',
        overlay=False,
        control=True,
        show=False
    )
    basemap_satellite.add_to(m)

    # --- Heatmap raster overlays ---
    # Weighted (by volume) - shown by default
    heatmap_weighted_group = folium.FeatureGroup(name='Heatmap - Weighted by Volume (log10)', show=True)
    img_bounds_w = [[raster_bounds_weighted[1], raster_bounds_weighted[0]],
                    [raster_bounds_weighted[3], raster_bounds_weighted[2]]]
    folium.raster_layers.ImageOverlay(
        image=f"data:image/png;base64,{png_b64_weighted}",
        bounds=img_bounds_w,
        opacity=1.0,
        interactive=False,
        zindex=1
    ).add_to(heatmap_weighted_group)
    heatmap_weighted_group.add_to(m)

    # Unweighted (count only) - hidden by default
    heatmap_unweighted_group = folium.FeatureGroup(name='Heatmap - Count Only (unweighted)', show=False)
    img_bounds_uw = [[raster_bounds_unweighted[1], raster_bounds_unweighted[0]],
                     [raster_bounds_unweighted[3], raster_bounds_unweighted[2]]]
    folium.raster_layers.ImageOverlay(
        image=f"data:image/png;base64,{png_b64_unweighted}",
        bounds=img_bounds_uw,
        opacity=1.0,
        interactive=False,
        zindex=1
    ).add_to(heatmap_unweighted_group)
    heatmap_unweighted_group.add_to(m)

    # --- AOI boundary layer ---
    aoi_group = folium.FeatureGroup(name="Hul'qumi'num Core Territory (AOI)", show=True)
    gdf_aoi_geom = gdf_aoi[['geometry']].copy()
    folium.GeoJson(
        data=gdf_aoi_geom,
        style_function=lambda x: {
            'fillColor': 'transparent',
            'color': '#222222',
            'weight': 3,
            'dashArray': '',
            'fillOpacity': 0
        },
        name="Hul'qumi'num Core Territory (AOI)"
    ).add_to(aoi_group)
    aoi_group.add_to(m)

    # --- Withdrawal Density Layers (added BEFORE points so points render on top) ---
    density_cmap = plt.get_cmap(DENSITY_CMAP, DENSITY_CLASSES)

    def classify_and_add_density(gdf, layer_name, show=False):
        """Classify density values with Jenks and add as styled GeoJson layer."""
        fg = folium.FeatureGroup(name=layer_name, show=show)

        # Get valid density values for classification
        valid_dens = gdf['density_cmd_km2'].dropna()
        valid_dens = valid_dens[valid_dens > 0]

        if len(valid_dens) < 2:
            fg.add_to(m)
            return fg, [], None

        # Natural breaks (Jenks)
        n_classes = min(DENSITY_CLASSES, len(valid_dens.unique()))
        if n_classes < 2:
            n_classes = 2
        breaks = jenkspy.jenks_breaks(valid_dens.tolist(), n_classes=n_classes)
        norm = mcolors.BoundaryNorm(breaks, density_cmap.N)

        def get_fill_colour(density_val):
            if density_val is None or density_val <= 0:
                return 'transparent'
            colour_rgba = density_cmap(norm(density_val))
            return mcolors.to_hex(colour_rgba)

        gdf_styled = gdf.copy()
        gdf_styled['_fill'] = gdf_styled['density_cmd_km2'].apply(get_fill_colour)

        keep_cols = ['AQUIFER_ID', 'MATERIAL', 'total_cmd', 'area_km2',
                     'density_cmd_km2', '_fill', 'geometry']
        keep_cols = [c for c in keep_cols if c in gdf_styled.columns]
        gdf_styled = gdf_styled[keep_cols]

        tip_fields = [c for c in keep_cols if c not in ('geometry', '_fill')]
        tip_aliases = {
            'AQUIFER_ID': 'Aquifer ID',
            'MATERIAL': 'Material',
            'total_cmd': 'Total (CMD)',
            'area_km2': 'Area (km²)',
            'density_cmd_km2': 'Density (CMD/km²)'
        }
        aliases = [tip_aliases.get(f, f) for f in tip_fields]

        folium.GeoJson(
            data=gdf_styled,
            style_function=lambda x: {
                'fillColor': x['properties'].get('_fill', 'transparent'),
                'color': '#333333',
                'weight': 1,
                'fillOpacity': DENSITY_OPACITY
            },
            tooltip=folium.GeoJsonTooltip(
                fields=tip_fields,
                aliases=aliases,
                style='font-size:11px;'
            ),
            popup=folium.GeoJsonPopup(
                fields=tip_fields,
                aliases=aliases,
                max_width=350
            ),
            name=layer_name
        ).add_to(fg)

        fg.add_to(m)
        return fg, breaks, norm

    dens_sand_group, dens_sand_breaks, dens_sand_norm = classify_and_add_density(
        gdf_dens_sand, 'Withdrawal Density - Sand & Gravel', show=False)

    dens_rock_group, dens_rock_breaks, dens_rock_norm = classify_and_add_density(
        gdf_dens_rock, 'Withdrawal Density - Bedrock', show=False)

    # --- EUGW well points ---
    # Create a custom pane with high z-index so points always render above polygons
    eugw_pane_js = """
    <script>
    (function() {
        var check = setInterval(function() {
            var mapObj = null;
            for (var key in window) {
                try { if (window[key] instanceof L.Map) { mapObj = window[key]; break; } }
                catch(e) {}
            }
            if (!mapObj) return;
            clearInterval(check);
            mapObj.createPane('eugwPane');
            mapObj.getPane('eugwPane').style.zIndex = 650;
        }, 100);
    })();
    </script>
    """
    m.get_root().html.add_child(Element(eugw_pane_js))
    gdf_eugw, active_colours = assign_colours(gdf_eugw)

    # Get log scale range
    log_vals = gdf_eugw['qty_cmd_log'].dropna()
    log_min = log_vals.min() if len(log_vals) > 0 else 0
    log_max = log_vals.max() if len(log_vals) > 0 else 1

    # Popup fields
    popup_fields = [
        ('cls_purpose', 'Purpose'),
        ('qty_cmd', 'Volume (CMD)'),
        ('qty_cmd_log', 'Volume (log10)'),
        ('qty_flag', 'Qty Flag'),
    ]
    optional_fields = [
        ('App_Purpose_Name', 'Original Purpose'),
        ('Quantity', 'Original Quantity'),
        ('Quantity_Units', 'Original Units'),
        ('vFCBC_Tracking_Number', 'Tracking #'),
        ('Well_Tag_Number', 'Well Tag'),
        ('Client_Name', 'Client'),
        ('WATER_LICENSING_WATERSHED_NAME', 'Watershed'),
        ('AQUIFER_IDS', 'Aquifer IDs'),
    ]
    for col, label in optional_fields:
        if col in gdf_eugw.columns:
            popup_fields.append((col, label))

    # Create a FeatureGroup per purpose
    purposes_in_data = sorted(gdf_eugw['cls_purpose'].unique())
    purpose_groups = {}

    # Detect overlapping points (same lat/lon, any purpose)
    # Build a dict of location -> list of rows
    location_rows = {}
    for idx, row in gdf_eugw.iterrows():
        key = (round(row.geometry.y, 6), round(row.geometry.x, 6))
        if key not in location_rows:
            location_rows[key] = []
        location_rows[key].append(row)

    # For overlapping locations, build combined popup
    def build_combined_popup(rows, popup_fields):
        """Build a popup showing all applications at this location"""
        if len(rows) == 1:
            return build_popup_html(rows[0], popup_fields)

        html = f'<div style="font-family:Arial;font-size:12px;font-weight:bold;'
        html += f'margin-bottom:6px;color:#2c3e50;">'
        html += f'{len(rows)} applications at this location</div>'

        for i, row in enumerate(rows):
            purpose = row.get('cls_purpose', 'Unknown')
            colour = active_colours.get(purpose, DEFAULT_COLOUR)
            html += f'<div style="border-left:4px solid {colour};padding:4px 8px;'
            html += f'margin:4px 0;background:#f8f8f8;border-radius:0 3px 3px 0;">'
            html += f'<div style="font-weight:bold;font-size:11px;margin-bottom:2px;">'
            html += f'{i+1}. {purpose}</div>'
            html += '<table style="font-size:10px;">'
            for col, label in popup_fields:
                if col == 'cls_purpose':
                    continue  # already shown as header
                val = row.get(col, '')
                if val is None or (isinstance(val, float) and np.isnan(val)):
                    val = '–'
                elif isinstance(val, float):
                    val = f"{val:.4f}" if abs(val) < 100 else f"{val:.2f}"
                html += f'<tr><td style="font-weight:bold;padding:1px 4px;">{label}</td>'
                html += f'<td style="padding:1px 4px;">{val}</td></tr>'
            html += '</table></div>'

        return html

    # Pre-build combined popups keyed by location
    combined_popups = {}
    for key, rows in location_rows.items():
        if len(rows) > 1:
            combined_popups[key] = build_combined_popup(rows, popup_fields)

    for purpose in purposes_in_data:
        colour = active_colours.get(purpose, DEFAULT_COLOUR)
        fg = folium.FeatureGroup(name=purpose, show=True)

        gdf_sub = gdf_eugw[gdf_eugw['cls_purpose'] == purpose]

        for idx, row in gdf_sub.iterrows():
            lat = row.geometry.y
            lon = row.geometry.x
            log_val = row.get('qty_cmd_log', None)
            radius = get_scaled_radius(log_val, log_min, log_max)

            # Use combined popup if overlapping, else single popup
            key = (round(lat, 6), round(lon, 6))
            if key in combined_popups:
                popup_html = combined_popups[key]
                overlap_count = len(location_rows[key])
                tooltip_txt = f"{purpose} | {overlap_count} applications here"
            else:
                popup_html = build_popup_html(row, popup_fields)
                tooltip_txt = f"{purpose} | {row.get('qty_cmd', 'N/A')} CMD"

            folium.CircleMarker(
                location=[lat, lon],
                radius=radius,
                color=colour,
                weight=1,
                fill=True,
                fill_color=colour,
                fill_opacity=0.7,
                pane='eugwPane',
                popup=folium.Popup(popup_html, max_width=420),
                tooltip=tooltip_txt
            ).add_to(fg)

        fg.add_to(m)
        purpose_groups[purpose] = fg

    # --- WMS Layers ---
    aq_group = folium.FeatureGroup(name='Aquifer Classification', show=False)
    aq_layer = folium.raster_layers.WmsTileLayer(
        url=WMS_AQUIFERS,
        fmt='image/png',
        layers='WHSE_WATER_MANAGEMENT.GW_AQUIFERS_CLASSIFICATION_SVW',
        transparent=True,
        overlay=False,
        opacity=0.5
    )
    aq_layer.add_to(aq_group)
    aq_group.add_to(m)

    ws_group = folium.FeatureGroup(name='Water Licensing Watersheds', show=False)
    # Keep only needed columns to reduce HTML size
    ws_cols = ['WATER_LICENSING_WATERSHED_NAME', 'geometry']
    ws_cols_available = [c for c in ws_cols if c in gdf_ws.columns]
    gdf_ws_slim = gdf_ws[ws_cols_available].copy()

    folium.GeoJson(
        data=gdf_ws_slim,
        style_function=lambda x: {
            'fillColor': 'transparent',
            'color': '#5C3317',
            'weight': 2,
            'fillOpacity': 0
        },
        tooltip=folium.GeoJsonTooltip(
            fields=['WATER_LICENSING_WATERSHED_NAME'],
            aliases=['Watershed:'],
            style='font-size:11px;font-weight:bold;'
        ),
        name='Water Licensing Watersheds'
    ).add_to(ws_group)
    ws_group.add_to(m)

    # --- Layer Controls ---
    purpose_group_list = [purpose_groups[p] for p in purposes_in_data]

    # --- Basemap selector (added BEFORE GroupedLayerControl so it sits above) ---
    folium.LayerControl(
        position='topright',
        collapsed=False
    ).add_to(m)

    GroupedLayerControl(
        groups={
            "EUGW APPLICATIONS": purpose_group_list,
            "HEATMAPS": [heatmap_weighted_group, heatmap_unweighted_group],
            "WITHDRAWAL DENSITY": [dens_sand_group, dens_rock_group],
            "BOUNDARIES": [aoi_group],
            "AQUIFERS & WATERSHEDS": [aq_group, ws_group],
        },
        exclusive_groups=False,
        collapsed=False
    ).add_to(m)

    # --- CSS for GroupedLayerControl group headers ---
    group_css = """
    <style>
        .leaflet-control-layers-group-name {
            font-weight: bold !important;
            color: #8b1a1a !important;
            padding: 5px 0 3px 0 !important;
            margin-top: 6px !important;
            border-top: 1px solid #ccc !important;
            display: block !important;
        }
        .leaflet-control-layers-group-name:first-child {
            margin-top: 0 !important;
            border-top: none !important;
        }
        .leaflet-control-layers-group label {
            font-size: 11px !important;
        }
        .leaflet-control-layers-overlays {
            max-height: 60vh;
            overflow-y: auto;
        }
        .leaflet-control-layers {
            max-height: 70vh;
            overflow-y: auto;
        }
    </style>
    """
    m.get_root().html.add_child(Element(group_css))

    # --- Toggle All EUGW button (injected JS) ---
    toggle_js = create_toggle_all_js(purposes_in_data)
    m.get_root().html.add_child(Element(toggle_js))

    # --- Legend ---
    title_txt = 'EUGW Consultation Map'
    subtitle_txt = "Hul'qumi'num Nations Core Territory"
    mapdate_txt = f"Map generated on: {dt.datetime.now().strftime('%B %d, %Y')}"

    legend_html = '''
        <div id="legend" style="position: fixed; 
        bottom: 30px; left: 10px; z-index: 1000; 
        background-color: #fff; padding: 12px 14px; 
        border-radius: 5px; border: 1px solid grey;
        max-height: 80vh; overflow-y: auto;
        font-family: Arial, sans-serif;">
        
        <h3 style="font-weight:bold;color:#2c3e50;margin:0 0 4px 0;">{}</h3>
        <h5 style="font-weight:normal;color:#555;margin:0 0 10px 0;">{}</h5>

        <div style="font-weight:bold;margin-bottom:5px;margin-top:10px;
                    border-bottom:1px solid #ccc;padding-bottom:3px;">
            EUGW Purpose</div>
        <div style="margin:6px 0 8px 0;padding:4px 6px;background:#f0f0f0;
                    border-radius:3px;border:1px solid #ddd;">
            <label style="cursor:pointer;font-size:12px;font-weight:bold;color:#2c3e50;">
                <input type="checkbox" id="eugw-toggle-cb" checked 
                       style="margin-right:6px;cursor:pointer;">
                Show/Hide All Points
            </label>
        </div>
        <div style="font-size:10px;color:#888;margin-bottom:4px;">
            Dot size = withdrawal volume (log10)</div>
        <div style="font-size:9px;color:#b00;margin-bottom:6px;font-style:italic;
                    padding:3px 4px;background:#fff5f5;border-radius:3px;">
            Note: Point locations represent parcel centroids<br>
            of EUGW applications, not actual well locations.</div>
    '''.format(title_txt, subtitle_txt)

    for purpose in purposes_in_data:
        colour = active_colours.get(purpose, DEFAULT_COLOUR)
        legend_html += '''
            <div style="display:flex;align-items:center;margin:2px 0;">
                <span style="background:{};width:12px;height:12px;border-radius:50%;
                             display:inline-block;margin-right:6px;border:1px solid #555;"></span>
                <span style="font-size:11px;">{}</span>
            </div>
        '''.format(colour, purpose)

    # Heatmap legend
    legend_html += '''
        <div style="font-weight:bold;margin:12px 0 5px 0;
                    border-bottom:1px solid #ccc;padding-bottom:3px;">
            Kernel Density (log10)</div>
    '''

    norm = mcolors.BoundaryNorm(breaks_weighted, cmap_weighted.N)
    for i in range(len(breaks_weighted) - 1):
        colour = cmap_weighted(norm((breaks_weighted[i] + breaks_weighted[i + 1]) / 2))
        hex_colour = mcolors.to_hex(colour)
        label = f"{breaks_weighted[i]:.1f} – {breaks_weighted[i + 1]:.1f}"
        legend_html += '''
            <div style="display:flex;align-items:center;margin:2px 0;">
                <span style="background:{};width:20px;height:14px;
                             display:inline-block;margin-right:6px;border:1px solid #555;
                             opacity:{};"></span>
                <span style="font-size:11px;">{}</span>
            </div>
        '''.format(hex_colour, HEATMAP_OPACITY, label)

    # Withdrawal Density legend (shared colour ramp for both material types)
    # Combine breaks from both layers for legend display
    def add_density_legend_section(legend_html, title, breaks, norm):
        if not breaks:
            return legend_html
        legend_html += '''
        <div style="font-weight:bold;margin:12px 0 5px 0;
                    border-bottom:1px solid #ccc;padding-bottom:3px;">
            {}</div>
        '''.format(title)
        for i in range(len(breaks) - 1):
            colour = density_cmap(norm((breaks[i] + breaks[i + 1]) / 2))
            hex_c = mcolors.to_hex(colour)
            label_txt = f"{breaks[i]:.1f} – {breaks[i + 1]:.1f}"
            legend_html += '''
            <div style="display:flex;align-items:center;margin:2px 0;">
                <span style="background:{};width:20px;height:14px;
                             display:inline-block;margin-right:6px;border:1px solid #555;
                             opacity:{};"></span>
                <span style="font-size:11px;">{}</span>
            </div>
            '''.format(hex_c, DENSITY_OPACITY, label_txt)
        return legend_html

    legend_html = add_density_legend_section(
        legend_html, 'Density - Sand & Gravel (CMD/km²)',
        dens_sand_breaks, dens_sand_norm)

    legend_html = add_density_legend_section(
        legend_html, 'Density - Bedrock (CMD/km²)',
        dens_rock_breaks, dens_rock_norm)

    legend_html += '''
        <p style="font-weight:bold;color:black;font-style:italic;
           font-size:10px;margin-top:15px;">{}</p>
        </div>
    '''.format(mapdate_txt)

    m.get_root().html.add_child(Element(legend_html))

    return m


# ============================================================
# MAIN
# ============================================================

if __name__ == '__main__':
    start_t = timeit.default_timer()

    print('\n' + '=' * 60)
    print('EUGW CONSULTATION MAP')
    print('=' * 60)

    # Read EUGW points
    print('\nReading EUGW wells')
    gdf_eugw = prepare_geo_data(EUGW_FC)
    print(f'...{len(gdf_eugw)} features loaded')

    # Read AOI boundary
    print('\nReading AOI boundary')
    gdf_aoi = prepare_geo_data(AOI_FC)
    print(f'...{len(gdf_aoi)} features loaded')

    # Read watersheds
    print('\nReading Water Licensing Watersheds')
    gdf_ws = prepare_geo_data(WATERSHEDS_FC)
    print(f'...{len(gdf_ws)} features loaded')

    # Read withdrawal density layers
    print('\nReading Withdrawal Density - Sand & Gravel')
    gdf_dens_sand = prepare_geo_data(DENSITY_SAND_FC)
    print(f'...{len(gdf_dens_sand)} features loaded')

    print('\nReading Withdrawal Density - Bedrock')
    gdf_dens_rock = prepare_geo_data(DENSITY_ROCK_FC)
    print(f'...{len(gdf_dens_rock)} features loaded')

    # Process heatmap rasters
    print('\nProcessing weighted heatmap raster')
    png_b64_w, bounds_w, breaks_w, cmap_w = process_heatmap_raster(HEATMAP_WEIGHTED_TIF, NUM_CLASSES)

    print('\nProcessing unweighted heatmap raster')
    png_b64_uw, bounds_uw, _, _ = process_heatmap_raster(HEATMAP_UNWEIGHTED_TIF, NUM_CLASSES)

    # Create the map
    print('\nBuilding HTML map')
    m = create_html_map(gdf_eugw, gdf_aoi, gdf_ws, gdf_dens_sand, gdf_dens_rock,
                        png_b64_w, bounds_w, breaks_w, cmap_w,
                        png_b64_uw, bounds_uw)

    # Save
    print('\nSaving map')
    m.save(OUTPUT_HTML)
    print(f'...Map saved to: {OUTPUT_HTML}')

    finish_t = timeit.default_timer()
    t_sec = round(finish_t - start_t)
    mins = int(t_sec / 60)
    secs = int(t_sec % 60)
    print('\nProcessing Completed in {} minutes and {} seconds'.format(mins, secs))