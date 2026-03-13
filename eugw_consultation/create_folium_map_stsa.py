#-------------------------------------------------------------------------------
# Name:        STSA EUGW Consultation Interactive Map
#
# Purpose:     Generates an HTML map for STSA EUGW consultation support
#              - EUGW wells: colour-coded by purpose, sized by volume (log10)
#              - STSA boundary (AOI)
#              - Holistic watersheds (individual toggle per watershed)
#              - Aquifer classification (WMS)
#              - Water licensing watersheds (local layer)
#
# Input(s):    GDB with EUGW wells, AOI, holistic watersheds, watersheds
#
# Output(s):   HTML interactive map
#
# Author:      
#
# Created:     2026-03-10
#-------------------------------------------------------------------------------

import warnings
warnings.simplefilter(action='ignore')

import os
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely import wkb
import folium
from folium.plugins import MiniMap, GroupedLayerControl
from branca.element import Element
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import datetime as dt
import timeit


# ============================================================
# CONFIGURATION
# ============================================================
GDB = r"\\spatialfiles.bcgov\work\srm\gss\projects\gr_2026_227_eugw_consultation_support\work\data.gdb"

EUGW_FC = os.path.join(GDB, "stsa_eugw_wells_centroids")
AOI_FC = os.path.join(GDB, "AOI_stsa_stolo_writ")
HOLISTIC_WS_FC = os.path.join(GDB, "stsa_holistic_watersheds")
WATERSHEDS_FC = os.path.join(GDB, "stsa_water_licencing_watersheds")

OUTPUT_HTML = r"\\spatialfiles.bcgov\work\srm\gss\projects\gr_2026_227_eugw_consultation_support\delivrables\stsa_eugw_consultation_map.html"

# WMS
WMS_AQUIFERS = 'https://openmaps.gov.bc.ca/geo/pub/WHSE_WATER_MANAGEMENT.GW_AQUIFERS_CLASSIFICATION_SVW/ows?service=WMS'

# Holistic watersheds name field (adjust if different)
HOLISTIC_NAME_FIELD = 'WTRSHDGRPN'

# Purpose colour scheme
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

# Holistic watershed fill colours (semi-transparent, distinct per watershed)
HOLISTIC_CMAP = 'Set2'

# Dot size range
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
# MAP HELPERS
# ============================================================

def get_scaled_radius(log_val, log_min, log_max):
    """Scale a log value to a circle radius."""
    if log_val is None or np.isnan(log_val) or log_max == log_min:
        return MIN_RADIUS
    scaled = (log_val - log_min) / (log_max - log_min)
    return MIN_RADIUS + scaled * (MAX_RADIUS - MIN_RADIUS)


def build_popup_html(row, popup_fields):
    """Build popup HTML table for a well point."""
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
    """Assign a unique colour to each purpose."""
    purposes_in_data = sorted(gdf['cls_purpose'].unique())
    unmapped = [p for p in purposes_in_data if p not in PURPOSE_COLOURS]
    if unmapped:
        extra_cmap = plt.get_cmap('tab20', len(unmapped))
        for i, purpose in enumerate(unmapped):
            PURPOSE_COLOURS[purpose] = mcolors.to_hex(extra_cmap(i))

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


def create_toggle_all_js(layer_names, checkbox_id, label_text):
    """Creates JS to toggle all layers matching the given names via checkbox clicks."""
    names_js = ', '.join(['"' + n + '"' for n in layer_names])

    js_code = """
    <script>
    (function() {
        var targetNames = [NAMES_PLACEHOLDER];
        var cbId = 'CB_ID_PLACEHOLDER';

        var checkExist = setInterval(function() {
            var cb = document.getElementById(cbId);
            if (!cb) return;

            var allLabels = document.querySelectorAll('.leaflet-control-layers label');
            if (allLabels.length === 0) return;

            clearInterval(checkExist);

            function getMatchingCheckboxes() {
                var checkboxes = [];
                var allLabels = document.querySelectorAll('.leaflet-control-layers label');
                allLabels.forEach(function(label) {
                    var span = label.querySelector('span');
                    var input = label.querySelector('input[type="checkbox"]');
                    if (!span || !input) return;
                    var labelText = span.textContent.trim();
                    for (var i = 0; i < targetNames.length; i++) {
                        if (labelText === targetNames[i]) {
                            checkboxes.push(input);
                            break;
                        }
                    }
                });
                return checkboxes;
            }

            cb.addEventListener('change', function() {
                var visible = cb.checked;
                var boxes = getMatchingCheckboxes();
                boxes.forEach(function(input) {
                    if (input.checked !== visible) {
                        input.click();
                    }
                });
            });

        }, 300);
    })();
    </script>
    """.replace('NAMES_PLACEHOLDER', names_js).replace('CB_ID_PLACEHOLDER', checkbox_id)

    return js_code


# ============================================================
# MAP BUILDING
# ============================================================

def create_html_map(gdf_eugw, gdf_aoi, gdf_ws, gdf_holistic):
    """Creates the STSA HTML map."""

    # --- Map extent from AOI ---
    xmin, ymin, xmax, ymax = gdf_aoi['geometry'].total_bounds
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
        overlay=False, control=True, show=True
    )
    basemap_positron.add_to(m)

    basemap_osm = folium.TileLayer(
        tiles='OpenStreetMap',
        name='OpenStreetMap',
        overlay=False, control=True, show=False
    )
    basemap_osm.add_to(m)

    satellite_url = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'
    basemap_satellite = folium.TileLayer(
        tiles=satellite_url,
        name='Imagery Basemap',
        attr='Tiles &copy; Esri',
        overlay=False, control=True, show=False
    )
    basemap_satellite.add_to(m)

    # --- AOI boundary ---
    aoi_group = folium.FeatureGroup(name='STSA Boundary (AOI)', show=True)
    gdf_aoi_geom = gdf_aoi[['geometry']].copy()
    folium.GeoJson(
        data=gdf_aoi_geom,
        style_function=lambda x: {
            'fillColor': 'transparent',
            'color': '#222222',
            'weight': 3,
            'fillOpacity': 0
        },
        name='STSA Boundary (AOI)'
    ).add_to(aoi_group)
    aoi_group.add_to(m)

    # --- Holistic Watersheds (individual toggle per watershed) ---
    # Detect name field
    name_field = HOLISTIC_NAME_FIELD
    if name_field not in gdf_holistic.columns:
        # Try common alternatives
        for candidate in ['NAME', 'WS_NAME', 'WATERSHED_NAME', 'GNIS_NAME']:
            if candidate in gdf_holistic.columns:
                name_field = candidate
                break

    watershed_names = sorted(gdf_holistic[name_field].dropna().unique())
    ws_cmap = plt.get_cmap(HOLISTIC_CMAP, max(len(watershed_names), 3))
    holistic_groups = {}

    for i, ws_name in enumerate(watershed_names):
        colour = mcolors.to_hex(ws_cmap(i / max(len(watershed_names) - 1, 1)))
        fg = folium.FeatureGroup(name=ws_name, show=True)

        gdf_sub = gdf_holistic[gdf_holistic[name_field] == ws_name]
        # Keep only geometry + name for serialization
        gdf_sub_clean = gdf_sub[[name_field, 'geometry']].copy()

        folium.GeoJson(
            data=gdf_sub_clean,
            style_function=lambda x, c=colour: {
                'fillColor': c,
                'color': '#333333',
                'weight': 2,
                'fillOpacity': 0.25
            },
            tooltip=folium.GeoJsonTooltip(
                fields=[name_field],
                aliases=['Holistic Watershed:'],
                style='font-size:12px;font-weight:bold;'
            ),
            name=ws_name
        ).add_to(fg)

        fg.add_to(m)
        holistic_groups[ws_name] = fg

    # --- EUGW points (custom pane for z-order) ---
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

    # Detect overlapping points
    location_rows = {}
    for idx, row in gdf_eugw.iterrows():
        key = (round(row.geometry.y, 6), round(row.geometry.x, 6))
        if key not in location_rows:
            location_rows[key] = []
        location_rows[key].append(row)

    def build_combined_popup(rows, popup_fields):
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
                    continue
                val = row.get(col, '')
                if val is None or (isinstance(val, float) and np.isnan(val)):
                    val = '–'
                elif isinstance(val, float):
                    val = f"{val:.4f}" if abs(val) < 100 else f"{val:.2f}"
                html += f'<tr><td style="font-weight:bold;padding:1px 4px;">{label}</td>'
                html += f'<td style="padding:1px 4px;">{val}</td></tr>'
            html += '</table></div>'
        return html

    combined_popups = {}
    for key, rows in location_rows.items():
        if len(rows) > 1:
            combined_popups[key] = build_combined_popup(rows, popup_fields)

    purposes_in_data = sorted(gdf_eugw['cls_purpose'].unique())
    purpose_groups = {}

    for purpose in purposes_in_data:
        colour = active_colours.get(purpose, DEFAULT_COLOUR)
        fg = folium.FeatureGroup(name=purpose, show=True)

        gdf_sub = gdf_eugw[gdf_eugw['cls_purpose'] == purpose]

        for idx, row in gdf_sub.iterrows():
            lat = row.geometry.y
            lon = row.geometry.x
            log_val = row.get('qty_cmd_log', None)
            radius = get_scaled_radius(log_val, log_min, log_max)

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

    # --- WMS Aquifers ---
    aq_group = folium.FeatureGroup(name='Aquifer Classification', show=False)
    folium.raster_layers.WmsTileLayer(
        url=WMS_AQUIFERS,
        fmt='image/png',
        layers='WHSE_WATER_MANAGEMENT.GW_AQUIFERS_CLASSIFICATION_SVW',
        transparent=True,
        overlay=False,
        opacity=0.5
    ).add_to(aq_group)
    aq_group.add_to(m)

    # --- Water Licensing Watersheds ---
    ws_group = folium.FeatureGroup(name='Water Licensing Watersheds', show=False)
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

    # --- Basemap selector ---
    folium.LayerControl(
        position='topright',
        collapsed=False
    ).add_to(m)

    # --- Grouped Layer Control ---
    purpose_group_list = [purpose_groups[p] for p in purposes_in_data]
    holistic_group_list = [holistic_groups[n] for n in watershed_names]

    GroupedLayerControl(
        groups={
            "EUGW APPLICATIONS": purpose_group_list,
            "HOLISTIC WATERSHEDS": holistic_group_list,
            "BOUNDARIES": [aoi_group],
            "AQUIFERS & WATERSHEDS": [aq_group, ws_group],
        },
        exclusive_groups=False,
        collapsed=False
    ).add_to(m)

    # --- CSS for group headers ---
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
            max-height: 50vh;
            overflow-y: auto;
        }
        .leaflet-control-layers {
            max-height: 55vh;
            overflow-y: auto;
        }
    </style>
    """
    m.get_root().html.add_child(Element(group_css))

    # --- Toggle All EUGW Points ---
    toggle_eugw_js = create_toggle_all_js(purposes_in_data, 'eugw-toggle-cb', 'Show/Hide All Points')
    m.get_root().html.add_child(Element(toggle_eugw_js))

    # --- Toggle All Holistic Watersheds ---
    toggle_holistic_js = create_toggle_all_js(watershed_names, 'holistic-toggle-cb', 'Show/Hide All Watersheds')
    m.get_root().html.add_child(Element(toggle_holistic_js))

    # --- Legend ---
    title_txt = 'STSA EUGW Consultation Map'
    subtitle_txt = "S'ólh Téméxw Stewardship Alliance"
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

    # Holistic Watersheds legend
    legend_html += '''
        <div style="font-weight:bold;margin:12px 0 5px 0;
                    border-bottom:1px solid #ccc;padding-bottom:3px;">
            Holistic Watersheds</div>
        <div style="margin:6px 0 8px 0;padding:4px 6px;background:#f0f0f0;
                    border-radius:3px;border:1px solid #ddd;">
            <label style="cursor:pointer;font-size:12px;font-weight:bold;color:#2c3e50;">
                <input type="checkbox" id="holistic-toggle-cb" checked 
                       style="margin-right:6px;cursor:pointer;">
                Show/Hide All Watersheds
            </label>
        </div>
    '''

    for i, ws_name in enumerate(watershed_names):
        colour = mcolors.to_hex(ws_cmap(i / max(len(watershed_names) - 1, 1)))
        legend_html += '''
            <div style="display:flex;align-items:center;margin:2px 0;">
                <span style="background:{};width:20px;height:14px;
                             display:inline-block;margin-right:6px;border:1px solid #555;
                             opacity:0.5;"></span>
                <span style="font-size:11px;">{}</span>
            </div>
        '''.format(colour, ws_name)

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
    print('STSA EUGW CONSULTATION MAP')
    print('=' * 60)

    print('\nReading EUGW wells')
    gdf_eugw = prepare_geo_data(EUGW_FC)
    print(f'...{len(gdf_eugw)} features loaded')

    print('\nReading STSA boundary')
    gdf_aoi = prepare_geo_data(AOI_FC)
    print(f'...{len(gdf_aoi)} features loaded')

    print('\nReading Water Licensing Watersheds')
    gdf_ws = prepare_geo_data(WATERSHEDS_FC)
    print(f'...{len(gdf_ws)} features loaded')

    print('\nReading Holistic Watersheds')
    gdf_holistic = prepare_geo_data(HOLISTIC_WS_FC)
    print(f'...{len(gdf_holistic)} features loaded')

    print('\nBuilding HTML map')
    m = create_html_map(gdf_eugw, gdf_aoi, gdf_ws, gdf_holistic)

    print('\nSaving map')
    m.save(OUTPUT_HTML)
    print(f'...Map saved to: {OUTPUT_HTML}')

    finish_t = timeit.default_timer()
    t_sec = round(finish_t - start_t)
    mins = int(t_sec / 60)
    secs = int(t_sec % 60)
    print('\nProcessing Completed in {} minutes and {} seconds'.format(mins, secs))
