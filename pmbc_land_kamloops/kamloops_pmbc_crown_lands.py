import warnings
warnings.simplefilter(action='ignore')

import os
import oracledb
import pandas as pd
import geopandas as gpd
from shapely import wkb


def connect_to_DB(username, password, hostname):
    """Returns a connection and cursor to Oracle database"""
    try:
        connection = oracledb.connect(user=username, password=password, dsn=hostname)
        cursor = connection.cursor()
        print("....Successfully connected to the database")
    except:
        raise Exception('....Connection failed! Please check your login parameters')

    return connection, cursor


def read_query(connection, cursor, query, bvars):
    """Returns a df containing SQL Query results"""
    cursor.execute(query, bvars)
    names = [x[0] for x in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=names)
    
    return df


def esri_to_gdf(aoi):
    """Returns a Geopandas file (gdf) based on 
       an ESRI format vector (shp or featureclass/gdb)"""
    
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


def get_wkb_srid(gdf):
    """Returns SRID and WKB objects from gdf"""
    srid = gdf.crs.to_epsg()
    geom = gdf['geometry'].iloc[0]

    wkb_aoi = wkb.dumps(geom, output_dimension=2)
        
    return wkb_aoi, srid


def df_2_gdf(df, crs):
    """Return a geopandas gdf based on a df with Geometry column"""
    df['SHAPE'] = df['SHAPE'].astype(str)
    df['geometry'] = gpd.GeoSeries.from_wkt(df['SHAPE'])
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    gdf.crs = "EPSG:" + str(crs)
    del df['SHAPE']
    
    return gdf


def load_query_by_owner_type(owner_type):
    """Returns SQL query for a specific owner type"""
    
    sql = f"""
WITH KAMLOOPS_BOUNDARY AS (
    SELECT SHAPE
    FROM WHSE_LEGAL_ADMIN_BOUNDARIES.ABMS_MUNICIPALITIES_SP
    WHERE ADMIN_AREA_ABBREVIATION = 'Kamloops'
),

KAMLOOPS_PARCELS AS (
    SELECT
        pmbc.SHAPE,
        pmbc.MUNICIPALITY,
        pmbc.OWNER_TYPE,
        pmbc.PARCEL_FABRIC_POLY_ID,
        pmbc.PLAN_NUMBER,
        pmbc.PIN,
        pmbc.PID,
        pmbc.PARCEL_CLASS,
        pmbc.FEATURE_AREA_SQM
    FROM 
        WHSE_CADASTRE.PMBC_PARCEL_FABRIC_POLY_SVW pmbc,
        KAMLOOPS_BOUNDARY kb
    WHERE 
        SDO_WITHIN_DISTANCE(pmbc.SHAPE, kb.SHAPE, 'distance=20000') = 'TRUE'
        AND pmbc.OWNER_TYPE = '{owner_type}'
        AND pmbc.FEATURE_AREA_SQM >= 8093.71
),

LAND_ACT_TENURE AS (
    SELECT
        CAST(IP.INTRID_SID AS NUMBER) INTEREST_PARCEL_ID,
        CAST(DT.DISPOSITION_TRANSACTION_SID AS NUMBER) DISPOSITION_TRANSACTION_ID,
        DS.FILE_CHR AS FILE_NBR,
        SG.STAGE_NME AS STAGE,
        TT.STATUS_NME AS STATUS,
        TY.TYPE_NME AS TENURE_TYPE,
        ST.SUBTYPE_NME AS TENURE_SUBTYPE,
        PU.PURPOSE_NME AS TENURE_PURPOSE,
        SP.SUBPURPOSE_NME AS TENURE_SUBPURPOSE,
        DT.COMMENCEMENT_DAT AS COMMENCEMENT_DATE,
        DT.EXPIRY_DAT AS EXPIRY_DATE,
        CONCAT(PR.LEGAL_NAME, PR.FIRST_NAME || ' ' || PR.LAST_NAME) AS CLIENT_NAME_PRIMARY,
        SH.SHAPE,
        SH.FEATURE_AREA_SQM AS TENURE_AREA_SQM
        
    FROM 
        KAMLOOPS_BOUNDARY kb,
        WHSE_TANTALIS.TA_DISPOSITION_TRANSACTIONS DT 
    JOIN WHSE_TANTALIS.TA_INTEREST_PARCELS IP 
        ON DT.DISPOSITION_TRANSACTION_SID = IP.DISPOSITION_TRANSACTION_SID
        AND IP.EXPIRY_DAT IS NULL
    JOIN WHSE_TANTALIS.TA_DISP_TRANS_STATUSES TS
        ON DT.DISPOSITION_TRANSACTION_SID = TS.DISPOSITION_TRANSACTION_SID 
        AND TS.EXPIRY_DAT IS NULL
    JOIN WHSE_TANTALIS.TA_DISPOSITIONS DS
        ON DS.DISPOSITION_SID = DT.DISPOSITION_SID
    JOIN WHSE_TANTALIS.TA_STAGES SG 
        ON SG.CODE_CHR = TS.CODE_CHR_STAGE
    JOIN WHSE_TANTALIS.TA_STATUS TT 
        ON TT.CODE_CHR = TS.CODE_CHR_STATUS
    JOIN WHSE_TANTALIS.TA_AVAILABLE_TYPES TY 
        ON TY.TYPE_SID = DT.TYPE_SID    
    JOIN WHSE_TANTALIS.TA_AVAILABLE_SUBTYPES ST 
        ON ST.SUBTYPE_SID = DT.SUBTYPE_SID 
        AND ST.TYPE_SID = DT.TYPE_SID 
    JOIN WHSE_TANTALIS.TA_AVAILABLE_PURPOSES PU 
        ON PU.PURPOSE_SID = DT.PURPOSE_SID    
    JOIN WHSE_TANTALIS.TA_AVAILABLE_SUBPURPOSES SP 
        ON SP.SUBPURPOSE_SID = DT.SUBPURPOSE_SID 
        AND SP.PURPOSE_SID = DT.PURPOSE_SID 
    JOIN WHSE_TANTALIS.TA_ORGANIZATION_UNITS OU 
        ON OU.ORG_UNIT_SID = DT.ORG_UNIT_SID 
    JOIN WHSE_TANTALIS.TA_TENANTS TE 
        ON TE.DISPOSITION_TRANSACTION_SID = DT.DISPOSITION_TRANSACTION_SID
        AND TE.SEPARATION_DAT IS NULL
        AND TE.PRIMARY_CONTACT_YRN = 'Y'
    JOIN WHSE_TANTALIS.TA_INTERESTED_PARTIES PR
        ON PR.INTERESTED_PARTY_SID = TE.INTERESTED_PARTY_SID
    JOIN WHSE_TANTALIS.TA_INTEREST_PARCEL_SHAPES SH
        ON SH.INTRID_SID = IP.INTRID_SID
    WHERE 
        TT.STATUS_NME = 'DISPOSITION IN GOOD STANDING'
        AND SDO_WITHIN_DISTANCE(SH.SHAPE, kb.SHAPE, 'distance=20000') = 'TRUE'
)

SELECT
    'Kamloops' AS ADMIN_AREA_ABBREVIATION,
    kp.MUNICIPALITY,
    kp.OWNER_TYPE,
    kp.PARCEL_FABRIC_POLY_ID,
    kp.PLAN_NUMBER,
    kp.PIN,
    kp.PID,
    kp.PARCEL_CLASS,
    ROUND(kp.FEATURE_AREA_SQM / 10000, 2) AS PARCEL_AREA_HA,
    CASE 
        WHEN lat.INTEREST_PARCEL_ID IS NOT NULL THEN 'YES'
        ELSE 'NO'
    END AS OVERLAPS_LAND_ACT_TENURE,
    lat.INTEREST_PARCEL_ID AS TENURE_INTEREST_PARCEL_ID,
    lat.DISPOSITION_TRANSACTION_ID AS TENURE_DISPOSITION_TRANSACTION_ID,
    lat.FILE_NBR AS TENURE_FILE_NBR,
    lat.TENURE_TYPE,
    lat.TENURE_SUBTYPE,
    lat.TENURE_PURPOSE,
    lat.TENURE_SUBPURPOSE,
    lat.STAGE AS TENURE_STAGE,
    lat.STATUS AS TENURE_STATUS,
    lat.COMMENCEMENT_DATE AS TENURE_COMMENCEMENT_DATE,
    lat.EXPIRY_DATE AS TENURE_EXPIRY_DATE,
    lat.CLIENT_NAME_PRIMARY AS TENURE_CLIENT_NAME,
    ROUND(lat.TENURE_AREA_SQM / 10000, 2) AS TENURE_AREA_HA,
    ROUND(SDO_GEOM.SDO_AREA(
        SDO_GEOM.SDO_INTERSECTION(lat.SHAPE, kp.SHAPE, 0.005),
        0.005, 'unit=SQ_METER') / 10000, 2) AS TENURE_PARCEL_OVERLAP_HA,
    ROUND(
        (SDO_GEOM.SDO_AREA(
            SDO_GEOM.SDO_INTERSECTION(lat.SHAPE, kp.SHAPE, 0.005),
            0.005, 'unit=SQ_METER') / kp.FEATURE_AREA_SQM) * 100, 2
    ) AS OVERLAP_PERCENT
  
FROM 
    KAMLOOPS_PARCELS kp
    LEFT JOIN LAND_ACT_TENURE lat
        ON SDO_RELATE(lat.SHAPE, kp.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
    """
    
    return sql


def generate_report(workspace, df_list, sheet_list, filename):
    """Exports dataframes to multi-tab excel spreadsheet"""
    outfile = os.path.join(workspace, filename + '.xlsx')

    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')

    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe = dataframe.reset_index(drop=True)
        dataframe.index = dataframe.index + 1

        dataframe.to_excel(writer, sheet_name=sheet, index=False, startrow=0, startcol=0)

        worksheet = writer.sheets[sheet]

        worksheet.set_column(0, dataframe.shape[1], 20)

        col_names = [{'header': col_name} for col_name in dataframe.columns[1:-1]]
        col_names.insert(0, {'header': dataframe.columns[0], 'total_string': 'Total'})
        col_names.append({'header': dataframe.columns[-1], 'total_function': 'sum'})

        worksheet.add_table(0, 0, dataframe.shape[0] + 1, dataframe.shape[1] - 1, {
            'total_row': True,
            'columns': col_names})

    writer.close()


if __name__ == "__main__":

    workspace = r"W:\srm\gss\projects\gr_2026_130_kamloops_overlap_analysis\work"

    print('Connecting to BCGW')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection, cursor = connect_to_DB(bcgw_user, bcgw_pwd, hostname)

    print('Running Queries')
    # List of owner types to process
    owner_types = ['Crown Provincial', 'Crown Agency', 'Untitled Provincial', 'Federal']
    
    # Lists to store results
    df_list = []
    sheet_list = []
    
    # Loop through each owner type and run query
    for owner_type in owner_types:
        print(f'**running query for: {owner_type}**')
        sql = load_query_by_owner_type(owner_type)
        df = pd.read_sql(sql, connection)
        df_list.append(df)
        sheet_list.append(owner_type)
        print(f'   - Retrieved {len(df)} records')

    # Close database connection
    cursor.close()
    connection.close()
    print('Database connection closed.')

    # Generate report with all owner types as separate sheets
    print('Generating report.')
    generate_report(workspace, df_list, sheet_list, '20260130_kamloops_crown_pmbc_tenure_analysis')
    
    print('Processing complete.')