import warnings
warnings.simplefilter(action='ignore')

import os
import oracledb
import pandas as pd
import geopandas as gpd
from shapely import wkb


def connect_to_DB (username,password,hostname):
    """ Returns a connection and cursor to Oracle database"""
    try:
        connection = oracledb.connect(user=username, password=password, dsn=hostname)
        cursor = connection.cursor()
        print  ("....Successffuly connected to the database")
    except:
        raise Exception('....Connection failed! Please check your login parameters')

    return connection, cursor


def read_query(connection,cursor,query,bvars):
    "Returns a df containing SQL Query results"
    cursor.execute(query, bvars)
    names = [x[0] for x in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=names)
    
    return df  


def esri_to_gdf (aoi):
    """Returns a Geopandas file (gdf) based on 
       an ESRI format vector (shp or featureclass/gdb)"""
    
    if '.shp' in aoi: 
        gdf = gpd.read_file(aoi)
    
    elif '.gdb' in aoi:
        l = aoi.split ('.gdb')
        gdb = l[0] + '.gdb'
        fc = os.path.basename(aoi)
        gdf = gpd.read_file(filename= gdb, layer= fc)
        
    else:
        raise Exception ('Format not recognized. Please provide a shp or featureclass (gdb)!')
    
    return gdf


def get_wkb_srid(gdf):
    """Returns SRID and WKB objects from gdf"""
    srid = gdf.crs.to_epsg()
    geom = gdf['geometry'].iloc[0]

    wkb_aoi = wkb.dumps(geom, output_dimension=2)
        
    return wkb_aoi, srid


def df_2_gdf (df, crs):
    """ Return a geopandas gdf based on a df with Geometry column"""
    df['SHAPE'] = df['SHAPE'].astype(str)
    df['geometry'] = gpd.GeoSeries.from_wkt(df['SHAPE'])
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    #df['geometry'] = df['SHAPE'].apply(wkt.loads)
    #gdf = gpd.GeoDataFrame(df, geometry = df['geometry'])
    gdf.crs = "EPSG:" + str(crs)
    del df['SHAPE']
    
    return gdf


def load_queries ():
    """ Returns SQL queries that will be executed"""
    sql = {}

    sql['clean_energy'] = """
        WITH ranked_data AS (
            SELECT
                CAST(IP.INTRID_SID AS NUMBER) INTEREST_PARCEL_ID,
                CAST(DT.DISPOSITION_TRANSACTION_SID AS NUMBER) DISPOSITION_TRANSACTION_ID,
                DS.FILE_CHR AS FILE_NBR,
                SG.STAGE_NME AS STAGE,
                TT.STATUS_NME AS STATUS,
                DT.APPLICATION_TYPE_CDE AS APPLICATION_TYPE,
                TS.EFFECTIVE_DAT AS EFFECTIVE_DATE,
                TY.TYPE_NME AS TENURE_TYPE,
                ST.SUBTYPE_NME AS TENURE_SUBTYPE,
                PU.PURPOSE_NME AS TENURE_PURPOSE,
                SP.SUBPURPOSE_NME AS TENURE_SUBPURPOSE,
                DT.COMMENCEMENT_DAT AS COMMENCEMENT_DATE,
                DT.EXPIRY_DAT AS EXPIRY_DATE,
                DT.LOCATION_DSC,
                OU.UNIT_NAME,
                ROUND(SDO_GEOM.SDO_AREA(SH.SHAPE, 0.005) / 10000, 2) AS PARCEL_AREA_HA,
                ROUND(SDO_GEOM.SDO_AREA(
                                SDO_GEOM.SDO_INTERSECTION(SH.SHAPE, SDO_GEOMETRY(:wkb_aoi, :srid), 0.005),
                                0.005
                            ) / 10000, 2) AS INTERSECTION_AREA_HA,
                CONCAT(PR.LEGAL_NAME, PR.FIRST_NAME || ' ' || PR.LAST_NAME) AS CLIENT_NAME_PRIMARY,
                SDO_UTIL.TO_WKTGEOMETRY(SH.SHAPE) SHAPE,
                MAX(CASE WHEN TT.STATUS_NME = 'DISPOSITION IN GOOD STANDING' THEN 1 ELSE 0 END) 
                    OVER (PARTITION BY DS.FILE_CHR) AS has_digs_status
                
            FROM WHSE_TANTALIS.TA_DISPOSITION_TRANSACTIONS DT 
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
                
            LEFT JOIN WHSE_TANTALIS.TA_INTEREST_PARCEL_SHAPES SH
                ON SH.INTRID_SID = IP.INTRID_SID
                
            WHERE 
                TT.STATUS_NME IN ('DISPOSITION IN GOOD STANDING', 'ACCEPTED') 
                AND TY.TYPE_NME IN ('LICENCE', 'LEASE') 
                AND PU.PURPOSE_NME LIKE '%POWER%'
                AND SDO_RELATE (SH.SHAPE, 
                                SDO_GEOMETRY(:wkb_aoi, :srid), 
                                'mask=ANYINTERACT') = 'TRUE'
        )
        SELECT 
            INTEREST_PARCEL_ID,
            DISPOSITION_TRANSACTION_ID,
            FILE_NBR,
            STAGE,
            STATUS,
            APPLICATION_TYPE,
            EFFECTIVE_DATE,
            TENURE_TYPE,
            TENURE_SUBTYPE,
            TENURE_PURPOSE,
            TENURE_SUBPURPOSE,
            COMMENCEMENT_DATE,
            EXPIRY_DATE,
            LOCATION_DSC,
            UNIT_NAME,
            PARCEL_AREA_HA,
            INTERSECTION_AREA_HA,
            CLIENT_NAME_PRIMARY,
            SHAPE
        FROM ranked_data
        WHERE 
            (has_digs_status = 1 AND STATUS = 'DISPOSITION IN GOOD STANDING')
            OR (has_digs_status = 0 AND STATUS = 'ACCEPTED')
        ORDER BY EFFECTIVE_DATE DESC
        """

    
    return sql

def generate_report (workspace, df_list, sheet_list,filename):
    """ Exports dataframes to multi-tab excel spreasheet"""
    outfile= os.path.join(workspace, filename + '.xlsx')

    writer = pd.ExcelWriter(outfile,engine='xlsxwriter')

    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe = dataframe.reset_index(drop=True)
        dataframe.index = dataframe.index + 1

        dataframe.to_excel(writer, sheet_name=sheet, index=False, startrow=0 , startcol=0)

        worksheet = writer.sheets[sheet]
        #workbook = writer.book

        worksheet.set_column(0, dataframe.shape[1], 20)

        col_names = [{'header': col_name} for col_name in dataframe.columns[1:-1]]
        col_names.insert(0,{'header' : dataframe.columns[0], 'total_string': 'Total'})
        col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'sum'})


        worksheet.add_table(0, 0, dataframe.shape[0]+1, dataframe.shape[1]-1, {
            'total_row': True,
            'columns': col_names})

    #writer.save()
    writer.close()



if __name__ == "__main__":

    workspace = r"W:\srm\gss\projects\gr_2026_129_nicola_clean_energy"

    print ('Connecting to BCGW.')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection, cursor = connect_to_DB (bcgw_user,bcgw_pwd,hostname)

    print ('Reading the shapefile.')
    shp = os.path.join(workspace, "data","nicolaWS_bcAlbers.shp")
    gdf_aoi = esri_to_gdf (shp)

    # Get WKB and SRID
    wkb_aoi, srid = get_wkb_srid(gdf_aoi)

    print ('Running query.')
    cursor.setinputsizes(wkb_aoi=oracledb.BLOB)
    bvars = {'wkb_aoi': wkb_aoi, 'srid': srid}
    
    sql = load_queries()
    df= read_query(connection, cursor, sql['clean_energy'], bvars)

    # attributes only - no geometry
    df_attr = df.drop(columns=['SHAPE'])

    # attributes + geometry
    #gdf = df_2_gdf (df, srid)

    print ('Aggregate results.')
    #aggregate by unique tenures
    watershed_area_ha = gdf_aoi['geometry'].iloc[0].area / 10000  # Convert m² to hectares

    df_grouped = df_attr.groupby([
        'FILE_NBR', 
        'STAGE', 
        'TENURE_TYPE', 
        'TENURE_SUBTYPE', 
        'TENURE_PURPOSE', 
        'TENURE_SUBPURPOSE'
    ], as_index=False).agg({
        'PARCEL_AREA_HA': 'sum',
        'INTERSECTION_AREA_HA': 'sum'
    }).rename(columns={'PARCEL_AREA_HA': 'TENURE_AREA_HA'})

    # Add percentage column
    df_grouped['INTERSECTION_PCT_OF_WSHD'] = round(
        (df_grouped['INTERSECTION_AREA_HA'] / watershed_area_ha) * 100, 4)

    # Save results
    print ('Generating report.')
    generate_report (workspace, [df_attr, df_grouped], ['query_results_full_list', 'aggregate_unique_files'], '20260127_clean_energy_tenures_nicolaWSHD')