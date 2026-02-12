"""
Haida Gwaii Interests Report - SQL Queries
==========================================
Each query spatially intersects the target dataset with Landscape Units
filtered to Haida Gwaii only. The first column returned is always
LANDSCAPE_UNIT_NAME.
"""

# Haida Gwaii Landscape Units used as a spatial filter in all queries
HAIDA_GWAII_LU = (
    "'Beresford','Jalun','Naikoon','Otun','Eden Lake','Ian',"
    "'Athlow Bay','Masset Inlet','Lower Yakoun','Rennell','Tlell',"
    "'Yakoun Lake','Gudal','Honna','Skidegate Lake','Hibben',"
    "'Sewell','Louise Island','Tasu','Lyell Island Group','Gowgaia',"
    "'Bigsby','Skincuttle','Kunghit Island'"
)


def load_queries():
    sql = {}

    # =========================================================================
    # 1 - Crown Land Tenures
    # =========================================================================
    sql['1-Crown Land Tenures'] = f"""
        SELECT
            lu.LANDSCAPE_UNIT_NAME,
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
            DT.LOCATION_DSC,
            CONCAT(PR.LEGAL_NAME, PR.FIRST_NAME || ' ' || PR.LAST_NAME) AS CLIENT_NAME_PRIMARY,
            ROUND((SH.FEATURE_AREA_SQM)/10000,2) AS AREA_HA
            
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
            
        JOIN WHSE_TANTALIS.TA_INTEREST_PARCEL_SHAPES SH
            ON SH.INTRID_SID = IP.INTRID_SID
        
        INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
            ON SDO_ANYINTERACT(SH.SHAPE, lu.GEOMETRY) = 'TRUE'

        WHERE 
            lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
            AND TT.STATUS_NME = 'DISPOSITION IN GOOD STANDING' 
            AND TY.TYPE_NME NOT IN ('RESERVE/NOTATION', 'TRANSFER OF ADMINISTRATION/CONTROL')
"""

    # =========================================================================
    # 2 - Crown Reserves & Notations
    # =========================================================================
    sql['2-Crown Reserves-Notations'] = f"""
        SELECT
            lu.LANDSCAPE_UNIT_NAME,
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
            DT.LOCATION_DSC,
            CONCAT(PR.LEGAL_NAME, PR.FIRST_NAME || ' ' || PR.LAST_NAME) AS CLIENT_NAME_PRIMARY,
            ROUND((SH.FEATURE_AREA_SQM)/10000,2) AS AREA_HA
            
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
            
        JOIN WHSE_TANTALIS.TA_INTEREST_PARCEL_SHAPES SH
            ON SH.INTRID_SID = IP.INTRID_SID
        
        INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
            ON SDO_ANYINTERACT(SH.SHAPE, lu.GEOMETRY) = 'TRUE'

        WHERE 
            lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
            AND TT.STATUS_NME = 'DISPOSITION IN GOOD STANDING' 
            AND TY.TYPE_NME ='RESERVE/NOTATION'
"""

    # =========================================================================
    # 3 - Crown Reversions
    # =========================================================================
    sql['3-Crown Reversions'] = f"""
SELECT
    lu.LANDSCAPE_UNIT_NAME,
    r.REVERSION_SID,
    r.WHEN_REVERSION_DAT,
    r.SURFACE_OR_UNDER_CDE,
    r.FOLIO_CHR,
    r.WHEN_ABSOLUTE_DAT,
    r.WHEN_REDEEMED_DAT,
    r.LTO_TITLE_CHR,
    r.NOTE_DSC,
    ROUND((r.FEATURE_AREA_SQM)/10000,2) AS AREA_HA
FROM WHSE_TANTALIS.TA_REVERSIONS_SVW r
INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
    ON SDO_ANYINTERACT(r.SHAPE, lu.GEOMETRY) = 'TRUE'
WHERE lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
"""

    # =========================================================================
    # 4 - Crown Acquisitions
    # =========================================================================
    sql['4-Crown Acquisitions'] = f"""
SELECT
    lu.LANDSCAPE_UNIT_NAME,
    a.ACQUISITION_SID,
    a.ACQUISITION_TYPE_CDE,
    a.ACQUISITION_DAT,
    a.LAND_REFERENCE_FILE_CHR,
    a.NOTE_CHR,
    a.SURFACE_OR_UNDER_CDE,
    a.CONVEYANCE_CHR,
    ROUND((a.FEATURE_AREA_SQM)/10000,2) AS AREA_HA
FROM WHSE_TANTALIS.TA_ACQUISITIONS_SVW a
INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
    ON SDO_ANYINTERACT(a.SHAPE, lu.GEOMETRY) = 'TRUE'
WHERE lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
"""

    # =========================================================================
    # 5 - Transfers Admin-Control
    # =========================================================================
    sql['5-Transfers Admin-Control'] = f"""
        SELECT
            lu.LANDSCAPE_UNIT_NAME,
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
            DT.LOCATION_DSC,
            CONCAT(PR.LEGAL_NAME, PR.FIRST_NAME || ' ' || PR.LAST_NAME) AS CLIENT_NAME_PRIMARY,
            ROUND((SH.FEATURE_AREA_SQM)/10000,2) AS AREA_HA
            
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
            
        JOIN WHSE_TANTALIS.TA_INTEREST_PARCEL_SHAPES SH
            ON SH.INTRID_SID = IP.INTRID_SID
        
        INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
            ON SDO_ANYINTERACT(SH.SHAPE, lu.GEOMETRY) = 'TRUE'

        WHERE 
            lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
            AND TT.STATUS_NME = 'DISPOSITION IN GOOD STANDING' 
            AND TY.TYPE_NME ='TRANSFER OF ADMINISTRATION/CONTROL'
"""


    # =========================================================================
    # 6 - Conservancy Areas
    # =========================================================================
    sql['6-Conservancy Areas'] = f"""
SELECT
    lu.LANDSCAPE_UNIT_NAME,
    c.CONSERVANCY_AREA_NAME,
    c.ESTABLISHMENT_DATE,
    ROUND((c.FEATURE_AREA_SQM)/10000,2) AS AREA_HA
FROM WHSE_TANTALIS.TA_CONSERVANCY_AREAS_SVW c
INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
    ON SDO_ANYINTERACT(c.SHAPE, lu.GEOMETRY) = 'TRUE'
WHERE 
    lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
"""

    # =========================================================================
    # 7 - Forest Managed Licences
    # =========================================================================
    sql['7-Forest Managed Licences'] = f"""
SELECT
    lu.LANDSCAPE_UNIT_NAME,
    m.FOREST_FILE_ID,
    m.MAP_BLOCK_ID,
    m.MAP_LABEL,
    m.ML_TYPE_CODE,
    m.ML_COMMENT,
    m.FILE_STATUS_CODE,
    m.CLIENT_NAME,
    ROUND((m.FEATURE_AREA_SQM)/10000,2) AS AREA_HA
FROM WHSE_FOREST_TENURE.FTEN_MANAGED_LICENCE_POLY_SVW m
INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
    ON SDO_ANYINTERACT(m.GEOMETRY, lu.GEOMETRY) = 'TRUE'
WHERE 
    lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
    AND LIFE_CYCLE_STATUS_CODE = 'ACTIVE'
"""

    # =========================================================================
    # 8 - Forest Harvest Auth
    # =========================================================================
    sql['8-Forest Harvest Auth'] = f"""
SELECT
    lu.LANDSCAPE_UNIT_NAME,
    h.FOREST_FILE_ID,
    h.CUTTING_PERMIT_ID,
    h.MAP_LABEL,
    h.FILE_TYPE_DESCRIPTION,
    h.FILE_STATUS_CODE,
    h.LIFE_CYCLE_STATUS_CODE,
    h.HARVEST_AUTH_STATUS_CODE,
    h.ISSUE_DATE,
    h.EXPIRY_DATE,
    h.EXTEND_DATE,
    h.LOCATION,
    h.CLIENT_NAME,
    ROUND((h.FEATURE_AREA_SQM)/10000,2) AS AREA_HA
FROM WHSE_FOREST_TENURE.FTEN_HARVEST_AUTH_POLY_SVW h
INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
    ON SDO_ANYINTERACT(h.GEOMETRY, lu.GEOMETRY) = 'TRUE'
WHERE 
    lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
    AND h.LIFE_CYCLE_STATUS_CODE = 'ACTIVE'
"""

    # =========================================================================
    # 9 - Old Growth Deferrals
    # =========================================================================
    sql['9-Old Growth Deferrals'] = f"""
SELECT
    d.LANDSCAPE_UNIT_NAME,
    d.CURRENT_PRIORITY_DEFERRAL_ID,
    d.TAP_CLASSIFICATION_LABEL,
    d.PRIORITY_BIG_TREED_OG_DESCR,
    d.ANCIENT_FOREST_DESCR,
    d.REMNANT_OLD_ECOSYS_DESCR,
    d.BGC_LABEL,
    d.SOURCE,
    ROUND((d.FEATURE_AREA_SQM)/10000,2) AS AREA_HA
FROM WHSE_FOREST_VEGETATION.OGSR_PRIORITY_DEF_AREA_CUR_SP d
WHERE 
    d.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
"""

    # =========================================================================
    # 10 - Range Tenures
    # =========================================================================
    sql['10-Range Tenures'] = f"""
SELECT
    lu.LANDSCAPE_UNIT_NAME,
    d.FOREST_FILE_ID,
    d.MAP_BLOCK_ID,
    d.MAP_LABEL,
    d.LIFE_CYCLE_STATUS_CODE,
    d.CLIENT_NUMBER,
    d.FILE_TYPE_CODE,
    d.AUTHORIZED_USE,
    d.TOTAL_ANNUAL_USE,
    ROUND((d.FEATURE_AREA_SQM)/10000,2) AS AREA_HA
FROM WHSE_FOREST_TENURE.FTEN_RANGE_POLY_SVW d
INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
    ON SDO_ANYINTERACT(d.GEOMETRY, lu.GEOMETRY) = 'TRUE'
WHERE 
    lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
    AND d.LIFE_CYCLE_STATUS_CODE = 'ACTIVE'
"""

    # =========================================================================
    # 11 - Recreation Polygons
    # =========================================================================
    sql['11-Recreation Polygons'] = f"""
SELECT
    lu.LANDSCAPE_UNIT_NAME,
    d.FOREST_FILE_ID,
    d.SECTION_ID,
    d.RECREATION_MAP_FEATURE_CODE,
    d.LIFE_CYCLE_STATUS_CODE,
    d.PROJECT_TYPE,
    d.PROJECT_NAME,
    d.ARCH_IMPACT_ASSESS_IND,
    d.SITE_LOCATION,
    d.PROJECT_ESTABLISHED_DATE,
    ROUND((d.FEATURE_AREA_SQM)/10000,2) AS AREA_HA
FROM WHSE_FOREST_TENURE.FTEN_RECREATION_POLY_SVW d
INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
    ON SDO_ANYINTERACT(d.GEOMETRY, lu.GEOMETRY) = 'TRUE'
WHERE 
    lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
    AND d.LIFE_CYCLE_STATUS_CODE = 'ACTIVE'
"""

     # =========================================================================
    # 12 - Recreation Lines
    # =========================================================================
    sql['12-Recreation Lines'] = f"""
SELECT
    lu.LANDSCAPE_UNIT_NAME,
    d.FOREST_FILE_ID,
    d.SECTION_ID,
    d.RECREATION_MAP_FEATURE_CODE,
    d.LIFE_CYCLE_STATUS_CODE,
    d.PROJECT_TYPE,
    d.PROJECT_NAME,
    d.ARCH_IMPACT_ASSESS_IND,
    d.SITE_LOCATION,
    d.PROJECT_ESTABLISHED_DATE,
    ROUND((d.FEATURE_LENGTH),2) AS LENGTH_M
FROM WHSE_FOREST_TENURE.FTEN_RECREATION_LINES_SVW d
INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
    ON SDO_ANYINTERACT(d.GEOMETRY, lu.GEOMETRY) = 'TRUE'
WHERE 
    lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
    AND d.LIFE_CYCLE_STATUS_CODE = 'ACTIVE'
"""
    
    # =========================================================================
    # 13 - Mineral Tenures
    # =========================================================================
    sql['13-Mineral Tenures'] = f"""
SELECT
    lu.LANDSCAPE_UNIT_NAME,
    mt.TENURE_NUMBER_ID,
    mt.CLAIM_NAME,
    mt.TENURE_TYPE_DESCRIPTION,
    mt.TENURE_SUB_TYPE_DESCRIPTION,
    mt.TITLE_TYPE_DESCRIPTION,
    mt.ISSUE_DATE,
    mt.GOOD_TO_DATE,
    mt.PROTECTED_IND,
    mt.REVISION_NUMBER,
    mt.TAG_NUMBER,
    mt.NUMBER_OF_OWNERS,
    mt.OWNER_NAME,
    mt.PERCENT_OWNERSHIP,
    ROUND((mt.FEATURE_AREA_SQM)/10000,2) AS AREA_HA
FROM WHSE_MINERAL_TENURE.MTA_ACQUIRED_TENURE_GOV_SVW mt
INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
    ON SDO_ANYINTERACT(mt.GEOMETRY, lu.GEOMETRY) = 'TRUE'
WHERE lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
"""

    # =========================================================================
    # 14 - Crown Granted Mineral Claims
    # =========================================================================
    sql['14-CG Mineral Claims'] = f"""
SELECT
    lu.LANDSCAPE_UNIT_NAME,
    cg.MTA_CGMC_SYSID,
    cg.PIN_SID,
    cg.DISTRICT_LOT,
    cg.LAND_DISTRICT,
    cg.MINING_DIVISION,
    cg.CLAIM_NAME,
    cg.LOT_STATUS,
    cg.SURV_LOT_METHOD_CDE,
    cg.LOT_METHOD,
    cg.DATE_OPENED,
    ROUND((cg.FEATURE_AREA_SQM)/10000,2) AS AREA_HA
FROM WHSE_MINERAL_TENURE.MTA_CROWN_GRANT_MIN_CLAIM_SVW cg
INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
    ON SDO_ANYINTERACT(cg.SHAPE, lu.GEOMETRY) = 'TRUE'
WHERE lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
"""

    # =========================================================================
    # 15 - Mining Permits
    # =========================================================================
    sql['15-Mining Permits'] = f"""
SELECT
    lu.LANDSCAPE_UNIT_NAME,
    mp.HSP_MMPA_SYSID,
    mp.STATUS_DESC,
    mp.ISSUE_DATE,
    mp.MINE_NAME,
    mp.OP_STATUS_TYPE,
    mp.OP_STATUS_DESC,
    mp.OP_STATUS_REASON_DESC,
    mp.OP_STATUS_DATE,
    mp.OP_STATUS_REASON_DESC,
    mp.PERMITTEE_NAME,
    ROUND((mp.FEATURE_AREA_SQM)/10000,2) AS AREA_HA
FROM WHSE_MINERAL_TENURE.HSP_MJR_MINES_PERMTTD_AREAS_SP mp
INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
    ON SDO_ANYINTERACT(mp.SHAPE, lu.GEOMETRY) = 'TRUE'
WHERE 
    lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
    AND OP_STATUS_CODE = 'Operating'
"""

    # =========================================================================
    # 16 - Water Licences
    # =========================================================================
    sql['16-Water Licences'] = f"""
SELECT
    lu.LANDSCAPE_UNIT_NAME,
    w.POD_NUMBER,
    w.POD_SUBTYPE,
    w.POD_DIVERSION_TYPE,
    w.POD_STATUS,
    w.FILE_NUMBER,
    w.WELL_TAG_NUMBER,
    w.LICENCE_NUMBER,
    w.LICENCE_STATUS,
    w.LICENCE_STATUS_DATE,
    w.PRIORITY_DATE,
    w.EXPIRY_DATE,
    w.PURPOSE_USE,
    w.SOURCE_NAME,
    w.QUANTITY,
    w.QUANTITY_UNITS,
    w.QUANTITY_FLAG_DESCRIPTION,
    w.HYDRAULIC_CONNECTIVITY,
    w.PERMIT_OVER_CROWN_LAND_NUMBER,
    w.PRIMARY_LICENSEE_NAME

FROM WHSE_WATER_MANAGEMENT.WLS_WATER_RIGHTS_LICENCES_SP w
INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
    ON SDO_ANYINTERACT(w.SHAPE, lu.GEOMETRY) = 'TRUE'
WHERE 
    lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
    AND LICENCE_STATUS = 'Current'
"""

    # =========================================================================
    # 17 - Community Watersheds
    # =========================================================================
    sql['17-Community Watersheds'] = f"""
SELECT
    lu.LANDSCAPE_UNIT_NAME,
    cw.CW_CODE,
    cw.CW_NAME,
    cw.CW_SOURCE_NAME,
    cw.CW_SOURCE_TYPE,
    cw.POD_NUMBER,
    cw.CW_LEGISLATION,
    cw.CW_DATE_CREATED,
    cw.CW_STATUS,
    cw.ORGANIZATION,
    cw.ORGANIZATION_TYPE,
    cw.WATER_SYSTEM_NAME,
    cw.CW_USE,
    cw.CW_ASSESSMENT_IND,
    cw.WATER_QLTY_OBJ_STATUS,
    cw.GENERAL_COMMENTS,
    ROUND((cw.FEATURE_AREA_SQM)/10000,2) AS AREA_HA
    
FROM WHSE_WATER_MANAGEMENT.WLS_COMMUNITY_WS_PUB_SVW cw
INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
    ON SDO_ANYINTERACT(cw.SHAPE, lu.GEOMETRY) = 'TRUE'
WHERE lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
"""

    # =========================================================================
    # 18 - Traplines
    # =========================================================================
    sql['18-Traplines'] = f"""
SELECT
    lu.LANDSCAPE_UNIT_NAME,
    tr.TRAPLINE_AREA_IDENTIFIER,
    ROUND((tr.FEATURE_AREA_SQM)/10000,2) AS AREA_HA
FROM WHSE_WILDLIFE_MANAGEMENT.WAA_TRAPLINE_AREAS_SP tr
INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
    ON SDO_ANYINTERACT(tr.GEOMETRY, lu.GEOMETRY) = 'TRUE'
WHERE lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
ORDER BY tr.TRAPLINE_AREA_IDENTIFIER
"""

    # =========================================================================
    # 19 - Guide Outfitter Areas
    # =========================================================================
    sql['19-Guide Outfitter Areas'] = f"""
SELECT
    lu.LANDSCAPE_UNIT_NAME,
    go.GUIDING_CERTIFICATE_NO,
    go.GUIDE_FULL_NAME,
    go.STATUS,
    go.TERRITORY,
    go.CERTIFICATE_HOLDER_FULL_NAME,
    ROUND((go.FEATURE_AREA_SQM)/10000,2) AS AREA_HA
FROM WHSE_WILDLIFE_MANAGEMENT.WAA_GUIDE_OUTFITTER_AREA_SVW go
INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
    ON SDO_ANYINTERACT(go.GEOMETRY, lu.GEOMETRY) = 'TRUE'
WHERE 
    lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
    AND go.STATUS = 'ACTIVE'
ORDER BY CERTIFICATE_HOLDER_FULL_NAME
"""

    # =========================================================================
    # 20 - Parks and Protected Areas
    # =========================================================================
    sql['20-Parks-Protected Areas'] = f"""
SELECT
    lu.LANDSCAPE_UNIT_NAME,
    p.PROTECTED_LANDS_DESIGNATION,
    p.PROTECTED_LANDS_NAME,
    p.PARK_CLASS,
    p.ESTABLISHMENT_DATE,
    ROUND((p.FEATURE_AREA_SQM)/10000,2) AS AREA_HA
FROM WHSE_TANTALIS.TA_PARK_ECORES_PA_SVW p
INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
    ON SDO_ANYINTERACT(p.SHAPE, lu.GEOMETRY) = 'TRUE'
WHERE lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
"""


    # =========================================================================
    # 21 - Ungulate Winter Ranges
    # =========================================================================
    sql['21-Ungulate Winter Ranges'] = f"""
SELECT
    lu.LANDSCAPE_UNIT_NAME,
    u.UWR_NUMBER,
    u.UWR_UNIT_NUMBER,
    u.SPECIES_1,
    u.SPECIES_2,
    u.APPROVAL_DATE,
    u.DATE_OF_NOTICE,
    u.FEATURE_NOTES,
    u.LEGISLATION_ACT_NAME,
    u.TIMBER_HARVEST_CODE,
    ROUND((u.FEATURE_AREA_SQM)/10000,2) AS AREA_HA
FROM WHSE_WILDLIFE_MANAGEMENT.WCP_UNGULATE_WINTER_RANGE_SP u
INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
    ON SDO_ANYINTERACT(u.GEOMETRY, lu.GEOMETRY) = 'TRUE'
WHERE lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
"""

    # =========================================================================
    # 22 - Wildlife Habitat Areas
    # =========================================================================
    sql['22-Wildlife Habitat Areas'] = f"""
SELECT
    lu.LANDSCAPE_UNIT_NAME,
    wha.TAG,
    wha.COMMON_SPECIES_NAME,
    wha.APPROVAL_DATE,
    wha.NOTICE_DATE,
    wha.LEGISLATION_ACT_NAME,
    wha.TIMBER_HARVEST_CODE,
    ROUND((wha.FEATURE_AREA_SQM)/10000,2) AS AREA_HA
FROM WHSE_WILDLIFE_MANAGEMENT.WCP_WILDLIFE_HABITAT_AREA_POLY wha
INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
    ON SDO_ANYINTERACT(wha.GEOMETRY, lu.GEOMETRY) = 'TRUE'
WHERE lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
"""

    # =========================================================================
    # 23 - Legal OGMAs
    # =========================================================================
    sql['23-Legal OGMAs'] = f"""
SELECT
    lu.LANDSCAPE_UNIT_NAME,
    og.LEGAL_OGMA_PROVID,
    og.OGMA_TYPE,
    og.LEGALIZATION_FRPA_DATE,
    ROUND((og.FEATURE_AREA_SQM)/10000,2) AS AREA_HA
FROM WHSE_LAND_USE_PLANNING.RMP_OGMA_LEGAL_CURRENT_SVW og
INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
    ON SDO_ANYINTERACT(og.GEOMETRY, lu.GEOMETRY) = 'TRUE'
WHERE lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
"""

    # =========================================================================
    # 24 - Agricultural Land Reserve
    # =========================================================================
    sql['24-ALR'] = f"""
SELECT
    lu.LANDSCAPE_UNIT_NAME,
    alr.ALR_POLY_ID,
    alr.STATUS,
    ROUND((alr.FEATURE_AREA_SQM)/10000,2) AS AREA_HA
FROM WHSE_LEGAL_ADMIN_BOUNDARIES.OATS_ALR_POLYS alr
INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
    ON SDO_ANYINTERACT(alr.GEOMETRY, lu.GEOMETRY) = 'TRUE'
WHERE lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
"""

    # =========================================================================
    # 25 - Archaeology Sites
    # =========================================================================
    sql['25-Archaeology Sites'] = f"""
SELECT
    lu.LANDSCAPE_UNIT_NAME,
    ar.BORDENNUMBER,
    ar.REGTY_TYPE,
    ar.REGTY_SUBTYPE,
    ar.REGTY_SITETYPEDESCRIPTOR,
    ar.REGISTRATIONSTATUS,
    ar.ISHERITAGESITE,
    ROUND((ar.FEATURE_AREA_SQM)/10000,5) AS AREA_HA
FROM WHSE_ARCHAEOLOGY.RAAD_TFM_SITES_SVW ar
INNER JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW lu
    ON SDO_ANYINTERACT(ar.SHAPE, lu.GEOMETRY) = 'TRUE'
WHERE lu.LANDSCAPE_UNIT_NAME IN ({HAIDA_GWAII_LU})
"""

    return sql
