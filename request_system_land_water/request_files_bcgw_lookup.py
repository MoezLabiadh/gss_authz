"""
Lands & water authorization files worked on by our team -> details from BCGW.

1. Pull requests from the AGOL request system (project_resources_view).
2. Extract clean file numbers from the free-text File_Number field
   (plus POD and licence numbers from PD_Number / CL_Number for water).
3. Look them up in BCGW:
     Lands -> Tantalis, latest disposition per file
     Water -> WLS licences, applications and approvals
4. Write to OUT_DIR:
     lands_water_requests.xlsx
        client deliverable: a Land Requests and a Water Requests table,
        one row per request and file found in BCGW
     lands_requests_parsed.csv / water_requests_parsed.csv
        every request row, what was extracted, and what was found in BCGW
     lands_tantalis_latest.csv
     water_licences.csv / water_applications.csv / water_approvals.csv
"""
import datetime
import os
import re
import zipfile

import oracledb
import pandas as pd
from arcgis.gis import GIS

OUT_DIR = "outputs"
XLSX_NAME = "lands_water_requests.xlsx"
REQUESTS_ITEM_ID = "a9e9a0387f6a409db4a14c49507d06a5"

WLS_LICENCES = "WHSE_WATER_MANAGEMENT.WLS_WATER_RIGHTS_LICENCES_SP"
WLS_APPLICATIONS = "WHSE_WATER_MANAGEMENT.WLS_WATER_RIGHTS_APPLICTNS_SP"
WLS_APPROVALS = "WHSE_WATER_MANAGEMENT.WLS_WATER_APPROVALS_GOV_SVW"

# Latest disposition transaction per file (one row per file).
# "Latest" = most recently entered transaction. To use the latest status change
# instead, order by TS.EFFECTIVE_DAT DESC.
# Parcels, tenants and parties are LEFT JOINs so a file isn't dropped just
# because one of them is missing. {binds} is filled in by query_in_chunks().
TANTALIS_SQL = """
SELECT * FROM (
  SELECT
      CAST(IP.INTRID_SID AS NUMBER) INTEREST_PARCEL_ID,
      CAST(DT.DISPOSITION_TRANSACTION_SID AS NUMBER) DISPOSITION_TRANSACTION_ID,
      DS.FILE_CHR AS FILE_NBR,
      SG.STAGE_NME AS STAGE,
      TT.ACTIVATION_CDE,
      TT.STATUS_NME AS STATUS,
      DT.APPLICATION_TYPE_CDE AS APPLICATION_TYPE,
      TS.EFFECTIVE_DAT AS EFFECTIVE_DATE,
      TY.TYPE_NME AS TENURE_TYPE,
      ST.SUBTYPE_NME AS TENURE_SUBTYPE,
      PU.PURPOSE_NME AS TENURE_PURPOSE,
      SP.SUBPURPOSE_NME AS TENURE_SUBPURPOSE,
      DT.DOCUMENT_CHR,
      DT.RECEIVED_DAT AS RECEIVED_DATE,
      DT.ENTERED_DAT AS ENTERED_DATE,
      DT.COMMENCEMENT_DAT AS COMMENCEMENT_DATE,
      DT.EXPIRY_DAT AS EXPIRY_DATE,
      IP.AREA_CALC_CDE,
      IP.AREA_HA_NUM AS AREA_HA,
      DT.LOCATION_DSC,
      OU.UNIT_NAME,
      IP.LEGAL_DSC,
      CONCAT(PR.LEGAL_NAME, PR.FIRST_NAME || ' ' || PR.LAST_NAME) AS CLIENT_NAME_PRIMARY,
      ROW_NUMBER() OVER (
          PARTITION BY DS.FILE_CHR
          ORDER BY DT.ENTERED_DAT DESC NULLS LAST,
                   DT.DISPOSITION_TRANSACTION_SID DESC,
                   IP.INTRID_SID
      ) AS RN

  FROM WHSE_TANTALIS.TA_DISPOSITION_TRANSACTIONS DT
    JOIN WHSE_TANTALIS.TA_DISPOSITIONS DS
      ON DS.DISPOSITION_SID = DT.DISPOSITION_SID
    JOIN WHSE_TANTALIS.TA_DISP_TRANS_STATUSES TS
      ON DT.DISPOSITION_TRANSACTION_SID = TS.DISPOSITION_TRANSACTION_SID
        AND TS.EXPIRY_DAT IS NULL
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
    LEFT JOIN WHSE_TANTALIS.TA_INTEREST_PARCELS IP
      ON DT.DISPOSITION_TRANSACTION_SID = IP.DISPOSITION_TRANSACTION_SID
        AND IP.EXPIRY_DAT IS NULL
    LEFT JOIN WHSE_TANTALIS.TA_TENANTS TE
      ON TE.DISPOSITION_TRANSACTION_SID = DT.DISPOSITION_TRANSACTION_SID
        AND TE.SEPARATION_DAT IS NULL
        AND TE.PRIMARY_CONTACT_YRN = 'Y'
    LEFT JOIN WHSE_TANTALIS.TA_INTERESTED_PARTIES PR
      ON PR.INTERESTED_PARTY_SID = TE.INTERESTED_PARTY_SID

  WHERE DS.FILE_CHR IN ({binds})
) TN
WHERE TN.RN = 1
"""


# ---------------------------------------------------------------- AGOL

def fetch_requests():
    """Requests from the AGOL request system, one row per request."""
    gis = GIS(
        os.getenv("AGO_HOST"),
        os.getenv("AGO_USERNAME_GSSHUB"),
        os.getenv("AGO_PASSWORD_GSSHUB"),
        verify_cert=False,
    )
    print(f"..connected to AGOL as {gis.users.me.username}")

    table = gis.content.get(REQUESTS_ITEM_ID).tables[0]
    req = table.query(where="1=1", as_df=True)
    # the view has one row per assigned resource -> drop the repeats
    return req[["Project_Number", "Date_Requested", "Request_Type", "Authorization_Type",
                "File_Number", "PD_Number", "CL_Number"]].drop_duplicates()


# ---------------------------------------------------------------- extraction

def extract_file_numbers(text):
    """Pull 7-8 digit file numbers out of free text.

    '3400600, 3400585; 3405349' -> all three
    'Lands File #4405337 - TM Mobile' -> 4405337
    '345388' (bare, leading zero lost) -> 0345388
    '44069514406592' (two files glued together) -> 4406951, 4406592
    Skipped: disposition/ATS numbers (6 digits next to text), tracking and
    application numbers (9 digits), NoW numbers like 1641250-2024-01.
    """
    if pd.isna(text):
        return []
    text = str(text).strip()
    if re.fullmatch(r"\d{5,6}", text):
        return [text.zfill(7)]
    if re.fullmatch(r"\d{14}|\d{16}", text):
        half = len(text) // 2
        return [text[:half], text[half:]]
    numbers = re.findall(r"(?<!\d)(\d{7,8})(?![\d-])", text)
    return list(dict.fromkeys(numbers))  # de-dupe, keep order


def extract_pods(text):
    """'PD209429dugoutPW209426well', 'pd208947, PD 206575' -> ['PD209429', 'PW209426', ...]"""
    if pd.isna(text):
        return []
    found = re.findall(r"P([DWG])\s*(\d{3,6})(?!\d)", str(text).upper())
    return list(dict.fromkeys(f"P{kind}{num}" for kind, num in found))


def extract_licences(text):
    """'C040628 to be amended to 505741' -> ['C040628', '505741', 'C505741']

    Water Sustainability Act licences (5xxxxx) get written both with and
    without a 'C' prefix, so both forms are looked up.
    """
    if pd.isna(text):
        return []
    licences = []
    for lic in re.findall(r"(?<![A-Z0-9])([CF]?\d{6})(?!\d)", str(text).upper()):
        licences.append(lic)
        wsa = re.fullmatch(r"C?(5\d{5})", lic)
        if wsa:
            licences += [wsa.group(1), "C" + wsa.group(1)]
    return list(dict.fromkeys(licences))


def flatten(list_column):
    """Unique values from a column of lists."""
    return sorted({v for values in list_column for v in values})


# ---------------------------------------------------------------- BCGW

def query_in_chunks(conn, sql, values, chunk_size=900):
    """Run sql (with an IN ({binds}) placeholder) for any number of values.
    Oracle caps IN lists at 1000 items, so values are sent in chunks."""
    values = sorted(set(values))
    frames = []
    with conn.cursor() as cur:
        for i in range(0, len(values), chunk_size):
            chunk = values[i:i + chunk_size]
            binds = ", ".join(f":{n}" for n in range(1, len(chunk) + 1))
            cur.execute(sql.format(binds=binds), chunk)
            columns = [d[0] for d in cur.description]
            frames.append(pd.DataFrame(cur.fetchall(), columns=columns))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def non_spatial_columns(conn, table):
    """All columns except the geometry, so results go straight to CSV."""
    owner, name = table.split(".")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM all_tab_columns "
            "WHERE owner = :1 AND table_name = :2 AND data_type <> 'SDO_GEOMETRY' "
            "ORDER BY column_id",
            [owner, name],
        )
        return ", ".join(row[0] for row in cur) or "*"


def fetch_by_key(conn, table, key_column, values, columns=None):
    columns = columns or non_spatial_columns(conn, table)
    sql = f"SELECT {columns} FROM {table} WHERE {key_column} IN ({{binds}})"
    return query_in_chunks(conn, sql, values)


def lookup(conn, table, key_column, values):
    """{key value: set of FILE_NUMBERs} — used to turn PODs / licences into files."""
    df = fetch_by_key(conn, table, key_column, values, f"{key_column}, FILE_NUMBER")
    if df.empty:
        return {}
    return df.dropna().groupby(key_column)["FILE_NUMBER"].apply(set).to_dict()


def report(label, df):
    print(
        f"..{label}: {len(df)} requests | "
        f"file number extracted: {(df['files'].str.len() > 0).sum()} | "
        f"found in BCGW: {(df['matched_files'].str.len() > 0).sum()} | "
        f"unique files found: {len(flatten(df['matched_files']))}"
    )


# ---------------------------------------------------------------- client workbook

TABLE_STYLE = "Table Style Medium 2"

# Excel 2023+ Office theme colours. XlsxWriter writes the older Office theme, in
# which TABLE_STYLE is blue; with these colours it is the Dark Teal version.
OFFICE_2023_COLOURS = {
    "1F497D": "0E2841", "EEECE1": "E8E8E8",                     # dark 2, light 2
    "4F81BD": "156082", "C0504D": "E97132", "9BBB59": "196B24",  # accents 1-3
    "8064A2": "0F9ED5", "4BACC6": "A02B93", "F79646": "4EA72E",  # accents 4-6
    "0000FF": "467886", "800080": "96607D",                     # hyperlinks
}

# source column -> workbook header, in workbook order
REQUEST_COLUMNS = {
    "Project_Number": "Project Number",
    "Date_Requested": "Date Requested",
    "Request_Type": "Request Type",
    "File_Number": "File Number (as entered)",
}
LAND_COLUMNS = {
    **REQUEST_COLUMNS,
    "matched_files": "File Number",
    "STAGE": "Stage",
    "STATUS": "Status",
    "APPLICATION_TYPE": "Application Type",
    "TENURE_TYPE": "Tenure Type",
    "TENURE_SUBTYPE": "Tenure Subtype",
    "TENURE_PURPOSE": "Tenure Purpose",
    "TENURE_SUBPURPOSE": "Tenure Subpurpose",
    "CLIENT_NAME_PRIMARY": "Primary Client",
    "LOCATION_DSC": "Location",
    "LEGAL_DSC": "Legal Description",
    "AREA_HA": "Area (ha)",
    "UNIT_NAME": "Office",
    "RECEIVED_DATE": "Received Date",
    "EFFECTIVE_DATE": "Status Effective Date",
    "COMMENCEMENT_DATE": "Commencement Date",
    "EXPIRY_DATE": "Expiry Date",
    "DOCUMENT_CHR": "Document Number",
    "DISPOSITION_TRANSACTION_ID": "Disposition Transaction ID",
    "INTEREST_PARCEL_ID": "Interest Parcel ID",
}
WATER_REQUEST_COLUMNS = {
    **REQUEST_COLUMNS,
    "PD_Number": "POD Number (as entered)",
    "CL_Number": "Licence Number (as entered)",
    "matched_files": "File Number",
}
# WLS records are stored per POD and purpose, so each source is summarised to
# one row per file (see summarise_files).
LICENCE_COLUMNS = {
    "LICENCE_STATUS": "Status",
    "LICENCE_NUMBER": "Licence Number",
    "PRIMARY_LICENSEE_NAME": "Holder / Applicant",
    "PURPOSE_USE": "Purpose",
    "SOURCE_NAME": "Source",
    "POD_NUMBER": "PODs",
    "DISTRICT_PRECINCT_NAME": "District / Precinct",
    "PRIORITY_DATE": "Priority Date",
    "EXPIRY_DATE": "Expiry Date",
}
APPLICATION_COLUMNS = {
    "APPLICATION_STATUS": "Status",
    "APPLICATION_JOB_NUMBER": "Application Job Number",
    "PRIMARY_APPLICANT_NAME": "Holder / Applicant",
    "PURPOSE_USE": "Purpose",
    "POD_NUMBER": "PODs",
    "DISTRICT_PRECINCT_NAME": "District / Precinct",
}
APPROVAL_COLUMNS = {
    "APPROVAL_STATUS": "Status",
    "APPROVAL_TYPE": "Approval Type",
    "CLIENT_NAME": "Holder / Applicant",
    "WORKS_DESCRIPTION": "Works Description",
    "SOURCE": "Source",
    "PRECINCT": "District / Precinct",
    "APPLICATION_DATE": "Application Date",
    "APPROVAL_START_DATE": "Start Date",
    "APPROVAL_EXPIRY_DATE": "Expiry Date",
}
WATER_RECORD_COLUMNS = [
    "Record Type", "Status", "Licence Number", "Application Job Number", "Approval Type",
    "Holder / Applicant", "Purpose", "Works Description", "Source", "PODs",
    "District / Precinct", "Priority Date", "Application Date", "Start Date", "Expiry Date",
]


def join_unique(values):
    """Unique values as one '; '-separated string (dates as YYYY-MM-DD)."""
    texts = set()
    for v in values.dropna():
        if isinstance(v, datetime.date):
            v = v.strftime("%Y-%m-%d")
        elif isinstance(v, float) and v.is_integer():
            v = int(v)
        texts.add(str(v).strip())
    return "; ".join(sorted(texts - {""})) or None


def summarise_files(df, key, record_type, columns):
    """One row per file, indexed by file number."""
    out = df.groupby(key)[list(columns)].agg(join_unique).rename(columns=columns)
    out.insert(0, "Record Type", record_type)
    return out


def land_sheet(lands, tantalis):
    """One row per request and matched file, with its latest Tantalis disposition.
    Requests with no file found keep one row with the Tantalis columns empty."""
    df = lands.explode("matched_files").merge(
        tantalis, how="left", left_on="matched_files", right_on="FILE_NBR")
    # ids become floats once unmatched rows add blanks
    for col in ("DISPOSITION_TRANSACTION_ID", "INTEREST_PARCEL_ID"):
        df[col] = df[col].astype("Int64")
    return df[list(LAND_COLUMNS)].rename(columns=LAND_COLUMNS)


def water_sheet(water, licences, applications, approvals):
    """One row per request, matched file and record type (licence, application
    or approval). Requests with no file found keep one row with the record columns empty."""
    files = pd.concat([
        summarise_files(licences, "FILE_NUMBER", "Licence", LICENCE_COLUMNS),
        summarise_files(applications, "FILE_NUMBER", "Application", APPLICATION_COLUMNS),
        summarise_files(approvals, "APPROVAL_FILE_NUMBER", "Approval", APPROVAL_COLUMNS),
    ])
    df = water.explode("matched_files").merge(
        files, how="left", left_on="matched_files", right_index=True)
    df = df.rename(columns=WATER_REQUEST_COLUMNS)
    return df.reindex(columns=[*WATER_REQUEST_COLUMNS.values(), *WATER_RECORD_COLUMNS])


def write_xlsx(path, sheets):
    """Each DataFrame on its own sheet as a formatted Excel table."""
    with pd.ExcelWriter(path, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as xl:
        decimals = xl.book.add_format({"num_format": "0.00"})
        for name, df in sheets.items():
            df = df.copy()
            dates = df.select_dtypes("datetime").columns
            for col in dates:
                # Excel can't show dates before 1900; Tantalis uses 1111-11-11 for unknown
                df[col] = df[col].where(df[col] >= "1900-01-01")

            df.to_excel(xl, sheet_name=name, index=False, header=False, startrow=1)
            ws = xl.sheets[name]
            ws.add_table(0, 0, max(len(df), 1), len(df.columns) - 1, {
                "name": name.replace(" ", ""),
                "style": TABLE_STYLE,
                "columns": [{"header": col} for col in df.columns],
            })
            for i, col in enumerate(df.columns):
                # bold header + filter button as the minimum width; autofit only widens
                ws.set_column(i, i, len(col) + 4, decimals if df[col].dtype == float else None)
            ws.autofit(max_width=420)  # pixels; longer text is cut off at the cell edge
            ws.freeze_panes(1, 1)
            ws.ignore_errors({"number_stored_as_text": "A1:XFD1048576"})  # file numbers
    set_theme_colours(path)


def set_theme_colours(path):
    """Swap the workbook theme colours for OFFICE_2023_COLOURS."""
    with zipfile.ZipFile(path) as z:
        parts = {name: z.read(name) for name in z.namelist()}
    for old, new in OFFICE_2023_COLOURS.items():
        parts["xl/theme/theme1.xml"] = parts["xl/theme/theme1.xml"].replace(
            f'val="{old}"'.encode(), f'val="{new}"'.encode())
    # With a defaultThemeVersion set, Excel ignores theme1.xml and uses its own
    # copy of that theme version, so drop it.
    parts["xl/workbook.xml"] = re.sub(
        rb' defaultThemeVersion="\d+"', b"", parts["xl/workbook.xml"])
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        for name, data in parts.items():
            z.writestr(name, data)


def main():
    # ------------------------------------------------ 1. requests from AGOL
    req = fetch_requests()
    lands = req[req["Authorization_Type"] == "Land"].copy()
    water = req[req["Authorization_Type"] == "Water"].copy()

    # ------------------------------------------------ 2. extract numbers
    lands["files"] = lands["File_Number"].apply(extract_file_numbers)
    water["files"] = water["File_Number"].apply(extract_file_numbers)
    water["pods"] = water["PD_Number"].apply(extract_pods)
    water["licences"] = water["CL_Number"].apply(extract_licences)

    # ------------------------------------------------ 3. BCGW lookups
    conn = oracledb.connect(
        user=os.getenv("BCGW_USER"),
        password=os.getenv("BCGW_PASSWORD"),
        dsn=os.getenv("BCGW_HOST"),
    )
    print("..connected to BCGW")

    # Lands: latest Tantalis disposition per file
    tantalis = query_in_chunks(conn, TANTALIS_SQL, flatten(lands["files"])).drop(columns="RN")
    found = set(tantalis["FILE_NBR"])
    lands["matched_files"] = lands["files"].apply(lambda files: [f for f in files if f in found])

    # Water: PODs and licence numbers point to files too. They catch requests
    # where the typed file number has a typo or is missing.
    pods = flatten(water["pods"])
    pod_files = lookup(conn, WLS_LICENCES, "POD_NUMBER", pods)
    for pod, files in lookup(conn, WLS_APPLICATIONS, "POD_NUMBER", pods).items():
        pod_files.setdefault(pod, set()).update(files)
    licence_files = lookup(conn, WLS_LICENCES, "LICENCE_NUMBER", flatten(water["licences"]))

    def candidate_files(row):
        files = set(row["files"])
        for pod in row["pods"]:
            files |= pod_files.get(pod, set())
        for lic in row["licences"]:
            files |= licence_files.get(lic, set())
        return sorted(files)

    water["candidate_files"] = water.apply(candidate_files, axis=1)
    water_files = flatten(water["candidate_files"])

    licences = fetch_by_key(conn, WLS_LICENCES, "FILE_NUMBER", water_files)
    applications = fetch_by_key(conn, WLS_APPLICATIONS, "FILE_NUMBER", water_files)
    approvals = fetch_by_key(conn, WLS_APPROVALS, "APPROVAL_FILE_NUMBER", water_files)
    conn.close()

    found = (set(licences.get("FILE_NUMBER", []))
             | set(applications.get("FILE_NUMBER", []))
             | set(approvals.get("APPROVAL_FILE_NUMBER", [])))
    water["matched_files"] = water["candidate_files"].apply(lambda files: [f for f in files if f in found])

    report("lands", lands)
    report("water", water)

    # ------------------------------------------------ 4. outputs
    os.makedirs(OUT_DIR, exist_ok=True)
    # needs the list columns, so before they're joined into text below
    write_xlsx(os.path.join(OUT_DIR, XLSX_NAME), {
        "Land Requests": land_sheet(lands, tantalis),
        "Water Requests": water_sheet(water, licences, applications, approvals),
    })
    print(f"..client workbook written to {os.path.abspath(os.path.join(OUT_DIR, XLSX_NAME))}")

    for df in (lands, water):
        for col in df.columns:
            if df[col].apply(lambda v: isinstance(v, list)).any():
                df[col] = df[col].str.join("; ")

    lands.to_csv(os.path.join(OUT_DIR, "lands_requests_parsed.csv"), index=False)
    tantalis.to_csv(os.path.join(OUT_DIR, "lands_tantalis_latest.csv"), index=False)
    water.to_csv(os.path.join(OUT_DIR, "water_requests_parsed.csv"), index=False)
    licences.to_csv(os.path.join(OUT_DIR, "water_licences.csv"), index=False)
    applications.to_csv(os.path.join(OUT_DIR, "water_applications.csv"), index=False)
    approvals.to_csv(os.path.join(OUT_DIR, "water_approvals.csv"), index=False)
    print(f"..outputs written to {os.path.abspath(OUT_DIR)}")


if __name__ == "__main__":
    main()
