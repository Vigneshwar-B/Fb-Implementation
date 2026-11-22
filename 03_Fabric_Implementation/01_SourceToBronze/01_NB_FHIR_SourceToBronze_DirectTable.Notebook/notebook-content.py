# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# ####  Notebook Documentation
# 
# #### 📝 Notebook Details
# 
# - **Notebook Name:** `01_NB_FHIR_SourceToBronze_DirectTable`
# - **Source:** HAPI FHIR R4 public API (`https://hapi.fhir.org/baseR4`)
# - **Metadata:** `WH_MetaData.FHIR.ConfigETL`
# 
# ---
# 
# #### 📌 Purpose
# 
# Incrementally extracts FHIR resources (Patient → Encounter → Observation → Condition) from the API using
# `_lastUpdated >= WaterMarkValue` with **pagination** (`Bundle.link[next]`), and then:
# 1. Saves every API response page **as-is** (JSON) in `LH_Bronze/Files/FHIR/Raw/<Table>/LoadDate=YYYY-MM-DD/Batch_<ETLBatchId>/`
# 2. Converts the responses into the Delta table `LH_Bronze.<BronzeSchemaName>.<BronzeTableName>` (one row per resource, raw JSON kept)
# 3. Logs table-level status to `WH_MetaData.Log.ETLBatchBronzeLogDetails` and every API call to `WH_MetaData.Log.FHIRApiCallLog`
# 
# ---
# 
# #### 📎 Notes
# 
# - Tables run **sequentially in `Id` order** (Patient → Encounter → Observation → Condition)
# - The watermark is **not** updated here: after the Silver load, the pipeline calls `FHIR.usp_UpdateWaterMark` (MAX of the Silver watermark column)


# CELL ********************

######################################### IMPORTING REQUIRED PACKAGES ###########################################
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import (
    StringType, StructField, StructType, TimestampType, IntegerType, LongType, BooleanType
)
from datetime import datetime, timezone, timedelta
from delta.tables import DeltaTable
from com.microsoft.spark.fabric import Constants
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants
import traceback
import json
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

################################################### NOTEBOOK PARAMETERS ####################################################
ETLBatchId = 1
Id = '*'            # '*' = all active tables, or a single ConfigETL.Id
PageSize = 100      # FHIR _count per page
MaxPages = 500    # safety cap per table per run

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

################################################### BUILDING CUSTOM SPARK SESSION ####################################################
spark = SparkSession.builder \
    .appName("FHIRDataLoader") \
    .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
    .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .config("spark.gluten.enabled", "false") \
    .config("spark.sql.caseSensitive", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

################################################### USER DEFINED VARIABLES ####################################################
current_utc_time = datetime.now(timezone.utc)
user_name = mssparkutils.env.getUserName()
ConfigSchemaName = 'FHIR'
MetadataWarehouse = 'WH_MetaData'
SourceName = 'FHIR_Data_Load'
load_date = current_utc_time.strftime("%Y-%m-%d")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

######################### DEFINING ABFSS PATH #######################
token = mssparkutils.credentials.getToken("https://api.fabric.microsoft.com")
headers = {"Authorization": f"Bearer {token}"}
workspace_id = mssparkutils.runtime.context.get("currentWorkspaceId")
if not workspace_id:
    raise Exception("No workspace context found. Attach notebook to a Lakehouse.")
print(f"Workspace ID: {workspace_id}")
url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
items = requests.get(url, headers=headers).json().get("value", [])
lakehouses = [i for i in items if i["type"].lower() == "lakehouse"]
warehouses = [i for i in items if i["type"].lower() == "warehouse"]
bronze_lh = next((lh for lh in lakehouses if lh["displayName"].upper().startswith("LH_BRONZE")), None)
silver_lh = next((lh for lh in lakehouses if lh["displayName"].upper().startswith("LH_SILVER")), None)
if not bronze_lh:
    raise Exception("LH_BRONZE lakehouse not found in workspace")
if not silver_lh:
    raise Exception("LH_SILVER lakehouse not found in workspace")
warehouse = next((wh for wh in warehouses if wh["displayName"].upper().startswith("WH_METADATA")), None)
if not warehouse:
    raise Exception("WH_METADATA warehouse not found in workspace")
print(f"Bronze Lakehouse ID: {bronze_lh['id']}")
print(f"Silver Lakehouse ID: {silver_lh['id']}")
print(f"Warehouse ID: {warehouse['id']}")

bronze_lakehouse_id = bronze_lh["id"]
silver_lakehouse_id = silver_lh["id"]
warehouse_id = warehouse["id"]

bronze_base_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{bronze_lakehouse_id}/Tables"
silver_base_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{silver_lakehouse_id}/Tables"
bronze_files_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{bronze_lakehouse_id}/Files"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

################################################### LOADING METADATA ####################################################
try:
    if Id == '*':
        ConfigQuery = f"SELECT * FROM {ConfigSchemaName}.ConfigETL WHERE IsActive = 1"
    else:
        ConfigQuery = f"SELECT * FROM {ConfigSchemaName}.ConfigETL WHERE IsActive = 1 AND Id = {Id}"
    ConfigETL_df = spark.read.option(Constants.DatabaseName, MetadataWarehouse).synapsesql(ConfigQuery)
    ConfigETL_df = ConfigETL_df.orderBy("Id")      # Patient -> Encounter -> Observation -> Condition
except Exception as e:
    print(e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

####################### INCREMENTAL API PARAMETER GENERATION ######################################
def format_fhir_datetime(value):
    """WaterMarkValue (datetime2 from the warehouse) -> FHIR instant 'YYYY-MM-DDTHH:MM:SSZ'."""
    if value is None:
        return None
    if isinstance(value, str):
        v = value.strip().replace("T", " ").split(".")[0].replace("Z", "")
        value = datetime.strptime(v[:19], "%Y-%m-%d %H:%M:%S") if len(v) > 10 else datetime.strptime(v[:10], "%Y-%m-%d")
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")

def build_api_params(row):
    params = {"_count": PageSize, "_sort": "_lastUpdated", "_format": "json"}
    if row["IsFullLoad"] == 0 and row["WaterMarkValue"] is not None:
        # WaterMarkField in ConfigETL is the Silver column; the FHIR search parameter is _lastUpdated
        params["_lastUpdated"] = f"ge{format_fhir_datetime(row['WaterMarkValue'])}"
    return params

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(ConfigETL_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

################################################### API HELPERS ####################################################
def get_http_session():
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"], raise_on_status=False)
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({"Accept": "application/fhir+json"})
    return session

def get_next_link(bundle):
    for link in bundle.get("link", []):
        if link.get("relation") == "next":
            return link.get("url")
    return None

def save_raw_response(table_name, page_number, response_text):
    """Store the API response exactly as received (bucketed by load date)."""
    raw_path = (f"{bronze_files_path}/{ConfigSchemaName}/Raw/{table_name}/LoadDate={load_date}/"
                f"Batch_{ETLBatchId}/{table_name}_page_{page_number:05d}.json")
    mssparkutils.fs.put(raw_path, response_text, True)
    return raw_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

################################################### DATA EXTRACTION ####################################################
BRONZE_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("resource_type", StringType()),
    StructField("version_id", StringType()),
    StructField("last_updated", StringType()),
    StructField("resource_json", StringType()),
    StructField("page_number", IntegerType()),
    StructField("api_url_or_params", StringType()),
    StructField("extraction_timestamp", TimestampType()),
    StructField("raw_saved_at", TimestampType()),
    StructField("raw_file_path", StringType()),
    StructField("ETLBatchId", StringType()),
])

def data_load_bronze(row):
    is_okay = 1
    error_message = ""
    status = "In-Progress"
    spark_df = None
    api_logs = []
    TableName = row["TableName"]
    bronze_defined_path = f"{bronze_base_path}/{row['BronzeSchemaName']}/{row['BronzeTableName']}"
    try:
        session = get_http_session()
        next_url, params = row["SourceAPI"], build_api_params(row)
        page_number, records = 0, []

        ####################### PAGINATION ######################
        while next_url and page_number < MaxPages:
            page_number += 1
            api_called_at = datetime.now(timezone.utc)
            response = session.get(next_url, params=params, timeout=120)
            response.raise_for_status()
            raw_path = save_raw_response(TableName, page_number, response.text)
            raw_saved_at = datetime.now(timezone.utc)

            bundle = response.json()
            entries = bundle.get("entry", []) or []
            for entry in entries:
                resource = entry.get("resource", {})
                if resource.get("resourceType") != TableName:      # skip OperationOutcome etc.
                    continue
                meta = resource.get("meta", {}) or {}
                records.append({
                    "id": resource.get("id"),
                    "resource_type": resource.get("resourceType"),
                    "version_id": meta.get("versionId"),
                    "last_updated": meta.get("lastUpdated"),
                    "resource_json": json.dumps(resource),
                    "page_number": page_number,
                    "api_url_or_params": response.url,
                    "extraction_timestamp": api_called_at,
                    "raw_saved_at": raw_saved_at,
                    "raw_file_path": raw_path,
                    "ETLBatchId": str(ETLBatchId),
                })
            next_url = get_next_link(bundle)
            params = None                              # the next link already contains the paging state
            api_logs.append({
                "BatchId": ETLBatchId, "TableId": row["Id"], "TableName": TableName,
                "PageNumber": page_number, "ApiUrl": response.url, "HttpStatus": response.status_code,
                "EntryCount": len(entries), "ApiCalledAt": api_called_at, "RawSavedAt": raw_saved_at,
                "RawFilePath": raw_path, "HasNextPage": next_url is not None,
            })

        if next_url:
            print(f"WARNING: {TableName} stopped at MaxPages={MaxPages}; more data exists")

        ####################### WRITE BRONZE DELTA TABLE ######################
        if records:
            spark_df = spark.createDataFrame(records, schema=BRONZE_SCHEMA)
            spark_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(bronze_defined_path)
        else:
            spark_df = spark.createDataFrame([], schema=BRONZE_SCHEMA)
        status = "Success"
    except Exception as e:
        is_okay = 0
        error_message = str(e)
        status = "Failure"
        traceback.print_exc()
    return spark_df, is_okay, error_message, status, api_logs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

################################################## PROCESSING FUNCTION ####################################################
def process(row, BatchId):
    TableName = row["TableName"]
    print(f"Process started for {TableName}")
    logs = []
    load_start_time = datetime.now(timezone.utc)
    spark_df, is_okay, error_message, status, api_logs = data_load_bronze(row)
    load_end_time = datetime.now(timezone.utc)
    extracted_row_count = spark_df.count() if spark_df is not None else 0
    print(f"Data Load completed for {TableName}: {extracted_row_count} rows, {len(api_logs)} pages, status {status}")
    logs.append({
        "BatchId": BatchId,
        "TableId": row["Id"],
        "SchemaName": row["BronzeSchemaName"],
        "TableName": row["BronzeTableName"],
        "ExtractedRowCount": extracted_row_count,
        "StartTime": load_start_time,
        "EndTime": load_end_time,
        "Status": status,
        "ErrorMessage": error_message,
        "SourceName": SourceName
    })
    return logs, api_logs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

################################################## PROCESS EXECUTOR ####################################################
def executor():
    """Sequential on purpose: Patient -> Encounter -> Observation -> Condition (ConfigETL.Id order)."""
    rows = ConfigETL_df.collect()
    table_level_logs, api_call_logs = [], []
    for row in rows:
        try:
            result_logs, api_logs = process(row, ETLBatchId)
            table_level_logs.extend(result_logs)
            api_call_logs.extend(api_logs)
        except Exception as e:
            table_level_logs.append({
                "BatchId": ETLBatchId, "TableId": row["Id"], "SchemaName": row["BronzeSchemaName"],
                "TableName": row["BronzeTableName"], "ExtractedRowCount": 0,
                "StartTime": datetime.now(timezone.utc), "EndTime": datetime.now(timezone.utc),
                "Status": "Failure", "ErrorMessage": str(e), "SourceName": SourceName
            })
            print(f"Error loading {row['BronzeTableName']}: {str(e)}")
    return table_level_logs, api_call_logs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

################################################## PROCESS EXECUTOR ####################################################
table_level_logs, api_call_logs = executor()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

################################################## BUILD LOG DATAFRAMES ####################################################
LOG_SCHEMA = StructType([
    StructField("BatchId", LongType()), StructField("TableId", LongType()),
    StructField("SchemaName", StringType()), StructField("TableName", StringType()),
    StructField("StartTime", TimestampType()), StructField("EndTime", TimestampType()),
    StructField("ExtractedRowCount", LongType()), StructField("ErrorMessage", StringType()),
    StructField("Status", StringType()), StructField("SourceName", StringType()),
])
API_LOG_SCHEMA = StructType([
    StructField("BatchId", LongType()), StructField("TableId", LongType()),
    StructField("TableName", StringType()), StructField("PageNumber", IntegerType()),
    StructField("ApiUrl", StringType()), StructField("HttpStatus", IntegerType()),
    StructField("EntryCount", IntegerType()), StructField("ApiCalledAt", TimestampType()),
    StructField("RawSavedAt", TimestampType()), StructField("RawFilePath", StringType()),
    StructField("HasNextPage", BooleanType()),
])
for l in table_level_logs:
    l["BatchId"], l["TableId"], l["ExtractedRowCount"] = int(l["BatchId"]), int(l["TableId"]), int(l["ExtractedRowCount"])
for l in api_call_logs:
    l["BatchId"], l["TableId"] = int(l["BatchId"]), int(l["TableId"])
log_df = spark.createDataFrame(table_level_logs, schema=LOG_SCHEMA)
api_log_df = spark.createDataFrame(api_call_logs, schema=API_LOG_SCHEMA)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(log_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

############################### STORING LOG TO THE LOG TABLE #####################################################
log_df.write.mode("append").synapsesql("WH_MetaData.Log.ETLBatchBronzeLogDetails")
if api_log_df.count() > 0:
    api_log_df.write.mode("append").synapsesql("WH_MetaData.Log.FHIRApiCallLog")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(log_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(api_log_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
