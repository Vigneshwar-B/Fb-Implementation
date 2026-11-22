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
# - **Notebook Name:** `02_NB_FHIR_BronzeToSilver_DirectTable`
# - **Metadata:** `WH_MetaData.FHIR.ConfigETL`, `WH_MetaData.FHIR.ColumnDetails`
# 
# ---
# 
# #### 📌 Purpose
# 
# Loads the **FHIR Bronze** tables into the **FHIR Silver** layer:
# 1. Picks only tables that succeeded in Bronze for this `ETLBatchId` (`Log.ETLBatchBronzeLogDetails`)
# 2. Flattens `resource_json` into the columns defined in `FHIR.ColumnDetails` (JSON paths in `FHIR_COLUMN_PATHS`)
# 3. Casts every column to the `DataType` from `FHIR.ColumnDetails`
# 4. De-duplicates to the latest record per `id` and adds audit columns + `HashKey`
# 5. **SCD Type 2** MERGE: a changed record closes the old version (`IsCurrent = 0`, `EffectiveTo`) and inserts a new one (`VersionNo + 1`)
# 6. Logs to `WH_MetaData.Log.ETLBatchSilverLogDetails`
# 
# ---
# 
# #### 📎 Notes
# 
# - After this notebook, the pipeline calls `FHIR.usp_UpdateWaterMark` per table (MAX of `last_updated` in Silver)
# - `HashKey` covers business columns only, so a change to `version_id` / `last_updated` alone does **not** create a new version


# CELL ********************

############################################# IMPORTING REQUIRED PACKAGES ##############################################
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, trim, coalesce, concat_ws, md5, row_number, to_date, to_timestamp,
    get_json_object, regexp_extract, current_timestamp
)
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, timezone
from delta.tables import DeltaTable
from com.microsoft.spark.fabric import Constants
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants
import concurrent.futures
import pandas as pd
import pytz
import traceback
import json
import requests

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

################################################### NOTEBOOK PARAMETERS ####################################################
ETLBatchId = 1
bronze_log_filter = 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

################################################### BUILDING CUSTOM SPARK SESSION ####################################################
spark = SparkSession.builder \
    .appName("FHIRDataLoad") \
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
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

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
PrimaryKey = 'id'
HIGH_DATE = datetime(9999, 12, 31)

############################################################ DEFINING ABFSS PATH ##########################################################################
token = mssparkutils.credentials.getToken("https://api.fabric.microsoft.com")
headers = {"Authorization": f"Bearer {token}"}
workspace_id = mssparkutils.runtime.context.get("currentWorkspaceId")
if not workspace_id:
    raise Exception("No workspace context found. Attach notebook to a Lakehouse.")
url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
items = requests.get(url, headers=headers).json().get("value", [])
lakehouses = [i for i in items if i["type"].lower() == "lakehouse"]
warehouses = [i for i in items if i["type"].lower() == "warehouse"]
bronze_lh = next((lh for lh in lakehouses if lh["displayName"].upper().startswith("LH_BRONZE")), None)
silver_lh = next((lh for lh in lakehouses if lh["displayName"].upper().startswith("LH_SILVER")), None)
warehouse = next((wh for wh in warehouses if wh["displayName"].upper().startswith("WH_METADATA")), None)
if not bronze_lh:
    raise Exception("LH_BRONZE lakehouse not found in workspace")
if not silver_lh:
    raise Exception("LH_SILVER lakehouse not found in workspace")
if not warehouse:
    raise Exception("WH_METADATA warehouse not found in workspace")

bronze_base_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{bronze_lh['id']}/Tables"
silver_base_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{silver_lh['id']}/Tables"
warehouse_config_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{warehouse['id']}/Tables"
Log_details = f"{warehouse_config_path}/Log/ETLBatchBronzeLogDetails"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

################################################### LOADING METADATA ####################################################
try:
    ConfigQuery = f"SELECT * FROM {ConfigSchemaName}.ConfigETL WHERE IsActive = 1"
    ColumnQuery = f"SELECT * FROM {ConfigSchemaName}.ColumnDetails"
    ConfigETL_df = spark.read.option(Constants.DatabaseName, MetadataWarehouse).synapsesql(ConfigQuery)
    ColumnDetails_df = spark.read.option(Constants.DatabaseName, MetadataWarehouse).synapsesql(ColumnQuery)
except Exception as e:
    print(e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

################# FILTERING THE BRONZE LAYER SUCCESS TABLES #################################
if bronze_log_filter == 1:
    BronzeSucess_df = spark.read.format('delta').load(Log_details)
    BronzeSucess_df.createOrReplaceTempView("ETLBatchBronzeDetails")
    BronzeSucessQuery = f"""SELECT * FROM ETLBatchBronzeDetails
                            WHERE BatchId = '{ETLBatchId}' AND Status = 'Success'
                              AND ExtractedRowCount <> 0 AND SourceName = '{SourceName}'"""
    bronze_success_table_id = [
        str(r["TableId"]) for r in spark.sql(BronzeSucessQuery).select("TableId").distinct().collect()
    ]
    ConfigETL_df = ConfigETL_df.filter(col("Id").cast("string").isin(bronze_success_table_id))

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

################################################### FHIR JSON PATH MAPPING ####################################################
# ColumnName (as in FHIR.ColumnDetails) -> list of JSON paths in resource_json (first non-null wins)
# "ref:" prefix = FHIR reference, "Patient/123" -> "123"
FHIR_COLUMN_PATHS = {
    "Patient": {
        "id": ["$.id"],
        "active": ["$.active"],
        "gender": ["$.gender"],
        "birth_date": ["$.birthDate"],
        "given_name": ["$.name[0].given[0]"],
        "family_name": ["$.name[0].family"],
        "marital_status": ["$.maritalStatus.coding[0].code", "$.maritalStatus.text"],
        "phone": ["$.telecom[0].value"],
        "city": ["$.address[0].city"],
        "state": ["$.address[0].state"],
        "postal_code": ["$.address[0].postalCode"],
        "country": ["$.address[0].country"],
        "deceased": ["$.deceasedBoolean"],
        "version_id": ["$.meta.versionId"],
        "last_updated": ["$.meta.lastUpdated"],
    },
    "Encounter": {
        "id": ["$.id"],
        "patient_id": ["ref:$.subject.reference"],
        "status": ["$.status"],
        "class_code": ["$.class.code"],
        "type_code": ["$.type[0].coding[0].code"],
        "type_display": ["$.type[0].coding[0].display", "$.type[0].text"],
        "period_start": ["$.period.start"],
        "period_end": ["$.period.end"],
        "reason_display": ["$.reasonCode[0].coding[0].display", "$.reasonCode[0].text"],
        "service_provider_id": ["ref:$.serviceProvider.reference"],
        "version_id": ["$.meta.versionId"],
        "last_updated": ["$.meta.lastUpdated"],
    },
    "Observation": {
        "id": ["$.id"],
        "patient_id": ["ref:$.subject.reference"],
        "encounter_id": ["ref:$.encounter.reference"],
        "status": ["$.status"],
        "category": ["$.category[0].coding[0].code"],
        "code": ["$.code.coding[0].code"],
        "code_display": ["$.code.coding[0].display", "$.code.text"],
        "effective_datetime": ["$.effectiveDateTime", "$.effectivePeriod.start", "$.effectiveInstant"],
        "value_quantity": ["$.valueQuantity.value"],
        "value_unit": ["$.valueQuantity.unit", "$.valueQuantity.code"],
        "value_string": ["$.valueString", "$.valueCodeableConcept.coding[0].display", "$.valueCodeableConcept.text"],
        "version_id": ["$.meta.versionId"],
        "last_updated": ["$.meta.lastUpdated"],
    },
    "Condition": {
        "id": ["$.id"],
        "patient_id": ["ref:$.subject.reference"],
        "encounter_id": ["ref:$.encounter.reference"],
        "clinical_status": ["$.clinicalStatus.coding[0].code"],
        "verification_status": ["$.verificationStatus.coding[0].code"],
        "category": ["$.category[0].coding[0].code"],
        "code": ["$.code.coding[0].code"],
        "code_display": ["$.code.coding[0].display", "$.code.text"],
        "onset_datetime": ["$.onsetDateTime", "$.onsetPeriod.start"],
        "abatement_datetime": ["$.abatementDateTime", "$.abatementPeriod.start"],
        "recorded_date": ["$.recordedDate"],
        "version_id": ["$.meta.versionId"],
        "last_updated": ["$.meta.lastUpdated"],
    },
}
# Bronze metadata columns carried to Silver
LINEAGE_COLUMNS = ["extraction_timestamp", "api_url_or_params", "ETLBatchId"]
# Excluded from HashKey so a meta-only change does not create a new version
HASH_EXCLUDE = {"version_id", "last_updated"}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

################################################### USER DEFINED FUNCTION: 1.EXTRACTING BRONZE DATA ####################################################
def Read_bronze_data(BronzeSchemaName, BronzeTableName):
    is_okay = 1
    error_message = None
    try:
        bronze_abfss_path = f"{bronze_base_path}/{BronzeSchemaName}/{BronzeTableName}"
        bronze_df = spark.read.format('delta').load(bronze_abfss_path).drop_duplicates()
        msg = f"Data has been successfully extracted for Schema Name {BronzeSchemaName}, Table Name: {BronzeTableName}"
    except Exception as e:
        msg = f"An error occurred while extracting data for Schema Name: {BronzeSchemaName}, Table Name: {BronzeTableName}"
        error_message = str(e)
        bronze_df = None
        is_okay = 0
    return is_okay, bronze_df, msg, error_message

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

################################################### USER DEFINED FUNCTION: 2.FLATTEN FHIR JSON ####################################################
def json_column(path):
    is_ref = path.startswith("ref:")
    expr = get_json_object(col("resource_json"), path[4:] if is_ref else path)
    if is_ref:  # Patient/123 or Patient/123/_history/2 -> 123
        expr = regexp_extract(expr, r"([^/]+?)(?:/_history/[^/]+)?$", 1)
    expr = trim(expr)
    return when(expr == "", lit(None)).otherwise(expr)

def flatten_fhir(table_name, bronze_df, column_list):
    is_okay = 1
    error_message = None
    msg = "FHIR JSON flattening was Successful"
    try:
        path_map = FHIR_COLUMN_PATHS[table_name]
        select_cols = []
        for c in column_list:
            if c not in path_map:
                print(f"No JSON path defined for {table_name}.{c} - column will be NULL")
                select_cols.append(lit(None).cast("string").alias(c))
                continue
            parts = [json_column(p) for p in path_map[c]]
            select_cols.append((parts[0] if len(parts) == 1 else coalesce(*parts)).alias(c))
        lineage = [col(c) for c in LINEAGE_COLUMNS if c in bronze_df.columns]
        bronze_df = bronze_df.select(*select_cols, *lineage)
    except Exception as e:
        is_okay = 0
        error_message = str(e)
        msg = "FHIR JSON flattening Failed"
    return is_okay, bronze_df, msg, error_message

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

################################################### USER DEFINED FUNCTION: 3.DATA TYPE CASTING ####################################################
data_type_mapping = {
    'string': StringType(), 'varchar': StringType(),
    'int': IntegerType(), 'integer': IntegerType(), 'long': LongType(), 'bigint': LongType(),
    'double': DoubleType(), 'decimal': DoubleType(), 'float': DoubleType(),
    'boolean': BooleanType(), 'date': DateType(), 'timestamp': TimestampType(), 'datetime': TimestampType(),
}

def to_ts(c):
    return coalesce(
        c.cast("timestamp"),                                          # ISO 8601 incl. Z / +05:30
        to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ssXXX"),
        to_timestamp(c, "yyyy-MM-dd"),
    )

def data_type_casting(table_name, df, column_config):
    is_okay = 1
    error_message = None
    msg = "Data Type Casting was Successful"
    try:
        for r in column_config:
            c, dtype = r["ColumnName"], r["DataType"].lower().strip()
            if c not in df.columns:
                print(f"Column '{c}' not found. Skipping...")
                continue
            if dtype in ("timestamp", "datetime"):
                df = df.withColumn(c, to_ts(col(c)))
            elif dtype == "date":
                df = df.withColumn(c, coalesce(to_date(col(c), "yyyy-MM-dd"), to_ts(col(c)).cast("date")))
            elif dtype == "boolean":
                df = df.withColumn(c, col(c).cast(BooleanType()))
            else:
                df = df.withColumn(c, col(c).cast(data_type_mapping.get(dtype, StringType())))
    except Exception as e:
        is_okay = 0
        error_message = str(e)
        msg = "Data Type Casting Failed"
    return is_okay, df, msg, error_message

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

################################################### USER DEFINED FUNCTION: 4.DEDUPLICATE (LATEST PER ID) ####################################################
def deduplicate(df):
    is_okay = 1
    error_message = None
    msg = "Deduplication was Successful"
    try:
        w = Window.partitionBy(PrimaryKey).orderBy(
            col("last_updated").desc_nulls_last(),
            col("version_id").cast("long").desc_nulls_last(),
            col("extraction_timestamp").desc_nulls_last())
        df = (df.filter(col(PrimaryKey).isNotNull())
                .withColumn("_rn", row_number().over(w)).filter("_rn = 1").drop("_rn"))
    except Exception as e:
        is_okay = 0
        error_message = str(e)
        msg = "Deduplication Failed"
    return is_okay, df, msg, error_message

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

################################################### USER DEFINED FUNCTION: 5.ADDING WAREHOUSE AUDIT COLUMNS ####################################################
def adding_ETLWarehousing_Columns(df: DataFrame, business_columns):
    is_okay = 1
    error_message = None
    msg = 'Adding ETL Warehousing Audit columns'
    current_utc_time = datetime.now(pytz.UTC)
    try:
        user_name = mssparkutils.env.getUserName()
    except Exception:
        user_name = "FabricDataEngineer"
    try:
        hash_cols = [c for c in business_columns if c not in HASH_EXCLUDE]
        df = df.withColumn("HashKey", md5(concat_ws("||", *[coalesce(col(c).cast("string"), lit("<NULL>")) for c in hash_cols])))
        df = df.withColumn("ETLLoadedDate", lit(current_utc_time).cast("timestamp")) \
               .withColumn("ETLLoadedBy", lit(user_name)) \
               .withColumn("ETLModifiedDate", lit(current_utc_time).cast("timestamp")) \
               .withColumn("ETLModifiedBy", lit(user_name))
    except Exception as e:
        is_okay = 0
        error_message = str(e)
        msg = "Failed During adding warehouse Audit Columns"
    return is_okay, df, msg, error_message

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

############################################# USER DEFINED FUNCTION: 6.SILVER DATA LOAD (SCD TYPE 2) #######################################################################
def add_scd_columns(df, version_expr, change_type_expr):
    return (df.withColumn("VersionNo", version_expr.cast("int"))
              .withColumn("EffectiveFrom", coalesce(col("last_updated"), col("ETLLoadedDate")))
              .withColumn("EffectiveTo", lit(HIGH_DATE).cast("timestamp"))
              .withColumn("IsCurrent", lit(True))
              .withColumn("ChangeType", change_type_expr))

def data_load(spark: SparkSession, df: DataFrame, SilverTableName: str, SilverSchemaName: str):
    is_okay = 1
    error_message = None
    absolute_path = f"{silver_base_path}/{SilverSchemaName}/{SilverTableName}"
    try:
        ####################################### 1. INITIAL LOAD ###################################
        if not DeltaTable.isDeltaTable(spark, absolute_path):
            initial_df = add_scd_columns(df, lit(1), lit("INSERT"))
            initial_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(absolute_path)
            cnt = initial_df.count()
            return is_okay, f"Initial Load Completed for {SilverSchemaName}.{SilverTableName} | Inserts: {cnt}", error_message

        ####################################### 2. COMPARE WITH CURRENT VERSIONS ###################################
        delta_table = DeltaTable.forPath(spark, absolute_path)
        current_df = (delta_table.toDF().filter(col("IsCurrent") == True)
                      .select(col(PrimaryKey), col("HashKey").alias("tgt_HashKey"),
                              col("VersionNo").alias("tgt_VersionNo"), col("last_updated").alias("tgt_last_updated")))
        joined = df.join(current_df, on=PrimaryKey, how="left")

        is_new = col("tgt_HashKey").isNull()
        is_changed = (col("tgt_HashKey").isNotNull() & (col("HashKey") != col("tgt_HashKey")) &
                      (col("tgt_last_updated").isNull() | col("last_updated").isNull() |
                       (col("last_updated") >= col("tgt_last_updated"))))

        changes_df = add_scd_columns(
            joined.filter(is_new | is_changed),
            when(is_new, lit(1)).otherwise(col("tgt_VersionNo") + 1),
            when(is_new, lit("INSERT")).otherwise(lit("UPDATE"))
        ).drop("tgt_HashKey", "tgt_VersionNo", "tgt_last_updated").cache()

        insert_count = changes_df.filter(col("ChangeType") == "INSERT").count()
        update_count = changes_df.filter(col("ChangeType") == "UPDATE").count()
        if insert_count + update_count == 0:
            changes_df.unpersist()
            return is_okay, f"No changes for {SilverSchemaName}.{SilverTableName}", error_message

        ####################################### 3. SCD2 MERGE (close old version + insert new) ###################################
        staged_df = (changes_df.filter(col("ChangeType") == "UPDATE").withColumn("_merge_key", col(PrimaryKey))
                     .unionByName(changes_df.withColumn("_merge_key", lit(None).cast("string"))))
        (delta_table.alias("t")
            .merge(staged_df.alias("s"), f"t.{PrimaryKey} = s._merge_key AND t.IsCurrent = true")
            .whenMatchedUpdate(set={
                "IsCurrent": "false",
                "EffectiveTo": "s.EffectiveFrom",
                "ETLModifiedDate": "s.ETLModifiedDate",
                "ETLModifiedBy": "s.ETLModifiedBy"})
            .whenNotMatchedInsert(values={c: f"s.{c}" for c in changes_df.columns})
            .execute())
        changes_df.unpersist()
        return is_okay, (f"SCD2 Load Completed for {SilverSchemaName}.{SilverTableName} | "
                         f"New Inserts: {insert_count}, New Versions: {update_count}"), error_message
    except Exception as e:
        is_okay = 0
        error_message = str(e)
        traceback.print_exc()
        return is_okay, f"Error While Loading {SilverSchemaName}.{SilverTableName}", error_message

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

################################################### USER DEFINED FUNCTION: 7.TABLE LEVEL LOG ####################################################
def update_table_level_log(log_dict, **kwargs):
    for k, v in kwargs.items():
        if v is not None:
            log_dict[k] = v
    return log_dict

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

############################################  MAIN BLOCK ##################################################
def process_table(row, ColumnDetails_df, ETLBatchId):
    start_utc_time = datetime.now(timezone.utc)
    try:
        user_name = mssparkutils.env.getUserName()
    except Exception:
        user_name = "FabricDataEngineer"
    failure_message = 'Not executed due to prior Failure'
    skip_message = "No new records were found; skip further processing"

    TableId = row['Id']
    TableName = row['TableName']
    BronzeSchemaName, BronzeTableName = row['BronzeSchemaName'], row['BronzeTableName']
    SilverSchemaName, SilverTableName = row['SilverSchemaName'], row['SilverTableName']

    TableLevelLog = {
        "LogHeaderId": ETLBatchId, "TableId": TableId, "TableName": SilverTableName, "SchemaName": SilverSchemaName,
        "StartTime": start_utc_time, "EndTime": '', "DurationInSec": '', "BronzeDataRead": '',
        "DataTypeCasting": '', "BronzeCount": None, "SilverCount": None, "SilverDataLoad": '',
        "ErrorMessage": '', "Status": '', "ETLLoadedBy": user_name
    }
    def fail(**kw):
        return update_table_level_log(TableLevelLog, EndTime=datetime.now(timezone.utc),
                                      DurationInSec=(datetime.now(timezone.utc) - start_utc_time).total_seconds(),
                                      Status="Failure", **kw)

    print(f"Started Bronze To Silver Process for {BronzeSchemaName}.{BronzeTableName}")
    ####################################### 1. BRONZE DATA READ ##############################################
    is_okay, df, msg, error_message = Read_bronze_data(BronzeSchemaName, BronzeTableName)
    if is_okay == 0 or df is None:
        return fail(BronzeDataRead=msg, DataTypeCasting=failure_message, SilverDataLoad=failure_message, ErrorMessage=error_message)
    bronze_count = df.count()
    if bronze_count == 0:
        return update_table_level_log(TableLevelLog, EndTime=datetime.now(timezone.utc),
                                      DurationInSec=(datetime.now(timezone.utc) - start_utc_time).total_seconds(),
                                      BronzeDataRead=msg, DataTypeCasting=skip_message, BronzeCount=0, SilverCount=0,
                                      SilverDataLoad=skip_message, Status="Success")
    TableLevelLog = update_table_level_log(TableLevelLog, BronzeDataRead=msg, BronzeCount=bronze_count)

    ####################################### 2. FLATTEN JSON ##############################################
    column_config = ColumnDetails_df.filter((col("TableName") == TableName) & (col("TableSchema") == SilverSchemaName)).collect()
    column_list = [r["ColumnName"] for r in column_config]
    if not column_list:
        return fail(DataTypeCasting=f"No FHIR.ColumnDetails rows for {TableName}", SilverDataLoad=failure_message)
    is_okay, df, msg, error_message = flatten_fhir(TableName, df, column_list)
    if is_okay == 0:
        return fail(DataTypeCasting=msg, SilverDataLoad=failure_message, ErrorMessage=error_message)

    ####################################### 3. DATA TYPE CASTING ##############################################
    is_okay, df, msg, error_message = data_type_casting(TableName, df, column_config)
    if is_okay == 0:
        return fail(DataTypeCasting=msg, SilverDataLoad=failure_message, ErrorMessage=error_message)
    TableLevelLog = update_table_level_log(TableLevelLog, DataTypeCasting=msg)

    ####################################### 4. DEDUPLICATE ##############################################
    is_okay, df, msg, error_message = deduplicate(df)
    if is_okay == 0:
        return fail(SilverDataLoad=msg, ErrorMessage=error_message)

    ####################################### 5. AUDIT COLUMNS + HASHKEY ##############################################
    is_okay, df, msg, error_message = adding_ETLWarehousing_Columns(df, column_list)
    if is_okay == 0:
        return fail(SilverDataLoad=msg, ErrorMessage=error_message)

    ####################################### 6. SILVER LOAD (SCD2) ##############################################
    is_okay, silver_data_msg, error_message = data_load(spark, df, SilverTableName, SilverSchemaName)
    if is_okay == 0:
        return fail(SilverDataLoad=silver_data_msg, ErrorMessage=error_message)
    TableLevelLog = update_table_level_log(TableLevelLog, EndTime=datetime.now(timezone.utc),
                                           DurationInSec=(datetime.now(timezone.utc) - start_utc_time).total_seconds(),
                                           SilverDataLoad=silver_data_msg, Status="Success")
    print(silver_data_msg)

    ####################################### 7. FINAL SILVER COUNT ##################################################
    try:
        silver_count = DeltaTable.forPath(spark, f"{silver_base_path}/{SilverSchemaName}/{SilverTableName}").toDF().count()
        TableLevelLog = update_table_level_log(TableLevelLog, SilverCount=silver_count)
    except Exception as e:
        print(f"Warning: Could not get silver count for {SilverSchemaName}.{SilverTableName}. Error: {str(e)}")
    return TableLevelLog

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

################################################### EXECUTION BLOCK ####################################################
log_list = []
max_executors = 4
def process_row(row):
    return process_table(row, ColumnDetails_df, ETLBatchId)
try:
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_executors) as executor:
        futures = {executor.submit(process_row, row): row for row in ConfigETL_df.collect()}
        for future in concurrent.futures.as_completed(futures):
            log_list.append(future.result())
except Exception as e:
    print(f"An error occurred: {e}")
log_df = pd.DataFrame(log_list).astype(str) if log_list else pd.DataFrame()
if not log_list:
    print("No tables to process for this ETLBatchId.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

################################################### SAVING LOG DETAILS ####################################################
schema = StructType([
    StructField("LogHeaderId", StringType(), True), StructField("TableId", StringType(), True),
    StructField("SchemaName", StringType(), True), StructField("TableName", StringType(), True),
    StructField("StartTime", StringType(), True), StructField("EndTime", StringType(), True),
    StructField("DurationInSec", StringType(), True), StructField("BronzeDataRead", StringType(), True),
    StructField("DataTypeCasting", StringType(), True), StructField("BronzeCount", StringType(), True),
    StructField("SilverCount", StringType(), True), StructField("SilverDataLoad", StringType(), True),
    StructField("ErrorMessage", StringType(), True), StructField("Status", StringType(), True),
    StructField("EtlLoadedBy", StringType(), True)
])
if log_list:
    cols = [f.name for f in schema.fields]
    log_df = log_df.rename(columns={"ETLLoadedBy": "EtlLoadedBy"})[cols]
    spark_df = spark.createDataFrame(log_df, schema=schema)
    spark_df = spark_df.withColumn("BronzeCount", col("BronzeCount").cast(LongType()).cast(StringType()))
    spark_df = spark_df.withColumn("SilverCount", col("SilverCount").cast(LongType()).cast(StringType()))
    spark_df = spark_df.withColumnRenamed("LogHeaderId", "BatchId")
    spark_df = spark_df.withColumn("SourceName", lit(SourceName))
    spark_df.write.mode("append").synapsesql(f"{MetadataWarehouse}.Log.ETLBatchSilverLogDetails")
    display(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
