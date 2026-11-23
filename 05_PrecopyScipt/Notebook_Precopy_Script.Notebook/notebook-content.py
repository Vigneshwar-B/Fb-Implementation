# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "932ba5ef-0392-47ae-8d11-93ec8ac4fd57",
# META       "default_lakehouse_name": "LH_Silver",
# META       "default_lakehouse_workspace_id": "fc5da95e-4417-4632-848b-1677f3a195be",
# META       "known_lakehouses": [
# META         {
# META           "id": "932ba5ef-0392-47ae-8d11-93ec8ac4fd57"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS LH_Silver.FHIR;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS LH_Silver.FHIR.silver_condition (
# MAGIC     id                    STRING      NOT NULL,
# MAGIC     patient_id            STRING,
# MAGIC     encounter_id          STRING,
# MAGIC     clinical_status       STRING,
# MAGIC     verification_status   STRING,
# MAGIC     category              STRING,
# MAGIC     code                  STRING,
# MAGIC     code_display          STRING,
# MAGIC     onset_datetime        TIMESTAMP,
# MAGIC     abatement_datetime    TIMESTAMP,
# MAGIC     recorded_date         TIMESTAMP,
# MAGIC     version_id            STRING,
# MAGIC     last_updated          TIMESTAMP,
# MAGIC     extraction_timestamp  TIMESTAMP,
# MAGIC     api_url_or_params     STRING,
# MAGIC     -- ETL / SCD2 audit columns
# MAGIC     ETLBatchId            STRING,
# MAGIC     HashKey               STRING,
# MAGIC     ETLLoadedDate         TIMESTAMP,
# MAGIC     ETLLoadedBy           STRING,
# MAGIC     ETLModifiedDate       TIMESTAMP,
# MAGIC     ETLModifiedBy         STRING,
# MAGIC     VersionNo             INT,
# MAGIC     EffectiveFrom         TIMESTAMP,
# MAGIC     EffectiveTo           TIMESTAMP,
# MAGIC     IsCurrent             BOOLEAN,
# MAGIC     ChangeType            STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact'   = 'true'
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
