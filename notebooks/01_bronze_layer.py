%md
STEP 1: CONFIGURATION AND SETUP
from pyspark.sql.functions import current_timestamp, lit, input_file_name, col
from pyspark.sql.types import *

catalog = "adb_healthcare_claims_serverless"
schema = "bronze"

base_path = "abfss://landing@scnilproject.dfs.core.windows.net/"

tables = ["claim_header","claim_line","member","provider","policy","payment"]
%md
STEP 2: INGEST DATA INTO BRONZE 
spark.sql(f"DESCRIBE TABLE {catalog}.bronze.claim_header").show(truncate=False)
from pyspark.sql.functions import current_timestamp, lit, col, regexp_extract, input_file_name
for table in tables:

    path = base_path + table
    table_name = f"{catalog}.{schema}.{table}"

    # =========================
    # READ DATA
    # =========================
    df = spark.read \
        .format("parquet") \
        .load(path + "/year=*/month=*/day=*")

    # =========================
    # BASIC DEDUP (SAFE FOR BRONZE)
    # =========================
    df = df.dropDuplicates()

    # =========================
    # EXTRACT PARTITION COLUMNS (FROM PATH)
    # =========================
    df = df.withColumn("year", regexp_extract(col("_metadata.file_path"), r"year=(\d+)", 1).cast("int")) \
           .withColumn("month", regexp_extract(col("_metadata.file_path"), r"month=(\d+)", 1).cast("int")) \
           .withColumn("day", regexp_extract(col("_metadata.file_path"), r"day=(\d+)", 1).cast("int"))

    # =========================
    # ADD METADATA (UC COMPLIANT)
    # =========================
    df = df.withColumn("ingestion_time", current_timestamp()) \
           .withColumn("source_table", lit(table)) \
           .withColumn("source_file", col("_metadata.file_path"))

    # =========================
    # WRITE TO BRONZE (SIMPLE + STABLE)
    # =========================
    df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(table_name)
