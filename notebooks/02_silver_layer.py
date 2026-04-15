%md
Silver pipeline
# =========================
# STEP 1: SETUP
# =========================
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

catalog = "adb_healthcare_claims_serverless"

# =========================
# STEP 2: READ BRONZE TABLES
# =========================
df_claim_header = spark.table(f"{catalog}.bronze.claim_header")
df_claim_line   = spark.table(f"{catalog}.bronze.claim_line")
df_payment      = spark.table(f"{catalog}.bronze.payment")
df_member       = spark.table(f"{catalog}.bronze.member")
df_provider     = spark.table(f"{catalog}.bronze.provider")
df_policy       = spark.table(f"{catalog}.bronze.policy")

# =========================
# STEP 3: CLEAN + STANDARDIZE
# =========================

df_ch_clean = df_claim_header.select(
    "claim_id",
    "member_id",
    "provider_id",
    "policy_id",
    to_date(col("claim_date")).alias("claim_date"),
    col("claim_status"),
    col("total_claimed_amount").cast(DecimalType(10,2)),
    col("approved_amount").cast(DecimalType(10,2)),
    col("deductible").cast(DecimalType(10,2)),
    col("copay").cast(DecimalType(10,2)),
    col("last_modified_date"),
    "year","month","day",
    "ingestion_time","source_table"
)

df_cl_clean = df_claim_line.select(
    "claim_line_id",
    "claim_id",
    trim(col("procedure_code")).alias("procedure_code"),
    col("claim_line_amount").cast(DecimalType(10,2)),
    col("last_modified_date"),
    "year","month","day",
    "ingestion_time","source_table"
)

df_pay_clean = df_payment.select(
    "payment_id",
    "claim_id",
    col("payment_amount").cast(DecimalType(10,2)),
    col("payment_date"),
    col("last_modified_date"),
    "year","month","day",
    "ingestion_time","source_table"
)

df_mem_clean = df_member.select(
    "member_id",
    "member_name",
    to_date(col("dob")).alias("dob"),
    "gender",
    col("last_modified_date"),
    "year","month","day",
    "ingestion_time","source_table"
)

df_prov_clean = df_provider.select(
    "provider_id",
    "provider_name",
    "specialty",
    col("last_modified_date"),
    "year","month","day",
    "ingestion_time","source_table"
)

df_pol_clean = df_policy.select(
    "policy_id",
    "policy_type",
    col("coverage_amount").cast(DecimalType(10,2)),
    col("last_modified_date"),
    "year","month","day",
    "ingestion_time","source_table"
)

# =========================
# STEP 4: DEDUPLICATION (CRITICAL FIX)
# =========================

window_ch = Window.partitionBy("claim_id").orderBy(col("last_modified_date").desc_nulls_last(), col("ingestion_time").desc())
window_cl = Window.partitionBy("claim_line_id").orderBy(col("last_modified_date").desc_nulls_last(), col("ingestion_time").desc())
window_pay = Window.partitionBy("payment_id").orderBy(col("last_modified_date").desc_nulls_last(), col("ingestion_time").desc())
window_mem = Window.partitionBy("member_id").orderBy(col("last_modified_date").desc_nulls_last(), col("ingestion_time").desc())
window_prov = Window.partitionBy("provider_id").orderBy(col("last_modified_date").desc_nulls_last(), col("ingestion_time").desc())
window_pol = Window.partitionBy("policy_id").orderBy(col("last_modified_date").desc_nulls_last(), col("ingestion_time").desc())

df_claim_header_silver = df_ch_clean.withColumn("rn", row_number().over(window_ch)).filter(col("rn") == 1).drop("rn")
df_claim_line_silver   = df_cl_clean.withColumn("rn", row_number().over(window_cl)).filter(col("rn") == 1).drop("rn")
df_payment_silver      = df_pay_clean.withColumn("rn", row_number().over(window_pay)).filter(col("rn") == 1).drop("rn")
df_member_silver       = df_mem_clean.withColumn("rn", row_number().over(window_mem)).filter(col("rn") == 1).drop("rn")
df_provider_silver     = df_prov_clean.withColumn("rn", row_number().over(window_prov)).filter(col("rn") == 1).drop("rn")
df_policy_silver       = df_pol_clean.withColumn("rn", row_number().over(window_pol)).filter(col("rn") == 1).drop("rn")

# =========================
# STEP 5: NULL HANDLING
# =========================

df_claim_header_silver = df_claim_header_silver.fillna({
    "total_claimed_amount": 0,
    "approved_amount": 0,
    "deductible": 0,
    "copay": 0
})

df_payment_silver = df_payment_silver.fillna({
    "payment_amount": 0
})

# =========================
# STEP 6: DATA QUALITY CHECKS
# =========================

dq_negative_payment = df_payment_silver.filter(col("payment_amount") < 0) \
    .withColumn("dq_issue", lit("NEGATIVE_PAYMENT")) \
    .withColumn("table_name", lit("payment"))

dq_negative_payment.write.format("delta") \
    .mode("append") \
    .saveAsTable(f"{catalog}.silver.data_quality_issues")

# =========================
# STEP 7: WRITE TO SILVER
# =========================

df_claim_header_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.claim_header")

df_claim_line_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.claim_line")

df_payment_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.payment")

df_member_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.member")

df_provider_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.provider")

df_policy_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.policy")
