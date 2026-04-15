%md
Gold Pipeline
# =========================
# STEP 1: SETUP
# =========================
from pyspark.sql.functions import *
from pyspark.sql.types import DecimalType

catalog = "adb_healthcare_claims_serverless"

# =========================
# STEP 2: READ SILVER TABLES
# =========================
df_ch = spark.table(f"{catalog}.silver.claim_header")
df_cl = spark.table(f"{catalog}.silver.claim_line")
df_pay = spark.table(f"{catalog}.silver.payment")
df_mem = spark.table(f"{catalog}.silver.member")
df_prov = spark.table(f"{catalog}.silver.provider")
df_pol = spark.table(f"{catalog}.silver.policy")

# =========================
# STEP 3: PAYMENT AGGREGATION
# =========================
df_pay_agg = df_pay.groupBy("claim_id").agg(
    sum(coalesce(col("payment_amount"), lit(0))).cast(DecimalType(22,2)).alias("payment_amount"),
    max("payment_date").alias("payment_date")
)

# =========================
# STEP 4: BUILD FACT TABLE
# =========================
fact_df = df_cl.alias("cl") \
    .join(df_ch.alias("ch"), "claim_id", "left") \
    .join(df_pay_agg.alias("pay"), "claim_id", "left") \
    .select(
        col("cl.claim_line_id"),
        col("cl.claim_id"),
        col("ch.member_id"),
        col("ch.provider_id"),
        col("ch.policy_id"),
        col("ch.claim_date"),
        col("cl.procedure_code"),
        col("cl.claim_line_amount").cast(DecimalType(22,2)),
        col("ch.total_claimed_amount").cast(DecimalType(22,2)),
        col("ch.approved_amount").cast(DecimalType(22,2)),
        col("ch.deductible").cast(DecimalType(22,2)),
        col("ch.copay").cast(DecimalType(22,2)),
        col("pay.payment_amount"),
        col("pay.payment_date")
    )

fact_df.createOrReplaceTempView("fact_src")

# =========================
# STEP 5: CREATE FACT TABLE (IF NOT EXISTS)
# =========================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_claims (
    claim_line_id STRING,
    claim_id STRING,
    member_id STRING,
    provider_id STRING,
    policy_id STRING,
    claim_date DATE,
    procedure_code STRING,
    claim_line_amount DECIMAL(22,2),
    total_claimed_amount DECIMAL(22,2),
    approved_amount DECIMAL(22,2),
    deductible DECIMAL(22,2),
    copay DECIMAL(22,2),
    payment_amount DECIMAL(22,2),
    payment_date DATE
)
""")

# =========================
# STEP 6: MERGE FACT TABLE
# =========================
spark.sql(f"""
MERGE INTO {catalog}.gold.fact_claims t
USING fact_src s
ON t.claim_line_id = s.claim_line_id

WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# =========================
# STEP 7: CREATE DIM TABLES (SCD READY)
# =========================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_member (
    member_id STRING,
    member_name STRING,
    dob DATE,
    gender STRING,
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_provider (
    provider_id STRING,
    provider_name STRING,
    specialty STRING,
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_policy (
    policy_id STRING,
    policy_type STRING,
    coverage_amount DECIMAL(22,2),
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
)
""")

# =========================
# STEP 8: CREATE SOURCE VIEWS
# =========================
df_mem.createOrReplaceTempView("src_member")
df_prov.createOrReplaceTempView("src_provider")
df_pol.createOrReplaceTempView("src_policy")

# =========================
# STEP 9: SCD TYPE 2 (CORRECT LOGIC)
# =========================

# -------- MEMBER --------
spark.sql(f"""
MERGE INTO {catalog}.gold.dim_member t
USING src_member s
ON t.member_id = s.member_id AND t.is_current = true

WHEN MATCHED AND (
    t.member_name <> s.member_name OR
    t.dob <> s.dob OR
    t.gender <> s.gender
)
THEN UPDATE SET
    t.end_date = current_date(),
    t.is_current = false

WHEN NOT MATCHED
THEN INSERT (
    member_id, member_name, dob, gender,
    effective_date, end_date, is_current
)
VALUES (
    s.member_id, s.member_name, s.dob, s.gender,
    current_date(), NULL, true
)
""")

# INSERT NEW VERSION (SAFE)
spark.sql(f"""
INSERT INTO {catalog}.gold.dim_member
SELECT
    s.member_id, s.member_name, s.dob, s.gender,
    current_date(), NULL, true
FROM src_member s
LEFT JOIN {catalog}.gold.dim_member t
ON s.member_id = t.member_id AND t.is_current = true
WHERE t.member_id IS NULL
""")

# -------- PROVIDER --------
spark.sql(f"""
MERGE INTO {catalog}.gold.dim_provider t
USING src_provider s
ON t.provider_id = s.provider_id AND t.is_current = true

WHEN MATCHED AND (
    t.provider_name <> s.provider_name OR
    t.specialty <> s.specialty
)
THEN UPDATE SET
    t.end_date = current_date(),
    t.is_current = false

WHEN NOT MATCHED
THEN INSERT (
    provider_id, provider_name, specialty,
    effective_date, end_date, is_current
)
VALUES (
    s.provider_id, s.provider_name, s.specialty,
    current_date(), NULL, true
)
""")

spark.sql(f"""
INSERT INTO {catalog}.gold.dim_provider
SELECT
    s.provider_id, s.provider_name, s.specialty,
    current_date(), NULL, true
FROM src_provider s
LEFT JOIN {catalog}.gold.dim_provider t
ON s.provider_id = t.provider_id AND t.is_current = true
WHERE t.provider_id IS NULL
""")

# -------- POLICY --------
spark.sql(f"""
MERGE INTO {catalog}.gold.dim_policy t
USING src_policy s
ON t.policy_id = s.policy_id AND t.is_current = true

WHEN MATCHED AND (
    t.policy_type <> s.policy_type OR
    t.coverage_amount <> s.coverage_amount
)
THEN UPDATE SET
    t.end_date = current_date(),
    t.is_current = false

WHEN NOT MATCHED
THEN INSERT (
    policy_id, policy_type, coverage_amount,
    effective_date, end_date, is_current
)
VALUES (
    s.policy_id, s.policy_type, s.coverage_amount,
    current_date(), NULL, true
)
""")

spark.sql(f"""
INSERT INTO {catalog}.gold.dim_policy
SELECT
    s.policy_id, s.policy_type, s.coverage_amount,
    current_date(), NULL, true
FROM src_policy s
LEFT JOIN {catalog}.gold.dim_policy t
ON s.policy_id = t.policy_id AND t.is_current = true
WHERE t.policy_id IS NULL
""")

# =========================
# STEP 10: DATE DIM
# =========================
dim_date = df_ch.select("claim_date").dropDuplicates() \
    .withColumn("year", year("claim_date")) \
    .withColumn("month", month("claim_date")) \
    .withColumn("day", dayofmonth("claim_date"))

dim_date.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.gold.dim_date")

# =========================
# STEP 11: KPI TABLE
# =========================
kpi_table = fact_df.groupBy().agg(
    count("*").alias("total_claims"),
    sum(coalesce(col("payment_amount"), lit(0))).cast(DecimalType(22,2)).alias("total_payment"),
    sum(coalesce(col("approved_amount"), lit(0))).cast(DecimalType(22,2)).alias("total_approved_amount")
)

kpi_table.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.gold.kpi_claims")
