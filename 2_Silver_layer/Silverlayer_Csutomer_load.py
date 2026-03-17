

# ==========================================================================================
# Notebook Name: Silverlayer_Csutomer_load
# Purpose:
#     Load customer data from the Bronze layer into the Silver layer in Microsoft Fabric.
#
# Description:
#     This notebook performs an incremental load of customer data from bronzelayer.customer
#     into silver_customers. It applies data validation and transformation rules so that
#     only clean, standardized, analytics-ready data is stored in the Silver layer.
#
# Business Rules Implemented:
#     1. Validate email addresses:
#        - email must not be null
#        - email must not be empty
#        - email must follow a valid email pattern
#
#     2. Validate age:
#        - keep only customers with age between 18 and 100
#
#     3. Create customer_segment:
#        - High Value   : total_purchases > 10000
#        - Medium Value : total_purchases > 5000 and <= 10000
#        - Low Value    : total_purchases <= 5000
#
#     4. Calculate days_since_registration:
#        - number of days from registration_date to current date
#
#     5. Remove invalid records:
#        - exclude rows where total_purchases < 0
#
# Processing Logic:
#     - Create Silver table if it does not exist
#     - Find the last processed timestamp from Silver
#     - Read only new Bronze records using ingestion_timestamp
#     - Clean and transform customer data
#     - Merge transformed data into Silver
#     - Verify the load with row counts and sample output
#
# Assumptions:
#     - Bronze source table: bronzelayer.customer
#     - Bronze source contains: customer_id, name, email, country, customer_type,
#       registration_date, age, gender, total_purchases, ingestion_timestamp
#     - Delta Lake merge is supported in the Fabric Lakehouse environment
#
# Notes:
#     - This notebook uses Silver.last_updated as the incremental watermark.
#     - In a production design, a dedicated watermark/control table is usually better.
# ==========================================================================================

# ---------------------------------------------
# Imports
# ---------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------
# Configuration
# ---------------------------------------------
BRONZE_TABLE = "bronzelayer.customer"
SILVER_TABLE = "silver_customers"
DEFAULT_TIMESTAMP = "1900-01-01 00:00:00"
EMAIL_REGEX = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'

print("Starting Silver layer customer load...")

# ==========================================================================================
# STEP 1: Create the Silver layer table for customer data
# ==========================================================================================
# This table stores the cleaned and transformed customer records.
# USING DELTA is included to support merge operations and ACID-compliant updates.
# ==========================================================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    customer_id STRING,
    name STRING,
    email STRING,
    country STRING,
    customer_type STRING,
    registration_date DATE,
    age INT,
    gender STRING,
    total_purchases INT,
    customer_segment STRING,
    days_since_registration INT,
    last_updated TIMESTAMP
)
USING DELTA
""")

print(f"Step 1 complete: Table '{SILVER_TABLE}' is ready.")

# ==========================================================================================
# STEP 2: Identify the last processed timestamp
# ==========================================================================================
# Find the most recent last_updated value from the Silver table.
# This timestamp is used as the watermark for incremental loading.
# If the table is empty, use a very old default timestamp so the first run loads all data.
# ==========================================================================================

last_processed_df = (
    spark.table(SILVER_TABLE)
    .select(F.max("last_updated").alias("last_processed"))
)

last_processed_timestamp = last_processed_df.collect()[0]["last_processed"]

if last_processed_timestamp is None:
    last_processed_timestamp = DEFAULT_TIMESTAMP

print(f"Step 2 complete: Last processed timestamp = {last_processed_timestamp}")

# ==========================================================================================
# STEP 3: Load incremental data from the Bronze layer
# ==========================================================================================
# Read only Bronze records with ingestion_timestamp greater than the last processed timestamp.
# This avoids reprocessing the full Bronze dataset every time the notebook runs.
# ==========================================================================================

bronze_incremental_df = (
    spark.table(BRONZE_TABLE)
    .filter(F.col("ingestion_timestamp") > F.lit(last_processed_timestamp))
)

bronze_count = bronze_incremental_df.count()
print(f"Step 3 complete: Loaded {bronze_count} incremental record(s) from Bronze.")

# Create a temp view for optional SQL inspection/debugging
bronze_incremental_df.createOrReplaceTempView("bronze_incremental")

# ==========================================================================================
# STEP 4: Transform and clean the customer data
# ==========================================================================================
# Apply the business rules:
#   - trim and standardize text fields
#   - validate email
#   - validate age
#   - remove negative total_purchases
#   - create customer_segment
#   - calculate days_since_registration
#   - add last_updated timestamp
#
# Optional deduplication is included to keep the latest record for each customer_id
# within the current incremental batch.
# ==========================================================================================

silver_incremental_df = (
    bronze_incremental_df
    # Standardize and clean text columns
    .withColumn("name", F.trim(F.col("name")))
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("country", F.trim(F.col("country")))
    .withColumn("customer_type", F.trim(F.col("customer_type")))
    .withColumn("gender", F.trim(F.col("gender")))

    # Convert data types
    .withColumn("registration_date", F.to_date(F.col("registration_date")))
    .withColumn("age", F.col("age").cast("int"))
    .withColumn("total_purchases", F.col("total_purchases").cast("int"))

    # Email validation
    .filter(F.col("email").isNotNull())
    .filter(F.trim(F.col("email")) != "")
    .filter(F.col("email").rlike(EMAIL_REGEX))

    # Age validation
    .filter(F.col("age").between(18, 100))

    # Remove junk records
    .filter(F.col("total_purchases") >= 0)

    # Derive customer segment
    .withColumn(
        "customer_segment",
        F.when(F.col("total_purchases") > 10000, "High Value")
         .when((F.col("total_purchases") > 5000) & (F.col("total_purchases") <= 10000), "Medium Value")
         .otherwise("Low Value")
    )

    # Calculate days since registration
    .withColumn(
        "days_since_registration",
        F.datediff(F.current_date(), F.col("registration_date"))
    )

    # Add load timestamp
    .withColumn("last_updated", F.current_timestamp())

    # Select final Silver schema
    .select(
        "customer_id",
        "name",
        "email",
        "country",
        "customer_type",
        "registration_date",
        "age",
        "gender",
        "total_purchases",
        "customer_segment",
        "days_since_registration",
        "last_updated"
    )
)

# Optional deduplication:
# If the incremental Bronze batch contains more than one record per customer_id,
# keep only the newest one by last_updated.
window_spec = Window.partitionBy("customer_id").orderBy(F.col("last_updated").desc())

silver_incremental_df = (
    silver_incremental_df
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

silver_count = silver_incremental_df.count()
print(f"Step 4 complete: {silver_count} record(s) remain after transformation and validation.")

# Create temp view for merge step
silver_incremental_df.createOrReplaceTempView("silver_incremental")

# ==========================================================================================
# STEP 5: Merge data into the Silver layer
# ==========================================================================================
# Perform an upsert:
#   - update existing records when customer_id already exists
#   - insert new records when customer_id does not exist
# ==========================================================================================

spark.sql(f"""
MERGE INTO {SILVER_TABLE} AS target
USING silver_incremental AS source
ON target.customer_id = source.customer_id

WHEN MATCHED THEN UPDATE SET
    target.name = source.name,
    target.email = source.email,
    target.country = source.country,
    target.customer_type = source.customer_type,
    target.registration_date = source.registration_date,
    target.age = source.age,
    target.gender = source.gender,
    target.total_purchases = source.total_purchases,
    target.customer_segment = source.customer_segment,
    target.days_since_registration = source.days_since_registration,
    target.last_updated = source.last_updated

WHEN NOT MATCHED THEN INSERT (
    customer_id,
    name,
    email,
    country,
    customer_type,
    registration_date,
    age,
    gender,
    total_purchases,
    customer_segment,
    days_since_registration,
    last_updated
)
VALUES (
    source.customer_id,
    source.name,
    source.email,
    source.country,
    source.customer_type,
    source.registration_date,
    source.age,
    source.gender,
    source.total_purchases,
    source.customer_segment,
    source.days_since_registration,
    source.last_updated
)
""")

print(f"Step 5 complete: Merge into '{SILVER_TABLE}' finished successfully.")

# ==========================================================================================
# STEP 6: Verify the data in the Silver layer
# ==========================================================================================
# Run validation checks to confirm:
#   - row count
#   - sample output
#   - segment distribution
#   - invalid records count
#   - duplicate customer_id count
# ==========================================================================================

print("Step 6: Verification results")

print("\n1. Total record count in Silver table")
spark.sql(f"""
SELECT COUNT(*) AS total_records
FROM {SILVER_TABLE}
""").show()

print("\n2. Sample records from Silver table")
spark.sql(f"""
SELECT *
FROM {SILVER_TABLE}
ORDER BY last_updated DESC
LIMIT 10
""").show(truncate=False)

print("\n3. Customer segment distribution")
spark.sql(f"""
SELECT
    customer_segment,
    COUNT(*) AS record_count
FROM {SILVER_TABLE}
GROUP BY customer_segment
ORDER BY record_count DESC
""").show()

print("\n4. Invalid record check")
spark.sql(f"""
SELECT COUNT(*) AS invalid_record_count
FROM {SILVER_TABLE}
WHERE email IS NULL
   OR TRIM(email) = ''
   OR email NOT RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{{2,}}$'
   OR age NOT BETWEEN 18 AND 100
   OR total_purchases < 0
""").show()

print("\n5. Duplicate customer_id check")
spark.sql(f"""
SELECT
    customer_id,
    COUNT(*) AS duplicate_count
FROM {SILVER_TABLE}
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
""").show()

print("Silver layer customer load completed successfully.")

