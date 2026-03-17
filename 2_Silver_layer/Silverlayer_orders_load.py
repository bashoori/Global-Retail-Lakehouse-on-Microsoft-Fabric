# ==========================================================================================
# Notebook Name: Silverlayer_orders_load
# Purpose:
#     Load order data from the Bronze layer into the Silver layer in Microsoft Fabric.
#
# Description:
#     This notebook performs an incremental load of order data from bronzelayer.orders
#     into silver_orders. It applies cleansing and transformation rules so that the
#     Silver layer contains standardized, analytics-ready order records.
#
# Business Rules Implemented:
#     1. Quantity Normalization:
#        - if quantity < 0, set quantity = 0
#
#     2. Total Amount Normalization:
#        - if total_amount < 0, set total_amount = 0
#
#     3. Date Casting:
#        - cast transaction_date to DATE
#
#     4. Order Status Derivation:
#        - Cancelled   : quantity = 0 and total_amount = 0
#        - Completed   : quantity > 0 and total_amount > 0
#        - In Progress : all other valid cases
#
#     5. Data Quality Checks:
#        - transaction_date must not be null
#        - customer_id must not be null or empty
#        - product_id must not be null or empty
#
# Processing Logic:
#     - Create Silver table if it does not exist
#     - Find the last processed timestamp from Silver
#     - Read only new Bronze records using ingestion_timestamp
#     - Clean and transform order data
#     - Merge transformed data into Silver
#     - Verify the load using counts and sample queries
#
# Assumptions:
#     - Bronze source table: bronzelayer.orders
#     - Bronze source contains: transaction_id, customer_id, product_id, quantity,
#       total_amount, transaction_date, ingestion_timestamp
#     - Delta Lake merge is supported in the Fabric Lakehouse environment
#
# Notes:
#     - This notebook uses Silver.last_updated as the incremental watermark.
#     - In production, a separate watermark/control table is usually better.
# ==========================================================================================

# ---------------------------------------------
# Imports
# ---------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------
# Configuration
# ---------------------------------------------
BRONZE_TABLE = "bronzelayer.orders"
SILVER_TABLE = "silver_orders"
DEFAULT_TIMESTAMP = "1900-01-01 00:00:00"

print("Starting Silver layer order load...")

# ==========================================================================================
# STEP 1: Create the Silver layer table for order data
# ==========================================================================================
# This table stores the cleaned and transformed order records.
# USING DELTA is included to support merge operations and ACID-compliant updates.
# ==========================================================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    total_amount DOUBLE,
    transaction_date DATE,
    order_status STRING,
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
# Read only Bronze order records with ingestion_timestamp greater than the last processed
# timestamp. This avoids reprocessing the full Bronze dataset every time the notebook runs.
# ==========================================================================================

bronze_incremental_orders_df = (
    spark.table(BRONZE_TABLE)
    .filter(F.col("ingestion_timestamp") > F.lit(last_processed_timestamp))
)

bronze_count = bronze_incremental_orders_df.count()
print(f"Step 3 complete: Loaded {bronze_count} incremental record(s) from Bronze.")

# Create a temp view for optional SQL inspection/debugging
bronze_incremental_orders_df.createOrReplaceTempView("bronze_incremental_orders")

# ==========================================================================================
# STEP 4: Transform and clean the order data
# ==========================================================================================
# Apply the business rules:
#   - map transaction_id to order_id
#   - normalize negative quantity to 0
#   - normalize negative total_amount to 0
#   - cast transaction_date to DATE
#   - derive order_status
#   - keep only records with valid transaction_date, customer_id, and product_id
#   - add last_updated timestamp
#
# Optional deduplication is included to keep the latest record for each order_id
# within the current incremental batch.
# ==========================================================================================

silver_incremental_orders_df = (
    bronze_incremental_orders_df
    # Rename transaction_id to order_id
    .withColumn("order_id", F.col("transaction_id"))

    # Standardize critical string fields
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("product_id", F.trim(F.col("product_id")))

    # Convert data types
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("total_amount", F.col("total_amount").cast("double"))
    .withColumn("transaction_date", F.to_date(F.col("transaction_date")))

    # Data quality filters
    .filter(F.col("transaction_date").isNotNull())
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("customer_id") != "")
    .filter(F.col("product_id").isNotNull())
    .filter(F.col("product_id") != "")

    # Quantity normalization
    .withColumn(
        "quantity",
        F.when(F.col("quantity").isNull(), F.lit(0))
         .when(F.col("quantity") < 0, F.lit(0))
         .otherwise(F.col("quantity"))
    )

    # Total amount normalization
    .withColumn(
        "total_amount",
        F.when(F.col("total_amount").isNull(), F.lit(0.0))
         .when(F.col("total_amount") < 0, F.lit(0.0))
         .otherwise(F.col("total_amount"))
    )

    # Order status derivation
    .withColumn(
        "order_status",
        F.when((F.col("quantity") == 0) & (F.col("total_amount") == 0), "Cancelled")
         .when((F.col("quantity") > 0) & (F.col("total_amount") > 0), "Completed")
         .otherwise("In Progress")
    )

    # Load timestamp
    .withColumn("last_updated", F.current_timestamp())

    # Select final Silver schema
    .select(
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "total_amount",
        "transaction_date",
        "order_status",
        "last_updated"
    )
)

# Optional deduplication:
# If the incremental Bronze batch contains more than one record per order_id,
# keep only the newest one by last_updated.
window_spec = Window.partitionBy("order_id").orderBy(F.col("last_updated").desc())

silver_incremental_orders_df = (
    silver_incremental_orders_df
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

silver_count = silver_incremental_orders_df.count()
print(f"Step 4 complete: {silver_count} record(s) remain after transformation and validation.")

# Create temp view for merge step
silver_incremental_orders_df.createOrReplaceTempView("silver_incremental_orders")

# ==========================================================================================
# STEP 5: Merge data into the Silver layer
# ==========================================================================================
# Perform an upsert:
#   - update existing records when order_id already exists
#   - insert new records when order_id does not exist
# ==========================================================================================

spark.sql(f"""
MERGE INTO {SILVER_TABLE} AS target
USING silver_incremental_orders AS source
ON target.order_id = source.order_id

WHEN MATCHED THEN UPDATE SET
    target.customer_id = source.customer_id,
    target.product_id = source.product_id,
    target.quantity = source.quantity,
    target.total_amount = source.total_amount,
    target.transaction_date = source.transaction_date,
    target.order_status = source.order_status,
    target.last_updated = source.last_updated

WHEN NOT MATCHED THEN INSERT (
    order_id,
    customer_id,
    product_id,
    quantity,
    total_amount,
    transaction_date,
    order_status,
    last_updated
)
VALUES (
    source.order_id,
    source.customer_id,
    source.product_id,
    source.quantity,
    source.total_amount,
    source.transaction_date,
    source.order_status,
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
#   - order status distribution
#   - invalid data checks
#   - duplicate order_id count
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

print("\n3. Order status distribution")
spark.sql(f"""
SELECT
    order_status,
    COUNT(*) AS record_count
FROM {SILVER_TABLE}
GROUP BY order_status
ORDER BY record_count DESC
""").show()

print("\n4. Invalid record check")
spark.sql(f"""
SELECT COUNT(*) AS invalid_record_count
FROM {SILVER_TABLE}
WHERE transaction_date IS NULL
   OR customer_id IS NULL
   OR TRIM(customer_id) = ''
   OR product_id IS NULL
   OR TRIM(product_id) = ''
   OR quantity < 0
   OR total_amount < 0
""").show()

print("\n5. Duplicate order_id check")
spark.sql(f"""
SELECT
    order_id,
    COUNT(*) AS duplicate_count
FROM {SILVER_TABLE}
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
""").show()

print("Silver layer order load completed successfully.")