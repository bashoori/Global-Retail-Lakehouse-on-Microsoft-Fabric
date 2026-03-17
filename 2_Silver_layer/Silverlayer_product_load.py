
# ==========================================================================================
# Notebook Name: Silverlayer_product_load
# Purpose:
#     Load product data from the Bronze layer into the Silver layer in Microsoft Fabric.
#
# Description:
#     This notebook performs an incremental load of product data from bronzelayer.product
#     into silver_products. It applies cleansing and transformation rules so that the
#     Silver layer contains standardized, analytics-ready product records.
#
# Business Rules Implemented:
#     1. Price Normalization:
#        - if price < 0, set price = 0
#
#     2. Stock Quantity Normalization:
#        - if stock_quantity < 0, set stock_quantity = 0
#
#     3. Rating Normalization:
#        - if rating < 0, set rating = 0
#        - if rating > 5, set rating = 5
#        - otherwise keep rating as is
#
#     4. Price Categorization:
#        - Premium : price > 1000
#        - Standard: price > 100 and <= 1000
#        - Budget  : price <= 100
#
#     5. Stock Status Calculation:
#        - Out of Stock    : stock_quantity = 0
#        - Low Stock       : stock_quantity < 10
#        - Moderate Stock  : stock_quantity between 10 and 49
#        - Sufficient Stock: stock_quantity >= 50
#
#     6. Basic Record Validation:
#        - name must not be null or empty
#        - category must not be null or empty
#
# Processing Logic:
#     - Create Silver table if it does not exist
#     - Find the last processed timestamp from Silver
#     - Read only new Bronze records using ingestion_timestamp
#     - Clean and transform product data
#     - Merge transformed data into Silver
#     - Verify the load using counts and sample queries
#
# Assumptions:
#     - Bronze source table: bronzelayer.product
#     - Bronze source contains: product_id, name, category, brand, price, stock_quantity,
#       rating, is_active, ingestion_timestamp
#     - Delta Lake merge is supported in the Fabric Lakehouse environment
#
# Notes:
#     - This notebook uses Silver.last_updated as the incremental watermark.
#     - In production, a separate watermark or control table is usually better.
# ==========================================================================================

# ---------------------------------------------
# Imports
# ---------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------
# Configuration
# ---------------------------------------------
BRONZE_TABLE = "bronzelayer.product"
SILVER_TABLE = "silver_products"
DEFAULT_TIMESTAMP = "1900-01-01 00:00:00"

print("Starting Silver layer product load...")

# ==========================================================================================
# STEP 1: Create the Silver layer table for product data
# ==========================================================================================
# This table stores the cleaned and transformed product records.
# USING DELTA is included to support merge operations and ACID-compliant updates.
# ==========================================================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    product_id STRING,
    name STRING,
    category STRING,
    brand STRING,
    price DOUBLE,
    stock_quantity INT,
    rating DOUBLE,
    is_active BOOLEAN,
    price_category STRING,
    stock_status STRING,
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
# Read only Bronze product records with ingestion_timestamp greater than the last processed
# timestamp. This avoids reprocessing the full Bronze dataset every time the notebook runs.
# ==========================================================================================

bronze_incremental_products_df = (
    spark.table(BRONZE_TABLE)
    .filter(F.col("ingestion_timestamp") > F.lit(last_processed_timestamp))
)

bronze_count = bronze_incremental_products_df.count()
print(f"Step 3 complete: Loaded {bronze_count} incremental record(s) from Bronze.")

# Create a temp view for optional SQL inspection/debugging
bronze_incremental_products_df.createOrReplaceTempView("bronze_incremental_products")

# ==========================================================================================
# STEP 4: Transform and clean the product data
# ==========================================================================================
# Apply the business rules:
#   - trim and standardize text fields
#   - normalize negative price to 0
#   - normalize negative stock_quantity to 0
#   - clamp rating to the valid range [0, 5]
#   - derive price_category
#   - derive stock_status
#   - keep only records with valid name and category
#   - add last_updated timestamp
#
# Optional deduplication is included to keep the latest record for each product_id
# within the current incremental batch.
# ==========================================================================================

silver_incremental_products_df = (
    bronze_incremental_products_df
    # Standardize text columns
    .withColumn("name", F.trim(F.col("name")))
    .withColumn("category", F.trim(F.col("category")))
    .withColumn("brand", F.trim(F.col("brand")))

    # Convert data types
    .withColumn("price", F.col("price").cast("double"))
    .withColumn("stock_quantity", F.col("stock_quantity").cast("int"))
    .withColumn("rating", F.col("rating").cast("double"))
    .withColumn("is_active", F.col("is_active").cast("boolean"))

    # Basic record validation
    .filter(F.col("name").isNotNull())
    .filter(F.trim(F.col("name")) != "")
    .filter(F.col("category").isNotNull())
    .filter(F.trim(F.col("category")) != "")

    # Price normalization
    .withColumn(
        "price",
        F.when(F.col("price").isNull(), F.lit(0.0))
         .when(F.col("price") < 0, F.lit(0.0))
         .otherwise(F.col("price"))
    )

    # Stock quantity normalization
    .withColumn(
        "stock_quantity",
        F.when(F.col("stock_quantity").isNull(), F.lit(0))
         .when(F.col("stock_quantity") < 0, F.lit(0))
         .otherwise(F.col("stock_quantity"))
    )

    # Rating normalization
    .withColumn(
        "rating",
        F.when(F.col("rating").isNull(), F.lit(0.0))
         .when(F.col("rating") < 0, F.lit(0.0))
         .when(F.col("rating") > 5, F.lit(5.0))
         .otherwise(F.col("rating"))
    )

    # Price category
    .withColumn(
        "price_category",
        F.when(F.col("price") > 1000, "Premium")
         .when(F.col("price") > 100, "Standard")
         .otherwise("Budget")
    )

    # Stock status
    .withColumn(
        "stock_status",
        F.when(F.col("stock_quantity") == 0, "Out of Stock")
         .when(F.col("stock_quantity") < 10, "Low Stock")
         .when(F.col("stock_quantity") < 50, "Moderate Stock")
         .otherwise("Sufficient Stock")
    )

    # Load timestamp
    .withColumn("last_updated", F.current_timestamp())

    # Select final Silver schema
    .select(
        "product_id",
        "name",
        "category",
        "brand",
        "price",
        "stock_quantity",
        "rating",
        "is_active",
        "price_category",
        "stock_status",
        "last_updated"
    )
)

# Optional deduplication:
# If the incremental Bronze batch contains more than one record per product_id,
# keep only the newest one by last_updated.
window_spec = Window.partitionBy("product_id").orderBy(F.col("last_updated").desc())

silver_incremental_products_df = (
    silver_incremental_products_df
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

silver_count = silver_incremental_products_df.count()
print(f"Step 4 complete: {silver_count} record(s) remain after transformation and validation.")

# Create temp view for merge step
silver_incremental_products_df.createOrReplaceTempView("silver_incremental_products")

# ==========================================================================================
# STEP 5: Merge data into the Silver layer
# ==========================================================================================
# Perform an upsert:
#   - update existing records when product_id already exists
#   - insert new records when product_id does not exist
# ==========================================================================================

spark.sql(f"""
MERGE INTO {SILVER_TABLE} AS target
USING silver_incremental_products AS source
ON target.product_id = source.product_id

WHEN MATCHED THEN UPDATE SET
    target.name = source.name,
    target.category = source.category,
    target.brand = source.brand,
    target.price = source.price,
    target.stock_quantity = source.stock_quantity,
    target.rating = source.rating,
    target.is_active = source.is_active,
    target.price_category = source.price_category,
    target.stock_status = source.stock_status,
    target.last_updated = source.last_updated

WHEN NOT MATCHED THEN INSERT (
    product_id,
    name,
    category,
    brand,
    price,
    stock_quantity,
    rating,
    is_active,
    price_category,
    stock_status,
    last_updated
)
VALUES (
    source.product_id,
    source.name,
    source.category,
    source.brand,
    source.price,
    source.stock_quantity,
    source.rating,
    source.is_active,
    source.price_category,
    source.stock_status,
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
#   - price category distribution
#   - stock status distribution
#   - invalid data checks
#   - duplicate product_id count
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

print("\n3. Price category distribution")
spark.sql(f"""
SELECT
    price_category,
    COUNT(*) AS record_count
FROM {SILVER_TABLE}
GROUP BY price_category
ORDER BY record_count DESC
""").show()

print("\n4. Stock status distribution")
spark.sql(f"""
SELECT
    stock_status,
    COUNT(*) AS record_count
FROM {SILVER_TABLE}
GROUP BY stock_status
ORDER BY record_count DESC
""").show()

print("\n5. Invalid record check")
spark.sql(f"""
SELECT COUNT(*) AS invalid_record_count
FROM {SILVER_TABLE}
WHERE name IS NULL
   OR TRIM(name) = ''
   OR category IS NULL
   OR TRIM(category) = ''
   OR price < 0
   OR stock_quantity < 0
   OR rating < 0
   OR rating > 5
""").show()

print("\n6. Duplicate product_id check")
spark.sql(f"""
SELECT
    product_id,
    COUNT(*) AS duplicate_count
FROM {SILVER_TABLE}
GROUP BY product_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
""").show()

print("Silver layer product load completed successfully.")

