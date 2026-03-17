# ==========================================================================================
# Notebook Name: Goldlayer_CategorySales
# Purpose:
#     Load aggregated sales-by-category data into the Gold layer in Microsoft Fabric.
#
# Description:
#     This notebook creates a business-ready Gold table named gold_category_sales by
#     joining order data from the Silver layer with product data from the Silver layer,
#     then aggregating total sales by product category.
#
# Business Objective:
#     Provide a category-level sales summary that helps the business understand which
#     product categories generate the most revenue and support decisions around
#     inventory, pricing, and marketing.
#
# Source Tables:
#     - silver_orders
#     - silver_products
#
# Target Table:
#     - gold_category_sales
#
# Transformation Logic:
#     - Read order data from the Silver layer
#     - Read product data from the Silver layer
#     - Join orders to products using product_id
#     - Group records by product category
#     - Aggregate total sales using SUM(total_amount)
#     - Store the result in the Gold layer
#
# Assumptions:
#     - silver_orders already exists and contains:
#         order_id, customer_id, product_id, quantity, total_amount,
#         transaction_date, order_status, last_updated
#     - silver_products already exists and contains:
#         product_id, name, category, brand, price, stock_quantity,
#         rating, is_active, price_category, stock_status, last_updated
#     - product_id is the correct business key for joining orders to products
#     - category is already cleansed and standardized in Silver
#
# Notes:
#     - This notebook uses CREATE OR REPLACE TABLE, which rebuilds the Gold table
#       each time it runs.
#     - For larger production pipelines, incremental refresh logic may be preferred.
#     - This logic keeps all matched order records, regardless of category size.
# ==========================================================================================

# ---------------------------------------------
# Configuration
# ---------------------------------------------
ORDERS_SOURCE_TABLE = "silver_orders"
PRODUCTS_SOURCE_TABLE = "silver_products"
TARGET_TABLE = "gold_category_sales"

print("Starting Gold layer category sales load...")

# ==========================================================================================
# STEP 1: Create or replace the Gold table
# ==========================================================================================
# Join Silver orders with Silver products using product_id, then aggregate total sales
# by product category. The result is written into the Gold table as a category-level
# business summary.
# ==========================================================================================

spark.sql(f"""
CREATE OR REPLACE TABLE {TARGET_TABLE}
USING DELTA
AS
SELECT
    p.category AS product_category,
    SUM(o.total_amount) AS category_total_sales
FROM {ORDERS_SOURCE_TABLE} o
JOIN {PRODUCTS_SOURCE_TABLE} p
    ON o.product_id = p.product_id
GROUP BY p.category
""")

print(f"Step 1 complete: Table '{TARGET_TABLE}' created successfully.")

# ==========================================================================================
# STEP 2: Verify the Gold table
# ==========================================================================================
# Run validation checks to confirm:
#   - row count
#   - sample output
#   - total aggregated category sales
#   - top categories by sales
# ==========================================================================================

print("Step 2: Verification results")

print("\n1. Total row count in Gold table")
spark.sql(f"""
SELECT COUNT(*) AS total_rows
FROM {TARGET_TABLE}
""").show()

print("\n2. Sample records from Gold table")
spark.sql(f"""
SELECT *
FROM {TARGET_TABLE}
ORDER BY category_total_sales DESC
LIMIT 10
""").show(truncate=False)

print("\n3. Total aggregated sales across all categories")
spark.sql(f"""
SELECT
    SUM(category_total_sales) AS grand_total_category_sales
FROM {TARGET_TABLE}
""").show()

print("\n4. Top product categories by sales")
spark.sql(f"""
SELECT
    product_category,
    category_total_sales
FROM {TARGET_TABLE}
ORDER BY category_total_sales DESC
LIMIT 10
""").show(truncate=False)

print("Gold layer category sales load completed successfully.")