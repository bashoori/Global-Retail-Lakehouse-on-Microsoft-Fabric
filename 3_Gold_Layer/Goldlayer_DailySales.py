# ==========================================================================================
# Notebook Name: Goldlayer_DailySales
# Purpose:
#     Load aggregated daily sales data into the Gold layer in Microsoft Fabric.
#
# Description:
#     This notebook creates a business-ready Gold table named gold_daily_sales by
#     aggregating order data from the Silver layer. The output provides total sales
#     per transaction date and is intended for reporting, dashboarding, and business
#     analysis.
#
# Business Objective:
#     Provide a clean daily sales summary that helps the business track sales
#     performance over time.
#
# Source Table:
#     - silver_orders
#
# Target Table:
#     - gold_daily_sales
#
# Transformation Logic:
#     - Read order data from the Silver layer
#     - Group records by transaction_date
#     - Aggregate total sales using SUM(total_amount)
#     - Store the result in the Gold layer
#
# Assumptions:
#     - silver_orders already exists and contains:
#         order_id, customer_id, product_id, quantity, total_amount,
#         transaction_date, order_status, last_updated
#     - Only valid Silver-layer records are used
#     - transaction_date is already stored as DATE in Silver
#
# Notes:
#     - This notebook uses CREATE OR REPLACE TABLE, which rebuilds the Gold table
#       each time it runs.
#     - For larger production pipelines, incremental refresh logic may be preferred.
# ==========================================================================================

# ---------------------------------------------
# Configuration
# ---------------------------------------------
SOURCE_TABLE = "silver_orders"
TARGET_TABLE = "gold_daily_sales"

print("Starting Gold layer daily sales load...")

# ==========================================================================================
# STEP 1: Create or replace the Gold table
# ==========================================================================================
# Aggregate total sales amount by transaction_date from the Silver layer.
# The result is written into the Gold table as a daily summary.
# ==========================================================================================

spark.sql(f"""
CREATE OR REPLACE TABLE {TARGET_TABLE}
USING DELTA
AS
SELECT
    transaction_date,
    SUM(total_amount) AS daily_total_sales
FROM {SOURCE_TABLE}
GROUP BY transaction_date
""")

print(f"Step 1 complete: Table '{TARGET_TABLE}' created successfully.")

# ==========================================================================================
# STEP 2: Verify the Gold table
# ==========================================================================================
# Run validation checks to confirm:
#   - row count
#   - sample output
#   - date range
#   - total aggregated sales
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
ORDER BY transaction_date DESC
LIMIT 10
""").show(truncate=False)

print("\n3. Date range in Gold table")
spark.sql(f"""
SELECT
    MIN(transaction_date) AS min_transaction_date,
    MAX(transaction_date) AS max_transaction_date
FROM {TARGET_TABLE}
""").show()

print("\n4. Total aggregated sales across all days")
spark.sql(f"""
SELECT
    SUM(daily_total_sales) AS grand_total_sales
FROM {TARGET_TABLE}
""").show()

print("Gold layer daily sales load completed successfully.")