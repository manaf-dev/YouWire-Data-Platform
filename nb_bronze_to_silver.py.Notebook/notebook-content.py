# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2f587482-62ae-4bb4-9574-9f26136cfc39",
# META       "default_lakehouse_name": "lh_youwire",
# META       "default_lakehouse_workspace_id": "2f3119d9-8f3c-459b-8816-0fba4a90e313",
# META       "known_lakehouses": [
# META         {
# META           "id": "2f587482-62ae-4bb4-9574-9f26136cfc39"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, DoubleType, IntegerType, TimestampType

LAKEHOUSE = "lh_youwire"
BRONZE = "Files/bronze"
SILVER_TABLE_PREFIX = "silver_"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customers_raw = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(f"{BRONZE}/customers/customers.csv")
)

customers = (
    customers_raw
    .dropDuplicates(["customer_id"])
    .withColumn("company_name", F.trim(F.col("company_name")))
    .withColumn("country_code", F.upper(F.trim(F.col("country_code"))))
    .withColumn("segment", F.trim(F.col("segment")))
    .withColumn("kyc_status", F.lower(F.trim(F.col("kyc_status"))))
    .withColumn("primary_currency", F.upper(F.trim(F.col("primary_currency"))))
    .withColumn("onboarded_date", F.to_date("onboarded_date"))
    .withColumn(
        "kyc_approved_date",
        F.when(F.col("kyc_approved_date") == "", None).otherwise(F.to_date("kyc_approved_date")),
    )
    .withColumn("is_active", F.col("is_active").cast(BooleanType()))
    .withColumn("annual_payment_volume_usd", F.col("annual_payment_volume_usd").cast(DoubleType()))
    .withColumn("_loaded_at", F.current_timestamp())
)

customers.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER_TABLE_PREFIX}customers")
print(f"silver_customers: {customers.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fx_raw = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(f"{BRONZE}/fx_rates/fx_rates.csv")
)

fx_rates = (
    fx_raw
    .dropDuplicates(["rate_date", "currency_code"])
    .withColumn("rate_date", F.to_date("rate_date"))
    .withColumn("currency_code", F.upper(F.trim(F.col("currency_code"))))
    .withColumn("rate_to_usd", F.col("rate_to_usd").cast(DoubleType()))
    .withColumn("_loaded_at", F.current_timestamp())
)

fx_rates.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER_TABLE_PREFIX}fx_rates")
print(f"silver_fx_rates: {fx_rates.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fee_raw = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(f"{BRONZE}/fee_schedule/fee_schedule.csv")
)

fee_schedule = (
    fee_raw
    .dropDuplicates(["fee_schedule_id"])
    .withColumn("segment", F.trim(F.col("segment")))
    .withColumn("payment_method", F.lower(F.trim(F.col("payment_method"))))
    .withColumn("corridor_type", F.lower(F.trim(F.col("corridor_type"))))
    .withColumn("min_amount_usd", F.col("min_amount_usd").cast(DoubleType()))
    .withColumn("max_amount_usd", F.col("max_amount_usd").cast(DoubleType()))
    .withColumn("fee_value", F.col("fee_value").cast(DoubleType()))
    .withColumn("effective_from", F.to_date("effective_from"))
    .withColumn("effective_to", F.to_date("effective_to"))
    .withColumn("is_active", F.col("is_active").cast(BooleanType()))
    .withColumn("_loaded_at", F.current_timestamp())
)

fee_schedule.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER_TABLE_PREFIX}fee_schedule")
print(f"silver_fee_schedule: {fee_schedule.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

payments_raw = (
    spark.read.option("multiline", True)
    .json(f"{BRONZE}/payments/*.json")
)

payments = (
    payments_raw
    .dropDuplicates(["payment_id"])
    .withColumn("source_country_code", F.upper(F.trim(F.col("source_country_code"))))
    .withColumn("target_country_code", F.upper(F.trim(F.col("target_country_code"))))
    .withColumn("source_currency", F.upper(F.trim(F.col("source_currency"))))
    .withColumn("target_currency", F.upper(F.trim(F.col("target_currency"))))
    .withColumn("payment_method", F.lower(F.trim(F.col("payment_method"))))
    .withColumn("corridor_type", F.lower(F.trim(F.col("corridor_type"))))
    .withColumn("status", F.lower(F.trim(F.col("status"))))
    .withColumn("amount_source", F.col("amount_source").cast(DoubleType()))
    .withColumn("amount_usd", F.col("amount_usd").cast(DoubleType()))
    .withColumn("fx_rate_to_usd", F.col("fx_rate_to_usd").cast(DoubleType()))
    .withColumn("fee_usd", F.col("fee_usd").cast(DoubleType()))
    .withColumn("initiated_at", F.to_timestamp("initiated_at"))
    .withColumn("completed_at", F.to_timestamp("completed_at"))
    .withColumn("settlement_hours", F.col("settlement_hours").cast(DoubleType()))
    .withColumn("initiated_date", F.to_date("initiated_at"))
    .withColumn("_loaded_at", F.current_timestamp())
)

payments.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER_TABLE_PREFIX}payments")
print(f"silver_payments: {payments.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Data quality checks ---
customer_ids = customers.select("customer_id").distinct()
orphan_payments = payments.join(customer_ids, on="customer_id", how="left_anti").count()
null_payment_ids = payments.filter(F.col("payment_id").isNull()).count()
null_customer_ids = payments.filter(F.col("customer_id").isNull()).count()

print("--- Data quality ---")
print(f"Orphan payments (no matching customer): {orphan_payments}")
print(f"Null payment_id: {null_payment_ids}")
print(f"Null customer_id: {null_customer_ids}")

assert orphan_payments == 0, "Found payments with unknown customer_id"
assert null_payment_ids == 0, "Found null payment_id values"

print("Silver load complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
