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
from pyspark.sql.types import DateType, IntegerType
from pyspark.sql.window import Window

GOLD_PREFIX = "gold_"

customers = spark.table("silver_customers")
payments = spark.table("silver_payments")
fx_rates = spark.table("silver_fx_rates")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_customer = (
    customers.select(
        F.monotonically_increasing_id().alias("customer_key"),
        "customer_id",
        "company_name",
        "country_code",
        "country_name",
        "segment",
        "industry",
        "kyc_status",
        "primary_currency",
        "is_active",
        "onboarded_date",
        "annual_payment_volume_usd",
    )
)

dim_customer.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_PREFIX}dim_customer")
print(f"gold_dim_customer: {dim_customer.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

corridor_pairs = (
    payments.select("source_country_code", "target_country_code")
    .distinct()
    .withColumn(
        "corridor_name",
        F.concat(F.col("source_country_code"), F.lit(" → "), F.col("target_country_code")),
    )
)

dim_corridor = corridor_pairs.select(
    F.row_number().over(Window.orderBy("source_country_code", "target_country_code")).alias("corridor_key"),
    "source_country_code",
    "target_country_code",
    "corridor_name",
)

dim_corridor.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_PREFIX}dim_corridor")
print(f"gold_dim_corridor: {dim_corridor.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

currency_codes = (
    payments.select(F.col("source_currency").alias("currency_code"))
    .union(payments.select(F.col("target_currency").alias("currency_code")))
    .union(customers.select(F.col("primary_currency").alias("currency_code")))
    .distinct()
    .filter(F.col("currency_code").isNotNull())
)

dim_currency = currency_codes.select(
    F.row_number().over(Window.orderBy("currency_code")).alias("currency_key"),
    "currency_code",
)

dim_currency.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_PREFIX}dim_currency")
print(f"gold_dim_currency: {dim_currency.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

currency_codes = (
    payments.select(F.col("source_currency").alias("currency_code"))
    .union(payments.select(F.col("target_currency").alias("currency_code")))
    .union(customers.select(F.col("primary_currency").alias("currency_code")))
    .distinct()
    .filter(F.col("currency_code").isNotNull())
)

dim_currency = currency_codes.select(
    F.row_number().over(Window.orderBy("currency_code")).alias("currency_key"),
    "currency_code",
)

dim_currency.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_PREFIX}dim_currency")
print(f"gold_dim_currency: {dim_currency.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

date_bounds = payments.agg(
    F.min("initiated_date").alias("min_date"),
    F.max("initiated_date").alias("max_date"),
).collect()[0]

start_date = date_bounds["min_date"]
end_date = date_bounds["max_date"]

date_range = spark.sql(
    f"""
    SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS full_date
    """
)

dim_date = (
    date_range
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast(IntegerType()))
    .withColumn("year", F.year("full_date"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("day_of_month", F.dayofmonth("full_date"))
    .withColumn("day_of_week", F.dayofweek("full_date"))
    .withColumn("day_name", F.date_format("full_date", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
)

dim_date.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_PREFIX}dim_date")
print(f"gold_dim_date: {dim_date.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_lookup = dim_customer.select(
    F.col("customer_key"),
    F.col("customer_id"),
    F.col("segment").alias("customer_segment"),
)

corridor_lookup = dim_corridor.select(
    F.col("corridor_key"),
    F.col("source_country_code"),
    F.col("target_country_code"),
)

source_currency_lookup = dim_currency.select(
    F.col("currency_key").alias("source_currency_key"),
    F.col("currency_code").alias("source_currency"),
)

target_currency_lookup = dim_currency.select(
    F.col("currency_key").alias("target_currency_key"),
    F.col("currency_code").alias("target_currency"),
)

date_lookup = dim_date.select(
    F.col("date_key"),
    F.col("full_date").alias("initiated_date"),
)

fx_daily = fx_rates.select(
    F.col("rate_date"),
    F.col("currency_code"),
    F.col("rate_to_usd").alias("daily_rate_to_usd"),
)

fact_base = (
    payments.alias("p")
    .join(customer_lookup.alias("c"), on="customer_id", how="inner")
    .join(
        corridor_lookup.alias("cor"),
        (F.col("p.source_country_code") == F.col("cor.source_country_code"))
        & (F.col("p.target_country_code") == F.col("cor.target_country_code")),
        how="left",
    )
    .join(source_currency_lookup.alias("sc"), on="source_currency", how="left")
    .join(target_currency_lookup.alias("tc"), on="target_currency", how="left")
    .join(date_lookup.alias("d"), on="initiated_date", how="left")
    .join(
        fx_daily.alias("fx"),
        (F.col("p.initiated_date") == F.col("fx.rate_date"))
        & (F.col("p.source_currency") == F.col("fx.currency_code")),
        how="left",
    )
)

fact_payment = fact_base.select(
    F.col("p.payment_id"),
    F.col("c.customer_key"),
    F.col("cor.corridor_key"),
    F.col("sc.source_currency_key"),
    F.col("tc.target_currency_key"),
    F.col("d.date_key"),
    F.col("p.customer_id"),
    F.col("c.customer_segment"),
    F.col("p.source_country_code"),
    F.col("p.target_country_code"),
    F.col("p.source_currency"),
    F.col("p.target_currency"),
    F.col("p.payment_method"),
    F.col("p.corridor_type"),
    F.col("p.status"),
    F.col("p.amount_source"),
    F.col("p.amount_usd"),
    F.col("p.fx_rate_to_usd"),
    F.col("fx.daily_rate_to_usd"),
    F.col("p.fee_usd"),
    F.col("p.initiated_at"),
    F.col("p.completed_at"),
    F.col("p.initiated_date"),
    F.col("p.settlement_hours"),
    F.when(F.col("p.status") == "completed", 1).otherwise(0).alias("is_successful"),
    F.when(F.col("p.status") == "failed", 1).otherwise(0).alias("is_failed"),
    F.current_timestamp().alias("_loaded_at"),
)

fact_payment.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_PREFIX}fact_payment")
print(f"gold_fact_payment: {fact_payment.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Data quality checks ---
fact_count = fact_payment.count()
payment_count = payments.count()
missing_corridor = fact_payment.filter(F.col("corridor_key").isNull()).count()
missing_date_key = fact_payment.filter(F.col("date_key").isNull()).count()
missing_fx = fact_payment.filter(F.col("daily_rate_to_usd").isNull()).count()

print("--- Gold data quality ---")
print(f"Silver payments: {payment_count}")
print(f"Gold fact_payment: {fact_count}")
print(f"Missing corridor_key: {missing_corridor}")
print(f"Missing date_key: {missing_date_key}")
print(f"Missing daily FX rate: {missing_fx}")

assert fact_count == payment_count, "Fact row count does not match silver payments"
assert missing_corridor == 0, "Found payments without corridor_key"
assert missing_date_key == 0, "Found payments without date_key"

print("Gold load complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
