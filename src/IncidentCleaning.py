import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, year, month, count, desc, collect_list, row_number
from pyspark.sql.window import Window

# Suppress Spark's noisy INFO logs
spark = SparkSession.builder.appName("ChicagoIncidentsClean").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

os.makedirs("outputs", exist_ok=True)
log_file = "outputs/t1_summary.txt"

def log_and_print(message):
    """Helper function to print to terminal and write to file simultaneously."""
    print(message)
    with open(log_file, "a") as f:
        f.write(message + "\n")

# Initialize the text file
with open(log_file, "w") as f:
    f.write("=== TASK 1 SUMMARY ===\n\n")

# ==========================================
# STEP 0: LOAD DATA
# ==========================================
log_and_print("--- Data Loading ---")
df = spark.read.parquet("data/raw/chicago_incidents_2022_2024.parquet")
initial_count = df.count()
log_and_print(f"Initial raw rows loaded: {initial_count}\n")

# ==========================================
# STEP 1: CLEANING
# ==========================================
log_and_print("--- Cleaning Steps ---")

# 1a. Remove duplicates
df_dedup = df.dropDuplicates()
dedup_count = df_dedup.count()
log_and_print(f"After removing duplicates: {dedup_count} (Removed: {initial_count - dedup_count})")

# 1b. Remove missing dates
df_date = df_dedup.dropna(subset=["date"])
date_count = df_date.count()
log_and_print(f"After removing missing dates: {date_count} (Removed: {dedup_count - date_count})")

# 1c. Remove missing neighborhood (community_area) & invalid coordinates
df_neigh = df_date.dropna(subset=["community_area"])

# Filter for Chicago's bounding box
df_clean = df_neigh.filter(
    (col("latitude").isNotNull()) & (col("longitude").isNotNull()) &
    (col("latitude") >= 41.6441) & (col("latitude") <= 42.0234) &
    (col("longitude") >= -87.9400) & (col("longitude") <= -87.5240)
)
clean_count = df_clean.count()
log_and_print(f"After removing missing neighborhood/coords: {clean_count} (Removed: {date_count - clean_count})\n")

# ==========================================
# STEP 2: CLASSIFY
# ==========================================
log_and_print("--- Classification Mapping ---")

# Apply the classification logic
df_classified = df_clean.withColumn(
    "category",
    when(col("primary_type").isin(
        # Crimes Against Persons
        "ASSAULT", "BATTERY", "ROBBERY", "CRIMINAL SEXUAL ASSAULT", "HOMICIDE", 
        "KIDNAPPING", "STALKING", "INTIMIDATION", "HUMAN TRAFFICKING", 
        "OFFENSE INVOLVING CHILDREN", "SEX OFFENSE"
    ), "Crimes Against Persons")
    .when(col("primary_type").isin(
        # Crimes Against Property
        "THEFT", "BURGLARY", "MOTOR VEHICLE THEFT", "ARSON", "CRIMINAL DAMAGE", 
        "DECEPTIVE PRACTICE", "CRIMINAL TRESPASS"
    ), "Crimes Against Property")
    # Other types will be classified as "Other"
    .otherwise("Other")
)

log_and_print("Crimes Against Persons: ASSAULT, BATTERY, ROBBERY, CRIMINAL SEXUAL ASSAULT, HOMICIDE, KIDNAPPING, STALKING, INTIMIDATION, HUMAN TRAFFICKING, OFFENSE INVOLVING CHILDREN, SEX OFFENSE.")
log_and_print("Crimes Against Property: THEFT, BURGLARY, MOTOR VEHICLE THEFT, ARSON, CRIMINAL DAMAGE, DECEPTIVE PRACTICE, CRIMINAL TRESPASS.")
log_and_print("Other: All remaining types, including NARCOTICS, OTHER NARCOTIC VIOLATION, WEAPONS VIOLATION, PUBLIC PEACE VIOLATION, etc.\n")

# ==========================================
# STEP 3: SUMMARISE
# ==========================================
log_and_print("--- Summarization ---")
print("Building aggregation table (this may take a moment)...")

# Extract year and month
df_time = df_classified.withColumn("year", year(col("date"))) \
                       .withColumn("month", month(col("date")))

# Group keys
group_cols = ["community_area", "year", "month"]

# 1. Aggregate total and category counts
df_summary = df_time.groupBy(*group_cols).agg(
    count("*").alias("total_incidents"),
    count(when(col("category") == "Crimes Against Persons", True)).alias("incidents_persons"),
    count(when(col("category") == "Crimes Against Property", True)).alias("incidents_property"),
    count(when(col("category") == "Other", True)).alias("incidents_other")
)

# 2. Find Top 3 Offences
offence_counts = df_time.groupBy(*group_cols, "primary_type") \
                        .agg(count("*").alias("offense_count"))

# Rank them using a Window function
windowSpec = Window.partitionBy(*group_cols).orderBy(desc("offense_count"))
top_offences = offence_counts.withColumn("rank", row_number().over(windowSpec)) \
    .filter(col("rank") <= 3) \
    .groupBy(*group_cols) \
    .agg(collect_list("primary_type").alias("top_3_offences"))

# Join the counts and top offences
final_summary = df_summary.join(top_offences, on=group_cols, how="left")

# ==========================================
# STEP 4: OUTPUT
# ==========================================
print("Saving final aggregation table...")

# Convert to pandas and use pyarrow to write
os.makedirs("data/processed", exist_ok=True)
final_summary.toPandas().to_parquet("data/processed/neighbourhood_month.parquet")
print("Saved summary table to 'neighbourhood_month.parquet'.")

# Gather final stats for the text file
num_neighbourhoods = df_clean.select("community_area").distinct().count()
date_range = df_clean.agg({"date": "min"}).collect()[0][0], df_clean.agg({"date": "max"}).collect()[0][0]

# Append final stats to the text file
with open(log_file, "a") as f:
    f.write("--- Dataset Info ---\n")
    f.write(f"Number of unique neighbourhoods: {num_neighbourhoods}\n")
    f.write(f"Date range: {date_range[0]} to {date_range[1]}\n")

print("\nAll tasks complete! Check 't1_summary.txt' and 'neighbourhood_month.parquet'.")

# Stop Spark
spark.stop()