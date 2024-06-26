# Databricks notebook source
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("DuplicatesHandlingExample").getOrCreate()

# Sample data with duplicates
data = [
    (1, "Alice", 25, 3000),
    (2, "Bob", 30, 4000),
    (2, "Bob", 30, 4000),
    (3, "Charlie", 35, 5000),
    (4, "David", 40, 6000),
    (4, "David", 40, 6000),
    (5, "Eve", 45, 7000)
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "name", "age", "salary"])

# Show the DataFrame
df.show()

# Drop duplicate rows based on all columns
df_no_duplicates = df.dropDuplicates()
df_no_duplicates.show()

# Drop duplicate rows based on specific columns
df_no_duplicates_subset = df.dropDuplicates(["id", "name"])
df_no_duplicates_subset.show()

# Stop the Spark session
spark.stop()


# COMMAND ----------

