# Databricks notebook source
from pyspark.sql.functions import col, mean

# Sample data with NULL values
data = [
    (1, "Alice", 25, None),
    (2, "Bob", None, 3000),
    (3, None, 30, 4000),
    (4, "David", None, None),
    (5, "Eve", 28, 2500)
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "name", "age", "salary"])

# Show the DataFrame
df.show()

# Drop rows with any NULL values
df_drop_any = df.dropna()
df_drop_any.show()

# Drop rows with NULL values in specific columns
df_drop_subset = df.dropna(subset=["age", "salary"])
df_drop_subset.show()

# Fill NULL values with a specified value
df_fill = df.fillna({"age": 0, "salary": 0})
df_fill.show()



# COMMAND ----------

