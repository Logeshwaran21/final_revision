# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Sample data
data = [
    (1, True, False),
    (2, False, True),
    (3, True, True),
    (4, False, False),
    (5, True, False)
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "condition1", "condition2"])

# Show the DataFrame
df.show()




# COMMAND ----------


# Perform AND operation
df_and = df.withColumn("condition_and", col("condition1") & col("condition2"))
df_and.show()



# COMMAND ----------

# Perform OR operation
df_or = df.withColumn("condition_or", col("condition1") | col("condition2"))
df_or.show()

# COMMAND ----------

