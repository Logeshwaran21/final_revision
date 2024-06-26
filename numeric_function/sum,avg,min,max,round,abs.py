# Databricks notebook source
from pyspark.sql.functions import *

# Sample data
data = [
    (1, 10.5),
    (2, -20.75),
    (3, 30.1),
    (4, -40.2),
    (5, 50.9)
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "value"])

# Show the DataFrame
df.show()


# COMMAND ----------

#sum of value
df_sum= df.select(sum("value").alias("sum_value")).show()

# COMMAND ----------

#avg of value
df_avg = df.select(avg("value").alias("avg_value")).show()

# COMMAND ----------

#min
df_min = df.select(min("value").alias("min_value")).show()

# COMMAND ----------

#max
df_max = df.select(max("value").alias("max_value")).show()

# COMMAND ----------

#rounded value
df_avg = df.withColumn("rounded value",round("value")).show()

# COMMAND ----------

#absolute value
df_avg = df.withColumn("absolute value",abs("value")).show()

# COMMAND ----------

