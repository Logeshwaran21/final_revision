# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window



# COMMAND ----------

# Sample data
data = [("logesh", "HR", 20000),
        ("subasree", "manager", 25000),
        ("thillai", "HR", 30000),
        ("maha", "IT", 23999),
        ("sathya", "IT", 23444),
        ("nihal", "DE", 24333),
        ("kathir", "manager", 25000),
        ("prasanna", "IT", 23999)]

# Schema
schema = ["name", "dept", "salary"]

# COMMAND ----------

df= spark.createDataFrame(data,schema)

# COMMAND ----------


windows= Window.partitionBy("dept").orderBy("salary")

# COMMAND ----------

# lead - next value
lead_record = df.withColumn("lead_value", lead("salary").over(windows))
lead_record.show()

# COMMAND ----------

#lag - previous value
lag_value = df.withColumn("lag_value",lag("salary").over(windows))
lag_value.show()

# COMMAND ----------

