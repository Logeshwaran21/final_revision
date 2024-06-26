# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("String Functions").getOrCreate()
data= [("Sathya@Priya","Computer Science",1),("Ravi@Chandran","Business",2),("Bhuvaneshwar@Karthick","Electronics",3)]
 
rdd = spark.sparkContext.parallelize(data)
df = spark.createDataFrame(rdd,["Name","Department","Id"])
 
df.printSchema()
df.show()
 


# COMMAND ----------

#regexp_extract
df_extract = df.withColumn("First_Word", regexp_extract(df["Name"], r"([^@]+)", 1)) \
               .withColumn("Last_Word", regexp_extract(df["Name"], r"@(.+)", 1))
df_extract.display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

# Initialize Spark session
spark = SparkSession.builder.appName("SplitColumnExample").getOrCreate()

# Create DataFrame with multiple rows
data = [("Sathya@Priya",), ("Ravi@Chandran",), ("Bhuvaneshwar@Karthick",)]
df = spark.createDataFrame(data, ["Name"])

# Use regexp_extract to split the column correctly
df_extract = df.withColumn("First_Word", regexp_extract(df["Name"], r"([^@]+)", 1)) \
               .withColumn("Last_Word", regexp_extract(df["Name"], r"@(.+)", 1))

# Display the result
df_extract.show()

# COMMAND ----------

