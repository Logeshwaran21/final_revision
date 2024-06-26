# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

# Define the schema
schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("doj", DateType(), True),
    StructField("dep_id", IntegerType(), True),
    StructField("salary", FloatType(), True)
])

# Sample data with datetime.date objects for the 'doj' field
data = [
    (1, "Alice", datetime.strptime("2020-01-15", "%Y-%m-%d").date(), 101, 70000.0),
    (2, "Bob", datetime.strptime("2019-03-10", "%Y-%m-%d").date(), 102, 80000.0),
    (3, "Cathy", datetime.strptime("2018-07-20", "%Y-%m-%d").date(), 103, 75000.0),
    (4, "David", datetime.strptime("2017-11-30", "%Y-%m-%d").date(), 104, 65000.0),
    (5, "Eva", datetime.strptime("2016-05-25", "%Y-%m-%d").date(), 105, 72000.0),
    (6, "Frank", datetime.strptime("2015-08-17", "%Y-%m-%d").date(), 101, 78000.0),
    (7, "Grace", datetime.strptime("2014-12-05", "%Y-%m-%d").date(), 102, 66000.0),
    (8, "Hank", datetime.strptime("2013-04-13", "%Y-%m-%d").date(), 103, 71000.0),
    (9, "Ivy", datetime.strptime("2012-09-21", "%Y-%m-%d").date(), 104, 69000.0),
    (10, "Jack", datetime.strptime("2011-06-14", "%Y-%m-%d").date(), 105, 73000.0)
]

# Create a DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
df.show()



# COMMAND ----------

# change the col name using UDF
def rename_columns(rename_df):
    for column in rename_df.columns:
        new_column= "col_"+column
        rename_df= rename_df.withColumnRenamed(column,new_column)
    return rename_df


# COMMAND ----------

rename= rename_columns(df)

# COMMAND ----------

rename.show()

# COMMAND ----------

df.show()

# COMMAND ----------

upper_df = df.withColumn("NAME", upper("Name") )
upper_df.show()

# COMMAND ----------

#changing the value in upper case
def upper_name(df_u,col_name):
    upper_df= df_u.withColumn("Name", upper(col_name))
    return upper_df


# COMMAND ----------

df_upper = upper_name(df,"Name")
df_upper.show()

# COMMAND ----------

