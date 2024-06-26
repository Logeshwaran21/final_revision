# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

data = [("logesh","HR",20000),("subasree","manager",25000),("thillai","HR",30000),("maha","IT",23999),("sathya","IT",23444),("nihal","DE",24333),("kathir","manager",25000),("prasanna","IT",23999),("mani","IT",25999)]
schema = ["name","dept","salary"]
df= spark.createDataFrame(data,schema)
df.show()



# COMMAND ----------

window_spec = Window.partitionBy("dept").orderBy("salary")

# COMMAND ----------

# row number - used to give the sequential row number startting from 1 
df_row_num = df_sorted.withColumn("rownumber", row_number().over(window_spec))
df_row_num.show()

# COMMAND ----------

#rank () window function is used to provide a rank to the result with in a window partition. leaves gaps in rank when there are ties

df_rank =df_sorted.withColumn("rank", rank().over(window_spec))
df_rank.show()



# COMMAND ----------

#dense_rank - get the result with rank of rows within a window partition without any gaps
df_dense= df_sorted.withColumn("denseRank",dense_rank().over(window_spec))
df_dense.show()

# COMMAND ----------

