# Databricks notebook source
data = [(1,"logesh",23),(2,"Subasree",None)]
schema = ["id","name","age"]
df1 = spark.createDataFrame(data,schema)

# COMMAND ----------

data = [(1,"logesh",23),(2,"Subasree",None)]
schema = ["id","name","age"]
df3 = spark.createDataFrame(data,schema)
df3.show()

# COMMAND ----------

data = [("logesh",1,23),("logesSuba",3,25),("thillai",4,32)]
schema=["name","id","age"]
df2=spark.createDataFrame(data,schema)


# COMMAND ----------

df1.show()
df1.printSchema()
df2.show()
df2.printSchema()

# COMMAND ----------

df1.union(df2).show()

# COMMAND ----------

df1.unionAll(df3).show()

# COMMAND ----------

# MAGIC %md
# MAGIC unionByName: Combines DataFrames by matching columns by name, useful for DataFrames with columns in different orders.

# COMMAND ----------

df1.unionByName(df2).show()

# COMMAND ----------

