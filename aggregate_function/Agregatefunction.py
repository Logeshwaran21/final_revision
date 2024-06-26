# Databricks notebook source
#reading the Json file 
employee_df = spark.read.format("csv").option("header","True").option("inferschema","True").load("dbfs:/FileStore/Employee/Employee_Salary_Dataset.csv")

# COMMAND ----------

employee_df.display()

# COMMAND ----------

#printing shcema
employee_df.printSchema()

# COMMAND ----------

# if salary id string convert it to integer
# df= empoyee_df.withColumn("salary",employee_df.salary.cast("int"))

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1=employee_df.groupBy("Age", "Gender").avg("salary").alias("avg_salary")
df1.orderBy("Gender").show()

df2 = employee_df.select(avg("salary"))
df2.show()



# COMMAND ----------

employee_df.select(avg("salary")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC collect_list vs collect_set
# MAGIC
# MAGIC how we can aggreate multiple rows into single rows

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

data = [
    (1,"logesh",[20,30,20]),
    (2,"subasree",[32,42,43]),
    (3,"thillai",[23,None,1]),
    (4,"kathir",[])
]
schema = StructType(
    [
        StructField("ID",IntegerType(),True),
        StructField("Name",StringType(),True),
        StructField("marks", ArrayType(IntegerType()),True)
    
    ]
)

# COMMAND ----------

df = spark.createDataFrame(data, schema)
# Display the DataFrame
df.display()

# Print Schema
df.printSchema()

# COMMAND ----------

explode_df = df.select("ID", "Name", explode("marks").alias("Marks"))
explode_df.display()

# COMMAND ----------

# Select and explode the "marks" column
explode_outer_df = df.select("ID", "Name", explode_outer("marks").alias("Marks"))
explode_outer_df.show()

# COMMAND ----------


#collect_list contains duplicates
#collect_list_df=explode_outer_df.groupBy("ID","Name").aggregate(collect_list("Marks"))
collect_list_df = explode_outer_df.groupBy("ID", "Marks").agg(collect_list("Name").alias("Name"))
collect_list_df.display()


# COMMAND ----------

#collect set no duplicates
collect_set_df = explode_outer_df.groupBy("ID", "Name").agg(collect_set("Marks").alias("Marks"))
collect_set_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Count distinct values

# COMMAND ----------

employee_df.groupBy("Age").agg(countDistinct("Gender").alias("gen")).display()

# COMMAND ----------

employee_df.select(collect_set("Gender")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC COUNT

# COMMAND ----------

employee_df.select(count("Gender")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC First and last value in a column

# COMMAND ----------

df.select(first("Name")).display()

# COMMAND ----------

df.select(last("Name")).display()

# COMMAND ----------

employee_df.select(mean("salary").alias("mean_salary")).display()
#result_df = .select(mean("salary").alias("mean_salary"))

# COMMAND ----------

