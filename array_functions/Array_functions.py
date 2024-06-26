# Databricks notebook source
# ArrayType = it is the data type
#array - combine 2 columns to create new column then the column will be in Array Type
#Array_contains - returns null if the array is null, true if the array contains the value else false

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder.appName("ArrayColumnExample").getOrCreate()

# Sample data
data = [("John","smith", 25, [80, 85, 90], ["Science","Math","History"]),
        ("Doe","Don", 30, [75, 88, 92], ["Science","Math","History"]),
        ("Jane","paul", 28, [82, 87, 89], ["Science","Math","History"])]

# Schema
schema = StructType([
    StructField("First_name", StringType(), ),
    StructField("last_name",StringType()),
    StructField("age", IntegerType(), ),
    StructField("marks", ArrayType(IntegerType())),
    StructField("sub", ArrayType(StringType()))
])


# Create DataFrame
df = spark.createDataFrame(data, schema)


# Show the DataFrame
df.show()


# COMMAND ----------

# adding new column to convert F_name and L_name in a single array = name
df1=df.withColumn("Name",array(df.First_name,df.last_name))
df1.show()
df1.printSchema()

# COMMAND ----------

df2 = df1.withColumn("F_Name", col("Name").getItem(0)) \
         .withColumn("L_Name", col("Name").getItem(1)) \
         .drop("Name")

        
df2.show()

# COMMAND ----------

#array contains
df2= df.withColumn("array_contains", array_contains(df.marks,80))
df2.show

# COMMAND ----------

#Array length 
df_mark_length = df.withColumn("Marks_length", size(df.marks))
df_mark_length.show()

# COMMAND ----------

# array repeat = duplicates the rray
df_repeat = df.withColumn("repeat_marks", array_repeat((df.marks),2))
df_repeat.display()

# COMMAND ----------

#array position - used to return the position of the value present in an array in each row of the array type column
df_position = df.withColumn("sub_position",array_position(df.marks,80))
df_position.show()

# COMMAND ----------

#array_remove
df_remove = df.withColumn("sub", array_remove(col("sub"), "Math"))
df_remove.show()

# COMMAND ----------

