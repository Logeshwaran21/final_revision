# Databricks notebook source
#pivot = used to transpose all values in columns to column
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Initialize Spark session
spark = SparkSession.builder.appName("PivotExample").getOrCreate()

# Sample data
data = [
    ("John", "HR", 3000, "January"),
    ("Doe", "IT", 4000, "January"),
    ("Jane", "HR", 3500, "February"),
    ("Michael", "Finance", 4500, "January"),
    ("Sarah", "IT", 5000, "February"),
    ("Chris", "Finance", 3000, "February"),
    ("John", "HR", 3200, "March"),
    ("Doe", "IT", 4200, "March"),
    ("Jane", "HR", 3300, "April"),
    ("Michael", "Finance", 4700, "March"),
    ("Sarah", "IT", 5200, "April"),
    ("Chris", "Finance", 3500, "April")
]

# Schema
schema = ["name", "department", "salary", "month"]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the original DataFrame
df.show()




# COMMAND ----------

# Pivot the DataFrame
pivot_df=  df.groupBy("month").pivot("department").sum("salary")
pivot_df.show()

# COMMAND ----------

#transposing the columns into list of values to a column Unpivoting, also known as melting, is the process of converting columns into rows. In PySpark, unpivoting can be achieved using selectExpr along with stack function.

unpivot_expr = "stack(3, 'Finance', Finance, 'HR', HR, 'IT', IT) as (department, salary)"
unpivot_df = df.selectExpr("month", unpivot_expr)

# Show the unpivoted DataFrame
unpivot_df.show()

# COMMAND ----------

