# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC Data set 1 Employees

# COMMAND ----------


# Sample data for Employees
data_employees = [
    (1, "John", "Engineer"),
    (2, "Jane", "Manager"),
    (3, "Doe", "Developer"),
    (4, "Smith", "Designer")
]
columns_employees = ["emp_id", "emp_name", "emp_role"]

# Schema for Employees
schema_employees = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("emp_name", StringType(), True),
    StructField("emp_role", StringType(), True)
])

# Create DataFrame for Employees
df_employees = spark.createDataFrame(data_employees, schema_employees)
df_employees.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Dataset 2 departments

# COMMAND ----------

# Sample data for Departments
data_departments = [
    (1, "Engineering"),
    (2, "Sales"),
    (3, "IT"),
    (5, "Marketing")
]
columns_departments = ["dept_id", "dept_name"]

# Schema for Departments
schema_departments = StructType([
    StructField("dept_id", IntegerType(), True),
    StructField("dept_name", StringType(), True)
])

# Create DataFrame for Departments
df_departments = spark.createDataFrame(data_departments, schema_departments)
df_departments.display()

# COMMAND ----------

# MAGIC %md
# MAGIC inner join

# COMMAND ----------

df_join= df_employees.join(df_departments, df_employees.emp_id==df_departments.dept_id, "inner")
df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Left join

# COMMAND ----------

df_join= df_employees.join(df_departments, df_employees.emp_id==df_departments.dept_id, "left")
df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Right join

# COMMAND ----------

df_join= df_employees.join(df_departments, df_employees.emp_id==df_departments.dept_id, "Right")
df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Full outer join

# COMMAND ----------

df_join= df_employees.join(df_departments, df_employees.emp_id==df_departments.dept_id, "outer")
df_join.display()

# COMMAND ----------

