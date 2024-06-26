# Databricks notebook source
from pyspark.sql.functions import *

# Sample data
data = [("2024-06-25",), ("2023-12-31",)]

# Create DataFrame
df = spark.createDataFrame(data, ["date_str"])

# Show the DataFrame
df.show()

# Get current date The current_date() function returns the current date
df_current_date = df.withColumn("current_date", current_date())
df_current_date.show()

# Get current timestamp function returns the current timestamp
df_current_timestamp = df.withColumn("current_timestamp", current_timestamp())
df_current_timestamp.show()

# Add 10 days to the date. The date_add() function adds a specified number of days to a date column:
df_date_add = df.withColumn("date_add", date_add(to_date("date_str"), 10))
df_date_add.show()

# Calculate the difference in days between two dates
df_datediff = df.withColumn("datediff", datediff(current_date(), to_date("date_str")))
df_datediff.show()

# Extract year from date
df_year = df.withColumn("year", year(to_date("date_str")))
df_year.show()

# Extract month from date
df_month = df.withColumn("month", month(to_date("date_str")))
df_month.show()

# Extract day from date
df_day = df.withColumn("day", dayofmonth(to_date("date_str")))
df_day.show()

# Convert string to date
df_to_date = df.withColumn("to_date", to_date("date_str"))
df_to_date.show()

# Format date
df_date_format = df.withColumn("date_format", date_format(to_date("date_str"), "MM-dd-yyyy"))
df_date_format.show()


# COMMAND ----------

