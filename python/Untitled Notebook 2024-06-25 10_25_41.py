# Databricks notebook source
try:
    # Attempt to divide by zero, which will raise an exception
    result = 10 / 0
except ZeroDivisionError as e:
    # Handle the exception
    print(f"Error: {e}")
else:
    # This block executes if no exceptions were raised
    print(f"The result is {result}")
finally:
    # This block always executes
    print("Execution completed.")


# COMMAND ----------

