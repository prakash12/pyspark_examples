# Databricks notebook source
import findspark
findspark.init()
from pyspark.shell import spark


''' Create a dataframe with following schema and sample data'''
sampledata = [
    ('Chandra', 'Sales', 35, 15000, None),
    ('Atul', 'Sales', 36, 16000, 'Delhi'),
    ('Amber', 'Finance', 34, 18000, 'Noida'),
    ('Pallav', 'Sales', 30, 15000, 'Singapore'),
    ('Arjun', 'Sales', 25, 17000, 'Bangalore'),
    ('Jonathan', 'Finance', 45, 25000, None),
    ('Ankit', 'Finance', 35, 20000, 'Jaipur'),
    ('Vimal', 'Finance', 35, 10000, None),
    ('Rohit', 'TAG', 35, 17000, 'Surat'),
    ('Chandra', 'TAG', 28, 35000, None)
]
columns = ['employee_name', 'department', 'age', 'salary', 'address']
# Create dataframe using above data and scheam
df = spark.createDataFrame(data=sampledata, schema=columns)
# Print Schem
df.printSchema
# Print data
df.show()
# DataFrame Group by department
df.groupBy('department').count().show()
# Perform all aggregation on salary column  for each department
from pyspark.sql.functions import sum, avg, max, mean, min

df.groupBy('department').agg(sum('salary').alias('sum_salary'),
                             max('salary').alias('max_salary'),
                             min('salary').alias('min_salary'),
                             avg('salary').alias('avg_salary'),
                             mean('salary').alias('mean_salary')
                             ).show()
# Window function example for python dataframe
# Defind window spec first
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

windowSpec = Window.partitionBy('department').orderBy('salary')
# Example to print rank for each emplyee based on their salary
df.withColumn('rank', rank().over(windowSpec)).show()
# Window aggregates
windowSpecAgg = Window.partitionBy('department')
from pyspark.sql.functions import col, avg, sum, min, max, row_number

# Query to get avg, min,max, salary for each department
df.withColumn("row", row_number().over(windowSpec)).withColumn("avg",
                                                               avg(col("salary")).over(windowSpecAgg)).withColumn("sum",
                                                                                                                  sum(col(
                                                                                                                      "salary")).over(
                                                                                                                      windowSpecAgg)).withColumn(
    "min", min(col("salary")).over(windowSpecAgg)).withColumn("max", max(col("salary")).over(windowSpecAgg)).where(
    col("row") == 1).select("department", "avg", "sum", "min", "max").show()
