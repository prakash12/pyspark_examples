# Databricks notebook source
import findspark
from pyspark.sql.functions import regexp_replace, split, trim, col, array_size, size, coalesce, lit

findspark.init()
from pyspark.shell import spark

data = [
    ("1", "The ZIP code is 12345 and the pin code is 67890 and 78902"),
    ("2", "The ZIP code is 54321 and the pin code is 09876"),
    ("3", "The ZIP code is 11223 and the pin code is 44556 and 64321"),
    ("4", "The ZIP code is 22334 and the pin code is 66778"),
    ("5", "The ZIP code is 33445 and the pin code is 88990 ")
]
schema = ["id", "add"]
initial_df = spark.createDataFrame(data=data, schema=schema)
'''Use regex_replace method to first simplified your string'''
initial_df.show(truncate=False)
df_replace_string = initial_df.withColumn("formatted_string",
                                          (regexp_replace(trim(regexp_replace(initial_df["add"], "[^\\d]", " ")),
                                                          "\\s+", "#")))
df_replace_string.show(truncate=False)
# Split formatted_string column to get all numbers as an array
cleanedDf = df_replace_string.withColumn("array_list", split(col("formatted_string"), "#"))
cleanedDf.show(truncate=False)
size_df = cleanedDf.withColumn("size_a", array_size(col("array_list")))
size_df.show()
# Get the largest array size from column array_list
max_value = size_df.select(size(col("array_list")).alias("size")).agg({"size": "max"}).collect()[0][0]
print(f"maxCol:{max_value}")

# Generate pin columns dynamically , here I am replacing null values with -
final_df = cleanedDf
for i in range(max_value):
    final_df = final_df.withColumn(f"pin{i + 1}", coalesce(col("array_list").getItem(i), lit("-")))

# Drop unnecessary columns and print the result
final_df.drop("add", "array_list", "formatted_string").show(truncate=False)
# Stop spark as a good practice
spark.stop()
