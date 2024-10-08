# Databricks notebook source
''' Purpose of this note book to do some hands on to remove duplicates from data frame using pyspark.
Follow me : https://www.linkedin.com/in/chandra-prakash-yadav-78278219/
This execrise will be prepared on some sample data set which will be create using dataframe create .
Let's do it and learn together.
'''
import findspark

findspark.init()

from pyspark.shell import spark

findspark.init()

data = [("James", "Sales", 3000),
        ("Michael", "Sales", 4600)
        ]
columns = ["employee_name", "department", "salary"]
# Create datafreame using above data

df = spark.createDataFrame(data=data, schema=columns)
df.distinct().show()
df.dropDuplicates(["department"]).show()
print("Number of rows with distinct :  " + str(
    df.distinct().count()) + " AND with dropDuplicates using department column : " + str(
    df.dropDuplicates(["department"]).count()))

# Stop spark at the end of your code is a best practice
spark.stop()