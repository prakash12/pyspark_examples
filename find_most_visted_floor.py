# Databricks notebook source
''' Let's solve a very frequently asked interview question about below data 
id,name,email,floor,resources
1,A,xyz@g1.com,1,CPU
2,A,pqr@g1.com,2,DESKTOP
3,A,zz@bc.com,1,CPU
4,B,xyzy@g1.com,1,CPU
5,B,pqrr@g1.com,2,DESKTOP
6,A,zzz@bc.com,1,MONITOR
We need to get the output as given below
name,total_visits,most_visited_floor,resources_used
A,3,1,CPU,DESKTOP
B,3,2,DESKTOP,MONITOR
'''
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import expr, row_number, col, collect_set, concat_ws, sort_array


data = [("A", "x@gm.com",1, "CPU"), 
    ("A", "S@abc.com",2, "CPU"), 
    ("A", "Sales@abc.com",1, "DEKSTOP"), 
    ("B", "F@c1.come",2,"DEKSTOP"), 
    ("B", "S@q1.com", 1,"DEKSTOP"), 
    ("B", "p@gm.com",2, "MONITOR")
  ]
columns= ["name", "email", "floor", "resource"]
#Create datafreame using above data
df = spark.createDataFrame(data = data, schema = columns)
#df.printSchema()
df.show(truncate=False)
''' Let's solve the problem in steps'''
#Step1: Get the total_visits by name
mvdf = df.groupBy("name").count()
#mvdf.show()
#Renamed the column count with total_visits
total_visitsDf = mvdf.withColumnRenamed("count","total_visits")
#total_visitsDf.show()
#Step2: Get to most visited floor
''' If you observed the input data ,we can get the required data set by window and row_number '''
# Get the most visited floor by grouping name and floor
name_floor_grpDf = df.groupBy("name","floor").count()
# Prepare window spec
window_spec = Window.partitionBy("name").orderBy(col("count").desc())
most_visited_floorDf = name_floor_grpDf.withColumn("rn", row_number().over(window_spec))
#most_visited_floorDf.show()
mostvisitedfloorDF = most_visited_floorDf.select("name","floor").filter(most_visited_floorDf["rn"]==1)
#mostvisitedfloorDF.show()
#Step3: We need to collect the resources to prepare csv data
''' We will use collect_set,sorted_array aggregate funtions to achieve the required output'''

collect_resorcesDf = df.groupBy("name").agg(sort_array(collect_set("resource")).alias("resources_used"))
#collect_resorcesDf.show()
#Convert set to string using concat_ws
stgDf = collect_resorcesDf.withColumn("resources_used",concat_ws(",",collect_resorcesDf["resources_used"]))
#stgDf.show()
#Join the all 3 steps dataframe to get the required result
join1 = total_visitsDf.join(mostvisitedfloorDF, on="name", how="inner")
join2 = join1.withColumnRenamed("floor","most_visited_floor")
finalDF = join2.join(stgDf,on="name", how="inner")
finalDF.show()
