# Databricks notebook source
import findspark
findspark.init()
''' Purpose of this notebook to do hands on practice for Spark 3.4.0 which introduce an elegant way to write parameterized SQL.
Paramterized sql is a secure way to avoid SQL injections
@author: chandraprakash.yadav
Connect with me https://www.linkedin.com/in/chandra-prakash-yadav-78278219/'''
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
mydf = spark.createDataFrame(data=sampledata, schema=columns)
# Get all records from df
spark.sql("SELECT * FROM {df}", df=mydf).show()
# Select all employees whose age is greater then 35 with variable name
spark.sql("SELECT * FROM {df} WHERE {df[age]} > :num", {"num": 30}, df=mydf).show()
# Get all employees age between 30 and 45
spark.sql("SELECT * FROM {df} WHERE {df[age]} > :num1 AND {df[age]} <= :num2", {"num1": 30, "num2": 45}, df=mydf).show()
# Get all employees age between 20 and 45 and address contains ore
spark.sql("SELECT * FROM {df} WHERE {df[age]} > :num1 AND {df[age]} <= :num2 AND {df[address]} LIKE :lk",
          {"num1": 20, "num2": 45, 'lk': "%ore%"}, df=mydf).show()
# Get all employees age between 20 and 45 and address not contains ore
spark.sql("SELECT * FROM {df} WHERE {df[age]} > :num1 AND {df[age]} <= :num2 AND {df[address]} NOT LIKE :lk",
          {"num1": 20, "num2": 45, 'lk': "%ore%"}, df=mydf).show()
# Get total salary by department using parameterged sql
spark.sql("SELECT department, SUM(salary) AS total_salary FROM {df} GROUP BY {df[department]}", df=mydf).show()
# Get total salary, min,max,avg,mean by department using parameterged sql
spark.sql(
    "SELECT department, SUM(salary) AS total_salary,MAX(salary) AS max_salary,MIN(salary) AS min_salary,MEAN(salary) AS mean_salary FROM {df} GROUP BY {df[department]}",
    df=mydf).show()
