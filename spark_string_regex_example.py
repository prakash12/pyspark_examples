# Databricks notebook source
# MAGIC %scala
# MAGIC
# MAGIC import org.apache.spark.sql.functions._
# MAGIC
# MAGIC  val df= Seq(
# MAGIC  ("1","The ZIP code is 12345 and the pin code is 67890 and 78902"),
# MAGIC  ("2","The ZIP code is 54321 and the pin code is 09876"),
# MAGIC  ("3","The ZIP code is 11223 and the pin code is 44556 and 64321"),
# MAGIC  ("4","The ZIP code is 22334 and the pin code is 66778"),
# MAGIC  ("5","The ZIP code is 33445 and the pin code is 88990 ") 
# MAGIC  ).toDF("id","add");
# MAGIC  df.show(false); 
# MAGIC  //Use regex_replace to first simplified your string
# MAGIC  val df_replace_string= df.withColumn("replaced_string",(regexp_replace(trim(regexp_replace(df("add"), "[^\\d]", " ")),"\\s+","#")));
# MAGIC  //df_replace_string.show(false);
# MAGIC  val cleanedDf = df_replace_string.withColumn("cleaned_array",split(col("replaced_string"),"#"));
# MAGIC  cleanedDf.show();
# MAGIC  val maxCol = cleanedDf.select(size(col("cleaned_array")).as("size")).agg(max("size")).as[Int].head()
# MAGIC  val finalDf = (0 until maxCol).foldLeft(cleanedDf){(df1,i) => df1.withColumn(s"pin${i+1}", col("cleaned_array").getItem(i))}.drop("replaced_string","cleaned_array"); 
# MAGIC  finalDf.select("id","pin1","pin2","pin3")show();
# MAGIC  
# MAGIC  
