from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when ,count, max
from pyspark.sql.functions import trim


import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\Owner\Documents\GitProjects\spark\spark\.venv\Scripts\python.exe'
  
os.environ['PYSPARK_driver_PYTHON'] =r'C:\Users\Owner\Documents\GitProjects\spark\spark\.venv\Scripts\python.exe'


spark=SparkSession.builder.appName("Testing pyspark example").getOrCreate()
print(spark.version)


sample_schema = "id int, Name STRING, Age INT , scores Int"

sample_data_list = [
                (1, "John   D",  30, 95), 
                (2, "Alice   G",  25, 89), 
                (3, "Bob   T",  35, 88), 
                (4, "EVE   D",  28, 95) ]

df=spark.createDataFrame(sample_data_list, schema=sample_schema)
df.show()


# 1 withColumn
df=df.withColumn("name", trim(df["name"]))

df=df.withColumn("target_score",  col("scores") + 10 )
df=df.withColumn("score_group", 
                  when (col("scores")   > 90, "Excellent")
                 .when(col("scores")  > 80, "Good")
                 .when(col("scores")  > 70, "Pass")
                 .otherwise("Need Improvement"))


df.orderBy(col("scores").desc()).show()
df.printSchema()


#select 
df_select =df.select(["name", "scores"]) 


# union group by 

sample_data_list = [
                (5, "Johnny   A",  31, 91), 
                (6, "Alicia   X",  21, 80) ]


df_2=spark.createDataFrame(sample_data_list, schema=sample_schema)
df_2=df_2.withColumn("target_score",  col("scores") + 10 )
df_2=df_2.withColumn("score_group", 
                  when (col("scores")   > 90, "Excellent")
                 .when(col("scores")  > 80, "Good")
                 .when(col("scores")  > 70, "Pass")
                 .otherwise("Need Improvement"))
 

df_union=df.union(df_2)  
print(df_union.count() )


df_union.groupby("score_group").count().show()
(
    df_union
    .groupBy("score_group")
    .agg(
        count("*").alias("num_records"),
        max("age").alias("max_age")
    )
    .show()
)
 
# join crossjoin
scholarship_schema = "score_group STRING, scholarship  INT"

scholarship_data = [
    ("Excellent", 1000),    
    ("Good", 500)]

df_scolarship=spark.createDataFrame(scholarship_data, schema=scholarship_schema)
df_union.crossJoin(df_scolarship).show()
df_union.join(df_scolarship,  "score_group" ).show()
df_union.join(df_scolarship,  "score_group", "left").show()