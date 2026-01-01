from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when ,count, max,avg
from pyspark.sql.functions import trim


import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\Owner\Documents\GitProjects\spark\spark\.venv\Scripts\python.exe'
  
os.environ['PYSPARK_driver_PYTHON'] =r'C:\Users\Owner\Documents\GitProjects\spark\spark\.venv\Scripts\python.exe'


spark=SparkSession.builder.appName("Testing pyspark example").getOrCreate()
print(spark.version)


sample_schema = "id int, Name STRING, Age INT , scores Int"

sample_data_list = [
                (1, "John   D",  30, 95), 
                (2, "Alice   G",  30, 81), 
                (3, "Bob   T",  35, 58), 
                (4, "EVE   D",  35, 65),
                 (5, "Johnny   A",  35, 91), 
                (6, "Alicia   X",  21, 80) ]

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
#df.printSchema()
 

df_pivot=df.groupBy("score_group").pivot("age").agg(
    count("*").alias("#students"),
    avg("age").alias("average_age")
)

df_pivot.show()

df_long = df_pivot.selectExpr(
    "score_group",
    """
    stack(3,
  '21', `21_#students`, `21_average_age`,
  '30', `30_#students`, `30_average_age`,
  '35', `35_#students`, `35_average_age`
)
 as (age, num_students, average_age)
    """
)
df_long.show()
 