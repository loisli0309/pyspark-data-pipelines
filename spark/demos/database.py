from pyspark.sql import SparkSession
 
spark=SparkSession.builder.appName("Spark Json and Parquet").getOrCreate()
print(spark.version)


df=spark.read.json("data/input/singleline_json.json") 
df.printSchema()
df.show()




df.createOrReplaceTempView("vw_customer")
rs=spark.sql("select * from vw_customer ")
rs.show()


import shutil
import os

df.write.saveAsTable("tbl_customer")

#create database first, then create talble
spark.sql(
    "CREATE DATABASE IF NOT EXISTS my_datewarehouse"
)


# spark.sql(
#     "drop DATABASE if exists my_datewarehouse"
# )


 