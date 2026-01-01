from pyspark.sql import SparkSession
 
spark=SparkSession.builder.appName("Spark Json and Parquet").getOrCreate()
print(spark.version)


df_sljson=spark.read.json("data/input/singleline_json.json") 
df_sljson.show()


df_mljson=(spark.read
           .option("multiline", "true")
           .json("data/input/multiline_json.json")) 
df_mljson.show()    


df_sljson.write.mode("overwrite").json("data/output/singleline_json.json")
df_mljson.write.mode("overwrite").option("multiline", "true").json("data/output/multiline_json.json")


df_mljson.write.mode("overwrite").parquet("data/output/abtest.parquet")

df_parquet=spark.read.parquet("data/input/abtest.parquet")
df_parquet.show()

 