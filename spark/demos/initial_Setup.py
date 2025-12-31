from pyspark.sql import SparkSession
from pyspark.sql.functions import col


import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\Owner\Documents\GitProjects\spark\spark\.venv\Scripts\python.exe'
  
os.environ['PYSPARK_driver_PYTHON'] =r'C:\Users\Owner\Documents\GitProjects\spark\spark\.venv\Scripts\python.exe'


spark=SparkSession.builder.appName("Testing pyspark example").getOrCreate()
print(spark.version)


sample_schema = "Name STRING, Age INT"

sample_data_list = [{"Name": "Alice", "Age": 30}, 
               {"Name": "Bob", "Age": 25}, 
               {"Name": "Cathy", "Age": 27}]


df=spark.createDataFrame(sample_data_list, schema=sample_schema)
df.show()



df_abtest=(
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/input/ab_data_4.csv")
)

df_abtest.filter(df_abtest.landing_page=="old_page") 
print(f'{df_abtest.count()}')   
df_abtest.show()

df_abtest.write.mode("overwrite").csv("data/output/ab_data_oldpage.csv", header=True)

#spark.stop()
