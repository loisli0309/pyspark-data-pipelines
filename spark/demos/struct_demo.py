from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when ,count, max,avg
from pyspark.sql.functions import trim


import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\Owner\Documents\GitProjects\spark\spark\.venv\Scripts\python.exe'
  
os.environ['PYSPARK_driver_PYTHON'] =r'C:\Users\Owner\Documents\GitProjects\spark\spark\.venv\Scripts\python.exe'




spark=SparkSession.builder.appName("semi structure").getOrCreate()
print(spark.version)

from pyspark.sql.functions import split

data_schema = "value STRING"
 
data = ["1,2,3", "4,5,6", "3", "4", "5,6,7,8", "7,8,9"]

df = spark.createDataFrame(
    [(v,) for v in data],   
    ["value"]
)

df = df.withColumn("value_list", split("value", ","))

df.show()
