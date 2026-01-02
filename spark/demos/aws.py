from pyspark.sql import SparkSession
from pyspark.sql.functions import col


import os, sys
from pyspark.sql import SparkSession
# os.environ['PYSPARK_PYTHON'] = r'C:\Users\Owner\Documents\GitProjects\spark\spark\.venv\Scripts\python.exe'
# os.environ['PYSPARK_DRIVER_PYTHON'] =r'C:\Users\Owner\Documents\GitProjects\spark\spark\.venv\Scripts\python.exe'

# ✅ 修正 driver env var
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# （Windows 常用增强稳定性）
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_DIRS"] = r"C:\temp\spark"

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .appName("Spark to S3")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.2"
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "s3.amazonaws.com"
        )
        .getOrCreate()
)


hadoop_conf = spark._jsc.hadoopConfiguration()

       

print(spark.version) 

df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("s3a://aws-spark-bucket-2025111/inputs/ab_data_4.csv")
)


df.write\
    .mode("overwrite")\
    .parquet("s3a://aws-spark-bucket-2025111/outputs/ab_data_output.parquet")

df.show()