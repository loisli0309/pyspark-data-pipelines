import openpyxl
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, length, count_distinct, col, min, max, sum



spark = SparkSession.builder\
    .appName("Customer Segmentation")\
    .getOrCreate()

from pathlib import Path

# 1) 从当前脚本往上两层当作项目根（可按需调整）
BASE_DIR = Path(__file__).resolve().parents[1]   # spark/
print("BASE_DIR =", BASE_DIR)

# 2) 在整个项目里搜 Online_Retail 目录（最多搜 3 层，避免太慢）
candidates = []
for p in BASE_DIR.rglob("Online_Retail"):
    if p.is_dir():
        part_files = list(p.glob("part-*.csv"))
        if part_files:
            candidates.append((p, len(part_files)))

print("Found candidates:", [(str(p), n) for p, n in candidates])

if not candidates:
    raise FileNotFoundError(
        "No Online_Retail folder with part-*.csv found under: " + str(BASE_DIR)
    )

# 3) 取 part 文件最多的那个目录
csv_dir = sorted(candidates, key=lambda x: x[1], reverse=True)[0][0]
print("Using csv_dir =", csv_dir)
print("Sample parts =", list(csv_dir.glob("part-*.csv"))[:3])


df = (spark.read
      .option("header", "true")          # 如果你写出时是 true，就保持 true
      .option("inferSchema", "true")
      .csv((csv_dir / "part-*.csv").as_posix())
)

 
 
# df = df.withColumn('CustomerID', col('_c0').cast('INT'))\
#         .withColumn('Elasticity', col('_c3'))\
#         .select(['CustomerID', 'Elasticity'])
#sample_schema= 'CustomerID String, age Int'
#df.schema = sample_schema

#df=df.select(['CustomerID', 'Elasticity'])
df.show()


from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import ClusteringEvaluator

#Assemble features
assmbler = VectorAssembler(inputCols=['Elasticity'], outputCol='features')
df = assmbler.transform(df)
df.show()

score_list = []
best_score = -1
optimal_k = 2
for k in range(optimal_k, 10):
    kmeans = KMeans(k=k, seed =42)
    model = kmeans.fit(df)
    pred = model.transform(df)
    score = ClusteringEvaluator().evaluate(pred)
    print(f'for {k}, the score is: {score}')
    
    if score > best_score:
        score_list.append({"n":k, "s":score})
        best_score = score
        optimal_k = k
print(f"optimal k is {k}")