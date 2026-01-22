from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import datetime

spark = SparkSession.builder \
.appName('proiect_tbd') \
.config("spark.driver.memory", "4g") \
.config("spark.executor.memory", "4g") \
.getOrCreate()

folder_path = "gs://data_tbd"
df = spark.read.parquet(f"{folder_path}/data_for_code.parquet")

df = df.drop('date')

df = df.withColumn("customer_id", df["customer_id"].cast("int"))
df = df.withColumn("movie_id", df["movie_id"].cast("int"))
df = df.withColumn("rating", df["rating"].cast("float"))

train, test = df.randomSplit([0.8, 0.2], seed=42)

train.cache()
test.cache()

als = ALS(
userCol="customer_id",
itemCol="movie_id",
ratingCol="rating",
rank=10,
maxIter=10,
regParam=0.1,
coldStartStrategy="drop"
)

model = als.fit(train)

predictions = model.transform(test)
predictions.show()

evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print(f"RMSE: {rmse}")

model.save(f"gs://data_tbd/als-model/{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}")