from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lower, lit
from pyspark.ml.recommendation import ALSModel
import argparse

parser = argparse.ArgumentParser(description="Recomandare")
parser.add_argument('--movie', type=str, required=True, help='Film favorit')
args = parser.parse_args()

spark = SparkSession.builder.appName("Recommendations").getOrCreate()

g_bucket = 'gs://data_tbd'
movies = spark.read.parquet(f"{g_bucket}/movie_titles.parquet", header=True, inferSchema=True).select("movie_id", "title")
ratings = spark.read.parquet(f"{g_bucket}/data_for_code.parquet", header=True, inferSchema=True).select("customer_id", "movie_id", "rating")

model = ALSModel.load("gs://data_tbd/als-model/full_model")

fav_movie = args.movie
top_n = 5

movie_row = movies.filter(lower(col("title")).like(f"%{fav_movie.lower()}%")) \
                    .select("movie_id").collect()

if not movie_row:
    print(f"Filmul '{fav_movie}' nu a fost gasit in baza noastra de date :((!")
    spark.stop()
    exit()
movie_id = movie_row[0][0]

new_user_id = ratings.agg({"customer_id": "max"}).collect()[0][0] + 1
user_row = [(new_user_id, movie_id, 5.0)]
user_df = spark.createDataFrame([Row(customer_id=int(x[0]), movie_id=int(x[1]), rating=float(x[2])) for x in user_row])
ratings = ratings.unionByName(user_df)

inferenceDF = movies.join(user_df.select("movie_id"), on="movie_id", how="left_anti") \
                    .withColumn("customer_id", lit(new_user_id).cast("int")) \
                    .select("customer_id", "movie_id")
print('lets seeee')
print(inferenceDF.show(10, truncate=False))
# predictions = model.transform(inferenceDF)
# top_recommendations = predictions.orderBy(col("prediction").desc()).limit(top_n)
#
# recommended_movie_ids = [row["movie_id"] for row in top_recommendations.collect()]
# recommended_titles = movies.filter(col("movie_id").isin(recommended_movie_ids)) \
#                              .select("title").rdd.map(lambda r: r[0]).collect()
#
# print(f"Top {top_n} recommendations for '{fav_movie}':")
# for i, title in enumerate(recommended_titles, 1):
#     print(f"{i}. {title}")
spark.stop()