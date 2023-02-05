# Start a Spark session
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
# Convert "tpep_pickup_datetime" column into a date-time format and extract hour and day of the week
spark = SparkSession.builder.appName("TaxiTrips").getOrCreate()
df2 = spark.read.csv("hdfs://master:9000/myfolder/taxi+_zone_lookup.csv", header=True, inferSchema=True)
df1 = spark.read.parquet("hdfs://master:9000/myfolder/taxi_trips/all_months.parquet")
df2_1 = df2.withColumnRenamed("LocationID", "start_location_id")
df2_2 = df2.withColumnRenamed("LocationID", "end_location_id")

joined_df = df1.join(df2_1, df1.PULocationID == df2_1.start_location_id, "inner") \
               .drop(df2_1.start_location_id) \
               .withColumnRenamed("Zone", "start_location_name")
joined_df.show()

joined_df = joined_df.join(df2_2, joined_df.DOLocationID == df2_2.end_location_id, "inner") \
               .drop(df2_2.end_location_id) \
               .withColumnRenamed("Zone", "end_location_name")
joined_df.show()
# Group by start and end location name and calculate the average distance and cost


joined_df = joined_df.withColumn("pickup_date", F.to_date(joined_df["tpep_pickup_datetime"], "yyyy-MM-dd HH:mm:ss"))
joined_df = joined_df.filter(joined_df["pickup_date"] >= F.lit("2022-01-01"))
joined_df = joined_df.filter(joined_df["pickup_date"] <= F.lit("2022-06-30"))
grouped_df = joined_df.groupBy(F.floor(F.datediff(joined_df["pickup_date"], F.lit("2022-01-01")) / 15) * 15).agg(
    F.avg("Trip_distance").alias("avg_distance"),
    F.avg("Total_amount").alias("avg_cost")
)
grouped_df = grouped_df.withColumnRenamed("floor((datediff(pickup_date, '2022-01-01') / 15) * 15)", "group_by_15_days")
# Show the result
grouped_df.show()
