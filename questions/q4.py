# Start a Spark session
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import time
# Convert "tpep_pickup_datetime" column into a date-time format and extract hour and day of the week
start = time.time()
spark = SparkSession.builder.appName("TaxiTrips").getOrCreate()
df = spark.read.parquet("hdfs://master:9000/myfolder/taxi_trips/all_final.parquet")
df = df.withColumn("pickup_datetime", F.to_timestamp(df["tpep_pickup_datetime"], "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("hour", F.hour(df["pickup_datetime"]))
df = df.withColumn("day_of_week", F.date_format(df["pickup_datetime"], "E"))

# Group by hour and day of the week and count the number of passengers
grouped_df = df.groupBy("hour", "day_of_week").agg(F.count("Passenger_count").alias("passenger_count"))
grouped_df.show(1000)
# Use window function to calculate running total of passengers for each hour and day
window_spec = Window.partitionBy("day_of_week").orderBy(F.desc("passenger_count"))
grouped_df = grouped_df.withColumn("running_total", F.sum("passenger_count").over(window_spec))

# Filter to only include the top three hours for each day of the week
grouped_df = grouped_df.withColumn("row_num", F.row_number().over(window_spec))
grouped_df = grouped_df.filter(grouped_df["row_num"] <= 3)
# Display the result
grouped_df.show(21)
end = time.time()
print(end-start)
grouped_df.write.csv("hdfs://master:9000/myfolder/q4_output.csv", header=True)
