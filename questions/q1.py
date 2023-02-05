from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import month
from pyspark.sql.functions import max
import time
# Start a Spark session
start = time.time()
spark = SparkSession.builder.appName("TaxiTrips").getOrCreate()
df2 = spark.read.csv("hdfs://master:9000/myfolder/taxi+_zone_lookup.csv", header=True, inferSchema=True)
df1 = spark.read.parquet("hdfs://master:9000/myfolder/all_final.parquet")
#df1.filter(df1["Tip_amount"] == 66.0).show(10000)
#df2.show()
df_joined = df1.join(df2, df1["DOLocationID"] == df2["LocationID"])
#df_joined.show()
df_march = df_joined.filter(month(df_joined["tpep_pickup_datetime"]) == 3)
#df_march.show()
df_battery_park = df_march.filter((df_march["DOLocationID"] == 13) | (df_march["DOLocationID"]==12))

df_battery_park.show()
#df_max_tip = df_battery_park.agg(max("Tip_amount"))
max_tip = df_battery_park.agg(max("Tip_amount")).collect()[0][0]
max_tip_df = df_battery_park.filter(df_battery_park["Tip_amount"] == max_tip)
max_tip_df.show()
end = time.time()
print(end-start)
#df_max_tip.show()
max_tip_df.write.csv("hdfs://master:9000/myfolder/q1_ouput.csv", header=True)

