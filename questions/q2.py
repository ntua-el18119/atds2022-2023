from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import month
from pyspark.sql.functions import max
import time

start = time.time()
# Start a Spark session
spark = SparkSession.builder.appName("TaxiTrips").getOrCreate()
#schema = StructType([
 #   StructField("LocationID", IntegerType(), True),
  #  StructField("Borough", StringType(), True),
   # StructField("Zone", StringType(), True),
    #StructField("service_zone", StringType(), True)])
#df2 = spark.read.schema(schema).csv("hdfs://master:9000/myfolder/taxi+_zone_lookup.csv",header=True,inferSchema=False)
#df2 = spark.read.csv("hdfs://master:9000/myfolder/taxi+_zone_lookup.csv", header=True, inferSchema=True)
#df1 = spark.read.parquet("hdfs://master:9000/myfolder/taxi_trips/all_months_data.parquet")
df1 = spark.read.parquet("hdfs://master:9000/myfolder/all_final.parquet")
#df1.show()

df_january = df1.filter(month(df1["tpep_pickup_datetime"]) == 1)
df_january_noz = df_january.where(df_january["Tolls_amount"]!=0.0)

df_february = df1.filter(month(df1["tpep_pickup_datetime"]) == 2)
df_february_noz = df_february.where(df_february["Tolls_amount"]!=0.0)

df_march = df1.filter(month(df1["tpep_pickup_datetime"]) == 3)
df_march_noz = df_march.where(df_march["Tolls_amount"]!=0.0)

df_april = df1.filter(month(df1["tpep_pickup_datetime"]) == 4)
df_april_noz = df_april.where(df_april["Tolls_amount"]!=0.0)

df_may = df1.filter(month(df1["tpep_pickup_datetime"]) == 5)
df_may_noz = df_may.where(df_may["Tolls_amount"]!=0.0)

df_june = df1.filter(month(df1["tpep_pickup_datetime"]) == 6)
df_june_noz = df_june.where(df_june["Tolls_amount"]!=0.0)

#df_march.show()
#df_march_noz.printSchema()
#df_march_noz.show()
#df_may.show()

df_january_max_tolls = df_january_noz.select(max(df_january_noz.tolls_amount))
max_tolls_jan = df_january_max_tolls.collect()[0][0]
max_tolls_jan_df = df1.filter(df1["Tolls_amount"] == max_tolls_jan)
df_february_max_tolls = df_february_noz.select(max(df_february_noz.tolls_amount))
max_tolls_feb = df_february_max_tolls.collect()[0][0]
max_tolls_feb_df = df1.filter(df1["Tolls_amount"] == max_tolls_feb)
df_march_max_tolls = df_march_noz.select(max(df_march_noz.tolls_amount))
max_tolls_mar = df_march_max_tolls.collect()[0][0]
max_tolls_mar_df = df1.filter(df1["Tolls_amount"] == max_tolls_mar)
df_april_max_tolls = df_april_noz.select(max(df_april_noz.tolls_amount))
max_tolls_apr = df_april_max_tolls.collect()[0][0]
max_tolls_apr_df = df1.filter(df1["Tolls_amount"] == max_tolls_apr)
df_may_max_tolls = df_may_noz.select(max(df_may_noz.tolls_amount))
max_tolls_may = df_may_max_tolls.collect()[0][0]
max_tolls_may_df = df1.filter(df1["Tolls_amount"] == max_tolls_may)
df_june_max_tolls = df_june_noz.select(max(df_june_noz.tolls_amount))
max_tolls_jun = df_june_max_tolls.collect()[0][0]
max_tolls_jun_df = df1.filter(df1["Tolls_amount"] == max_tolls_jun)

#df_max_tip_noz = df_march_noz.agg(max("Tolls_amount"))
#df_max_tip = df_march.agg(max("Tolls_amount"))
#df_max_tip = df_battery_park.agg(max("Tip_amount"))

#df_january_max_tolls.show()
max_tolls_jan_df.show()
#df_february_max_tolls.show()
max_tolls_feb_df.show()
#df_march_max_tolls.show()
max_tolls_mar_df.show()
#df_april_max_tolls.show()
max_tolls_apr_df.show()
#df_may_max_tolls.show()
max_tolls_may_df.show()
#df_june_max_tolls.show()
max_tolls_jun_df.show()

end = time.time()
print(end-start)
#simpleData = [(max_tolls_jan,0.0,0.0,0.0,0.0,0.0)]
#schema = ["January","February","March","April","May","June"]
#df = spark.createDataFrame(data=simpleData, schema = schema)
#df.show()
#max_tip_df = df1.filter(df1["Tolls_amount"] == max_tolls_jan)
#max_tip = df_battery_park.agg(max("Tip_amount")).collect()[0][0]
#max_tip_df = df_battery_park.filter(df_battery_park["Tip_amount"] == max_tip)
#max_tip_df.show()

