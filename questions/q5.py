from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import month,dayofmonth
from pyspark.sql.functions import max, mean
from pyspark.sql.functions import col, row_number
import time
# Start a Spark session
start time.time()
spark = SparkSession.builder.appName("TaxiTrips").getOrCreate()
#schema = StructType([
 #   StructField("LocationID", IntegerType(), True),
  #  StructField("Borough", StringType(), True),
   # StructField("Zone", StringType(), True),
    #StructField("service_zone", StringType(), True)])
#df2 = spark.read.schema(schema).csv("hdfs://master:9000/myfolder/taxi+_zone_lookup.csv",header=True,inferSchema=False)
#df2 = spark.read.csv("hdfs://master:9000/myfolder/taxi+_zone_lookup.csv", header=True, inferSchema=True)
df1 = spark.read.parquet("hdfs://master:9000/myfolder/taxi_trips/all_final.parquet")
#df1.show()
df1.printSchema()
new_df = df1.withColumn('Tip_percentage', df1.tip_amount*100/df1.fare_amount)
#new_df.show()
df_jan = new_df.filter(month(new_df["tpep_pickup_datetime"]) == 1)
df_jan_pday = df_jan.groupby(dayofmonth(df_jan["tpep_pickup_datetime"])).agg({'Tip_percentage': 'mean'})
df_jan_pday10 = df_jan_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[0][0]
df_jan_pday11 = df_jan_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[0][1]
df_jan_pday20 = df_jan_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[1][0]
df_jan_pday21 = df_jan_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[1][1]
df_jan_pday30 = df_jan_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[2][0]
df_jan_pday31 = df_jan_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[2][1]
df_jan_pday40 = df_jan_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[3][0]
df_jan_pday41 = df_jan_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[3][1]
df_jan_pday50 = df_jan_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[4][0]
df_jan_pday51 = df_jan_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[4][1]
#df_jan_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().show()

df_feb = new_df.filter(month(new_df["tpep_pickup_datetime"]) == 2)
df_feb_pday = df_feb.groupby(dayofmonth(df_feb["tpep_pickup_datetime"])).agg({'Tip_percentage': 'mean'})
df_feb_pday10 = df_feb_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[0][0]
df_feb_pday11 = df_feb_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[0][1]
df_feb_pday20 = df_feb_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[1][0]
df_feb_pday21 = df_feb_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[1][1]
df_feb_pday30 = df_feb_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[2][0]
df_feb_pday31 = df_feb_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[2][1]
df_feb_pday40 = df_feb_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[3][0]
df_feb_pday41 = df_feb_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[3][1]
df_feb_pday50 = df_feb_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[4][0]
df_feb_pday51 = df_feb_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[4][1]
#df_feb_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().show()

df_mar = new_df.filter(month(new_df["tpep_pickup_datetime"]) == 3)
df_mar_pday = df_mar.groupby(dayofmonth(df_mar["tpep_pickup_datetime"])).agg({'Tip_percentage': 'mean'})
df_mar_pday10 = df_mar_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[0][0]
df_mar_pday11 = df_mar_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[0][1]
df_mar_pday20 = df_mar_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[1][0]
df_mar_pday21 = df_mar_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[1][1]
df_mar_pday30 = df_mar_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[2][0]
df_mar_pday31 = df_mar_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[2][1]
df_mar_pday40 = df_mar_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[3][0]
df_mar_pday41 = df_mar_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[3][1]
df_mar_pday50 = df_mar_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[4][0]
df_mar_pday51 = df_mar_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[4][1]
#df_mar_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().show()

df_apr = new_df.filter(month(new_df["tpep_pickup_datetime"]) == 4)
df_apr_pday = df_apr.groupby(dayofmonth(df_apr["tpep_pickup_datetime"])).agg({'Tip_percentage': 'mean'})
df_apr_pday10 = df_apr_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[0][0]
df_apr_pday11 = df_apr_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[0][1]
df_apr_pday20 = df_apr_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[1][0]
df_apr_pday21 = df_apr_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[1][1]
df_apr_pday30 = df_apr_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[2][0]
df_apr_pday31 = df_apr_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[2][1]
df_apr_pday40 = df_apr_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[3][0]
df_apr_pday41 = df_apr_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[3][1]
df_apr_pday50 = df_apr_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[4][0]
df_apr_pday51 = df_apr_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[4][1]
#df_apr_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().show()

df_may = new_df.filter(month(new_df["tpep_pickup_datetime"]) == 5)
df_may_pday = df_may.groupby(dayofmonth(df_may["tpep_pickup_datetime"])).agg({'Tip_percentage': 'mean'})
df_may_pday10 = df_may_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[0][0]
df_may_pday11 = df_may_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[0][1]
df_may_pday20 = df_may_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[1][0]
df_may_pday21 = df_may_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[1][1]
df_may_pday30 = df_may_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[2][0]
df_may_pday31 = df_may_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[2][1]
df_may_pday40 = df_may_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[3][0]
df_may_pday41 = df_may_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[3][1]
df_may_pday50 = df_may_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[4][0]
df_may_pday51 = df_may_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[4][1]
#df_may_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().show()

df_jun = new_df.filter(month(new_df["tpep_pickup_datetime"]) == 6)
df_jun_pday = df_jun.groupby(dayofmonth(df_jun["tpep_pickup_datetime"])).agg({'Tip_percentage': 'mean'})
df_jun_pday10 = df_jun_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[0][0]
df_jun_pday11 = df_jun_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[0][1]
df_jun_pday20 = df_jun_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[1][0]
df_jun_pday21 = df_jun_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[1][1]
df_jun_pday30 = df_jun_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[2][0]
df_jun_pday31 = df_jun_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[2][1]
df_jun_pday40 = df_jun_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[3][0]
df_jun_pday41 = df_jun_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[3][1]
df_jun_pday50 = df_jun_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[4][0]
df_jun_pday51 = df_jun_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().collect()[4][1]
#df_jun_pday.orderBy(col("avg(Tip_percentage)").desc()).dropna().show()

simpleData = [(df_jan_pday10,df_jan_pday11,df_feb_pday10,df_feb_pday11,df_mar_pday10,df_mar_pday11,df_apr_pday10,df_apr_pday11,df_may_pday10,df_may_pday11,df_jun_pday10,df_jun_pday11),
                (df_jan_pday20,df_jan_pday21,df_feb_pday20,df_feb_pday21,df_mar_pday20,df_mar_pday21,df_apr_pday20,df_apr_pday21,df_may_pday20,df_may_pday21,df_jun_pday20,df_jun_pday21),
                (df_jan_pday30,df_jan_pday31,df_feb_pday30,df_feb_pday31,df_mar_pday30,df_mar_pday31,df_apr_pday30,df_apr_pday31,df_may_pday30,df_may_pday31,df_jun_pday30,df_jun_pday31),
                (df_jan_pday40,df_jan_pday41,df_feb_pday40,df_feb_pday41,df_mar_pday40,df_mar_pday41,df_apr_pday40,df_apr_pday41,df_may_pday40,df_may_pday41,df_jun_pday40,df_jun_pday41),
                (df_jan_pday50,df_jan_pday51,df_feb_pday50,df_feb_pday51,df_mar_pday50,df_mar_pday51,df_apr_pday50,df_apr_pday51,df_may_pday50,df_may_pday51,df_jun_pday50,df_jun_pday51)]
schema = ["January top 5 days","avg(Tip_percentage)","February top 5 days","avg(Tip_percentage)","March top 5 days","avg(Tip_percentage)","April top 5 days","avg(Tip_percentage)","May top 5 days","avg(Tip_percentage)","June top 5 days","avg(Tip_percentage)"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.show()
end = time.time()
print(end-start)
df.write.csv("hdfs://master:9000/myfolder/q5_ouput.csv", header=True)
