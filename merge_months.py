from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df1 = spark.read.parquet("hdfs://master:9000/myfolder/january.parquet")

df2 = spark.read.parquet("hdfs://master:9000/myfolder/february.parquet")

df3 = spark.read.parquet("hdfs://master:9000/myfolder/march.parquet")

df4 = spark.read.parquet("hdfs://master:9000/myfolder/april.parquet")

df5 = spark.read.parquet("hdfs://master:9000/myfolder/may.parquet")

df6 = spark.read.parquet("hdfs://master:9000/myfolder/june.parquet")
mergedDF = df1.union(df2).union(df3).union(df4).union(df5).union(df6)
#mergedDF.write.mode("overwrite").parquet("file:///home/user/all_months.parquet")
mergedDF.write.mode("overwrite").parquet("/myfolder/all_final.parquet")
