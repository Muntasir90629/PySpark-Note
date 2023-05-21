from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
import csv
import pandas as pd
import numpy as np
import sys
from pyspark.sql import Window
from pyspark.sql.functions import rank, col
import geohash2 as geohash
import pygeohash as pgh
from functools import reduce
from pyspark.sql import *

from pyspark.sql import SparkSession



master_df = spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/app/master_df/*', header=True)

# master_df.printSchema()
level_df = spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/app/level_df/*', header=True)

# level_df.printSchema()
lifestage_df = spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/app/lifestage_df/*', header=True)

# lifestage_df.printSchema()

#joining table
join_df1 = master_df.join(level_df, on='app_level_id', how='left').cache()
join_df2 = join_df1.join(lifestage_df, on='app_lifestage_id', how='left').cache()

select_columns = ['bundle', 'app_l1_name', 'app_l2_name', 'app_l3_name', 'lifestage_name']
finalapp_df = join_df2.select(*select_columns)


#taking time line data
brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2023{03}/*.parquet')


brq2 = brq.select('ifa', F.explode('app')).select('ifa', 'col.*')

app =finalapp_df.join( brq2,'bundle','inner')


app2=app.select('ifa','asn')

app_count=app2.groupBy('ifa').agg(F.countDistinct('asn').alias('count')).sort('count', ascending = False)

# app_count.agg({'count': 'avg'}).show()


from pyspark.sql.types import IntegerType

app_count =app_count.withColumn("version", app_count["count"].cast(FloatType()))


app_count=app_count.filter(app_count.version >= 3.0)

# app_count.sort(app_count.version.asc()).show(truncate=False)

# app_count.show(100)




app_count=app_count.select('ifa')
app_count=app_count.distinct()
app_count.count()

app_count=app_count.withColumnRenamed("ifa","Mobile Device ID")


app_count.printSchema()

app_count.coalesce(1).write.csv('s3a://ada-dev/BD-DataScience/muntasir/HV/', mode='overwrite', header=True)


master_df = spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/HV/*', header=True)

master_df.count()

10385240 