#coding:utf-8
import sys
import json
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.ml.feature import Bucketizer
import math

if __name__ == '__main__':
    appusage_path = sys.argv[1]
    country = sys.argv[2]
    des_path = sys.argv[3]

    sc = SparkContext(
        appName='ling.fang-dmp2.0-normal-temporary-appusage_agg'
    )
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)
    # read appusage info
    appusage_data_df = spark.read.json(appusage_path) 
    #appusage_data_df.show()
    appusage_data_df = appusage_data_df.filter(appusage_data_df.ip_city == country)
    appusage_data_agg = appusage_data_df.groupBy(['gaid', 'package_name', 'category']).agg(
        F.mean('use_cnt').alias('use_cnt_all'),
        F.mean('use_time').alias('use_time_all'))
    appusage_data_agg = appusage_data_agg.dropDuplicates(['gaid', 'package_name'])
    appusage_data_agg.show(10, False)
    appusage_data_agg.write.json(des_path + '/json', "overwrite")
