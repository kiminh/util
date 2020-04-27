#coding:utf-8
import sys
import json
import os
import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.ml.linalg import Vectors
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import SparkContext

from datetime import datetime, date, timedelta

reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == '__main__':

    join_path = sys.argv[1]
    shitu_log_path = sys.argv[2]
    date = sys.argv[3]
 
    spark = SparkSession\
        .builder \
        .appName("naga DSP interactive: OCPC_get_shitu_log_%s" % date)\
        .enableHiveSupport()\
        .config("hive.exec.dynamic.partition.mode","nonstrict")\
        .config("hive.exec.max.dynamic.partitions","30000")\
        .config("hive.exec.max.dynamic.partitions.pernode","30000")\
        .config("spark.sql.caseSensitive", "true")\
        .getOrCreate()
   
    df = spark.read.json(join_path)
    df = df.filter((df.click_log.promoted_app <> '') 
            & (df.click_log.ad_style == '8')
            & (df.click_time.substr(0, 8) > 20200204))
    df.repartition(500).write.json(shitu_log_path, 'overwrite')
