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
    if len(sys.argv) < 3:
        print "Usage: py ed_join_click.py" \
              " yesterday_ed yesterday_click befor_yes_ed befor_yes_click"
        sys.exit(0)

    clk_path = sys.argv[1]
    trans_path = sys.argv[2]
    sspstat_path = sys.argv[3]
    DATE = sys.argv[4]

    temp_dt = datetime.strptime(DATE, '%Y%m%d')
    stable_dt = (temp_dt + timedelta(days = -7)).strftime("%Y%m%d")

    spark = SparkSession\
        .builder \
        .appName("naga DSP: ed_join_click_%s" % (DATE))\
        .enableHiveSupport()\
        .config("hive.exec.dynamic.partition.mode","nonstrict")\
        .config("hive.exec.max.dynamic.partitions","30000")\
        .config("hive.exec.max.dynamic.partitions.pernode","30000")\
        .config("spark.sql.caseSensitive", "true")\
        .getOrCreate()
    
    clk_df = spark.sql("select extra, log_time as time from dw.dw_usage_naga_dsp_click_h where dt >= %s and dt <= %s" % (stable_dt, DATE))
    trans_df = spark.sql("select extra, log_time as time from dw.dw_usage_naga_dsp_transform_h where dt >= %s and dt <= %s" % (stable_dt, DATE))
    #sspstat_df = spark.sql("select * from dw.dw_usage_naga_adx_sspstat_h where dt >= %s and dt <= %s" % (stable_dt, DATE))
    clk_df = clk_df.withColumn('src', F.lit("DSPCLICK"))
    trans_df = trans_df.withColumn('src', F.lit("DSPTRANSFORM")) 
 
    clk_df.show(10, False)
    trans_df.show(10, False)
    #sspstat_df.show(10, False)
    
    clk_df.write.json(clk_path, 'overwrite')
    trans_df.write.json(trans_path, 'overwrite')
    #sspstat_df.write.json(sspstat_path, 'overwrite')
