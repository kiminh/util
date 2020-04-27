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

    DATE = sys.argv[4]
    join_path = sys.argv[1]
    plan_path = sys.argv[2]
    pkg_path = sys.argv[3]

    spark = SparkSession\
        .builder \
        .appName("naga interactive DSP: click_join_trans_%s (hourly)" % (DATE))\
        .enableHiveSupport()\
        .config("hive.exec.dynamic.partition.mode","nonstrict")\
        .config("hive.exec.max.dynamic.partitions","30000")\
        .config("hive.exec.max.dynamic.partitions.pernode","30000")\
        .config("spark.sql.caseSensitive", "true")\
        .getOrCreate()
    
    clk_df = spark.sql("select extra as click_log, log_time as click_time, reqid, adid, spam, planid, promoted_app, adpcvr as adpcvr_tmp, pcvr_cal as pcvr_cal_tmp, is_ocpc from dw.dw_usage_naga_dsp_click_h where dt > %s and ad_style = 8 and promoted_app <> '' and spam = 0" % (DATE))
    trans_df = spark.sql("select extra as trans_log, log_time as trans_time, reqid, adid, spam from dw.dw_usage_naga_dsp_transform_h where dt > %s and ad_style = 8 and promoted_app <> '' and spam = 0 and event_type = '0'" % (DATE))
    
    clk_df = clk_df.dropDuplicates(['reqid', 'adid'])
    trans_df = trans_df.dropDuplicates(['reqid', 'adid'])
    clk_df = clk_df.withColumn('key', F.concat_ws('_', 'reqid', 'adid'))
    trans_df = trans_df.withColumn('tkey', F.concat_ws('_', 'reqid', 'adid'))

    clk_join_trans = clk_df.join(trans_df, clk_df.key == trans_df.tkey, 'left')
    #for pid calibrate
    #plan_clk_trans = clk_join_trans.filter(clk_join_trans.is_ocpc == 'true').select(
    #    F.when(clk_join_trans.trans_log.isNull(), 0).otherwise(1).alias('is_trans'), 
    #    'planid', 'adpcvr_tmp', 'pcvr_cal_tmp', F.substring_index('time', ':', 1).alias('hour'))
    
    plan_clk_trans = clk_join_trans.select(
        F.when(clk_join_trans.trans_log.isNull(), 0).otherwise(1).alias('is_trans'), 
        'planid', 'adpcvr_tmp', 'pcvr_cal_tmp', clk_join_trans.click_time.substr(1, 10).alias('hour'))
   
    plan_clk_trans = plan_clk_trans.groupBy(['planid', 'hour']).agg(
        F.sum('adpcvr_tmp').alias('adpcvr'),
        F.sum('pcvr_cal_tmp').alias('pcvr_cal'),
        F.count('*').alias('click'),
        F.sum('is_trans').alias('trans')
    )

    #pkg_clk_trans = clk_join_trans.filter(clk_join_trans.is_ocpc == 'true').select(
    #    F.when(clk_join_trans.trans_log.isNull(), 0).otherwise(1).alias('is_trans'), 
    #    'promoted_app', 'adpcvr_tmp', 'pcvr_cal_tmp', F.substring_index('time', ':', 1).alias('hour'))
    
    pkg_clk_trans = clk_join_trans.select(
        F.when(clk_join_trans.trans_log.isNull(), 0).otherwise(1).alias('is_trans'), 
        'promoted_app', 'adpcvr_tmp', 'pcvr_cal_tmp', clk_join_trans.click_time.substr(1, 10).alias('hour'))
     
    pkg_clk_trans = pkg_clk_trans.groupBy(['promoted_app', 'hour']).agg(
        F.sum('adpcvr_tmp').alias('adpcvr'),
        F.sum('pcvr_cal_tmp').alias('pcvr_cal'),
        F.count('*').alias('click'),
        F.sum('is_trans').alias('trans')
    )

    clk_join_trans = clk_join_trans.drop('tkey', 'key', 'reqid', 'adid', 'spam', 'adpcvr_tmp', 'pcvr_cal_tmp', 'is_ocpc')  
    clk_join_trans.write.json(join_path, 'overwrite')
    plan_clk_trans.write.json(plan_path, 'overwrite')
    pkg_clk_trans.write.json(pkg_path, 'overwrite')
