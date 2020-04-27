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

    join_path = sys.argv[1]
    yest_join_path = sys.argv[2]
    unjoin_path = sys.argv[3]
    cur_time = sys.argv[4]

    temp_dt = datetime.strptime(cur_time, '%Y%m%d')
    pre_time = (temp_dt + timedelta(days = -1)).strftime("%Y%m%d")
        
    spark = SparkSession\
        .builder \
        .appName("naga DSP interactive: CTR1 ed_join_click_%s" % (cur_time))\
        .enableHiveSupport()\
        .config("hive.exec.dynamic.partition.mode","nonstrict")\
        .config("hive.exec.max.dynamic.partitions","30000")\
        .config("hive.exec.max.dynamic.partitions.pernode","30000")\
        .config("spark.sql.caseSensitive", "true")\
        .getOrCreate()
    
    first_ed_df = spark.sql("select extra as first_ed_log, log_time as first_ed_time, reqid from dw.dw_usage_ad_naga_interactive_first_ed_d where dt=%s or dt=%s" % (cur_time, pre_time))
    first_clk_df = spark.sql("select extra as first_click_log, log_time as first_click_time, reqid as treqid from dw.dw_usage_ad_naga_interactive_first_click_d where dt=%s or dt=%s" % (cur_time, pre_time))
    second_ed_df = spark.sql("select extra as second_ed_log, log_time as second_ed_time, reqid as treqid from dw.dw_usage_ad_naga_interactive_second_ed_d where dt=%s or dt=%s" % (cur_time, pre_time))
    second_clk_df = spark.sql("select extra as second_click_log, log_time as second_click_time, reqid as treqid, number from dw.dw_usage_ad_naga_interactive_second_click_d where dt=%s or dt=%s" % (cur_time, pre_time))

    print "first ed count: %s, first click count: %s" % (first_ed_df.count(), first_clk_df.count())
    print "second ed count: %s, second click count: %s" % (second_ed_df.count(), second_clk_df.count())
    
    first_ed_df = first_ed_df.filter(first_ed_df.reqid <> "").dropDuplicates(['reqid'])
    first_clk_df = first_clk_df.filter(first_clk_df.treqid <> "").dropDuplicates(['treqid'])
   
    print "After dropDuplicates first ed count: %s, first click count: %s" % (first_ed_df.count(), second_clk_df.count())
    first_ed_join_clk = first_ed_df.join(first_clk_df, first_ed_df.reqid == first_clk_df.treqid, 'left')
    first_ed_join_clk = first_ed_join_clk.drop('treqid')
    first_ed_join_clk.show(10, False)
   
    second_ed_df = second_ed_df.filter(second_ed_df.treqid <> "").dropDuplicates(['treqid'])
    second_clk_df = second_clk_df.filter(second_clk_df.treqid <> "").dropDuplicates(['treqid', 'number'])
    second_clk_gb = second_clk_df.groupBy('treqid').agg(F.count('number').alias('click_num'))
    second_clk_df = second_clk_df.dropDuplicates(['treqid']).withColumnRenamed('treqid', 'sreqid')
    second_clk_df_ = second_clk_df.join(second_clk_gb, second_clk_df.sreqid == second_clk_gb.treqid)
    second_clk_df_ = second_clk_df_.drop('treqid')

    second_ed_join_clk = second_ed_df.join(second_clk_df_, 
        second_ed_df.treqid == second_clk_df_.sreqid, 'left')
    second_ed_join_clk = second_ed_join_clk.drop('sreqid', 'number')

    ed_join_clk = first_ed_join_clk.join(second_ed_join_clk, 
        first_ed_join_clk.reqid == second_ed_join_clk.treqid, 'left')
    ed_join_clk = ed_join_clk.drop('reqid', 'treqid')

    cur_ed_join_clk = ed_join_clk.filter(ed_join_clk.first_ed_time.substr(1, 8) == cur_time) # 20191012
    pre_ed_join_clk = ed_join_clk.filter(ed_join_clk.first_ed_time.substr(1, 8) == pre_time)
   
    print "After join click, ed count: %s, pre ed count: %s" \
        % (cur_ed_join_clk.count(), pre_ed_join_clk.count()) 
    
    cur_ed_join_clk.write.json(join_path, 'overwrite')
    pre_ed_join_clk.write.json(yest_join_path, 'overwrite')
