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
    DATE = sys.argv[3]

    temp_dt = datetime.strptime(DATE, '%Y%m%d')
    yestday = (temp_dt + timedelta(days = -1)).strftime("%Y%m%d")
   
    spark = SparkSession\
        .builder \
        .appName("naga iteractive DSP: CTR2 ed_join_click_%s" % (DATE))\
        .enableHiveSupport()\
        .config("hive.exec.dynamic.partition.mode","nonstrict")\
        .config("hive.exec.max.dynamic.partitions","30000")\
        .config("hive.exec.max.dynamic.partitions.pernode","30000")\
        .config("spark.sql.caseSensitive", "true")\
        .getOrCreate()

    ed_df = spark.sql("select extra as ed_log, log_time as ed_time, reqid, adid, spam from dw.dw_usage_naga_dsp_ed_h where ad_style = 8 and (dt=%s or dt=%s)" % (DATE, yestday))
    clk_df = spark.sql("select extra as click_log, log_time as click_time, reqid, adid, spam as tspam from dw.dw_usage_naga_dsp_click_h where ad_style = 8 and (dt=%s or dt=%s)" % (DATE, yestday))
    ed_df.show(10, False)
    clk_df.show(10, False)
    print "ed count: %s, click count: %s" % (ed_df.count(), clk_df.count())
    
    ed_df = ed_df.filter((ed_df.spam == 0) & (ed_df.reqid <> "")) \
        .dropDuplicates(['reqid', 'adid'])
    clk_df = clk_df.filter((clk_df.tspam == 0) & ((clk_df.reqid <> ""))) \
        .dropDuplicates(['reqid', 'adid'])
   
    ed_df = ed_df.withColumn('key', F.concat_ws('_', 'reqid', 'adid'))
    clk_df = clk_df.withColumn('tkey', F.concat_ws('_', 'reqid', 'adid'))
    
    print "After filter spam != 0, ed count: %s, click count: %s" % (ed_df.count(), clk_df.count())
    ed_join_clk = ed_df.join(clk_df, ed_df.key == clk_df.tkey, 'left')
    
    today_ed_join_clk = ed_join_clk.filter(ed_join_clk.ed_time.substr(1, 8) == DATE) # 20191012
    pre_ed_join_clk = ed_join_clk.filter(ed_join_clk.ed_time.substr(1, 8) == yestday)
   
    today_ed_join_clk = today_ed_join_clk.drop('spam', 'tspam', 'reqid', 'adid', 'key', 'tkey')
    pre_ed_join_clk = pre_ed_join_clk.drop('spam', 'tspam', 'reqid', 'adid', 'key', 'tkey')
    
    print "After join click, ed count: %s, click count: %s, pre ed count: %s" \
        % (today_ed_join_clk.count(), today_ed_join_clk.filter(ed_join_clk.click_log.isNotNull()).count(), pre_ed_join_clk.count()) 
    
    today_ed_join_clk.write.json(join_path, 'overwrite')
    pre_ed_join_clk.write.json(yest_join_path, 'overwrite')
