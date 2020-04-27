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

    ed_path = sys.argv[1]
    clk_path = sys.argv[2]

    join_path = sys.argv[3]
    yest_join_path = sys.argv[4]
    unjoin_path = sys.argv[5]
    DATE = sys.argv[6]

    temp_dt = datetime.strptime(DATE, '%Y%m%d%H')
    yestday = (temp_dt + timedelta(hours = -1)).strftime("%Y%m%d%H")

    print DATE, yestday
    sc = SparkContext(
        appName='naga DSP: ed_join_click_%s' % DATE
    )
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)

    ed_df = spark.read.json(ed_path)
    clk_df = spark.read.json(clk_path)   
 
    ed_df = ed_df.select(
        F.col('time').alias('ed_time'),
        F.col('request.value.DSPED_LOG').alias('ed_log'),
        F.col('request.value.DSPED_LOG.reqid').alias('reqid'),
        F.col('request.value.DSPED_LOG.adid').alias('adid'),
        F.col('request.value.DSPED_LOG.spam').alias('spam')
    )
    ed_df.show()

    clk_df = clk_df.select(
        F.col('time').alias('click_time'),
        F.lit(1).alias('click_log'), #F.col('request.value.DSPCLICK_LOG').alias('click_log'),
        F.col('request.value.DSPCLICK_LOG.reqid').alias('reqid'),
        F.col('request.value.DSPCLICK_LOG.adid').alias('adid'),
        F.col('request.value.DSPCLICK_LOG.spam').alias('tspam')
    )
    clk_df.show()

    #print "ed count: %s, click count: %s" % (ed_df.count(), clk_df.count())
    ed_df = ed_df.filter((ed_df.spam == 0) & (ed_df.reqid <> "")) \
        .dropDuplicates(['reqid', 'adid'])
    clk_df = clk_df.filter((clk_df.tspam == 0) & ((clk_df.reqid <> ""))) \
        .dropDuplicates(['reqid', 'adid'])
   
    ed_df = ed_df.withColumn('key', F.concat_ws('_', 'reqid', 'adid'))
    clk_df = clk_df.withColumn('tkey', F.concat_ws('_', 'reqid', 'adid'))
    
    #print "After filter spam != 0, ed count: %s, click count: %s" % (ed_df.count(), clk_df.count())
    ed_join_clk = ed_df.join(clk_df, ed_df.key == clk_df.tkey, 'left')
   
    today_ed_join_clk = ed_join_clk.filter(ed_join_clk.ed_time.substr(1, 10) == DATE) # 2019101211
    pre_ed_join_clk = ed_join_clk.filter(ed_join_clk.ed_time.substr(1, 10) == yestday)
   
    today_ed_join_clk = today_ed_join_clk.drop('spam', 'tspam', 'reqid', 'adid', 'key', 'tkey')
    pre_ed_join_clk = pre_ed_join_clk.drop('spam', 'tspam', 'reqid', 'adid', 'key', 'tkey')
    
    #print "After join click, ed count: %s, click count: %s, pre ed count: %s" \
    #    % (today_ed_join_clk.count(), today_ed_join_clk.filter(ed_join_clk.click_log.isNotNull()).count(), pre_ed_join_clk.count()) 

    today_ed_join_clk.write.json(join_path, 'overwrite')
    pre_ed_join_clk.write.json(yest_join_path, 'overwrite')
    #clk_unjoin_df.write.json(unjoin_path, 'overwrite')
