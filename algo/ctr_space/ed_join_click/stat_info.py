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

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print "Usage: py stat_info.py" \
              " ed_log click_log des_path"
        sys.exit(0)

    ed_path = sys.argv[1]
    clk_path = sys.argv[2]
    des_path = sys.argv[3]

    sc = SparkContext(
        appName='naga DSP: ed_join_click'
    )
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)

    ed_df = spark.read.json(ed_path)
    clk_df = spark.read.json(clk_path)   
 
    ed_df = ed_df.select(ed_df.time.alias('ed_time'),
        ed_df.request.value.DSPED_LOG.reqid.alias('ed_reqid'),
        ed_df.request.value.DSPED_LOG.spam.alias('spam'),
        ed_df.request.value.DSPED_LOG.plid.alias('plid'))

    clk_df = clk_df.select(clk_df.time.alias('click_time'),
        clk_df.request.value.DSPCLICK_LOG.reqid.alias('clk_reqid'),
        clk_df.request.value.DSPCLICK_LOG.spam.alias('tspam'))

    print "ed count: %s, click count: %s" \
        % (ed_df.count(), clk_df.count())
    ed_df = ed_df.filter(ed_df.spam == 0) \
        .dropDuplicates(['ed_reqid'])
    clk_df = clk_df.filter(clk_df.tspam == 0) \
        .dropDuplicates(['clk_reqid'])
    
    print "After filter spam != 0, ed count: %s, click count: %s" \
        % (ed_df.count(), clk_df.count())
    ed_join_clk = ed_df.join(clk_df, 
        ed_df.ed_reqid == clk_df.clk_reqid, 'left')
    
    #stat plid ed and click
    plid_ed_click = ed_join_clk.groupBy('plid').agg(
        F.count("*").alias('ed'),
        F.sum(F.when(ed_join_clk.clk_reqid.isNotNull(), 1).otherwise(0)).alias('click')) 
    
    plid_ed_click.write.json(des_path + '/json', 'overwrite')
