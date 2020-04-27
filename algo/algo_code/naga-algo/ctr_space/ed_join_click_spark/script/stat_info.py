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
    click_path = sys.argv[2]
    des_path = sys.argv[3]

    sc = SparkContext(
        appName='naga DSP: ed_join_click'
    )
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)

    ed_df = spark.read.json(yest_ed_path)
    pre_ed_df = spark.read.json(befor_yest_ed_path)
    clk_df = spark.read.json(clk_path)   
 
    ed_df = ed_df.select(ed_df.time.alias('ed_time'),
        ed_df.request.value.DSPED_LOG.alias('ed_log'),
        ed_df.request.value.DSPED_LOG.reqid.alias('ed_reqid'),
        ed_df.request.value.DSPED_LOG.spam.alias('spam'),
        ed_df.request.value.DSPED_LOG.plid.alias('plid'))

    clk_df = clk_df.select(clk_df.time.alias('click_time'),
        clk_df.request.value.DSPCLICK_LOG.alias('click_log'),
        clk_df.request.value.DSPCLICK_LOG.reqid.alias('clk_reqid'),
        clk_df.request.value.DSPCLICK_LOG.spam.alias('tspam'))

    pre_ed_df = pre_ed_df.select(pre_ed_df.time.alias('ed_time'),
        pre_ed_df.request.value.DSPED_LOG.alias('ed_log'),
        pre_ed_df.request.value.DSPED_LOG.reqid.alias('ed_reqid'),
        pre_ed_df.request.value.DSPED_LOG.spam.alias('spam'),
        pre_ed_df.request.value.DSPED_LOG.plid.alias('plid'))

    print "ed count: %s, click count: %s, pre ed count: %s" \
        % (ed_df.count(), clk_df.count(), pre_ed_df.count())
    ed_df = ed_df.filter(ed_df.spam == 0) \
        .dropDuplicates(['ed_reqid'])
    pre_ed_df = pre_ed_df.filter(pre_ed_df.spam == 0) \
        .dropDuplicates(['ed_reqid'])
    clk_df = clk_df.filter(clk_df.tspam == 0) \
        .dropDuplicates(['clk_reqid'])
    
    print "After filter spam != 0, ed count: %s, click count: %s, pre ed count: %s" \
        % (ed_df.count(), clk_df.count(), pre_ed_df.count())
    ed_join_clk = ed_df.join(clk_df, 
        ed_df.ed_reqid == clk_df.clk_reqid, 'left')
    pre_ed_join_clk = pre_ed_df.join(clk_df, 
        pre_ed_df.ed_reqid == clk_df.clk_reqid, 'left')
    
    #stat plid ed and click
    plid_ed_click = ed_join_clk.groupBy('plid').agg(
        F.count("*").alias('ed'),
        F.sum(F.when(ed_join_clk.clk_reqid.isNotNull(), 1).otherwise(0)).alias('click')) 
    pre_plid_ed_click = pre_ed_join_clk.groupBy('plid').agg(
        F.count("*").alias('ed'),
        F.sum(F.when(pre_ed_join_clk.clk_reqid.isNotNull(), 1).otherwise(0)).alias('click')) 
    #plid_filter = plid_ed_click.filter(
    #    ((plid_ed_click.click * 1.0) / plid_ed_click.ed > 0.5))
    #pre_plid_filter = pre_plid_ed_click.filter(
    #    ((pre_plid_ed_click.click * 1.0) / pre_plid_ed_click.ed > 0.5))
    
    clk_join_reqid1 = ed_join_clk.filter(ed_join_clk.clk_reqid.isNotNull()) \
        .select(ed_join_clk.clk_reqid).withColumnRenamed('clk_reqid', 'reqid')
    clk_join_reqid2 = pre_ed_join_clk.filter(pre_ed_join_clk.clk_reqid.isNotNull()) \
        .select(pre_ed_join_clk.clk_reqid).withColumnRenamed('clk_reqid', 'reqid')

    clk_join_reqid = clk_join_reqid1.unionAll(clk_join_reqid2)

    clk_df = clk_df.join(clk_join_reqid, 
        clk_df.clk_reqid == clk_join_reqid.reqid, 'left')
    clk_unjoin_df = clk_df.filter(clk_df.reqid.isNull())
    
    ed_join_clk = ed_join_clk.drop('ed_reqid', 'spam', 'tspam', 'clk_reqid')
    pre_ed_join_clk = pre_ed_join_clk.drop('ed_reqid', 'spam', 'tspam', 'clk_reqid')
    clk_unjoin_df = clk_unjoin_df.drop('clk_reqid', 'reqid')
    
    print "After join click, ed count: %s, pre ed count: %s" \
        % (ed_join_clk.count(), pre_ed_join_clk.count()) 
    
    ed_join_clk.write.json(yest_join_path, 'overwrite')
    pre_ed_join_clk.write.json(befor_yest_join_path, 'overwrite')
    clk_unjoin_df.write.json(unjoin_path, 'overwrite')
    plid_ed_click.write.json(plid_ed_click_path, 'overwrite')
