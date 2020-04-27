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

    data_path = sys.argv[1]
    out_path = sys.argv[2]
    TIME = sys.argv[3]
    print TIME
    sc = SparkContext(
        appName='naga DSP: stat_adstyle_edclk_%s' % TIME
    )
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)

    data_df = spark.read.json(data_path)
 
    data_df = data_df.select(
        F.col('ed_time').substr(1, 10).alias('time'), F.col('ed_log.ad_style').alias('ad_style'),
        F.when(data_df.click_log.isNotNull(), 1).otherwise(0).alias('is_click'),
        F.col('ed_log.pctr_cal').alias('pctr_cal')
    )
    data_df.show()
    
    data_gb = data_df.groupBy(['ad_style', 'time']).agg(
        F.count('*').alias('ed'),
        F.sum('is_click').alias('click'),
        F.sum('pctr_cal').alias('pctr')
    )

    data_gb.show()
    data_gb.write.json(out_path, 'overwrite')
    print "output hdfs path: %s" % out_path
