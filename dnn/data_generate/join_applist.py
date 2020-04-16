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

def get_identifier(click_log):
    try:
        j = json.loads(click_log)
    except:
        return ''
    return j['identifier']

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print "Usage: py ed_join_click.py" \
              " yesterday_ed yesterday_click befor_yes_ed befor_yes_click"
        sys.exit(0)

    data_path = sys.argv[1]
    applist_path = sys.argv[2]
    des_path = sys.argv[3]

    get_identifier_udf = F.udf(get_identifier, T.StringType())

    spark = SparkSession\
        .builder \
        .appName("naga interactive DSP: join_applist")\
        .enableHiveSupport()\
        .config("hive.exec.dynamic.partition.mode","nonstrict")\
        .config("hive.exec.max.dynamic.partitions","30000")\
        .config("hive.exec.max.dynamic.partitions.pernode","30000")\
        .config("spark.sql.caseSensitive", "true")\
        .getOrCreate()
    
    df = spark.read.json(data_path)
    print "==============^^^^^=======%s" % df.count()
    applist_df = spark.read.parquet(applist_path)
    applist_df = applist_df.dropDuplicates(['identifier'])
    #df = df.withColumn('t_identifier', get_identifier_udf('click_log'))
    applist_df.show()
    df = df.join(applist_df, df.click_log.identifier == applist_df.identifier, 'left')
    #df = df.join(applist_df, df.t_identifier == applist_df.identifier, 'left')
    print "==================>%s, %s" % (df.filter(df.identifier.isNotNull()).count(), df.count())
    df.write.json(des_path, 'overwrite')
