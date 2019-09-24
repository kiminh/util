#coding:utf-8
import sys
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import StringIndexer
from pyspark import SparkContext
from pyspark.sql import Row, SparkSession
from pyspark import SparkFiles
import json
import datetime
import time
import math

reload(sys)
sys.setdefaultencoding('utf-8') 

def get_use_cnt(action_time_list):
    return len(action_time_list)

def get_use_time(action_time_list):
    diff_time = 0.0
    for item in action_time_list:
        start_time = float(item[1][:10])
        end_time = float(item[0][:10])
        diff = math.fabs(end_time - start_time)
        diff_time += diff

    return diff_time

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print "args lens is less than 5"
        exit(0)

    #user_applist_path = sys.argv[1]
    appusage_path = sys.argv[2]
    ed_path = sys.argv[1]
    #appdb_path = sys.argv[3]
    #idf2gaid_path = sys.argv[4]
    #des_path = sys.argv[5]

    sc = SparkContext(appName="DMP.pangu:appusage data")

    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)
    
    df = spark.read.parquet("/user/ling.fang/1/2/3/4/5/automated_targeting/model/stringindex/pkg_index/data/")
    df.write.json("/user/ling.fang/1/2/3/4/5/automated_targeting/model/pkg_labels/json", 'overwrite') 
    #appusage_df = spark.read.json(appusage_path)
    #ed_df = spark.read.json(ed_path)

    #appusage_df = appusage_df.dropDuplicates(['gaid'])
    #ed_df = ed_df.dropDuplicates(['gaid'])
    #ed_df = ed_df.withColumnRenamed('gaid', 'tgaid')

    #ed_join_appusage = appusage_df.join(ed_df, appusage_df.gaid == ed_df.tgaid)
