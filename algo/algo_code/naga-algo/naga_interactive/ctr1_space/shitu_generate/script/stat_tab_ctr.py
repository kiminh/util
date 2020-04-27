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

def get_pageid(col):
    j = json.loads(col)
    try:
        return j['pageid']
    except:
        return '0'
get_pageid_udf = F.udf(get_pageid, T.StringType())

def get_plid(col):
    j = json.loads(col)
    try:
        return j['plid']
    except:
        return '0'
get_plid_udf = F.udf(get_plid, T.StringType())

def get_reqstyle(col):
    j = json.loads(col)
    try:
        return j['req_style']
    except:
        return '0'
get_reqstyle_udf = F.udf(get_reqstyle, T.StringType())

def get_ctr(ed, click):
    if click < 20:
        return -1.0
    return click*1.0/ed
get_ctr_udf = F.udf(get_ctr, T.FloatType())

if len(sys.argv) < 3:
    exit(1)

srcs = sys.argv[1]
out_path = sys.argv[2]

app_name = 'naga interactive: stat_click_num_dist_%s' % date
sc = SparkContext(appName=app_name)
sc.setLogLevel('ERROR')
spark = SparkSession.builder.master("yarn")\
    .appName(app_name)\
    .getOrCreate()

df = spark.read.json(srcs)
df = df.select(get_pageid_udf('first_ed_log').alias('pageid'),
    get_plid_udf('first_ed_log').alias('plid'),get_reqstyle_udf('first_ed_log').alias('req_style'),
    F.when(df.second_click_log.isNull(), 0).otherwise(1).alias('is_click'))
df.show()
df = df.filter((df.req_style == '6') | (df.plid == '4b37ca6bc174dea67e57d868cf303330'))
df.show()

global_df = df.agg(
    F.sum('is_click').alias('click'), F.count('*').alias('ed'))
global_df = global_df.withColumn(
    'ctr', get_ctr_udf('ed', 'click'))
global_df = global_df.filter(global_df.ctr != -1.0)

plid_df = df.groupBy(['plid']).agg(
    F.sum('is_click').alias('click'), F.count('*').alias('ed'))
plid_df = plid_df.withColumn(
    'ctr', get_ctr_udf('ed', 'click'))
plid_df = plid_df.filter(plid_df.ctr != -1.0)

plid_pageid_df = df.groupBy(['plid', 'pageid']).agg(
    F.sum('is_click').alias('click'), F.count('*').alias('ed'))
plid_pageid_df = plid_pageid_df.withColumn(
    'ctr', get_ctr_udf('ed', 'click'))
plid_pageid_df = plid_pageid_df.filter(plid_pageid_df.ctr != -1.0)

global_ctr = global_df.collect()
plid_ctr = plid_df.collect()
plid_pageid_ctr = plid_pageid_df.collect()

print global_ctr
print plid_ctr
print plid_pageid_ctr

import json
import sys 
import redis
import time
r = redis.Redis(host='ali-cache07.corp.cootek.com', port=16359)
r1 = redis.Redis(host='ali-cache01.corp.cootek.com', port=17131)
global_prefix = 'dsp_interactive_global_ctr1'
plid_prefix = 'dsp_interactive_plid_ctr1'
plid_pageid_prefix = 'dsp_interactive_plid_page_ctr1' 

with r.pipeline(transaction=False) as p, r1.pipeline(transaction=False) as p1:
    for item in global_ctr:
        dict_name = '%s' % (global_prefix)
        ctr = float(item['ctr'])
        if ctr == 0:
            continue
        p.set(dict_name, ctr)
        p1.set(dict_name, ctr)
        print dict_name, ctr

    for item in plid_ctr:
        dict_name = '%s:%s' % (plid_prefix, item['plid'])
        ctr = float(item['ctr'])
        if ctr == 0:
            continue
        p.set(dict_name, ctr)
        p1.set(dict_name, ctr)
        print dict_name, ctr

    for item in plid_pageid_ctr:
        dict_name = '%s:%s_%s' % (plid_pageid_prefix, item['plid'], item['pageid'])
        ctr = float(item['ctr'])
        if ctr == 0:
            continue
        p.set(dict_name, ctr)
        p1.set(dict_name, ctr)
        print dict_name, ctr

    try:
        p.execute()
        p1.execute()
    except Exception as e:
        print "redis set dict_name [%s] error [%s]." % (dict_name, e)
        exit(1)
