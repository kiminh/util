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
from pyspark.sql import Window

import datetime
from email_sender import MailSender

reload(sys)
sys.setdefaultencoding('utf-8')

CONFIENCE_ED_TH = 1000
def get_pageid(col):
    j = json.loads(col)
    try:
        return j['pageid']
    except:
        return '0'
get_pageid_udf = F.udf(get_pageid, T.StringType())

def get_cover_image(col):
    j = json.loads(col)
    try:
        return j['cover_image']
    except:
        return '0'
get_cover_image_udf = F.udf(get_cover_image, T.StringType())

def get_reqstyle(col):
    j = json.loads(col)
    try:
        return j['req_style']
    except:
        return '0'
get_reqstyle_udf = F.udf(get_reqstyle, T.StringType())

def get_ctr(ed, click):
    if ed < CONFIENCE_ED_TH:
        return -1.0
    return click*1.0/ed
get_ctr_udf = F.udf(get_ctr, T.FloatType())

def get_plid(col):
    j = json.loads(col)
    try:
        return j['plid']
    except:
        return '0'
get_plid_udf = F.udf(get_plid, T.StringType())

if len(sys.argv) < 3:
    exit(1)

srcs = sys.argv[1]
out_path = sys.argv[2]
out_path1 = sys.argv[3]
date = datetime.date.today()
app_name = 'naga interactive: stat_icon_image_ctr_%s' % date
sc = SparkContext(appName=app_name)
sc.setLogLevel('ERROR')
spark = SparkSession.builder.master("yarn")\
    .appName(app_name)\
    .getOrCreate()

df = spark.read.json(srcs)
df = df.select(
    get_reqstyle_udf('first_ed_log').alias('req_style'),
    get_pageid_udf('first_ed_log').alias('pageid'),
    get_cover_image_udf('first_ed_log').alias('cover_image'),
    get_plid_udf('first_ed_log').alias('plid'),
    F.when(df.first_click_log.isNull(), 0).otherwise(1).alias('is_click')
)
df.show()

cover_image_df = df.filter(df.req_style == 5)
cover_image_df.show()
feeds_df = df.filter(df.req_style == 1)
feeds_df.show()

cover_image_df = cover_image_df.groupBy(['cover_image']).agg(
    F.sum('is_click').alias('click'), F.count('*').alias('ed')
)
cover_image_df = cover_image_df.withColumn(
    'ctr', get_ctr_udf('ed', 'click')
)
cover_image_df = cover_image_df.filter(cover_image_df.ctr <> -1)
cover_image_df.show(100, False)
cover_image_df.repartition(10).write.json(out_path, 'overwrite')
data = cover_image_df.collect()
avg_ctr = 0
for item in data:
    ctr = float(item['ctr'])
    avg_ctr += ctr
avg_ctr = avg_ctr / len(data)

feeds_df = feeds_df.groupBy(['pageid', 'cover_image']).agg(
    F.sum('is_click').alias('click'), F.count('*').alias('ed')
)
feeds_df = feeds_df.withColumn(
    'ctr', get_ctr_udf('ed', 'click')
)
feeds_df = feeds_df.filter(feeds_df.ctr <> -1)
feeds_df.show()
feeds_df.repartition(10).write.json(out_path1, 'overwrite')
feeds_data = feeds_df.collect()
feeds_avg_ctr = 0
for item in feeds_data:
    ctr = float(item['ctr'])
    feeds_avg_ctr += ctr
feeds_avg_ctr = feeds_avg_ctr / len(feeds_data)

import json
import sys 
import redis
import time
r = redis.Redis(host='ali-cache07.corp.cootek.com', port=16359)
r1 = redis.Redis(host='ali-cache01.corp.cootek.com', port=17131)
prefix = "naga_interactive_cs"

with r.pipeline(transaction=False) as p, r1.pipeline(transaction=False) as p1: 
    p.set("naga_interactive_cs:default", avg_ctr)
    p1.set("naga_interactive_cs:default", avg_ctr)
    p.set("naga_interactive_cs:feed_default", feeds_avg_ctr)
    p1.set("naga_interactive_cs:feed_default", feeds_avg_ctr)
    for item in data:
        cover_image = item['cover_image']
        ctr = float(item['ctr'])
        dict_name = '%s:%s' % (prefix, cover_image)
        p.set(dict_name, ctr)
        p1.set(dict_name, ctr)
        print dict_name, ctr

    for item in feeds_data:
        cover_image = item['cover_image']
        ctr = float(item['ctr'])
        pageid = item['pageid']
        dict_name = '%s:%s_%s' % (prefix, cover_image, pageid)
        p.set(dict_name, ctr)
        p1.set(dict_name, ctr)
        print dict_name, ctr

    try:
        p.execute()
        p1.execute()
    except Exception as e:
        print "redis set dict_name [%s] error [%s]." % (dict_name, e)
        exit(1)
