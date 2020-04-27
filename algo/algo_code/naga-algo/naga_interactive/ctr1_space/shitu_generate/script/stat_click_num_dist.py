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
MAX_CLICK_NUM=8

def get_pageid(col):
    j = json.loads(col)
    try:
        return j['pageid']
    except:
        return '0'
get_pageid_udf = F.udf(get_pageid, T.StringType())

if len(sys.argv) < 3:
    exit(1)
srcs = sys.argv[1]
out_path = sys.argv[2]
out_path1 = sys.argv[3]

app_name = 'naga interactive: stat_click_num_dist'
sc = SparkContext(appName=app_name)
sc.setLogLevel('ERROR')
spark = SparkSession.builder.master("yarn")\
    .appName(app_name)\
    .enableHiveSupport()\
    .config("hive.exec.dynamic.partition.mode","nonstrict")\
    .config("hive.exec.max.dynamic.partitions","30000")\
    .config("hive.exec.max.dynamic.partitions.pernode","30000")\
    .config("spark.sql.caseSensitive", "true")\
    .getOrCreate()

data_df = spark.sql("SELECT predictClickNum, pageid, sum(ed) / sum(request) AS show_rate FROM ( SELECT reqid, 1 AS request, get_json_object(extra, '$.predictClickNum') AS predictClickNum, get_json_object(extra, '$.pageid') as pageid FROM dw.dw_usage_naga_dsp_request_h WHERE ( dt = get_date(-1) OR dt = get_date(-2) OR dt = get_date(-3) ) AND substr(time, 4, 2) = '20' AND get_json_object(extra, '$.dsp_src') = '2' AND rtc = '200' GROUP BY reqid, get_json_object(extra, '$.predictClickNum'), get_json_object(extra, '$.pageid')) a LEFT JOIN ( SELECT reqid, 1 AS ed FROM dw.dw_usage_ad_naga_interactive_second_click_d WHERE ( dt = get_date(-1) OR dt = get_date(-2) OR dt = get_date(-3) ) GROUP BY reqid ) b ON (a.reqid = b.reqid) GROUP BY predictClickNum, pageid")

data_df.write.mode('overwrite').json(out_path1)
show_rate_data = data_df.collect()
print show_rate_data

show_rate = [ 0 for i in range(MAX_CLICK_NUM) ]
page_show_rate = {}
for item in show_rate_data:
    pageid = item['pageid']
    predictClickNum = int(item['predictClickNum'])
    if pageid not in page_show_rate:
        page_show_rate[pageid] = [ 0 for i in range(MAX_CLICK_NUM) ]
    if predictClickNum - 1 >= 0 and predictClickNum <= MAX_CLICK_NUM \
        and float(item['show_rate']) != 0:
        page_show_rate[pageid][predictClickNum-1] = float(item['show_rate'])
    else:
        print "predict click num %d is zero" % predictClickNum
        exit(1)
    show_rate[predictClickNum-1] += float(item['show_rate'])

print page_show_rate
print show_rate

df = spark.read.json(srcs)
df = df.filter(df.click_num.isNotNull()) \
    .select('click_num', get_pageid_udf('first_ed_log').alias('pageid'))\
    .groupBy(['click_num', 'pageid']).agg(F.count("*").alias('cnt')).cache()
df.write.mode('overwrite').json(out_path)
data = df.collect()
print data
from collections import defaultdict
data2 = {}
data_all = {}
data_all = [ 0 for i in range(MAX_CLICK_NUM) ]
for d in data:
    click_num = MAX_CLICK_NUM if d['click_num'] > MAX_CLICK_NUM else d['click_num']
    if d['pageid'] not in data2:
        data2[d['pageid']] = [ 0 for i in range(MAX_CLICK_NUM) ]
    data2[d['pageid']][click_num-1] += d['cnt']
    data_all[click_num-1] += d['cnt']

for pageid in data2:
    for i in range(MAX_CLICK_NUM):
        data2[pageid][i] = data2[pageid][i] / page_show_rate[pageid][i]
        data_all[i] = data_all[i] / show_rate[i]
print data2

ret_dict = defaultdict(list)
for pageid, value in data2.items():
    all_cnt = sum(value)
    ret = [ 0 for i in range(MAX_CLICK_NUM) ]
    for i, cnt in enumerate(value):
        if i != 0:
            ret[i] = cnt*1.0/all_cnt + ret[i-1]
        else:
            ret[i] = cnt*1.0/all_cnt
    ret_dict[pageid] = ret

all_cnt = sum(data_all)
ret = [ 0 for i in range(MAX_CLICK_NUM) ]
for i, cnt in enumerate(data_all):
    if i != 0:
        ret[i] = cnt*1.0/all_cnt + ret[i-1]
    else:
        ret[i] = cnt*1.0/all_cnt
ret_dict['default'] = ret
print ret_dict
import json
import sys 
import redis
import time
r = redis.Redis(host='ali-cache07.corp.cootek.com', port=16359)
r1 = redis.Redis(host='ali-cache01.corp.cootek.com', port=17131)
prefix = 'naga_interactive_cdf'

with r.pipeline(transaction=False) as p, r1.pipeline(transaction=False) as p1:
    for pageid, value in ret_dict.items():
        value_str = "_".join(str(v) for v in value)
        dict_name = '%s:%s' % (prefix, pageid)
        p.set(dict_name, value_str)
        p1.set(dict_name, value_str)
        print dict_name, value_str
    try:
        p.execute()
        p1.execute()
    except Exception as e:
        print "redis set dict_name [%s] error [%s]." % (dict_name, e)
        exit(1)
