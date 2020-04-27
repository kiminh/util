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

MIN_ICON_COUNT = 20
CONFIENCE_ED_TH = 100000

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

if len(sys.argv) < 3:
    exit(1)

srcs = sys.argv[1]
out_path = sys.argv[2]
date = datetime.date.today()
app_name = 'naga interactive: stat_icon_image_ctr_%s' % date
sc = SparkContext(appName=app_name)
sc.setLogLevel('ERROR')
spark = SparkSession.builder.master("yarn")\
    .appName(app_name)\
    .getOrCreate()

df = spark.read.json(srcs)
df = df.select(get_pageid_udf('first_ed_log').alias('pageid'),
    get_reqstyle_udf('first_ed_log').alias('req_style'), 
    get_cover_image_udf('first_ed_log').alias('cover_image'),
    F.when(df.first_click_log.isNull(), 0).otherwise(1).alias('is_click'))
df.show()

cover_image_df = df.groupBy(['req_style', 'cover_image']).agg(
    F.sum('is_click').alias('click'), F.count('*').alias('ed'))
cover_image_df = cover_image_df.withColumn(
    'ctr', get_ctr_udf('ed', 'click'))
cover_image_df = cover_image_df.filter(cover_image_df.ctr <> -1)
cover_image_df.show(100, False)

stream_df = cover_image_df.filter(cover_image_df.req_style == '1')
icon_df = cover_image_df.filter(cover_image_df.req_style == '5')
stream_df.show(100, False)
icon_df.show(100, False)

from guldan_util import Guldan

guldan = Guldan()
guldan_icon_list = guldan.get_icon_list()
guldan_icon_set = set(guldan_icon_list)
guldan_icon_cnt = len(guldan_icon_set)

### update icon list form icon ###
icon_url = icon_df.collect()
icon_dict = {}
trigger_del_icons = []
for item in icon_url:
    cover_image = item['cover_image']
    ctr = float(item['ctr'])
    ed = float(item['ed'])
    click = float(item['click'])
    if cover_image in guldan_icon_set:
        icon_dict[cover_image] = [ctr, ed, click]
icon_sort = sorted(icon_dict.items(), key=lambda d:d[1][0])

del_guldan_icon_cnt = int(guldan_icon_cnt*0.1)
if del_guldan_icon_cnt > 8:
    del_guldan_icon_cnt = 8
if guldan_icon_cnt - del_guldan_icon_cnt > MIN_ICON_COUNT:
    del_icon_item = icon_sort[:del_guldan_icon_cnt]
    del_icon_list = []
    for icon_item in del_icon_item:
        del_icon_list.append(icon_item[0])
    trigger_del_icons, msg = guldan.del_icon_list(del_icon_list)
    if msg == 200:
        print "update guldan icon list sucessfully. \
            all icon cnt %s, delete icon cnt %s" % (guldan_icon_cnt, del_guldan_icon_cnt)
        print "trigger del icons:"
        print trigger_del_icons
    else:
        print "update guldan icon list fail."
else:
    print "guldan icon count %s, del cnt %s" % (guldan_icon_cnt, del_guldan_icon_cnt)

### update icon list for stream ###
guldan_stream_image_list = guldan.get_stream_image_list()
guldan_stream_image_set = set(guldan_stream_image_list)

guldan_stream_image_cnt = len(guldan_stream_image_set)
image_url = stream_df.collect()
image_dict = {}
trigger_del_images = []
for item in image_url:
    cover_image = item['cover_image']
    ctr = float(item['ctr'])
    ed = float(item['ed'])
    click = float(item['click'])
    if cover_image in guldan_stream_image_set:
         image_dict[cover_image] = [ctr, ed, click]

image_sort = sorted(image_dict.items(), key=lambda d:d[1][0])
del_guldan_image_cnt = int(guldan_stream_image_cnt*0.1)
if del_guldan_image_cnt > 8:
    del_guldan_image_cnt = 8
if guldan_stream_image_cnt - del_guldan_image_cnt > MIN_ICON_COUNT:
    del_image_item = image_sort[:del_guldan_image_cnt]
    del_image_list = []
    for image_item in del_image_item:
        del_image_list.append(image_item[0])
    print "del image list:"
    trigger_del_images, msg = guldan.del_stream_image_list(del_image_list)
    if msg == 200:
        print "update guldan stream low ctr image list sucessfully, \
            all image count %s, delete image count %s" % (guldan_stream_image_cnt, del_guldan_image_cnt)
        print "trigger del image:"
        print trigger_del_images
    else:
        print "update guldan stream low ctr image list fail."
else:
    print "guldan image count %s, del cnt %s" % (guldan_stream_image_cnt, del_guldan_image_cnt)

trigger_del_images_info = {}
for image in trigger_del_images:
    trigger_del_images_info[image] = image_dict[image]

trigger_del_icons_info = {}
for icon in trigger_del_icons:
    trigger_del_icons_info[icon] = icon_dict[icon]
 
### email report ###
def get_trigger_del_icon_table(data):
    html_table = "<h3>本周icon位删除的cover_image(总数:%s, 删除:%s):" % (guldan_icon_cnt, del_guldan_icon_cnt)
    html_table += "</h3><table border=\"2\", width=\"100%\">\
    <tr>\
        <td>cover_image</td>\
        <td>曝光</td>\
        <td>点击</td>\
        <td>点击率</td>\
    </tr>"
    for icon, value in data.items():
        html_table += "<tr><td>%s</td>\
                    <td>%.1f</td>\
                    <td>%.1f</td>\
                    <td>%.3f%%</td>\
                  </tr>" % (icon, value[1], value[2], value[0] * 100)
    html_table += "</table>"
    return html_table

def get_trigger_del_image_table(data):
    html_table = "<h3>本周信息流删除的cover_image(总数:%s, 删除:%s):" % (guldan_stream_image_cnt, del_guldan_image_cnt)
    html_table += "</h3><table border=\"2\", width=\"100%\">\
    <tr>\
        <td>cover_image</td>\
        <td>曝光</td>\
        <td>点击</td>\
        <td>点击率</td>\
    </tr>"
    for image, value in data.items():
        html_table += "<tr><td>%s</td>\
                    <td>%.1f</td>\
                    <td>%.1f</td>\
                    <td>%.3f%%</td>\
                  </tr>" % (image, value[1], value[2], value[0] * 100)
    html_table += "</table>"
    return html_table

icon_html_table = get_trigger_del_icon_table(trigger_del_icons_info)
image_html_table = get_trigger_del_image_table(trigger_del_images_info)

table = icon_html_table + image_html_table
mailsender = MailSender()
mailsender.init()
mailsender.send_email(subject="%s Naga DSP Interactive Icon and Image Delete Report" % date, msg=table)
