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
from email_sender import MailSender

reload(sys)
sys.setdefaultencoding('utf-8')

def get_all_data_table(data):
    
    html_table = "<h3>近七天预测点击次数总和与实际点击次数总和偏差: </h3><table border=\"2\", width=\"100%\">\
    <tr>\
        <td>目期</td>\
        <td>实际点击次数总和</td>\
        <td>预测点击次数总和</td>\
        <td>偏差</td>\
    </tr>"

    for d in data:
        diff = 0 if d['predict_num'] == 0 else (d['predict_num'] - d['clc_num'])/d['clc_num']
        html_table += "<tr><td>%s</td>\
                    <td>%.2f</td>\
                    <td>%.2f</td>\
                    <td>%.2f%%</td>\
                </tr>" % \
                (d['dt'], d['clc_num'], d['predict_num'], diff*100)
    html_table += "</table>"
    return html_table

def get_pageid_data_table(data):
    
    html_table = "<h3>分pageid预测点击次数总和与实际点击次数总和偏差: </h3><table border=\"2\", width=\"100%\">\
    <tr>\
        <td>pageid</td>\
        <td>实际点击次数总和</td>\
        <td>预测点击次数总和</td>\
        <td>偏差</td>\
    </tr>"

    for d in data:
        diff = 0 if d['predict_num'] == 0 else (d['predict_num'] - d['clc_num'])/d['clc_num']
        html_table += "<tr><td>%s</td>\
                    <td>%.2f</td>\
                    <td>%.2f</td>\
                    <td>%.2f%%</td>\
                </tr>" % \
                (d['pageid'], d['clc_num'], d['predict_num'], diff*100)
    html_table += "</table>"
    return html_table

def get_pageid_dist_table(data):
    data_ = {}
    html_table = "<h3>分pageid的预测点击分布: </h3><table border=\"2\", width=\"100%\">\
    <tr>\
        <td>pageid</td>\
        <td>点击数</td>\
        <td>预测点击次数</td>\
        <td>实际点击次数</td>\
        <td>偏差</td>\
    </tr>"
    for d in data:
        if d['pageid'] not in data_:
            data_[d['pageid']] = []
        data_[d['pageid']].append([float(d['click_num']), float(d['predict_num']), float(d['real_num']), float(d['bias'])])
    for key, value in data_.items():
        line_num = len(value)
        html_table += "<tr><td rowspan=\"%s\">%s</td>\
                  </tr>" % \
                (line_num+1, key)
        for v in value:
            html_table += "<tr><td>%d</td>\
                    <td>%.2f</td>\
                    <td>%.2f</td>\
                    <td>%.3f%%</td>\
                  </tr>" % \
                (v[0], v[1], v[2], v[3]*100)
    return html_table

if __name__ == '__main__':
    import datetime 
    yesterday = datetime.date.today() + datetime.timedelta(-1)
    spark = SparkSession\
        .builder \
        .appName("naga DSP interactive: predict click num metric val")\
        .enableHiveSupport()\
        .config("hive.exec.dynamic.partition.mode","nonstrict")\
        .config("hive.exec.max.dynamic.partitions","30000")\
        .config("hive.exec.max.dynamic.partitions.pernode","30000")\
        .config("spark.sql.caseSensitive", "true")\
        .getOrCreate()

    df = spark.sql("select a.dt as dt, clc_num, predict_num, (predict_num - clc_num) / clc_num as bias from (select dt, sum(max_number*cnt) as clc_num from (select dt, max_number, count(*) as cnt from (select dt, reqid, case when max(number) > 8 then 8 else max(number) end max_number from (select distinct dt, reqid, pageid, number from dw.dw_usage_ad_naga_interactive_second_click_d where dt <= get_date(-1) and dt > get_date(-7) and get_json_object(extra, '$.predict_clk_num') is not null and number <> '' ) group by reqid, dt) group by max_number,dt) group by dt) a join (select dt, sum(cnt*pcn) as predict_num from (select dt, pcn, count(*) as cnt from (select distinct dt, reqid, get_json_object(extra, '$.predict_clk_num') as pcn from dw.dw_usage_ad_naga_interactive_second_click_d where dt <= get_date(-1) and dt > get_date(-7) and get_json_object(extra, '$.predict_clk_num') is not null and number <> '') group by pcn,dt) group by dt) b on a.dt = b.dt order by dt")
    data_all = df.collect()
    html_table = get_all_data_table(data_all) 
    
    df = spark.sql("select a.pageid as pageid, clc_num, predict_num, (predict_num - clc_num) / clc_num as bias from (select pageid, sum(max_number*cnt) as clc_num from (select pageid, max_number, count(*) as cnt from (select pageid, reqid, case when max(number) > 8 then 8 else max(number) end max_number from (select distinct pageid, reqid, number from dw.dw_usage_ad_naga_interactive_second_click_d  where dt = get_date(-1) and get_json_object(extra, '$.predict_clk_num') is not null and number <> '') group by reqid, pageid) group by max_number,pageid) group by pageid) a join (select pageid, sum(cnt*pcn) as predict_num from (select pageid, pcn, count(*) as cnt from (select distinct pageid, reqid, get_json_object(extra, '$.predict_clk_num') as pcn from dw.dw_usage_ad_naga_interactive_second_click_d where dt = get_date(-1) and get_json_object(extra, '$.predict_clk_num') is not null and number <> '') group by pcn,pageid) group by pageid) b on a.pageid = b.pageid")
    data_pageid = df.collect()
    pageid_html_table = get_pageid_data_table(data_pageid)

    df = spark.sql("select a.pageid, a.click_num, predict_num, real_num, (predict_num - real_num) / real_num as bias from ( select pageid, pcn as click_num, count(*) as predict_num from (select distinct pageid, reqid, get_json_object(extra, '$.predict_clk_num') as pcn from dw.dw_usage_ad_naga_interactive_second_click_d where dt = get_date(-1) and get_json_object(extra, '$.predict_clk_num') is not null and number <> '') group by pcn, pageid) a join ( select pageid, max_number as click_num, count(*) as real_num from (select pageid, reqid, case when max(number) > 8 then 8 else max(number) end max_number from (select distinct reqid, pageid, number from dw.dw_usage_ad_naga_interactive_second_click_d where dt = get_date(-1) and get_json_object(extra, '$.predict_clk_num') is not null and number <> '') group by reqid, pageid) group by max_number, pageid ) b on a.pageid = b.pageid and a.click_num = b.click_num order by pageid, click_num")
    data_pageid_dist = df.collect()
    pageid_dist_html_table = get_pageid_dist_table(data_pageid_dist)

    table = html_table + pageid_html_table + pageid_dist_html_table
    mailsender = MailSender()
    mailsender.init()
    mailsender.send_email(subject="%s Naga DSP Interactive Click Number Predict Report" % yesterday, msg=table)

