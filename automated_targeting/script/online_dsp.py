# coding: utf-8
import hashlib
import base64
import json
import time
import sys
import os
import logging
import subprocess

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row, SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark import SparkFiles


DEFAULT_LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
DEFAULT_LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'
DEFAULT_LOG_LEVEL = 'INFO'

logging.basicConfig(format=DEFAULT_LOG_FORMAT,
                    datefmt=DEFAULT_LOG_DATEFORMAT,
                    level=DEFAULT_LOG_LEVEL)


def check_hdfs_exist(path):
    your_script = '/usr/local/hadoop-2.6.3/bin/hadoop fs -ls %s/_SUCCESS' % path.rstrip('/')
    try:
        process = subprocess.check_call(
            args=your_script,
            shell=True
        )
        logging.info('%s exist' % path)
        return True
    except:
        logging.info('%s dont exist' % path)
        return False


def extract_request(iter):

    for row in iter:
        try:
            d = json.loads(row[1])
            DSP_LOG = d['request']['value']['DSPREQUEST_LOG']
            #DSP_LOG = d['request']['value']['DSPED_LOG']
        except:
            continue
        #g_id = DSP_LOG.get("userid", "")
        g_id = DSP_LOG.get("uid", "")
        reqid = DSP_LOG.get("reqid", "")
        cc2 = DSP_LOG.get("cc2", "")
        cc3 = DSP_LOG.get("cc3", "")
        bundle_id = DSP_LOG.get("bundle_id","")
        yield Row(reqid=reqid, req_gaid=g_id, cc2=cc2, cc3=cc3, bundle_id=bundle_id)

def extract_ed(iter):

    for row in iter:
        try:
            d = json.loads(row[1])
            DSP_LOG = d['request']['value']['DSPED_LOG']
        except:
            continue
        g_id = DSP_LOG.get("userid", "")
        #g_id = DSP_LOG.get("uid", "")

        cc2 = DSP_LOG.get("cc2", "")
        cc3 = DSP_LOG.get("cc3", "")
        reqid = DSP_LOG.get("ed_reqid", "")
        planid = DSP_LOG.get("planid", "")
        promoted_app = DSP_LOG.get("promoted_app", "")
        yield Row(ed_reqid=reqid, ed_gaid=g_id, planid=planid,  promoted_app=promoted_app)


if __name__ == '__main__':
    request_path = sys.argv[1]
    ed_path = sys.argv[2]

    sc = SparkContext(
        appName='wenwu.tang-dmp_monitor-normal-tempory-dmp_applist',
    )
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)

    req_rdd = sc.newAPIHadoopFile(request_path,
                              'com.hadoop.mapreduce.LzoTextInputFormat',
                              'org.apache.hadoop.io.LongWritable',
                              'org.apache.hadoop.io.Text')
    ed_rdd = sc.newAPIHadoopFile(ed_path,
                              'com.hadoop.mapreduce.LzoTextInputFormat',
                              'org.apache.hadoop.io.LongWritable',
                              'org.apache.hadoop.io.Text')
    request_df = req_rdd.mapPartitions(extract_request).toDF().dropDuplicates(['req_gaid'])
    ed_df = ed_rdd.mapPartitions(extract_ed).toDF().dropDuplicates(['ed_gaid', 'promoted_app'])
    
    request_df = request_df.join(ed_df, request_df.req_gaid == ed_df.ed_gaid, 'left')
    request_df = request_df.withColumn('is_ed', F.when(request_df.ed_gaid.isNull(), 0).otherwise(1))

    request_df.show() 
    request_df.write.json("/user/ling.fang/1/2/3/4/5/automated_targeting/online_dsp_req/json", "overwrite")
    sys.exit(0)
