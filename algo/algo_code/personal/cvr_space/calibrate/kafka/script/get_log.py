#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import sys
import os
from pykafka import KafkaClient
from pykafka.balancedconsumer import BalancedConsumer
from pykafka.simpleconsumer import OwnedPartition, OffsetType
import json
import time
from datetime import datetime, date
import random

import logging
DEFAULT_LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
DEFAULT_LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'
DEFAULT_LOG_LEVEL = 'INFO'
logging.basicConfig(format=DEFAULT_LOG_FORMAT,
                    datefmt=DEFAULT_LOG_DATEFORMAT,
                    level=DEFAULT_LOG_LEVEL)

import threading
def getHour():
    return str(time.strftime("%Y%m%d%H"))

init_hour = getHour()

output_path = "/home/ad_user/personal/ling.fang/cvr_space/calibrate/kafka/online_log/"

def fun_timer():
    global timer
    global transform_fo
    global click_fo
    global init_hour
    cur_hour = getHour() 
    if cur_hour != init_hour:
        logging.info("%s ---> %s" % (init_hour, cur_hour))
        init_hour = cur_hour

        transform_output_file = "%s/transform/transform.%s.log" % (output_path, init_hour)
        click_output_file = "%s/click/click.%s.log" % (output_path, init_hour)
        if not os.path.exists(transform_output_file):
            os.system("touch {}".format(transform_output_file))
        if not os.path.exists(click_output_file):
            os.system("touch {}".format(click_output_file))

        init_hour = cur_hour
 
    timer = threading.Timer(1, fun_timer)
    timer.start()

timer = threading.Timer(1,fun_timer)
timer.start()

class PyKafka:
    output_path = "/home/ad_user/personal/ling.fang/cvr_space/calibrate/kafka/online_log/"
    log_prefix = 'kafka_transformclick'
    consumer = None
    TOPIC = "Cootek_ad_naga_dsp_data"
    BROKER_LIST = "cn-bdpkafka04.corp.cootek.com:9092,cn-bdpkafka05.corp.cootek.com:9092,cn-bdpkafka06.corp.cootek.com:9092"
    ZK_LIST = "ad01.corp.cootek.com:2181,ad02.corp.cootek.com:2181,ad03.corp.cootek.com:2181,ad04.corp.cootek.com:2181,ad05.corp.cootek.com:2181"

    server = topic = zsServer = None

    def __init__(self):
        print("begin pykafka")
        self.server = self.BROKER_LIST
        self.topic = self.TOPIC
        self.zkServer = self.ZK_LIST

    def getConnect(self):
        client = KafkaClient(hosts=self.server)
        topic = client.topics[self.topic]
        self.consumer = topic.get_balanced_consumer(
            consumer_group="cvronline_20191031",
            auto_offset_reset=OffsetType.LATEST,  # 在consumer_group存在的情况下，设置此变量，表示从最新的开始取
            managed=True
            # auto_offset_reset=OffsetType.EARLIEST,
            # reset_offset_on_start=True,
            # auto_commit_enable=True,
            # zookeeper_connect=self.zkServer
        )
        reset_offset_on_start=False
        self.consumer.consume()
        self.consumer.commit_offsets()
        #self.consumer = topic.get_simple_consumer()
        return self.consumer

    def disConnect(self):
        # self.consumer.close()
        pass

    def getMinuteTime(self):
        return str(time.strftime("%Y%m%d%H%M"))

    def getHour(self):
        return str(time.strftime("%Y%m%d%H"))

    def beginConsumer(self):
        logging.info("begin consumer...")
        start_time = time.time()
        current_hour = self.getHour()
        show_output_file = "%s/transform/transform.%s.log" % (self.output_path, current_hour)
        click_output_file = "%s/click/click.%s.log" % (self.output_path, current_hour)

        trans_fo = open(show_output_file, "w")
        click_fo = open(click_output_file, "w")
        logging.info("write into %s and %s" % (show_output_file, click_output_file))

        self.init_ts = time.time()
        self.pre_ts = time.time()
        for oneLog in self.consumer:
            hour = self.getHour()
            if current_hour != hour:
                current_hour = hour
                trans_fo.close()
                click_fo.close()

                show_output_file = "%s/transform/transform.%s.log" % (self.output_path, current_hour)
                click_output_file = "%s/click/click.%s.log" % (self.output_path, current_hour)

                trans_fo = open(show_output_file, "w")
                click_fo = open(click_output_file, "w")
                logging.info("write into %s and %s" % (show_output_file, click_output_file))
            click_trans = json.loads(oneLog.value)
            url = click_trans['url']
            log_time = click_trans['time']

            if url != 'dsp/transform' and url != 'dsp/click':
                continue
            if not click_trans.has_key('request'):
                continue
            request = click_trans['request']
            if not request.has_key('value'):
                continue
            request_value = request['value']
            if not request_value.has_key('DSPTRANSFORM_LOG') and not request_value.has_key('DSPCLICK_LOG'):
                continue

            if request_value.has_key('DSPTRANSFORM_LOG'):
                log = request_value['DSPTRANSFORM_LOG']
                # spam去重
                if not log.has_key('spam') or log['spam'] != 0:
                    continue
                # 过滤video事件
                if log.has_key('type') and log['type'] != '':
                    continue
                log['log_time'] = log_time
                trans_fo.write("%s\n" % json.dumps(log))
                #logging.info("write one transform data")
            else:
                log = request_value['DSPCLICK_LOG']
                promoted_app = log.get('promoted_app', '')
                if promoted_app == '':
                    continue
                log['log_time'] = log_time
                click_fo.write("%s\n" % json.dumps(log))
                #logging.info("write one click data")

if __name__ == '__main__':
    pk = PyKafka()
    pk.getConnect()
    pk.beginConsumer()
