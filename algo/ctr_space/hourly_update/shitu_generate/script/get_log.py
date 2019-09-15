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
output_path = "/home/ling.fang/ctr_space/hourly_update/log/"

def fun_timer():
    global timer
    global ed_fo
    global click_fo
    global init_hour
    cur_hour = getHour() 
    if cur_hour != init_hour:
        logging.info("%s ---> %s" % (init_hour, cur_hour))
        init_hour = cur_hour

        ed_output_file = "%s/ed/ed.%s.log" % (output_path, init_hour)
        click_output_file = "%s/click/click.%s.log" % (output_path, init_hour)
        if not os.path.exists(ed_output_file):
            os.system("touch {}".format(ed_output_file))
        if not os.path.exists(click_output_file):
            os.system("touch {}".format(click_output_file))

        init_hour = cur_hour
 
    timer = threading.Timer(1, fun_timer)
    timer.start()

timer = threading.Timer(1,fun_timer)
timer.start()

class PyKafka:
    output_path = "/home/ling.fang/ctr_space/hourly_update/log/"
    log_prefix = 'kafka_edclick'
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
            consumer_group="ctronline_20190812",
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

    def init_file_fo(self):
        current_hour = self.getHour()
        show_output_file = "%s/ed/ed.%s.log" % (self.output_path, current_hour)
        click_output_file = "%s/click/click.%s.log" % (self.output_path, current_hour)
        self.ed_fo = open(ed_output_file, "w")
        self.click_fo = open(click_output_file, 'w')

    def getMinuteTime(self):
        return str(time.strftime("%Y%m%d%H%M"))

    def getHour(self):
        return str(time.strftime("%Y%m%d%H"))

    def beginConsumer(self):
        logging.info("begin consumer...")
        start_time = time.time()
        current_hour = self.getHour()
        show_output_file = "%s/ed/ed.%s.log" % (self.output_path, current_hour)
        click_output_file = "%s/click/click.%s.log" % (self.output_path, current_hour)

        show_fo = open(show_output_file, "w")
        click_fo = open(click_output_file, "w")
        logging.info("write into %s and %s" % (show_output_file, click_output_file))

        self.init_ts = time.time()
        self.pre_ts = time.time()
        for oneLog in self.consumer:
            hour = self.getHour()
            if current_hour != hour:
                current_hour = hour
                show_fo.close()
                click_fo.close()

                show_output_file = "%s/ed/ed.%s.log" % (self.output_path, current_hour)
                click_output_file = "%s/click/click.%s.log" % (self.output_path, current_hour)

                show_fo = open(show_output_file, "w")
                click_fo = open(click_output_file, "w")
                logging.info("write into %s and %s" % (show_output_file, click_output_file))
            edclick = json.loads(oneLog.value)
            url = edclick['url']
            log_time = edclick['time']

            if url != 'dsp/ed' and url != 'dsp/click':
                continue
            if not edclick.has_key('request'):
                continue
            request = edclick['request']
            if not request.has_key('value'):
                continue
            request_value = request['value']
            if not request_value.has_key('DSPED_LOG') and not request_value.has_key('DSPCLICK_LOG'):
                continue

            if request_value.has_key('DSPED_LOG'):
                log = request_value['DSPED_LOG']
                # spam去重
                if not log.has_key('spam') or log['spam'] != 0:
                    continue
                # 过滤video事件
                if log.has_key('type') and log['type'] != '':
                    continue
                log['log_time'] = log_time
                show_fo.write("%s\n" % json.dumps(log))
                #logging.info("write one ed data")
            else:
                log = request_value['DSPCLICK_LOG']
                log['log_time'] = log_time
                click_fo.write("%s\n" % json.dumps(log))
                #logging.info("write one click data")

if __name__ == '__main__':
    pk = PyKafka()
    pk.getConnect()
    pk.beginConsumer()
