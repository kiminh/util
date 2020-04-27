#coding:utf-8
"""
File: model_push_util.py
Author: ling.fang (ling.fang@cootek.cn）
Date: 2019/8/15 5:53:17
"""
import meepo
import os
import sys 
import json
import math
import time
import redis
from datetime import datetime, date, timedelta

class ModelPushUtil:
    def __init__(self, new_version, model_push_config):
        self.config_file = model_push_config
        self.new_version = int(new_version)
        self.meepo_path = "ad"
        self.meepo_writer = None
        self.meepo_task = None
        self.r_pipeline = None

        self.model_file = None
        self.model_meepo_dictname = None
        self.fealist_meepo_dictname = None
        self.fea_list = None
        self.key_prefix = None
        self.redis_host = None
        self.redis_key = None
        self.local_addr = None
        self.remote_addr = None

        self.init()
        
    def parse_config(self):
        with open(self.config_file) as f_in:
            for raw_line in f_in:
                line = raw_line.strip("\n\r")
                if line == "" or line[0] == '#': 
                    continue
                
                key, value = line.split("=")
                key_strip = key.strip("\t ")
                value_strip = value.strip("\t ")
                
                if key_strip == "model_file":
                    self.model_file = value_strip
                elif key_strip == "fea_list":
                    self.fea_list = value_strip
                elif key_strip == "redis_host":
                    self.redis_host = value_strip
                elif key_strip == "redis_port":
                    self.redis_port = value_strip
                elif key_strip == "redis_key":
                    self.redis_key = value_strip
                elif key_strip == "local_addr":
                    self.local_addr = value_strip
                elif key_strip == "remote_addr":
                    self.remote_addr = value_strip
                elif key_strip == "model_meepo_dictname":
                    self.model_meepo_dictname = value_strip
                    print self.model_meepo_dictname
                elif key_strip == "fealist_meepo_dictname":
                    self.fealist_meepo_dictname = value_strip 
                else:
                    continue

        if self.local_addr is not None:
            str_sp = self.local_addr.split(":")
            if len(str_sp) == 2:
                local_host = str_sp[0]
                local_port = str_sp[1]
                self.local_addr = {"host": local_host,
                                   "port": int(local_port)}
            else:
                print("local address format error!")
                exit(1)
        else:
            print("must set the local address!!")
            exit(1)

        remote_hosts = []
        if self.remote_addr is not None:
            machine_list = self.remote_addr.strip("\n\r ").split(",")
            for ml in machine_list:
                ml_sp = ml.strip().split(":")
                if len(ml_sp) == 2:
                    remote_hosts.append({"host": ml_sp[0], "port": int(ml_sp[1])})
                else:
                    print("remote address format error!")
                    exit(1)
        else:
            print("must set the remote address!!")
            exit(1)
        self.remote_addr = remote_hosts
    
    def init(self):
        self.parse_config()
        self.init_redis_client()
        self.meepo_writer = self.init_meepo_writer()

    def init_redis_client(self):
        pool = redis.ConnectionPool(host=self.redis_host,
                                    port=self.redis_port, db=0)
        r = redis.StrictRedis(connection_pool=pool)
        self.r_pipeline = r.pipeline(transaction=False)
        #add test redis
        #test_pool = redis.ConnectionPool(host="111.231.79.146",
        #                                 port="19000", db=0)
        #test_r = redis.StrictRedis(connection_pool=test_pool)
        #self.test_r_pipeline = test_r.pipeline(transaction=False)

    def init_meepo_task(self, data_type):
        meepo_name = "%s" % (data_type)
        print meepo_name, len(meepo_name)
        meepo_task = meepo.Task(meepo.MEEPO_ENTRY_TYPE_DOUBLE, 
                                self.meepo_path, meepo_name)
        meepo_task.version(self.new_version)
        meepo_task.description("naga ctr model")
        meepo_task.slots(500000)
        meepo_task.buf_size(1024*1024)
        return meepo_task

    def init_meepo_writer(self):
        try:
            meepo_writer = meepo.Writer(self.remote_addr, 
                                        self.local_addr)
            meepo_writer.set_send_buffer(4 * 1024)
            meepo_writer.set_send_timeout(20000)
            if meepo_writer.initialize():
               return meepo_writer
            else:
               return None
        except Exception as e:
            print("meepo write create error! [%s]" % e)
            exit(1)

    def parse_model_file(self):
        for raw_line in open(self.model_file):
            flds = raw_line.strip("\n\r ").split(" ")
            if len(flds) > 1:
                yield flds[0], flds[1]
            else:
                continue
    
    def upload(self, task):
        try:
            self.meepo_writer.begin(task)
            while task.if_can_commit() == False:
                try:
                    self.meepo_writer.write(task)
                    print("commit percent:%f" % (task.precent()))
                except Exception as e:
                    time.sleep(1)
                    print(str(e))
            self.meepo_writer.commit(task)
            self.meepo_writer.set_current(task.group(), task.name(), task.version())
        except Exception as e:
            print("update meepo occured exception: %s" % str(e))
            exit(1)
 
    def push_model(self):
        meepo_task = self.init_meepo_task(self.model_meepo_dictname)
        for key, value in self.parse_model_file():
            #if key == "1430112322":
            #    continue
            meepo_task.write_number(key, float(value))
        #meepo_task.write_number("1430112322", -0.4259)
        self.upload(meepo_task)
        self.delete_old_version(self.model_meepo_dictname)

    def push_fea_list(self):
        meepo_task = self.init_meepo_task(self.fealist_meepo_dictname)
        for key, value in self.parse_fea_list():
            meepo_task.write_string(str(key), value)
        self.upload(meepo_task)
        
    def parse_fea_list(self):
        for raw_line in open(self.fea_list):
            line_sp = raw_line.strip("\n\r ").split("\t")
            fea_name = line_sp[0]
            slot = line_sp[1]
            yield fea_name, slot
    
    def delete_old_version(self, data_type):
        try:
            meepo_name = "%s" % (data_type)
            sanpshot_version_path = "./model_version/%s.ver" % (meepo_name)
            lines = []
            if os.path.exists(sanpshot_version_path):
                #stable_dt = (date.today() + timedelta(days = -1)).strftime("%Y%m%d%H%M%S")
                stable_dt = (datetime.now() + timedelta(hours = -2)).strftime("%Y%m%d%H%S")
                print stable_dt
                with open(sanpshot_version_path, 'r') as f_r:
                    lines = f_r.readlines()
            cur_ver = lines[-1]
            with open(sanpshot_version_path, 'w') as f_w:
                for line in lines:
                    ver = line.strip('\n')
                    dt_str = int(ver)
                    if dt_str < int(stable_dt+'00') and dt_str != self.new_version and dt_str != int(cur_ver):
                        self.meepo_writer.remove_snapshot("ad", meepo_name, int(ver))
                        self.meepo_writer.remove_snapshot("ad", 'ctr_fea_list', int(ver))
                        continue
                    f_w.write("%s\n"%(ver))
                # add latest version 
                f_w.write("%s\n"%(self.new_version))
        except Exception as e:
            print("delete meepo[%s] occured exception: %s" % 
                (str(self.new_version), str(e)))
            return

    def update_redis(self):
        try:
            print('set redis_key[%s] redis_value[%s]' % (self.redis_key, self.new_version))
            self.r_pipeline.set(self.redis_key, self.new_version)
            self.r_pipeline.execute()
            #self.test_r_pipeline.set(self.redis_key, self.new_version)
            #self.test_r_pipeline.execute()
        except Exception as e:
            print(e)
            exit(1)

    def run(self):
        self.push_model()
        self.push_fea_list()
        self.update_redis()
        #self.delete_old_version(self.model_meepo_dictname)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python model_push_util.py ver conf")
        exit(1)

    model_push_util = ModelPushUtil(sys.argv[1], sys.argv[2])
    model_push_util.run()
