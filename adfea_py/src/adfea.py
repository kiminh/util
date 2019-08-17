#coding:utf-8
"""
File: adfea.py
Author: ling.fang (ling.fang@cootek.cn）
Date: 2019/8/5 12:04:17
"""
from __future__ import print_function
import sys
import json
#import cjson
import time

from fea_extractor import FeaExtractor

class AdFea(object):
    def __init__(self, train_file, train_ins, adfea_config):
        self.fea_extractor = None
        self.adfea_config = adfea_config
        self.fea_conf = None
        self.train_file = train_file
        self.train_ins = train_ins
        self.enable_hash = None
        self.hash_num = 5000000
        self.with_splotid_feaval = False

    def parse_adfea_config(self):
        with open(self.adfea_config) as f_in:
            for raw in f_in:
                line = raw.strip("\n\r")
                if line == "":
                    continue
                key, value = line.split("=")
                key_strip = key.strip("\t ")
                value_strip = value.strip("\t ")

                if key_strip == "enable_hash":
                    self.enable_hash = int(value_strip)
                elif key_strip == "fea_conf":
                    self.fea_conf = value_strip
                elif key_strip == "train_ins":
                    self.train_ins = value_strip
                elif key_strip == "train_file":
                    self.train_file = value_strip
                elif key_strip == "hash_num":
                    self.hash_num = int(value_strip)
                else:
                    continue
        print("adfea param:")
        print("enable_hash=%s, hash_num=%s, fea_conf=%s" % (self.enable_hash, self.hash_num, self.fea_conf))
        return True

    def adfea_init(self):
        self.parse_adfea_config()
        extractor_param = {"fea_conf": self.fea_conf, 
                           "enable_hash": self.enable_hash, 
                           "hash_num": self.hash_num}
        self.fea_extractor = FeaExtractor(**extractor_param)
        self.fea_extractor.init()

    def dump_featext(self, f_out, featext_resut, label):
        f_out.write("%s " % label)
        for featext in featext_resut:
            f_out.write("%s " % (featext))
        f_out.write("\n")

    def adfea_run(self):
        start_time = time.time()
        with open(self.train_file) as f_in, open(self.train_ins, 'w') as f_out:
            for i, raw_line in enumerate(f_in):
                try:
                    line_json = json.loads(raw_line.strip("\n\r "))
                except Exception as e:
                    print("parse json data error [%s]." % e)
                    continue
                #line_json = cjson.decode(raw_line.strip("\n\r "))
                label = line_json['label']
                self.fea_extractor.record_reset()
                self.fea_extractor.add_record(line_json)
                self.fea_extractor.extract_fea()
                featext_resut = self.fea_extractor.get_featext_result()
                self.dump_featext(f_out, featext_resut, label)
                if i % 10000 == 0:
                    print(i)
        end_time = time.time()
        print("eslapse %s sec." % (end_time - start_time))
#for test
if __name__ == '__main__':
    if len(sys.argv) < 4:
        print("Usage: python adfea.py shitu_log shitu_ins config")
        exit(1)

    adfea = AdFea(sys.argv[1], sys.argv[2], sys.argv[3])
    adfea.adfea_init()
    adfea.adfea_run()
