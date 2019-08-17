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

fea_extractor = None
fea_conf = None
enable_hash = None
hash_num = 5000000

def parse_adfea_config():
    with open(sys.argv[1]) as f_in:
        for raw in f_in:
            line = raw.strip("\n\r")
            print(line)
            if line == "":
                continue
            key, value = line.split("=")
            key_strip = key.strip("\t ")
            value_strip = value.strip("\t ")

            if key_strip == "enable_hash":
                enable_hash = int(value_strip)
            elif key_strip == "fea_conf":
                fea_conf = value_strip
            elif key_strip == "hash_num":
                hash_num = int(value_strip)
            else:
                continue
    print("adfea param:")
    print("enable_hash=%s, hash_num=%s, fea_conf=%s" % (enable_hash, hash_num, fea_conf))
    return enable_hash, hash_num, fea_conf

#enable_hash, hash_num, fea_conf = parse_adfea_config()
#extractor_param = {"fea_conf": fea_conf, 
#                   "enable_hash": enable_hash, 
#                   "hash_num": hash_num}
#print(extractor_param)
#fea_extractor = FeaExtractor(**extractor_param)
#fea_extractor.init()

for raw_line in sys.stdin:
    #try:
    #    line_json = json.loads(raw_line.strip("\n\r "))
    #except Exception as e:
    #    print("parse json data error [%s]." % e)
    #    continue
    #line_json = cjson.decode(raw_line.strip("\n\r "))
    #label = line_json['label']
    print("%s" % 1)
    #fea_extractor.record_reset()
    #fea_extractor.add_record(line_json)
    #fea_extractor.extract_fea()
    #featext_result = fea_extractor.get_featext_result()
    #featext_str = [ str(item) for item in featext_result ]
    #print("%s %s" % (label, " ".join(featext_str)))
