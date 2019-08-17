#coding:utf-8
"""
File: fea_base.py
Author: ling.fang (ling.fang@cootek.cn）
Date: 2019/8/5 12:04:17
"""
from __future__ import print_function
from cityhash import CityHash32
#import hashlib
from fea_result import Fea_t
import json
#import mmh3

class FeaArg(object):
    def __init__(self):
        self.fea_name = ""
        self.method = ""
        self.slot = ""
        self.arg = ""
        self.dep = ""
        self.out = ""
        self.is_output = ""

    def parse_fea_arg(self, line):
        line_sp = line.strip("\n\r").split(";")
        if len(line_sp) < 3:
            print("Fea conf is error, must large 3")
            return False
        #parse fea name
        item_sp = line_sp[0].split("=")
        if item_sp[0] == "fea_name":
            self.fea_name = item_sp[1].strip()
            self.is_output = True
        elif item_sp[0] == ".fea_name":
            self.fea_name = item_sp[1].strip()
            self.is_output = False
        else:
            return False
        #parse method
        item_sp = line_sp[1].split("=")
        if item_sp[0] == "method":
            self.method = item_sp[1].strip()
        else:
            return False
        #parse slot id
        item_sp = line_sp[2].split("=")
        if item_sp[0] == "slot":
            self.slot = item_sp[1].strip()
        else:
            return False
        #parse dep and args
        for index in range(3, len(line_sp)):
            item_sp = line_sp[index].split("=")
            if item_sp[0] == "dep":
                self.dep = item_sp[1].strip()
            elif item_sp[0] == "arg":
                self.arg = item_sp[1].strip()
            elif item_sp[0] == "out":
                self.out = item_sp[1].strip()
            else:
                return False
        return True


class FeaBase(object):
    def __init__(self):
        self.m_fea_arg = None
        self.is_output = True
        self.UNK_FEA = 'unk'
        #process

    def set_fea_arg(self, fea_arg):
        self.m_fea_arg = fea_arg

    def check_arg(self):
        return True

    def init(self):
        return True

    def extract_fea(self, m_record):
        return True

    def get_fea_name(self):
        return self.m_fea_arg.fea_name

    def is_output_fea(self):
        return self.is_output

    def set_is_output(self, value):
        self.is_output = value

    def get_slot(self):
        return self.m_fea_arg.slot
    
    def get_out(self):
        return self.m_fea_arg.out

    def commit_single_fea(self, fvalue):
        if self.get_fea_name() == "Beta0":
            feature_key = fvalue
        else:
            feature_key = "%s:%s" % (self.m_fea_arg.slot, fvalue)
        sign = self.hashstr(feature_key)
        fea_item = Fea_t()
        #fea_item.commit_fea_sign(sign)
        fea_item.commit_fea(fvalue, sign)
        return fea_item

    def commit_combine_fea(self, fvalue1, fvalue2):
        feature_key = "%s:%s_%s" % (self.m_fea_arg.slot, fvalue1, fvalue2) 
        fea_item = Fea_t()
        sign = self.hashstr(feature_key)
        fea_item.commit_fea_sign(sign)
        return fea_item
        
    def commit_anycombine_fea(self):
        return True

    def hashstr(self, str):
        return 0
        #return mmh3.hash(str, signed=False)
        #return CityHash32(str)
