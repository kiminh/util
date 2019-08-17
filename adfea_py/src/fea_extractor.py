#coding:utf-8
"""
File: fea_extractor.py
Author: ling.fang (ling.fang@cootek.cn）
Date: 2019/8/5 12:04:17
"""
from __future__ import print_function
import json
import sys
from fea_record import FeaRecord
from fea_result import FeaResult
from fea_base import FeaArg, FeaBase
from beta0 import Beta0
from direct_fea import DirectFea
from combined_fea import CombinedFea
from date_fea import DateFea
from ip_fea import IPFea

class FeaExtractor(object):
    def __init__(self, fea_conf, 
                  enable_hash, hash_num):
        self.fea_conf = fea_conf
        self.enable_hash = enable_hash
        self.hash_num = hash_num
        self.vec_fea_list = []
        self.direct_fea_list = []

    def init(self):
        self.init_fea_list()
        self.m_fea_record = FeaRecord(self.direct_fea_list)
        self.m_fea_result = FeaResult(self.enable_hash, self.hash_num)

    def init_fea_list(self):
        print("begin parse fea list")
        with open(self.fea_conf) as f_in:
            for raw_line in f_in:
                line_strip = raw_line.strip("\n\r ")
                if line_strip == "" or line_strip[0] == '#':
                    continue

                is_output = True
                if line_strip[0] == '.':
                    line_strip = line_strip[1:]
                    is_output = False

                m_fea_arg = FeaArg()
                if not m_fea_arg.parse_fea_arg(line_strip):
                    print("parse fea arg error")
                    return False

                fea_base = self.get_fea_method(m_fea_arg.method)
                if fea_base == None:
                    print("init fea item %s error!" % m_fea_arg.fea_name)
                    exit()

                fea_base.set_is_output(is_output)
                fea_base.set_fea_arg(m_fea_arg)
                fea_base.init()

                self.vec_fea_list.append(fea_base)
                if m_fea_arg.method != "combined_fea" and \
                    m_fea_arg.method != "anycombined_fea" and m_fea_arg.method != "beta0":
                    self.direct_fea_list.append(m_fea_arg.dep)

    def get_fea_method(self, fea_name):
        if fea_name == "beta0":
            return Beta0()
        elif fea_name == "direct_fea":
            return DirectFea()
        elif fea_name == "combined_fea":
            return CombinedFea()
        elif fea_name == "date_fea":
            return DateFea()
        elif fea_name == "ip_fea":
            return IPFea()
        else:
            return None

    def record_reset(self):
        self.m_fea_record.clear()
        self.m_fea_result.clear()

    def extract_fea(self):
        for fea in self.vec_fea_list:
            fea_item = fea.extract_fea(self.m_fea_record)
            fea_item.slot = fea.get_slot()
            fea_item.is_output = fea.is_output_fea()
            if fea.get_out() != "":
                self.add_item2record(fea.get_out(), fea_item.get_fea_value())
            self.m_fea_result.put_fea(fea.get_fea_name(), fea_item)

    def get_featext_result(self):
        return self.m_fea_result.to_featext_list()

    def add_record(self, data_json):
        self.m_fea_record.add_record(data_json)

    def add_item2record(self, fea_name, fea_value):
        self.m_fea_record.add_item2record(fea_name, fea_value)
