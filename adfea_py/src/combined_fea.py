#coding:utf-8
"""
File: combined_fea.py
Author: ling.fang (ling.fang@cootek.cn）
Date: 2019/8/5 12:04:17
"""
from __future__ import print_function
from fea_base import FeaBase

class CombinedFea(FeaBase):
    def __init__(self):
        super(FeaBase, self).__init__()

    def extract_fea(self, m_record):
        dep = self.m_fea_arg.dep
        dep1, dep2 = dep.split(",")
        dep1 = dep1.strip()
        dep2 = dep2.strip()

        record = m_record.record_dict
        if dep1 not in record or dep2 not in record:
            print ("dep segment (%s or %s) not in record json" % (dep1, dep2))
            exit(1)
        fvalue1 = record[dep1]
        fvalue2 = record[dep2]
        return self.commit_combine_fea(fvalue1, fvalue2)
