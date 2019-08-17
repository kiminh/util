#coding:utf-8
"""
File: direct_fea.py
Author: ling.fang (ling.fang@cootek.cn）
Date: 2019/8/5 12:04:17
"""
from fea_base import FeaBase

class DirectFea(FeaBase):
    def __init__(self):
        super(FeaBase, self).__init__()

    def extract_fea(self, m_record):
        dep = self.m_fea_arg.dep
        record = m_record.record_dict
        if dep not in record:
            print("dep segment not in record json")
            exit(1)
        fvalue = record[dep]
        return self.commit_single_fea(fvalue)
