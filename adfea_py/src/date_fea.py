#coding:utf-8
"""
File: data_fea.py
Author: ling.fang (ling.fang@cootek.cn）
Date: 2019/8/5 12:04:17
"""
from fea_base import FeaBase
import time
import datetime

class DateFea(FeaBase):
    def __init__(self):
        super(FeaBase, self).__init__()

    def extract_fea(self, m_record):
        arg = self.m_fea_arg.arg
        dep = self.m_fea_arg.dep

        record = m_record.record_dict
        if dep not in record:
            print("dep segment not in record json")
            exit(1)
        timestamp = record[dep]
        timearray = time.localtime(float(timestamp))
        day = time.strftime("%Y%m%d%H", timearray)
        week = datetime.datetime.strptime(day, "%Y%m%d%H").strftime("%w")

        if arg == 'hour':
            fvalue = day[8:10]
        elif arg == 'week':
            fvalue = week
        elif arg == 'is_weekend':
            if week in ['0', '6']:
                fvalue = "1"
            else:
                fvalue = "0"
        return self.commit_single_fea(fvalue)
