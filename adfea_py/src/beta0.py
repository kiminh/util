#coding:utf-8
"""
File: beta0.py
Author: ling.fang (ling.fang@cootek.cn）
Date: 2019/8/5 12:04:17
"""
from fea_base import FeaBase

class Beta0(FeaBase):
    def __init__(self):
        super(FeaBase, self).__init__()

    def extract_fea(self, record):
        return self.commit_single_fea("beta0")
