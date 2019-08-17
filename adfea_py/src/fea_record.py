#coding:utf-8
"""
File: fea_record.py
Author: ling.fang (ling.fang@cootek.cn）
Date: 2019/8/5 12:04:17
"""
class FeaRecord(object):
    def __init__(self, direct_fea_list):
        self.direct_fea_list = direct_fea_list
        self.record_dict = {}

    def clear(self):
        self.record_dict = {}

    def get_record(self):
        return self.record_dict

    def add_record(self, data_json):
        for fea in self.direct_fea_list:
            if fea not in data_json:
                print("fea_name %s is not in dataset" % fea)
                exit()
            self.record_dict[fea] = data_json[fea]
    
    def add_item2record(self, fea_name, fea_value):
        if fea_name in self.record_dict:
            print("fea_name exist in record")
            exit()
        self.record_dict[fea_name] = fea_value
