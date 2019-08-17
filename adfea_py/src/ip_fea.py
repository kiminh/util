#coding:utf-8
"""
File: ip_fea.py
Author: ling.fang (ling.fang@cootek.cn）
Date: 2019/8/5 12:04:17
"""
from fea_base import FeaBase

class IPFea(FeaBase):
    def __init__(self):
        super(FeaBase, self).__init__()

    def addr2bin(self, addr): 
        if addr == "none":
            return 0
        try:
            items = [int(x) for x in addr.split(".")]  
        except:
            return 0
        return sum([items[i] << [24, 16, 8, 0][i] for i in range(4)])

    def ip_function(self, ip, l):
        bin_ip = self.addr2bin(ip.strip("\r\n "))
        if bin_ip == 0:
            return -1
        try:
            ip_ = int(bin_ip >> l)
        except:
            return -1
        return (ip_)

    def extract_fea(self, m_record):
        m_bit = int(self.m_fea_arg.arg)
        dep = self.m_fea_arg.dep
        record = m_record.record_dict
        if dep not in record:
            print ("dep segment not in record json")
            exit(1)
        fvalue = self.ip_function(record[dep], m_bit)
        return self.commit_single_fea(fvalue)
