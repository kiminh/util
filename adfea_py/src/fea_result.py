#coding:utf-8
"""
File: fea_result.py
Author: ling.fang (ling.fang@cootek.cn）
Date: 2019/8/5 12:04:17
"""

class Fea_t(object):
    def __init__(self):
        #feature string
        self.m_fea_value = []
        self.slot = -1
        #after hash32
        self.m_sign = []

        self.is_output = True

    def clear(self):
        self.m_fea_value = []
        self.m_sign = []

    def get_fea_value(self):
        """
        提供给一条样本只有一个值的特征
        返回加工过后的特征值
        """
        return self.m_fea_value[0]
    
    def get_fea_sign(self):
        return self.m_sign[0]

    def commit_fea(self, fvalue, fsign):
        self.m_fea_value.append(fvalue)
        self.m_sign.append(fsign)

    def commit_fea_sign(self, fsign):
        self.m_sign.append(fsign)

class FeaResult(object):
    def __init__(self, enable_hash, hash_num):
        #<fea_name, fea_t>
        #self.m_fea_out = OrderedDict()
        #按fea list输出抽完fea的特征
        self.m_fea_out = []
        self.fea_name_list = []

        self.enable_hash = enable_hash
        self.hash_num = hash_num

    def clear(self):
        self.m_fea_out = []
        self.fea_name_list = []

    def put_fea(self, fea_name, m_fea_t):
        self.m_fea_out.append(m_fea_t)
        self.fea_name_list.append(fea_name)
        #self.m_fea_out[fea_name] = m_fea_t

    def fea_at(self, fea_name):
        return self.m_fea_out[fea_name_list.index(fea_name)]

    def to_featext_list(self):
        fea_out = []
        for fea_name, m_fea_t in zip(self.fea_name_list, self.m_fea_out):
            m_fea_value = m_fea_t.m_fea_value
            is_output = m_fea_t.is_output
            m_sign = m_fea_t.m_sign

            if is_output:
                for fvalue in m_sign:
                    if self.enable_hash:
                        #fea_out.append("%s:%s" % (fea_name, int(fvalue) % self.hash_num))
                        fea_out.append(int(fvalue) % self.hash_num)
                    else:
                        fea_out.append(int(fvalue))
                        #fea_out.append("%s:%s" % (fea_name, int(fvalue)))
        return fea_out
