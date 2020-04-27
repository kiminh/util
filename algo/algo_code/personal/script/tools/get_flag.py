#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2018  chubao.cn, Inc. All Rights Reserved
#

# @File    : push_data2region_redis.py.py
# @Author  : gaoyuan.shao (gaoyuan.shao@cootek.cn)
# @Time    : 2018/12/6 下午3:29

import sys
import redis

REDIS_CONFIG = {
    'eu': {
        'host': 'eu-codis02.eucentral.cootek.com',
        'port': '17100'
    },
    'usa': {
        'host': 'redis02.uscasv2.cootek.com',
        'port': '17999'
    },
    'ap': {
        'host': 'ap-cache01.southeastdcw.cootek.com',
        'port': '18029'
    },
}


def get_redis_context(region):
    if not region:
        return None
    pool = redis.ConnectionPool(host=REDIS_CONFIG[region]['host'],
                                port=REDIS_CONFIG[region]['port'], db=0)
    r = redis.StrictRedis(connection_pool=pool)
    return r.pipeline(transaction=False)


def judge_data_flag(key, value):
    region_list = ['usa']
    for region in region_list:
        redis_pipeline = get_redis_context(region)
        try:
            redis_pipeline.hgetall(key)
            result = redis_pipeline.execute()
            tag = 'False'
            if value in result[0]:
                tag = result[0][value]
            print tag
        except Exception as e:
            print e

if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.stderr.write("usage: python %s key_value!\n" % (sys.argv[0]))
        sys.exit(-1)
    kv = sys.argv[1]
    flds = kv.split('##')
    if len(flds) == 2:
        k = flds[0] 
        v = flds[1]
        judge_data_flag(k, v)
