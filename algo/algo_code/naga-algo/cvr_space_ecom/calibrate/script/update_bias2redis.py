import json
import sys
import redis
import time

if len(sys.argv) < 3:
    exit(1)
day = time.strftime("%Y%m%d%H%M%S", time.localtime())
hour = day[8:10]
plan_theta_dict = json.load(open(sys.argv[1]))

r = redis.Redis(host='ali-cache07.corp.cootek.com', port=16359)
prefix = 'dsp_ocpc2_discount'

with r.pipeline(transaction=False) as p:
    for planid, bias in plan_theta_dict.items():
        dict_name = '%s:%s' % (prefix, planid)
        if hour == '23':
            bias = 1.0 
        p.set(dict_name, bias)
        print dict_name, bias
    try:
        p.execute()
    except Exception as e:
        print "redis set dict_name [%s] error [%s]." % (dict_name, e)
        exit(1)

r2 = redis.Redis(host='ali-cache01.corp.cootek.com', port=17131)
#r2 = redis.Redis(host='ali-cache07.corp.cootek.com', port=16359)

with r2.pipeline(transaction=False) as p:
    for planid, bias in plan_theta_dict.items():
        dict_name = '%s:%s' % (prefix, planid)
        if hour == '23':
            bias = 1.0 
        p.set(dict_name, bias)
        print dict_name, bias
    try:
        p.execute()
    except Exception as e:
        print "redis2 set dict_name [%s] error [%s]." % (dict_name, e)
        exit(1)

