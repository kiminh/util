import json
import sys
import redis
import time

if len(sys.argv) < 3:
    exit(1)
day = time.strftime("%Y%m%d%H%M%S", time.localtime())
hour = day[8:10]
theta_dict = json.load(open(sys.argv[1]))
plan_theta_dict = json.load(open(sys.argv[2]))

prefix = 'dsp_ocpc_discount_exp'
r = redis.Redis(host='cache03.corp.cootek.com', port=19832)
r1 = redis.Redis(host='ali-cache01.corp.cootek.com', port=17131)

with r.pipeline(transaction=False) as p, r1.pipeline(transaction=False) as p1:
    for raw in open(sys.argv[3]):
        raw = raw.strip().split('\t')
        app = raw[0]
        felds = raw[1].split('#')
        cali_val = float(felds[0])
        dict_name = '%s:%s' % (prefix, app)
        if app in theta_dict:
            cali_val = float(theta_dict[app])
        p.set(dict_name, float(cali_val))
        p1.set(dict_name, float(cali_val))
        print dict_name, float(cali_val)

    for planid, bias in plan_theta_dict.items():
        dict_name = '%s:%s' % (prefix, planid)
        if hour == '23':
            bias = 1.0
        p.set(dict_name, bias)
        p1.set(dict_name, bias)
        print dict_name, bias
    try:
        p.execute()
        p1.execute()
    except Exception as e:
        print "redis set dict_name [%s] error [%s]." % (dict_name, e)
        exit(1)

