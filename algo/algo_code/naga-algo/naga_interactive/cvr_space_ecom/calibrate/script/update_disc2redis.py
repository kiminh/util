import json
import sys
import redis
import time

if len(sys.argv) < 2:
    exit(1)
day = time.strftime("%Y%m%d%H%M%S", time.localtime())
hour = day[8:10]
plan_cali_dict = json.load(open(sys.argv[1]))

r = redis.Redis(host='ali-cache07.corp.cootek.com', port=16359)
prefix = 'dsp_interactive_ocpc2_discount'
r1 = redis.Redis(host='ali-cache01.corp.cootek.com', port=17131)

with r.pipeline(transaction=False) as p, r1.pipeline(transaction=False) as p1:
    """
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
    """
    for planid, discount in plan_cali_dict.items():
        dict_name = '%s:%s' % (prefix, planid)
        p.set(dict_name, discount)
        p1.set(dict_name, discount)
        print dict_name, discount
    try:
        p.execute()
        p1.execute()
    except Exception as e:
        print "redis set dict_name [%s] error [%s]." % (dict_name, e)
        exit(1)

