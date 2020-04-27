import json
import sys
import redis
import time

if len(sys.argv) < 2:
    exit(1)
day = time.strftime("%Y%m%d%H%M%S", time.localtime())
hour = day[8:10]
cali_dict = json.load(open(sys.argv[1]))
print cali_dict
r = redis.Redis(host='cache03.corp.cootek.com', port=19832)
r1 = redis.Redis(host='ali-cache01.corp.cootek.com', port=17131)
prefix = 'dsp_ctr_discount'


with r.pipeline(transaction=False) as p, r1.pipeline(transaction=False) as p1:
    for ad_style, cali in cali_dict.items():
        dict_name = '%s:%s' % (prefix, ad_style)
        p1.set(dict_name, cali)
        p.set(dict_name, cali)
        print dict_name, cali
    try:
        p.execute()
        p1.execute()
    except Exception as e:
        print "redis set dict_name [%s] error [%s]." % (dict_name, e)
        exit(1)

