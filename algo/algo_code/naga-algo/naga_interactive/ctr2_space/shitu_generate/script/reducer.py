import sys
import json
import traceback
import random
#import pymurmur
import mmh3
from collections import defaultdict

'''
suffix notes
A: for train shitu log
B: statinfo
C: for train shitu instance
'''
class Item(object):
    def __init__(self):
        self.pv = 0
        self.click = 0

def newItem():
    return Item()

stat_info = defaultdict(newItem)
nowkey = ''
now_applist = ''
output_dict = {}
for line in sys.stdin:
    try:
        flds = line.strip().split('\t')
        key = flds[0].split('\002')[0]
        tag = flds[0].split('\002')[1]
        if tag == 'ins':
            feas = flds[1]
            label = int(flds[2])
            weight = int(flds[3])            
            ins = [str(label)]
            for fea in feas.split():
                fea_val = fea.split("\001")[1].strip()
                ins.append('%s:%s' % (mmh3.hash(str(fea_val), signed=False), weight))
            print "%s\t#C" % (" ".join(ins))
        elif tag == 'stat':
            (pv, click)  = flds[1:3]
            item = stat_info[key]
            item.pv += int(pv)
            item.click += int(click)
    except Exception as e:
        sys.stderr.write("parse error:%s\n" % e)
        traceback.print_exc()
        pass

for k,v in stat_info.iteritems():
    if v.pv > 10:
        print "%s\t%s\t%s\t%s#B" % (k, v.pv, v.click, (1.0*v.click)/v.pv)
