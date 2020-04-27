import sys
import json
import traceback
import random
#import pymurmur
import mmh3
import math
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

model_dict = {}
with open(sys.argv[1]) as f_in:
    for raw in f_in:
        line = raw.strip().split(" ")
        if len(line) < 2 or line[1] == "0":
            continue
        model_dict[line[0]] = float(line[1])

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
            fea = flds[1]
            label = int(flds[2])
            weight = int(flds[3]) 
            ins = [str(label)]
            s = 0
            for fea in fea.split():
                fea_val = fea.split("\001")[1].strip()
                fea_hash = "%s" % mmh3.hash(str(fea_val), signed=False)
                #ins.append("%s:%s" % (fea_hash, weight)
                w = model_dict[fea_hash] if fea_hash in model_dict else 0
                s += w
                print "%s %s %s" % (fea, fea_hash, w)
            print 1.0/(1.0+math.exp(-s)), s
            #print "%s" % (" ".join(ins))
        elif tag == 'stat':
            (pv, click)  = flds[1:3]
            item = stat_info[key]
            item.pv += int(pv)
            item.click += int(click)
    except Exception as e:
        sys.stderr.write("parse error:%s\n" % e)
        traceback.print_exc()
        pass

#for k,v in stat_info.iteritems():
#    if v.pv > 10:
#        print "%s\t%s\t%s\t%s#B" % (k, v.pv, v.click, (1.0*v.click)/v.pv)
