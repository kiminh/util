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
        if tag in ['ins']:
            #add label
            ins = [flds[2]]
            #add fea index(use murmur hash)
            for fea in flds[1].split():
                fea_sp = fea.split('\001')
                fea_val = fea_sp[1].strip()
                s = "%s:%s:%s" % (fea_sp[0], fea_val, str(mmh3.hash(str(fea_val), signed=False)))
                print s
                #ins.append("%s:%s:%s" % (fea_sp[0], fea_val, str(mmh3.hash(str(fea_val), signed=False))))
            #print "%s" % (" ".join(ins))
    except Exception as e:
        sys.stderr.write("parse error:%s\n" % e)
        traceback.print_exc()
        pass
