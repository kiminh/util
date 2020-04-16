import sys
import json
import traceback
import random
#import pymurmur
import mmh3
from collections import defaultdict

for line in sys.stdin:
    try:
        flds = line.strip().split('\t')
        print flds[1].strip('\t')
    except Exception as e:
        sys.stderr.write("parse error:%s\n" % e)
        traceback.print_exc()
        pass
