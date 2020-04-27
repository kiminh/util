import sys
import json
import traceback
import random
#import pymurmur
from collections import defaultdict

for line in sys.stdin:
    if random.random() < 0.01:
        flds = line.strip().split('\t')
        data_json = flds[1]
        print data_json
