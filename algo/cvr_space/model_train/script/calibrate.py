#!/usr/bin/python
# coding=utf8
import os
import sys
import json
import time
from collections import defaultdict

stat_dict = defaultdict(lambda:[0.0]*2)
for line in sys.stdin:
    try:
        raw_json = json.loads(line.strip('\r\n')) 
        promoted_app = raw_json['click_log']['promoted_app']
        trans_log = raw_json['trans_log']
        if isinstance(trans_log, dict):
            stat_dict[promoted_app][1] += 1
        stat_dict[promoted_app][0] += 1

    except Exception  as e:
        sys.stderr.write("parse line fail[%s]\n" % (e.message))
        sys.exit(-1)

for promoted_app, value in stat_dict.items():
    click = value[0]
    trans = value[1]
    if click > 500 or trans > 10:
        print "%s\t1#0.1" % (promoted_app)
