import sys
import json
import traceback
from collections import defaultdict
from datetime import datetime, date
sys.path.append('script')
import time
import random

for line in sys.stdin:
    try:
        line = line.strip('\r\n')
        try:
            ld = json.loads(line)
            if not isinstance(ld['ed_log'], dict):
                ed_log = json.loads(ld['ed_log'])
            else:
                ed_log = ld['ed_log']
        except Exception as e:
            sys.stderr.write("parse log failed, err_msg[%s]\n" % (e))
            traceback.print_exc()
            continue
        ad_style = ed_log.get('ad_style', '')
        if ad_style == '' or ad_style == '8':
            continue
        ed_log['is_clk'] = ld['is_click'] #1 if 'label' in ld else 0  #ld['is_click']
        if 'reqprt' not in ed_log:
            continue 
        randint = random.randint(0, sys.maxsize)
        print "%s\t%s" % (randint, json.dumps(ed_log))
    except Exception as e:
        sys.stderr.write("parse log failed, err_msg[%s]\n" % (e))
        traceback.print_exc()
        pass
