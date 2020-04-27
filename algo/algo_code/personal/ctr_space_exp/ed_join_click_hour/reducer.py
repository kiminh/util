import sys
import json
from collections import defaultdict
from datetime import datetime, date, timedelta

'''
  suffix notes:
  A: join log
  B: unjoin log
  C: join log, yestday ed

'''

yestday = (datetime.now() + timedelta(hours = -2)).strftime("%Y%m%d%H")
if len(sys.argv) == 2:
    temp_dt = datetime.strptime(sys.argv[1], '%Y%m%d%H')
    yestday = (temp_dt + timedelta(hours = -1)).strftime("%Y%m%d%H")

stat_info = defaultdict(lambda: [0]*2) #[clc, act]
dimensions = ['plid']

def calc_stat_info(output_dict):
    dim_lst = []
    
    if 'ed_log' not in output_dict:
        return 
    ed_log = output_dict['ed_log']
    dt = output_dict['ed_time'][0:8]
    for iter in dimensions:
        if iter == "promoted_app" and iter not in ed_log:
            iter = 'bundle_id'
        v = ed_log.get(iter, '-') if iter != 'date' else dt
        dim_lst.append("%s:%s" % (iter, v))
        
    key = '\t'.join(dim_lst)
    clc = 1 if 'click_log' in output_dict else 0
    #stat_info[key] = [stat_info[key][0]+1, stat_info[key][1]+clc]
    stat_info[key] = map(lambda x,y : x+y, stat_info[key], [1, clc])

def merge_info(ed_info, click_info):
    output_dict = {}
    try:
        suffix = "A"
        for iter in ed_info:
            (val_log, src, time) = iter
            val_dict = json.loads(val_log)
            if src == "ed":
                output_dict['ed_log'] = val_dict
                output_dict['ed_time'] = time
                if time[0:10] == yestday:
                    suffix = "C"
            elif src == "request":
                output_dict['request_log'] = val_dict
                output_dict['request_time'] = time
        for iter in click_info:
            (val_log, time) = iter
            val_dict = json.loads(val_log)
            output_dict['click_log'] = val_dict
            output_dict['click_time'] = time
        if 'ed_log' not in output_dict and 'click_log' in output_dict:
            suffix = "B"

        # only request, not output
        if 'request_log' in output_dict \
            and 'ed_log' not in output_dict \
                and 'click_log' not in output_dict:
            #suffix = "D"
            return 
        print "%s\t#%s" % (json.dumps(output_dict), suffix)
        
        #calc_stat_info(output_dict)
    except:
        pass 

key = ''
ed_info = []
click_info = []
for line in sys.stdin:
    flds = line.strip('\n').split('\t')
    if len(flds) != 4:
        continue
    [reqid, input_src, val_log, time] = flds[0:4]
    
    if key != reqid:
        if key != '':
            merge_info(ed_info, click_info)
        key = reqid
        ed_info = []
        click_info = []
    if input_src in ['ed', 'request']:
        ed_info.append((val_log, input_src, time))
    elif input_src == 'click':
        click_info.append((val_log,time))
if key != '':
    merge_info(ed_info, click_info)
