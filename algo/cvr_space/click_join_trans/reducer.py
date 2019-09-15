import sys
import json
from collections import defaultdict
from datetime import datetime, date, timedelta

'''
  suffix notes:
  A: temp log
  B: stable log
  C: stable log, stable date oneday ago, for retention_data
  D: unjoin appflyers activation log without click
'''

stable_dt = (date.today() + timedelta(days = -7)).strftime("%Y%m%d")
stable_dt_oneday_ago = (date.today() + timedelta(days = -8)).strftime("%Y%m%d")
if len(sys.argv) == 2:
    temp_dt = datetime.strptime(sys.argv[1], '%Y%m%d')
    stable_dt = (temp_dt + timedelta(days = -6)).strftime("%Y%m%d")
    stable_dt_oneday_ago = (temp_dt + timedelta(days = -7)).strftime("%Y%m%d")

stat_info = defaultdict(lambda: [0]*2) #[clc, act]
dimensions = ['date', 'plid', 'appid', 'promoted_app', 'advid', 'campaignid', 'planid']

def calc_stat_info(output_dict):
    dim_lst = []
    
    if 'click_log' not in output_dict:
        return 
    clc_log = output_dict['click_log']
    dt = output_dict['click_time'][0:8]
    for iter in dimensions:
        if iter == "promoted_app" and iter not in clc_log:
            iter = 'bundle_id'
        v = clc_log.get(iter, '-') if iter != 'date' else dt
        dim_lst.append("%s:%s" % (iter, v))
        
    key = '\t'.join(dim_lst)
    act = 1 if 'trans_log' in output_dict else 0
    if dt == stable_dt_oneday_ago:
        return
    suffix = 'E' if dt == stable_dt else 'D'
    stat_info[(key, suffix)] = map(lambda x,y : x+y, stat_info[(key, suffix)], [1, act])

def judge_retention_event(open_event_list, trans_time):
    '''
    calculate retention event by app open time
    arg: open_even_list, app open time
    arg: trans_time, transform time
    return: retention_time and af_postback_retention_time
    '''
    trans_dt = datetime.strptime(trans_time[0:8], "%Y%m%d")
    retention_dt = (trans_dt + timedelta(days = 1)).strftime("%Y%m%d")
    for tm,af_tm in open_event_list:
        if retention_dt == tm[0:8]:
            return (tm, af_tm)
    return False

def select_valid_click(output_dict, time, tran_time, spam):
    if 'click_log' not in output_dict:
        return True
    if spam != '0':
        return False
    clc_tm = output_dict.get('click_time', '')
    if not tran_time:
        if time < clc_tm:
            return True
    else:
        if time <= tran_time and (clc_tm > tran_time or clc_tm < time):
            return True
        
    return False

def merge_info(clc_info, trans_info):
    output_dict = {'trans_event': {}}
    et_oldest_trans_time = {} #22001010000000
    et_trans_time = {}
    try:
        for iter in trans_info:
            (val_log, time) = iter
            val_dict = json.loads(val_log)
            event_type = val_dict.get('event_type', '')
            
            if event_type not in et_oldest_trans_time:
                output_dict['trans_event'][event_type] = [val_dict, time, event_type]
                et_oldest_trans_time[event_type] = int(time)    
                et_trans_time[event_type] = time
            elif et_oldest_trans_time[event_type] > int(time):
                output_dict['trans_event'][event_type] = [val_dict, time, event_type]
                et_oldest_trans_time[event_type] = int(time)
                et_trans_time[event_type] = time

        oldest_click_time = 22001010000000
        suffix = "A"
        for iter in clc_info:
            (val_log, time, src, spam) = iter
            val_dict = json.loads(val_log)
            if src == "click":
                if spam == 0 or 'click_log' not in output_dict:
                #if select_valid_click(output_dict, time, tran_tm, spam):
                    if oldest_click_time > int(time): 
                        output_dict['click_log'] = val_dict
                        output_dict['click_time'] = time
                        oldest_click_time = int(time)

                    if time[0:8] == stable_dt:
                        suffix = "B"
                    elif time[0:8] == stable_dt_oneday_ago:
                        suffix = "C"
            elif src == "request":
                if spam == '0' or 'request_log' not in output_dict:
                    output_dict['request_log'] = val_dict
                    output_dict['request_time'] = time

        if 'click_log' not in output_dict and 'trans_log' in output_dict:
            suffix = "D"

        # remove only has event data or non-join request 
        if 'click_log' not in output_dict \
            and 'trans_log' not in output_dict:
            return
        if len(output_dict['trans_event']) == 0:
            log = {}
            log['click_log'] = output_dict['click_log']
            log['click_time'] = output_dict['click_time']
            print "%s\t#%s" % (json.dumps(log), suffix)
        else:
            for et, trans_event_log in output_dict['trans_event'].items():
                log = {}
                log['click_log'] = output_dict['click_log']
                log['click_time'] = output_dict['click_time']
                log['trans_log'] = trans_event_log[0]
                log['trans_time'] = trans_event_log[1]
                log['event_type'] = trans_event_log[2]
                print "%s\t#%s" % (json.dumps(log), suffix)
        
        #calc_stat_info(output_dict)
    except:
        pass 

key = ''
clc_info = []
trans_info = []
sdk_trans_info = []
is_wakeup = False
for line in sys.stdin:
    flds = line.strip('\n').split('\t')
    if len(flds) != 5:
        continue
    [reqid, input_src, val_log, time, spam] = flds[0:5]
    
    if key != reqid:
        if key != '':
            merge_info(clc_info, trans_info)
        key = reqid
        clc_info = []
        trans_info = []
    if input_src in ['click', 'request']:
        clc_info.append((val_log, time, input_src, spam))
    elif input_src == 'trans':
        trans_info.append((val_log, time))

if key != '':
    merge_info(clc_info, trans_info)

for k,v in stat_info.iteritems():
    print "%s\t%s#%s" % (k[0] 
                        ,'\t'.join(str(x) for x in v)
                        , k[1])
