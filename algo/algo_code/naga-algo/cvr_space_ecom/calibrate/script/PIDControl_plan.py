#coding:utf-8
import json
import sys
import base64
import time
import os
import math

if len(sys.argv) < 3:
    print("Usage: python join_transform.py click_path transform_path")
    exit(1)

click_path = sys.argv[1]
trans_path = sys.argv[2]
today = time.strftime("%Y%m%d", time.localtime())

reqid_dict = dict()
for trans_file in os.listdir(trans_path):
    if today not in trans_file:
        continue
    print "process %s" % trans_file
    trans_file_abs = '%s/%s' % (trans_path, trans_file)
    with open(trans_file_abs) as f_in:
        for raw in f_in:
            try:
                line_json = json.loads(raw.strip("\n\r ")) 
                adid = line_json.get('adid', '')
                event_type = ''
                if 'event_type' in line_json:
                    event_type = line_json['event_type']
                if event_type != '35':
                    continue
                if 'reqid' not in line_json:
                    if 'clickid' not in line_json:
                        continue
                    clickid = line_json['clickid']
                    clickid = clickid.replace('.', '=')
                    decode_str = base64.b64decode(clickid)
                    reqid = decode_str.split('_')[0]
                else:
                    reqid = line_json['reqid']
                cost = line_json.get('cost', 0)
                key = '%s_%s' % (reqid, adid)
                reqid_dict[key] = {}
                reqid_dict[key]['cost'] = cost
            except Exception as e:
                continue

tw = 60
today_timestamp = time.mktime(time.strptime(today, "%Y%m%d"))
real_timestamp = int(time.time())
cur_slot = int((real_timestamp - today_timestamp)*1.0/(tw*60))
click_reqid = set()
clc_trans = dict()
for click_file in os.listdir(click_path):
    if today not in click_file:
        continue
    print "process %s" % click_file
    click_file_abs = '%s/%s' % (click_path, click_file)
    for raw in open(click_file_abs):
        try:
            line_json = json.loads(raw.strip("\n\r "))
            industry = line_json['industry']
            promoted_app = line_json['promoted_app']
            if (not industry.startswith('105')) or promoted_app != '':
                continue
            planid = line_json.get('planid', '')
            reqid = line_json.get('reqid', '')
            adid = line_json.get('adid', '')
            
            key = '%s_%s' % (reqid, adid)
            if key in click_reqid:
                continue
            click_reqid.add(key)
            
            adpcvr = float(line_json['adpcvr'])
            pcvr_cal = float(line_json['pcvr_cal'])
            is_ocpc = line_json['is_ocpc']
            #if is_ocpc == "false":
            #    continue
            is_trans = 1 if key in reqid_dict else 0
            reqprt = int(line_json['reqprt'][0:10])
            if reqprt < today_timestamp:
                continue
            time_slot = int((reqprt - today_timestamp)*1.0 / (tw * 60))
            if time_slot == cur_slot:
                continue
            
            if planid not in clc_trans:
                clc_trans[planid] = {}
            if time_slot not in clc_trans[planid]:
                clc_trans[planid][time_slot] = {
                    'click': 0,
                    'trans': 0,
                    'adpcvr': 0.0,
                    'pcvr_cal': 0.0
                }
            clc_trans[planid][time_slot]['click'] += 1
            clc_trans[planid][time_slot]['trans'] += is_trans
            clc_trans[planid][time_slot]['adpcvr'] += adpcvr
            clc_trans[planid][time_slot]['pcvr_cal'] += pcvr_cal
    
        except Exception as e:
            #print(raw)
            continue

#json.dump(clc_trans, open('clc_trans.json', 'w'), indent=4)

#------------------PID------------------------
clc_confience_th = 100
slot_error = {}
global_plan_error = {}
for planid, value in clc_trans.items():
    value_sort = sorted(value.items(), key=lambda d: d[0])
    slot_error[planid] = []
    global_plan_error[planid] = 0
    plan_click = 0
    plan_trans = 0
    plan_pcvr = 0.
    plan_pcvr_cal = 0.
    for ts, item in value_sort:
        plan_click += item['click']
        plan_trans += item['trans']
        plan_pcvr += item['adpcvr']
        plan_pcvr_cal += item['pcvr_cal']
       
        pcvr = item['adpcvr']
        pcvr_cal = item['pcvr_cal']
        real_trans = item['trans']
 
        if item['click'] < 200 and item['trans'] == 0:
            slot_error[planid].append((ts, 0))
            continue
        if item['click'] < 200 and item['trans'] < 3:
            slot_error[planid].append((ts, 0))
            continue
        if item['click'] > 200 and item['trans'] == 0:
            error = 1.2 - (pcvr_cal / (real_trans+1))
            slot_error[planid].append((ts, error))
            continue
        
        diff = pcvr_cal / real_trans
        error = 1.2 - diff
        slot_error[planid].append((ts, error))

    if plan_click < 200:
        global_plan_diff = 0
    else:
        global_plan_diff = 1.1 - (plan_pcvr_cal / (plan_trans+0.2))
    global_plan_error[planid] = {}
    global_plan_error[planid]['diff'] = global_plan_diff
    global_plan_error[planid]['pcvr'] = plan_pcvr_cal
    global_plan_error[planid]['trans'] = plan_trans
    global_plan_error[planid]['click'] = plan_click

json.dump(global_plan_error, open('global_plan_error.json', 'w'), indent=4)
lamb_p = 0.04 #error
lamb_d = 0.02
lamb_l = 0.9
theta = {}
theta_exp = {}
for planid, value in slot_error.items():
    if len(value) == 0:
        theta_exp[planid] = 1
        continue
    theta[planid] = lamb_p * value[-1][1]
    theta[planid] += lamb_l * global_plan_error[planid]['diff'] 

    if len(value) > 1:
        theta[planid] += lamb_d * (value[-1][1] - value[-2][1])
    if global_plan_error[planid]['pcvr'] < 0.01:
        theta_exp[planid] = math.exp(theta[planid])*0.5
        print theta[planid], math.exp(theta[planid])
    else:
        theta_exp[planid] = math.exp(theta[planid])

    theta_exp[planid] = min(max(theta_exp[planid], 0.05), 1.3)
print "==================================global error=============================="
print global_plan_error
print "======================================theta================================="
print theta
print "===================================theta exp================================"
print theta_exp
print "============================================================================"

out_path='./data/plan_theta_%s.json' % sys.argv[3]
out_path1='./data/plan_result_%s.json' % sys.argv[3]

json.dump(theta_exp, open(out_path, 'w'), indent=4)
json.dump(clc_trans, open(out_path1, 'w'), indent=4)
