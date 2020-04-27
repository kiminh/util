#coding:utf-8
import json
import sys
import base64
import time
import os
import math

if len(sys.argv) < 3:
    print("Usage: python join_transform.py plan_clc_trans version")
    exit(1)

plan_clc_trans = {}
for raw in open(sys.argv[1]):
    line_json = json.loads(raw.strip("\n\r "))

    planid = line_json['planid']
    adpcvr = float(line_json['adpcvr'])
    pcvr_cal = float(line_json['pcvr_cal'])
    plan_click = float(line_json['click'])
    plan_trans = float(line_json['trans'])
    hour = line_json['hour']

    if planid not in plan_clc_trans:
        plan_clc_trans[planid] = {}
    if hour not in plan_clc_trans[planid]:
        plan_clc_trans[planid][hour] = {'click': 0, 'trans': 0, 'adpcvr': 0, 'pcvr_cal': 0}
    plan_clc_trans[planid][hour]['click'] = plan_click
    plan_clc_trans[planid][hour]['trans'] = plan_trans
    plan_clc_trans[planid][hour]['adpcvr'] = adpcvr
    plan_clc_trans[planid][hour]['pcvr_cal'] = pcvr_cal

slot_error = {}
global_plan_error = {}
for planid, value in plan_clc_trans.items():
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
            error = 1.0 - (pcvr_cal / (real_trans+1))
            slot_error[planid].append((ts, error))
            continue
        
        diff = pcvr_cal / (real_trans+0.2)
        error = 1.0 - diff
        slot_error[planid].append((ts, error))

    if plan_click < 200:
        global_plan_diff = 0
    else:
        global_plan_diff = 1.0 - (plan_pcvr_cal / (plan_trans+0.2))

    global_plan_error[planid] = {}
    global_plan_error[planid]['diff'] = global_plan_diff
    global_plan_error[planid]['pcvr'] = plan_pcvr_cal
    global_plan_error[planid]['trans'] = plan_trans
    global_plan_error[planid]['click'] = plan_click

json.dump(global_plan_error, open('global_plan_error.json', 'w'), indent=4)
lamb_p = 0.04 #error
lamb_d = 0.02
lamb_i = 0.9
cali = {}
cali_exp = {}
for planid, value in slot_error.items():
    if len(value) == 0:
        cali_exp[planid] = 1
        continue
    cali[planid] = lamb_p * value[-1][1]
    cali[planid] += lamb_i * global_plan_error[planid]['diff'] 

    if len(value) > 1:
        cali[planid] += lamb_d * (value[-1][1] - value[-2][1])
    
    cali_exp[planid] = math.exp(cali[planid])
    cali_exp[planid] = min(max(cali_exp[planid], 0.05), 1.3)
print "==================================global error=============================="
print global_plan_error
print "======================================cali================================="
print cali
print "===================================cali exp================================"
print cali_exp
print "============================================================================"

out_path='./data/plan_cali_%s.json' % sys.argv[2]
out_path1='./data/plan_result_%s.json' % sys.argv[2]

json.dump(cali_exp, open(out_path, 'w'), indent=4)
json.dump(plan_clc_trans, open(out_path1, 'w'), indent=4)
