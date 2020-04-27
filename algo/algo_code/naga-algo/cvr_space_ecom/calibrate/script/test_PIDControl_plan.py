#coding:utf-8
import json
import sys
import base64
import time
import os
import math

clc_trans = json.load(open(sys.argv[1]))
slot_error = {} #json.load(open(sys.argv[2]))
global_plan_error = {} #json.load(open(sys.argv[3]))

for i in range(1, 24):
    slot = str(i)

    for planid, value in clc_trans.items():
        value_sort = sorted(value.items(), key=lambda d: d[0])
        slot_error[planid] = []
        global_plan_error[planid] = 0
        plan_click = 0
        plan_trans = 0
        plan_pcvr = 0.
        plan_pcvr_cal = 0.
        for ts, item in value_sort:
            if int(ts) >= i:
                continue
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
            global_plan_diff = 1.1 - (plan_pcvr_cal / (plan_trans+1))
        global_plan_error[planid] = {}
        global_plan_error[planid]['diff'] = global_plan_diff
        global_plan_error[planid]['pcvr'] = plan_pcvr_cal
        global_plan_error[planid]['trans'] = plan_trans

    lamb_p = 0.01 #error
    lamb_d = 0.02
    lamb_l = 0.7
    theta = {}
    theta_exp = {}
    for pb_app, value in slot_error.items():
        if len(value) == 0:
            theta_exp[pb_app] = 1
            continue
        theta[pb_app] = lamb_p * value[-1][1]
        #print pb_app, "error:", value[-1][1], lamb_p * value[-1][1]
        #for i in range(1, len(value)):
        #    theta[pb_app] += (lamb_l * value[i][1])
        theta[pb_app] += lamb_l * global_plan_error[pb_app]['diff']
        #print pb_app, "jifeng:", global_app_error[pb_app]['diff'], lamb_l * global_app_error[pb_app]['diff']
        if len(value) > 1:
            theta[pb_app] += lamb_d * (value[-1][1] - value[-2][1])
            #print pb_app, "weifeng:", lamb_d * (value[-1][1] - value[-2][1])

        theta_exp[pb_app] = math.exp(theta[pb_app])
        theta_exp[pb_app] = min(max(theta_exp[pb_app], 0.4), 1.4)
    
    for pb_app, value in clc_trans.items():
        if slot in value:
            #if value[slot]['trans'] < 10:
            #    continue
            diff = value[slot]['adpcvr'] / (value[slot]['trans']+1)
            if pb_app in theta_exp and pb_app in theta:
                print pb_app, slot, value[slot]['click'], value[slot]['trans'], theta[pb_app], theta_exp[pb_app], diff, diff * theta_exp[pb_app] 
