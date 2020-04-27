#coding:utf-8
import json
import sys
import base64
import time
import os
import math

clc_trans = json.load(open(sys.argv[1]))
for i in range(1, 24):
    slot = str(i)
    
    clc_confience_th = 200
    slot_error = {}
    global_app_error = {}
    for pb_app, value in clc_trans.items():
        value_sort = sorted(value.items(), key=lambda d: d[0])
        slot_error[pb_app] = []
        global_app_error[pb_app] = 0
        global_click = 0
        global_trans = 0
        global_pcvr = 0.
        for ts, item in value_sort:
            if int(ts) >= i:
                continue
            global_click += item['click']
            global_trans += item['trans']
            global_pcvr += item['adpcvr']
            if item['click'] < clc_confience_th and item['trans'] == 0:
                slot_error[pb_app].append((ts, 0))
                continue
            #if item['trans'] < 5:
            #    slot_error[pb_app].append((ts, 0))
            #    continue
            real_trans = 1.0 if item['trans'] == 0 else item['trans']
            pcvr = item['adpcvr']
            diff = pcvr / real_trans
            error = 1 - diff
            slot_error[pb_app].append((ts, error))
        global_trans = 1 if global_trans == 0 else global_trans
        global_app_error[pb_app] = 1 - (global_pcvr/global_trans)

    lamb_p = 0.3 #error
    lamb_d = 0.2
    lamb_l = 0.5
    theta = {}
    theta_exp = {}
    for pb_app, value in slot_error.items():
        if len(value) == 0:
            theta[pb_app] = 0
            theta_exp[pb_app] = 1
            continue
        theta[pb_app] = lamb_p * value[-1][1]
        print pb_app, "error:", value[-1][1], lamb_p * value[-1][1]
        #for i in range(1, len(value)):
        #    theta[pb_app] += (lamb_l * value[i][1])
        #theta[pb_app] += lamb_l * global_app_error[pb_app]
        print pb_app, "jifeng:", global_app_error[pb_app], lamb_l * global_app_error[pb_app]
        if len(value) > 1:
            theta[pb_app] += lamb_d * (value[-1][1] - value[-2][1])
            #print pb_app, "weifeng:", lamb_d * (value[-1][1] - value[-2][1])

        theta_exp[pb_app] = math.exp(1.1 * theta[pb_app])
        theta_exp[pb_app] = min(max(theta_exp[pb_app], 0.4), 1.4)
    
    for pb_app, value in clc_trans.items():
        if slot in value:
            #if value[slot]['trans'] < 10:
            #    continue
            diff = value[slot]['adpcvr'] / (value[slot]['trans']+1)
            print pb_app, slot, value[slot]['click'], value[slot]['trans'], theta[pb_app], theta_exp[pb_app], diff, diff * theta_exp[pb_app] 
