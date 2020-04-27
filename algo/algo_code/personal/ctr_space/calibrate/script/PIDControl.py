#coding:utf-8
import json
import sys
import base64
import time
import os
import math

if len(sys.argv) < 3:
    print("Usage: py data_json time")
    exit(1)

click_path = sys.argv[1]
ed_click = dict()
for raw in open(sys.argv[1]):
    line_json = json.loads(raw.strip())
    if 'ad_style' not in line_json:
        continue
    ad_style = line_json['ad_style']
    if ad_style == '6':
        continue
    time_slot = line_json['time']
    ed = int(line_json['ed'])
    click = int(line_json['click'])
    if 'pctr' not in line_json:
        continue
    pctr_cal = float(line_json['pctr'])

    if ad_style not in ed_click:
        ed_click[ad_style] = {}
    if time_slot not in ed_click[ad_style]:
        ed_click[ad_style][time_slot] = {}
    ed_click[ad_style][time_slot]['ed'] = ed
    ed_click[ad_style][time_slot]['click'] = click
    ed_click[ad_style][time_slot]['pctr_cal'] = pctr_cal
        
#------------------PID------------------------
slot_error = {}
global_ad_error = {}
for ad_style, value in ed_click.items():
    value_sort = sorted(value.items(), key=lambda d: d[0])
    slot_error[ad_style] = []
    global_ad_error[ad_style] = 0
    ad_click = 0
    ad_ed = 0
    ad_pctr_cal = 0.
    for ts, item in value_sort:
        ad_ed += item['ed']
        ad_click += item['click']
        ad_pctr_cal += item['pctr_cal']
       
        pctr_cal = item['pctr_cal']
        real_click = item['click']
 
        if item['ed'] < 200 and item['click'] == 0:
            slot_error[ad_style].append((ts, 0))
            continue
        if item['ed'] < 200 and item['click'] < 3:
            slot_error[ad_style].append((ts, 0))
            continue
        if item['ed'] > 200 and item['click'] == 0:
            error = 1. - (pctr_cal / (real_click+1))
            slot_error[ad_style].append((ts, error))
            continue
        
        diff = pctr_cal / real_click
        error = 1. - diff
        slot_error[ad_style].append((ts, error))

    if ad_click < 200:
        global_ad_diff = 0
    else:
        global_ad_diff = 1. - (ad_pctr_cal / (ad_click+1))
    global_ad_error[ad_style] = {}
    global_ad_error[ad_style]['diff'] = global_ad_diff
    global_ad_error[ad_style]['pctr'] = ad_pctr_cal
    global_ad_error[ad_style]['click'] = ad_click

lamb_p = 0.04 #error
lamb_d = 0.02
lamb_l = 0.8
cali = {}
cali_exp = {}
for ad_style, value in slot_error.items():
    if len(value) == 0:
        cali_exp[ad_style] = 1
        continue
    cali[ad_style] = lamb_p * value[-1][1]
    cali[ad_style] += lamb_l * global_ad_error[ad_style]['diff'] 

    if len(value) > 1:
        cali[ad_style] += lamb_d * (value[-1][1] - value[-2][1])
    cali_exp[ad_style] = math.exp(1.2*cali[ad_style])
    cali_exp[ad_style] = min(max(cali_exp[ad_style], 0.9), 1.05)
print "==================================global error=============================="
print global_ad_error
print "======================================cali================================="
print cali
print "===================================cali exp================================"
print cali_exp
print "============================================================================"

out_path='./data/cali_%s.json' % sys.argv[2]
json.dump(cali_exp, open(out_path, 'w'), indent=4)
