#coding:utf-8
import json
import sys
import base64
import time
import os
import math

if len(sys.argv) < 3:
    print("Usage: python join_transform.py pkg_clc_trans version")
    exit(1)

pkg_clc_trans = {}
for raw in open(sys.argv[1]):
    line_json = json.loads(raw.strip("\n\r "))

    pkg_name = line_json['promoted_app']
    adpcvr = float(line_json['adpcvr'])
    pcvr_cal = float(line_json['pcvr_cal'])
    pkg_click = float(line_json['click'])
    pkg_trans = float(line_json['trans'])
    hour = line_json['hour']

    if pkg_name not in pkg_clc_trans:
        pkg_clc_trans[pkg_name] = {}
    if hour not in pkg_clc_trans[pkg_name]:
        pkg_clc_trans[pkg_name][hour] = {'click': 0, 'trans': 0, 'adpcvr': 0, 'pcvr_cal': 0}
    pkg_clc_trans[pkg_name][hour]['click'] = pkg_click
    pkg_clc_trans[pkg_name][hour]['trans'] = pkg_trans
    pkg_clc_trans[pkg_name][hour]['adpcvr'] = adpcvr
    pkg_clc_trans[pkg_name][hour]['pcvr_cal'] = pcvr_cal

slot_error = {}
global_pkg_error = {}
for pkg_name, value in pkg_clc_trans.items():
    value_sort = sorted(value.items(), key=lambda d: d[0])
    slot_error[pkg_name] = []
    global_pkg_error[pkg_name] = 0
    pkg_click = 0
    pkg_trans = 0
    pkg_pcvr = 0.
    pkg_pcvr_cal = 0.
    for ts, item in value_sort:
        pkg_click += item['click']
        pkg_trans += item['trans']
        pkg_pcvr += item['adpcvr']
        pkg_pcvr_cal += item['pcvr_cal']
       
        pcvr = item['adpcvr']
        pcvr_cal = item['pcvr_cal']
        real_trans = item['trans']
 
        if item['click'] < 200 and item['trans'] == 0:
            slot_error[pkg_name].append((ts, 0))
            continue
        if item['click'] < 200 and item['trans'] < 3:
            slot_error[pkg_name].append((ts, 0))
            continue
        if item['click'] > 200 and item['trans'] == 0:
            error = 1.1 - (pcvr_cal / (real_trans+1))
            slot_error[pkg_name].append((ts, error))
            continue
        
        diff = pcvr_cal / (real_trans+0.2)
        error = 1.1 - diff
        slot_error[pkg_name].append((ts, error))

    if pkg_click < 200:
        global_pkg_diff = 0
    else:
        global_pkg_diff = 1.1 - (pkg_pcvr_cal / (pkg_trans+1))
    global_pkg_error[pkg_name] = {}
    global_pkg_error[pkg_name]['diff'] = global_pkg_diff
    global_pkg_error[pkg_name]['pcvr'] = pkg_pcvr_cal
    global_pkg_error[pkg_name]['trans'] = pkg_trans

lamb_p = 0.04 #error
lamb_d = 0.02
lamb_i = 0.8
cali = {}
cali_exp = {}
for pkg_name, value in slot_error.items():
    if len(value) == 0:
        cali_exp[pkg_name] = 1
        continue
    cali[pkg_name] = lamb_p * value[-1][1]
    cali[pkg_name] += lamb_i * global_pkg_error[pkg_name]['diff'] 

    if len(value) > 1:
        cali[pkg_name] += lamb_d * (value[-1][1] - value[-2][1])
    cali_exp[pkg_name] = math.exp(cali[pkg_name])
    cali_exp[pkg_name] = min(max(cali_exp[pkg_name], 0.1), 1.4)


print "==================================global error=============================="
print global_pkg_error
print "======================================theta================================="
print cali
print "===================================theta exp================================"
print cali_exp
print "============================================================================"

out_path='./data/pkg_cali_%s.json' % sys.argv[2]
out_path1='./data/pkg_result_%s.json' % sys.argv[2]

json.dump(cali_exp, open(out_path, 'w'), indent=4)
json.dump(pkg_clc_trans, open(out_path1, 'w'), indent=4)
