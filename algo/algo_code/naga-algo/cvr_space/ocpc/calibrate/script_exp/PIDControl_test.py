#coding:utf-8
import json
import sys
import base64
import time
import os
import math

# 模拟测试脚本

plan_result_file = 'plan_result_20200108230301.json'
plan_theta_file = 'plan_theta_20200108'
def get_plan_app_dict():
    dict = {}
    file = '/home/ad_user/personal/ling.fang/kafka_stream_data/data/transform/transform.2020010823.log'
    with open(file) as f_in:
        for raw in f_in:
            line_json = json.loads(raw.strip("\n\r "))
            if 'planid' not in line_json:
                continue
            planid = line_json['planid']
            promoted_app = line_json['promoted_app']
            dict[planid] = promoted_app
    return dict

def get_app_back_rate():
    file = '../stat_app_back_rate.json'
    app_dict = {}
    with open(file) as f_in:
        for raw in f_in:
            line_json = json.loads(raw.strip("\n\r "))
            promoted_app = line_json['promoted_app']
            if 'day_transforms' not in line_json or 'back_rate' not in line_json or line_json['day_transforms'] < 100:
                continue
            back_rate = float(line_json['back_rate'])
            if back_rate < 0 or back_rate >= 1:
                continue
            day_back_rate = 1/(1 - back_rate)
            hour_back_rate = day_back_rate * 1.1
            app_dict[promoted_app] = [hour_back_rate, day_back_rate]
    return app_dict

def get_click_rate(theta_value):
    # 系数和缩量映射关系
    theta_click_list = [
        [1, 1],
        [0.95, 0.79],
        [0.9, 0.63],
        [0.85, 0.51],
        [0.8, 0.42],
        [0.7, 0.29],
        [0.6, 0.19],
        [0, 0.1]
    ]
    click_rate = 1
    for arr in theta_click_list:
        if theta_value >= arr[0]:
            click_rate = arr[1]
            break
    return click_rate

def get_pid_value(g_v, p_v, pp_v):
    lamb_p = 0.04  # error
    lamb_d = 0.02
    lamb_l = 0.8
    theta_min = 0.6  # 系数下限
    theta_max = 1.4  # 系数上限
    theta_value = math.exp(lamb_p * p_v + lamb_l * g_v + lamb_d * (p_v - pp_v))
    return min(max(theta_value, theta_min), theta_max)

def get_plan_theta_list():
    dict = {}
    path = '/home/work/xiaohong.shen/cvr_space/calibrate/data'
    for file in os.listdir(path):
        if file.startswith(plan_theta_file):
            theta_json = json.load(open(path+'/'+file))
            hour = str(int(file[19:21]))
            for planid, value in theta_json.items():
                if planid not in dict:
                    dict[planid] = {}
                if hour not in dict[planid]:
                    dict[planid][hour] = value
    return dict




clc_trans = json.load(open('../data/' + plan_result_file))
# promoted_app回流系数
default_hour_back_rate = 1.2
default_day_back_rate = 1.1
app_back_dict = get_app_back_rate()
plan_app_dict = get_plan_app_dict()
plan_theta_dict = get_plan_theta_list()
#print plan_theta_dict



#------------------PID------------------------
clc_confience_th = 100
slot_error = {}
global_app_error = {}
fo = open('debug_log','w')
for planid, value in clc_trans.items():
    value_sort = sorted(value.items(), key=lambda d: int(d[0]))
    slot_error[planid] = []
    global_app_error[planid] = 0
    app_click = 0
    app_trans = 0
    app_pcvr = 0.
    app_pcvr_cal = 0.
    hour_back_rate = default_hour_back_rate
    day_back_rate = default_day_back_rate
    promote_app = '-'
    if planid in plan_app_dict and plan_app_dict[planid] in app_back_dict:
        promote_app = plan_app_dict[planid]
        [hour_back_rate, day_back_rate] = app_back_dict[plan_app_dict[planid]]
    for ts, item in value_sort:
        app_click += item['click']
        app_trans += item['trans']
        app_pcvr += item['adpcvr']
        app_pcvr_cal += item['pcvr_cal']
       
        pcvr = item['adpcvr']
        pcvr_cal = item['pcvr_cal']
        real_trans = item['trans']
 
        if item['click'] < 200 and real_trans == 0:
            slot_error[planid].append((ts, 0))
            continue
        if item['click'] < 200 and real_trans < 3:
            slot_error[planid].append((ts, 0))
            continue
        if item['click'] >= 200 and real_trans == 0:
            error = hour_back_rate - (pcvr_cal / (real_trans+1))
            slot_error[planid].append((ts, error))
            continue
        
        diff = pcvr_cal / (real_trans + 0.1)
        error = hour_back_rate - diff
        slot_error[planid].append((ts, error))

        if app_click < 200:
            global_app_diff = 0
        else:
            global_app_diff = day_back_rate - (app_pcvr_cal / (app_trans+0.2))

        p_v = error
        pp_v = 0
        if len(slot_error[planid]) >= 2:
            pp_v = slot_error[planid][-2][1]
        theta_value = get_pid_value(global_app_diff, p_v, pp_v)
        click_rate = get_click_rate(theta_value)
        ori_theta = plan_theta_dict[planid][ts] if planid in plan_theta_dict and ts in plan_theta_dict[planid] else -1
        log = '%s %s %s %i %i %.1f %i %i %.4f %.4f %.4f %.4f %.2f' % (ts,planid, promote_app,item['click'],item['trans'],
                                                           item['pcvr_cal'],app_click,app_trans,day_back_rate,
                                                                global_app_diff,ori_theta, theta_value, click_rate)
        print log
        fo.write(log + '\n')
fo.close()
    # global_app_error[planid] = {}
    # global_app_error[planid]['diff'] = global_app_diff
    # global_app_error[planid]['pcvr'] = app_pcvr_cal
    # global_app_error[planid]['trans'] = app_trans


#
# lamb_p = 0.04 #error
# lamb_d = 0.02
# lamb_l = 0.8
# theta_min = 0.6  # 系数下限
# theta_max = 1.4  # 系数上限
# theta = {}
# theta_exp = {}
#
# for planid, value in slot_error.items():
#     if len(value) == 0:
#         theta_exp[planid] = [1, 1]
#         continue
#     print "app: %s" % planid
#     theta[planid] = lamb_p * value[-1][1]
#     print "error: %s" % (lamb_p * value[-1][1])
#     theta[planid] += lamb_l * global_app_error[planid]['diff']
#     print "jifeng: %s" % (lamb_l * global_app_error[planid]['diff'])
#
#     if len(value) > 1:
#         theta[planid] += lamb_d * (value[-1][1] - value[-2][1])
#         print "weifeng: %s" % (lamb_d * (value[-1][1] - value[-2][1]))
#     theta_value = math.exp(theta[planid])
#     theta_value = min(max(theta_value, theta_min), theta_max)
#     click_rate = 1
#     for arr in theta_click_list:
#         if theta_value >= arr[0]:
#             click_rate = arr[1]
#             break
#     theta_exp[planid] = [theta_value, click_rate]
#     fo.write('%s %.4f %.2f\n' % (planid, theta_value, click_rate))
# print "==================================global error=============================="
# print global_app_error
# print "======================================theta================================="
# print theta
# print "===================================theta exp================================"
# print theta_exp
#print "============================================================================"
#print theta_exp
