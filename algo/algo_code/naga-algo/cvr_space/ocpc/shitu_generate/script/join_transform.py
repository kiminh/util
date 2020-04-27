#coding:utf-8
import json
import sys
import base64
import time
import os
import math

def read_calibrate_dat(calibrate_file):
    fo = open(calibrate_file, 'r')
    promoted_app_dict = {}
    for line in fo.readlines():
        [promoted_app, value] = line.strip('\n').split('\t')
        promoted_app_dict[promoted_app] = value
    fo.close()
    print 'read %s' % calibrate_file
    return promoted_app_dict

def write_calibrate_dat(calibrate_file, promoted_app_dict):
    fo = open(calibrate_file, 'w')
    for key, value in promoted_app_dict.items():
        fo.write('%s\t%s\n' % (key, value))
    print 'write %s' % calibrate_file
    fo.close()

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
                if event_type != '0':
                    continue
                if 'reqid' not in line_json:
                    if 'clickid' not in line_json:
                        continue
                    clickid = line_json['clickid']
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

hour = time.strftime("%Y%m%d%H", time.localtime())
delay_window = 0
click_reqid = set()
f_out = open(sys.argv[3], 'w')
promoted_app_stat = {}
for click_file in os.listdir(click_path):
    if today not in click_file:
        continue
    #click.2019102803.log 
    timestamp = int(click_file[6:16])
    if int(hour) - timestamp < delay_window:
        print "filter the click file: %s" % click_file
        continue
    print "process %s" % click_file
    click_file_abs = '%s/%s' % (click_path, click_file)
    for raw in open(click_file_abs):
        try:
            line_json = json.loads(raw.strip("\n\r "))
            promoted_app = line_json['promoted_app']
            if promoted_app == '':
                continue

            reqid = line_json.get('reqid', '')
            adid = line_json.get('adid', '')
            key = '%s_%s' % (reqid, adid)
            if key in click_reqid:
                continue
            click_reqid.add(key)
            trans_log = 1 if key in reqid_dict else 0
            trans_cost = 0 if trans_log == 0 else reqid_dict[key]['cost']
            click_cost = line_json.get('cost', 0)
            log = {}
            log['click_log'] = line_json
            log['clict_cost'] = click_cost
            log['trans_log'] = trans_log
            log['trans_cost'] = trans_cost
            # 包粒度统计点击转化
            if promoted_app not in promoted_app_stat:
                promoted_app_stat[promoted_app] = [0, 0]
            promoted_app_stat[0] += 1  # click
            promoted_app_stat[1] += trans_log  # trans

            f_out.write("%s\n" % json.dumps(log))
        except Exception as e:
            #print(raw)
            continue
f_out.close()

# for 小时冷启动
calibrate_file = '../model_train/model/calibrate.dat'
promoted_app_dict = read_calibrate_dat(calibrate_file)
for promoted_app, stat in promoted_app_stat.items():
    if stat[1] > 30:
        promoted_app_dict[promoted_app] = '1.0#0.1'
write_calibrate_dat(calibrate_file, promoted_app_dict)