#coding:utf-8
import os
import sys
import json
import random
import traceback
from collections import defaultdict
from datetime import datetime, date
sys.path.append('script')
import time
reload(sys);
sys.setdefaultencoding("utf-8")

UNK_FEA = 'unk'
EMPTY_FEA = ""
COMBINE_FEA_SEP = '_'
def get_hour(timestamp):
    try:
        return datetime.strptime(timestamp, "%Y%m%d%H%M%S").strftime("%H")
    except:
        return UNK_FEA 

def get_dayofweek(timestamp):
    try:
        return str(datetime.strptime(timestamp, "%Y%m%d%H%M%S").strftime("%w"))
    except:
        return UNK_FEA 

def get_isholiday(timestamp):
    try:
        wk = str(datetime.strptime(timestamp, "%Y%m%d%H%M%S").strftime("%w"))
        if wk in ['0', '6']:
            return '1'
        else:
            return '0'
    except:
        return UNK_FEA 

def get_iswifi(rawlog):
    if rawlog == '1':
        return '1'
    else:
        return '0'

def format_str(rawlog):
    if rawlog in ["", None]:
        rawlog = UNK_FEA
    return str(rawlog).strip().lower().replace(' ', '#')

def concat_str(str1, str2):
    return "%s_%s" % (str1, str2)

class Item(object):
    def __init__(self):
        self.pv = 0
        self.click = 0

def newItem():
    return Item()

stat_info = defaultdict(newItem)
promotedapp_clktrans = {}
fea_suffix_dict = dict()
for line in open("./script/features.list"):
    if '#' in line:
        continue
    fea_suffix_dict[line.strip().split('\t')[0]] = line.strip().split('\t')[1]

mode = "daily"
if len(sys.argv) == 2:
    mode = sys.argv[1]

for line in sys.stdin:
    try:
        line = line.strip('\r\n')
        ld = json.loads(line)
        
        fea_dict = defaultdict(lambda:'unk')
        try:
            ed_log = ld['click_log']
            if not isinstance(ed_log, dict):
                ed_log = json.loads(ld['click_log'])
        except Exception as e:
            print e
            continue
        ad_style = ed_log.get('ad_style', '')
        if ad_style != '8':    
            continue        
        promoted_app = ed_log.get('promoted_app', '')
        if promoted_app == '':
            continue
        if 'reqprt' not in ed_log:
            continue 
        reqprt = ed_log['reqprt'] 
        timestamp = time.strftime('%Y%m%d%H%M%S', 
            time.localtime(float(reqprt[:10])))

        fea_dict['date'] = timestamp[0:8]
        # 临时加入逻辑，后面需要去除
        fea_dict['ad_style_id'] = ed_log.get('ad_style_id', "0")
        if 'interactive_expids' in ed_log:
            if ed_log['interactive_expids'] == '2002':
                fea_dict['ad_style_id'] = "1"

        fea_dict['flow_style_id'] = ed_log.get('flow_style_id', "0")
        fea_dict['timestamp'] = timestamp
        #user features
        fea_dict['did'] = format_str(ed_log.get('did', UNK_FEA))
        fea_dict['hour'] = get_hour(timestamp)
        fea_dict['wk'] = get_dayofweek(timestamp)
        fea_dict['daytp'] = get_isholiday(timestamp)
        fea_dict['operator'] = format_str(ed_log.get('operator', UNK_FEA))
        fea_dict['make'] = format_str(ed_log.get('make', UNK_FEA))
        fea_dict['model'] = format_str(ed_log.get('model', UNK_FEA))
        nt = ed_log.get('nt', UNK_FEA)
        fea_dict['nt'] = nt
        fea_dict['is_wifi'] = format_str(get_iswifi(nt))
        fea_dict['city'] = format_str(ed_log.get('city_id', UNK_FEA))
        fea_dict['dev_type'] = format_str(ed_log.get('dt', UNK_FEA))
        fea_dict['osv'] = format_str(ed_log.get('osv', UNK_FEA))
        fea_dict['req_style'] = format_str(ed_log.get('req_style', UNK_FEA))
        track_type = format_str(ed_log.get('track_type', UNK_FEA))
        track_id = format_str(ed_log.get('track_id', UNK_FEA))
        if track_id == UNK_FEA and track_type != '2':
            continue
        #app feature
        fea_dict['plid'] = format_str(ed_log.get('plid', UNK_FEA)) 
        fea_dict['reqtype'] = format_str(ed_log.get('reqtype', UNK_FEA))
        fea_dict['appid'] = format_str(ed_log.get('appid', UNK_FEA))
        fea_dict['pw'] = format_str(ed_log.get('pw', UNK_FEA))
        fea_dict['ph'] = format_str(ed_log.get('ph', UNK_FEA))
        fea_dict['launch_type'] = format_str(ed_log.get('lan_t', UNK_FEA))
        raw_str = format_str(ed_log['tu'])
        # example '210867'
        if raw_str and len(raw_str) == 6:
            raw_str = raw_str[3:]
        fea_dict['tu'] = raw_str
        app_series = format_str(ed_log.get('app_series', UNK_FEA))
        fea_dict['app_series'] = format_str(app_series)
        fea_dict['sdk_version'] = format_str(ed_log.get('sdk_version', UNK_FEA))
        fea_dict['adid'] = format_str(ed_log.get('adid', UNK_FEA))
        fea_dict['industry'] = format_str(ed_log.get('industry', UNK_FEA))
        
        #ad featyre
        fea_dict['adstyle'] = format_str(ed_log.get('ad_style', UNK_FEA))
        fea_dict['promoted_app'] = format_str(ed_log.get('promoted_app', UNK_FEA))
        fea_dict['adw'] = format_str(ed_log.get('adw', UNK_FEA))
        fea_dict['adh'] = format_str(ed_log.get('adh', UNK_FEA))
        fea_dict['planid'] = format_str(ed_log.get('planid', UNK_FEA))
        fea_dict['cmpid'] = format_str(ed_log.get('campaignid', UNK_FEA))
        fea_dict['orgid'] = format_str(ed_log.get('orgid', UNK_FEA))
        
        #combined feature 
        fea_dict['adwh'] = concat_str(fea_dict['adw'], fea_dict['adh'])
        fea_dict['pwh'] = concat_str(fea_dict['pw'], fea_dict['ph'])
        fea_dict['make_model'] = concat_str(fea_dict['make'], fea_dict['model'])
        fea_dict['make_model_plid'] = concat_str(concat_str(fea_dict['make'], fea_dict['model']), fea_dict['plid'])
        fea_dict['make_model_promotedapp'] = concat_str(concat_str(fea_dict['make'], fea_dict['model']), fea_dict['promoted_app'])
        fea_dict['plid_osv'] = concat_str(fea_dict['plid'], fea_dict['osv'])
        fea_dict['plid_planid'] = concat_str(fea_dict['plid'], fea_dict['planid'])
        fea_dict['plid_industry'] = concat_str(fea_dict['plid'], fea_dict['industry'])
        fea_dict['plid_promotedapp'] = concat_str(fea_dict['plid'], fea_dict['promoted_app'])
        fea_dict['iswifi_promotedapp'] = concat_str(fea_dict['is_wifi'], fea_dict['promoted_app'])
        fea_dict['planid_tu'] = concat_str(fea_dict['planid'], fea_dict['tu'])
        fea_dict['tu_promotedapp'] = concat_str(fea_dict['tu'], fea_dict['promoted_app'])
        fea_dict['planid_promotedapp'] = concat_str(fea_dict['planid'], fea_dict['promoted_app'])

        is_trans = 0 
        if mode == 'daily': 
            if 'trans_log' in ld:
                trans_log = ld['trans_log']
                events = ld['trans_log']['events']
                if '0' in events:
                    is_trans = 1
        else:
            if 'trans_log' in ld:
                is_trans = 1

        ins_encode_list = []

        for fea in fea_suffix_dict:
            if fea not in fea_dict:
                print "Warning: %s segment not in fea_dict" % fea
                continue
            suffix = fea_suffix_dict[fea]
            v = fea_dict[fea]
            value = "%s\001%s:%s" % (fea, suffix, v)
            ins_encode_list.append(value)
            item = stat_info[value]
            item.pv += 1
            item.click += int(is_trans)
        ins_encode_list.append("%s\001%s" % ('beta0', '9999:1'))
        randint = random.randint(0, sys.maxint)
        print "%s\002ins\t%s\t%s" % (randint, ' '.join(str(x) for x in ins_encode_list), str(is_trans))
 
        if promoted_app not in promotedapp_clktrans:
            promotedapp_clktrans[promoted_app] = {'click': 0, 'trans': 0}
        promotedapp_clktrans[promoted_app]['click'] += 1
        promotedapp_clktrans[promoted_app]['trans'] += int(is_trans)
    except Exception as e:
        sys.stderr.write("parse log failed, err_msg[%s]\n" % (e))
        traceback.print_exc()
        pass

for k,v in stat_info.iteritems():
    print "%s\002stat\t%s\t%s" % (k, v.pv, v.click)

for key, value in promotedapp_clktrans.items():
    print "%s\002appclktrans\t%s\t%s" % (key, value['click'], value['trans'])
