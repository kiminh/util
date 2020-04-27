#coding:utf-8
import os
import sys
import json
import traceback
from collections import defaultdict
from datetime import datetime, date
sys.path.append('script')
import time

reload(sys)
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
fea_suffix_dict = {}
for line in open('./script/features.list').readlines():
    if line[0] == '#':
        continue
    flds = line.strip("\n\r ").split('\t')
    fea_name = flds[0]
    fea_code = flds[1]
    fea_suffix_dict[fea_name] = fea_code

for line in sys.stdin:
    try:
        line = line.strip('\r\n')
        ld = json.loads(line)
        
        fea_dict = defaultdict(lambda:'unk')
        try:
            if not isinstance(ld['ed_log'], dict):
                ed_log = json.loads(ld['ed_log'])
            else:
                ed_log = ld['ed_log']
        except Exception as e:
            print e
            continue
        fea_dict['adstyle'] = format_str(ed_log.get('ad_style', UNK_FEA))
        if fea_dict['adstyle'] != "8":
            continue
        if 'reqprt' not in ed_log:
            continue 
        timestamp = time.strftime('%Y%m%d%H%M%S', 
            time.localtime(float(str(ed_log['reqprt'])[:10])))
        fea_dict['date'] = timestamp[0:8]
        fea_dict['ad_style_id'] = ed_log.get('ad_style_id', "0")
        # 临时加入逻辑，后续需要去除
        if 'interactive_expids' in ed_log:
            if ed_log['interactive_expids'] == '2002':
                fea_dict['ad_style_id'] = "1" 

        fea_dict['flow_style_id'] = ed_log.get('flow_style_id', "0")

        fea_dict['timestamp'] = timestamp 
        fea_dict['did'] = format_str(ed_log.get('did', UNK_FEA))
        fea_dict['hour'] = get_hour(timestamp)
        fea_dict['wk'] = get_dayofweek(timestamp)
        fea_dict['daytp'] = get_isholiday(timestamp)
        fea_dict['make'] = format_str(ed_log.get('make', UNK_FEA))
        fea_dict['model'] = format_str(ed_log.get('model', UNK_FEA))
        nt = ed_log.get('nt', UNK_FEA)
        fea_dict['nt'] = nt
        fea_dict['is_wifi'] = format_str(get_iswifi(nt))
        fea_dict['city'] = format_str(ed_log.get('city_id', UNK_FEA))
        fea_dict['osv'] = format_str(ed_log.get('osv', UNK_FEA))
        fea_dict['plid'] = format_str(ed_log.get('plid', UNK_FEA)) 
        fea_dict['appid'] = format_str(ed_log.get('appid', UNK_FEA))
        fea_dict['pw'] = format_str(ed_log.get('pw', UNK_FEA))
        fea_dict['ph'] = format_str(ed_log.get('ph', UNK_FEA))
        #ad featyre
        fea_dict['adid'] = format_str(ed_log.get('adid', UNK_FEA)) 
        fea_dict['adw'] = format_str(ed_log.get('adw', UNK_FEA))
        fea_dict['adh'] = format_str(ed_log.get('adh', UNK_FEA))
        fea_dict['planid'] = format_str(ed_log.get('planid', UNK_FEA))
        fea_dict['cmpid'] = format_str(ed_log.get('campaignid', UNK_FEA))
        fea_dict['orgid'] = format_str(ed_log.get('orgid', UNK_FEA))
        
        #combined feature 
        fea_dict['adwh'] = concat_str(fea_dict['adw'], fea_dict['adh'])
        fea_dict['pwh'] = concat_str(fea_dict['pw'], fea_dict['ph'])
        fea_dict['adwh_pwh'] = concat_str(fea_dict['adwh'], fea_dict['pwh'])
        fea_dict['pwh_plid'] = concat_str(fea_dict['pwh'], fea_dict['plid'])
 
        fea_dict['make_model'] = concat_str(fea_dict['make'], fea_dict['model'])
        fea_dict['make_model_plid'] = concat_str(fea_dict['make_model'], fea_dict['plid'])
        fea_dict['make_model_adid'] = concat_str(fea_dict['make_model'], fea_dict['adid'])
        
        fea_dict['make_appid'] = concat_str(fea_dict['make'], fea_dict['appid'])
        fea_dict['make_plid'] = concat_str(fea_dict['make'], fea_dict['plid'])
        fea_dict['make_adid'] = concat_str(fea_dict['make'], fea_dict['adid'])
        fea_dict['make_planid'] = concat_str(fea_dict['make'], fea_dict['planid'])
        fea_dict['make_cmpid'] = concat_str(fea_dict['make'], fea_dict['cmpid'])
        fea_dict['make_orgid'] = concat_str(fea_dict['make'], fea_dict['orgid'])
        fea_dict['plid_osv'] = concat_str(fea_dict['plid'], fea_dict['osv'])
        fea_dict['adid_osv'] = concat_str(fea_dict['adid'], fea_dict['osv'])

        fea_dict['appid_planid'] = concat_str(fea_dict['appid'], fea_dict['planid'])
        fea_dict['appid_hour'] = concat_str(fea_dict['appid'], fea_dict['hour'])
        fea_dict['appid_adid'] = concat_str(fea_dict['appid'], fea_dict['adid'])

        fea_dict['plid_hour'] = concat_str(fea_dict['plid'], fea_dict['hour'])
        fea_dict['plid_adid'] = concat_str(fea_dict['plid'], fea_dict['adid'])
        fea_dict['plid_planid'] = concat_str(fea_dict['plid'], fea_dict['planid'])
        
        fea_dict['adid_hour'] = concat_str(fea_dict['adid'], fea_dict['hour'])
        fea_dict['city_adid'] = concat_str(fea_dict['city'], fea_dict['adid'])
        fea_dict['city_plid'] = concat_str(fea_dict['city'], fea_dict['plid'])
        fea_dict['city_appid'] = concat_str(fea_dict['city'], fea_dict['appid'])
        fea_dict['iswifi_adid'] = concat_str(fea_dict['is_wifi'], fea_dict['adid'])
        fea_dict['nt_adid'] = concat_str(fea_dict['nt'], fea_dict['adid'])
        fea_dict['nt_plid'] = concat_str(fea_dict['nt'], fea_dict['plid'])

        fea_dict['industry'] = format_str(ed_log.get('industry', UNK_FEA))
        fea_dict['app_series'] = format_str(ed_log.get('app_series', UNK_FEA))
        fea_dict['appseries_industry'] = concat_str(fea_dict['app_series'], fea_dict['industry'])

        if 'fac_ts' not in ed_log or 'reqprt' not in ed_log:
            continue
        if ed_log['fac_ts'] == '' or ed_log['reqprt'] == '': 
            continue 

        fac_ts = int(ed_log['fac_ts'])
        prt = int(int(ed_log['reqprt'])*1.0/1000)
        if fac_ts == 0:
            user_act = -1
        else:
            user_act = int((prt - fac_ts)*1.0/(60*60))
        if user_act > 48: 
            user_act = 48
        fea_dict['user_act_time'] = user_act
        fea_dict['useracttime_planid'] = concat_str(fea_dict['user_act_time'], fea_dict['planid'])

        click  = 1 if 'click_log' in ld else 0
        weight = 1
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
            item.click += click
        ins_encode_list.append("%s\001%s" % ('beta0', '9999:1'))
        print "%s\002ins\t%s\t%s\t%s" % (fea_dict['ifa'], ' '.join(str(x) for x in ins_encode_list), str(click), weight)
    except Exception as e:
        sys.stderr.write("parse log failed, err_msg[%s]\n" % (e))
        traceback.print_exc()
        pass

for k,v in stat_info.iteritems():
    print "%s\002stat\t%s\t%s" % (k, v.pv, v.click)
