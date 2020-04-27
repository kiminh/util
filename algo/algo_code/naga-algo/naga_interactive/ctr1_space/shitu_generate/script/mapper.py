
import sys
import json
import traceback
from collections import defaultdict
from datetime import datetime, date
sys.path.append('script')
import time
import random

reload(sys)
sys.setdefaultencoding("utf-8")

UNK_FEA = 'unk'

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

def addr2bin(addr):
    if addr == "none":
        return 0
    try:
        items = [int(x) for x in addr.split(".")]  
    except:
        return 0
    return sum([items[i] << [24, 16, 8, 0][i] for i in range(4)])

def ip_function(ip, l):
    bin_ip = addr2bin(ip.strip("\r\n "))
    if bin_ip == 0:
        return -1
    try:
        ip_ = int(bin_ip >> l)
    except:
        return -1
    return (ip_)

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
        fea_dict = defaultdict(lambda:'unk')
        try:
            ld = json.loads(line)
            if not isinstance(ld['first_ed_log'], dict):
                ed_log = json.loads(ld['first_ed_log'])
            else:
                ed_log = ld['first_ed_log']
        except Exception as e:
            sys.stderr.write("parse log failed, err_msg[%s]\n" % (e))
            traceback.print_exc()
            continue

        click  = 1 if 'second_click_log' in ld else 0
        weight = 1 #if 'click_num' not in ld else int(ld['click_num'])

        if 'reqprt' not in ed_log:
            continue 
        timestamp = time.strftime('%Y%m%d%H%M%S', 
            time.localtime(float(str(ed_log['reqprt'])[:10])))
        fea_dict['date'] = timestamp[0:8]
        fea_dict['timestamp'] = timestamp 
        
        fea_dict['did'] = format_str(ed_log.get('did', UNK_FEA))
        fea_dict['hour'] = get_hour(timestamp)
        fea_dict['wk'] = get_dayofweek(timestamp)
        fea_dict['daytp'] = get_isholiday(timestamp)
        fea_dict['operator'] = format_str(ed_log.get('operator', UNK_FEA))
        fea_dict['req_style'] = format_str(ed_log.get('req_style', UNK_FEA))
        fea_dict['make'] = format_str(ed_log.get('make', UNK_FEA))
        fea_dict['model'] = format_str(ed_log.get('model', UNK_FEA))
        nt = ed_log.get('nt', UNK_FEA)
        fea_dict['nt'] = nt
        fea_dict['is_wifi'] = format_str(get_iswifi(nt))
        fea_dict['city'] = format_str(ed_log.get('city_id', UNK_FEA))
        fea_dict['osv'] = format_str(ed_log.get('osv', UNK_FEA))
        fea_dict['plid'] = format_str(ed_log.get('plid', UNK_FEA)) 
        fea_dict['appid'] = format_str(ed_log.get('appid', UNK_FEA))
        fea_dict['adstyle'] = format_str(ed_log.get('ad_style', UNK_FEA))
        fea_dict['appseries'] = format_str(ed_log.get('app_series', UNK_FEA))
            
        fea_dict['cimg'] = format_str(ed_log.get('cover_image', UNK_FEA)) 
        if fea_dict['req_style'] == '6':
            fea_dict['cimg'] = UNK_FEA
        fea_dict['pageid'] = format_str(ed_log.get('pageid', UNK_FEA))
        fea_dict['cimg_pageid'] = concat_str(fea_dict['cimg'], fea_dict['pageid'])
        ip = ed_log.get('ip', UNK_FEA)
        fea_dict['ip30'] = ip_function(ip, 30)
        fea_dict['ip24'] = ip_function(ip, 24)
        fea_dict['ip20'] = ip_function(ip, 20)
        fea_dict['ip18'] = ip_function(ip, 18)
 
        tu = format_str(ed_log.get('tu', UNK_FEA))
        fea_dict['tu'] = tu
        if len(tu) == 6:
            fea_dict['tu'] = tu[3:]
        if 'fac_ts' in ed_log:
            fac_ts = int(ed_log.get('fac_ts'))
            prt = int(int(ed_log['reqprt'])*1.0/1000)
            if fac_ts == 0:
                user_act = -1
            else:
                user_act = int((prt - fac_ts)*1.0/(60*60))
            if user_act > 48: 
                user_act = 48
        else:
            user_act = UNK_FEA
        fea_dict['user_act_time'] = user_act
        
        #combined feature 
        fea_dict['make_model'] = concat_str(fea_dict['make'], fea_dict['model'])
        fea_dict['plid_cimg_pageid'] = concat_str(fea_dict['plid'], fea_dict['cimg_pageid'])
        fea_dict['tu_cimg_pageid'] = concat_str(fea_dict['tu'], fea_dict['cimg_pageid'])
        fea_dict['hour_cimg_pageid'] = concat_str(fea_dict['hour'], fea_dict['cimg_pageid'])
        fea_dict['osv_cimg_pageid'] = concat_str(fea_dict['osv'], fea_dict['cimg_pageid'])
        fea_dict['make_model_cimg_pageid'] = concat_str(fea_dict['make_model'], fea_dict['cimg_pageid'])
        fea_dict['useracttime_cimg_pageid'] = concat_str(fea_dict['user_act_time'], fea_dict['cimg_pageid'])
        fea_dict['iswifi_cimg_pageid'] = concat_str(fea_dict['is_wifi'], fea_dict['cimg_pageid'])
        fea_dict['city_cimg_pageid'] = concat_str(fea_dict['city'], fea_dict['cimg_pageid']) 
        fea_dict['ip30_cimg_pageid'] = concat_str(fea_dict['ip30'], fea_dict['cimg_pageid'])
        fea_dict['ip24_cimg_pageid'] = concat_str(fea_dict['ip24'], fea_dict['cimg_pageid'])
        fea_dict['ip20_cimg_pageid'] = concat_str(fea_dict['ip20'], fea_dict['cimg_pageid'])
        fea_dict['ip18_cimg_pageid'] = concat_str(fea_dict['ip18'], fea_dict['cimg_pageid'])

        fea_dict['make_model_pageid'] = concat_str(fea_dict['make_model'], fea_dict['pageid'])
        fea_dict['hour_pageid'] = concat_str(fea_dict['hour'], fea_dict['pageid'])
        fea_dict['useracttime_pageid'] = concat_str(fea_dict['user_act_time'], fea_dict['pageid'])
        fea_dict['osv_pageid'] = concat_str(fea_dict['osv'], fea_dict['pageid'])
        fea_dict['iswifi_pageid'] = concat_str(fea_dict['is_wifi'], fea_dict['pageid'])
        fea_dict['city_pageid'] = concat_str(fea_dict['city'], fea_dict['pageid'])
        fea_dict['ip30_pageid'] = concat_str(fea_dict['ip30'], fea_dict['pageid'])
        fea_dict['ip24_pageid'] = concat_str(fea_dict['ip24'], fea_dict['pageid'])
        fea_dict['ip20_pageid'] = concat_str(fea_dict['ip20'], fea_dict['pageid'])
        fea_dict['ip18_pageid'] = concat_str(fea_dict['ip18'], fea_dict['pageid'])

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
        randint = random.randint(0, sys.maxsize)
        print "%s\002ins\t%s\t%s\t%s" % (randint, ' '.join(str(x) for x in ins_encode_list), str(click), weight)
    except Exception as e:
        sys.stderr.write("parse log failed, err_msg[%s]\n" % (e))
        traceback.print_exc()
        pass

#for k,v in stat_info.iteritems():
#    print "%s\002stat\t%s\t%s" % (k, v.pv, v.click)
