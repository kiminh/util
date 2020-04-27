#coding:utf-8
import os
import sys
import json
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

def parse_ad_info(path):
    ad_dict = {}
    for line in open(path).readlines():
        flds = line.strip().split('\t')
        if len(flds) != 3:
            continue
        adid = format_str(flds[0])
        ad_level = flds[1]
        industry = flds[2]
        ad_dict[adid] = (ad_level, industry)
    return ad_dict

class Item(object):
    def __init__(self):
        self.pv = 0
        self.click = 0

def newItem():
    return Item()

stat_info = defaultdict(newItem)
promotedapp_clktrans = {}
ad_dict = parse_ad_info('./script/ad_info')
fea_suffix_dict = dict()
for line in open("./script/features.list"):
    if '#' in line:
        continue
    fea_suffix_dict[line.strip().split('\t')[0]] = line.strip().split('\t')[1]

app_clktrans = {}
for raw in open('./script/app_clktrans.json'):
    raw = json.loads(raw.strip("\n\r "))
    app_clktrans[raw['promoted_app']] = {
        'click': raw['click'], 
        'trans': raw['trans']
    }

mode = "online"
if len(sys.argv) == 2:
    mode = sys.argv[1]

#if len(sys.argv) == 2:
#    tag = sys.argv[1]
#else:
#    input_hdfs = os.environ["mapreduce_map_input_file"]
#    if input_hdfs.split("/")[-3] == "applist":
#        tag = 'applist'
#    else:
#        tag = 'shitu'
tag = 'shitu'

for line in sys.stdin:
    try:
        line = line.strip('\r\n')
        ld = json.loads(line)
        
        """
        if tag == 'applist':
            gaid = ld['gaid']
            applist = ld['applist']
            applist_lst =[]
            for app in applist:
                val = "applist:%s:%s" % (fea_suffix_dict['applist'], app)
                applist_lst.append(str(hashstr(val)))
            #print "%s#applist\t%s" % (gaid, ' '.join(applist_lst))
            print "%s#applist\t%s" % (gaid, ' '.join("applist:%s:%s" % (fea_suffix_dict['applist'], iter) for iter in  applist))
            continue
        """
        fea_dict = defaultdict(lambda:'unk')
        try:
            ed_log = ld['click_log']
        except Exception as e:
            print e
            continue
        req_style = ed_log.get('req_style', '')
        if req_style == '6':    
            continue        
        promoted_app = ed_log.get('promoted_app', '')
        if promoted_app == '':
            continue
        #if promoted_app in app_clktrans:
        #    click = app_clktrans[promoted_app]['click']
        #    trans = app_clktrans[promoted_app]['trans']
        #    if click < 100 or trans == 0:
        #        continue
        #    cvr = trans * 1.0 / click
        #    if cvr < 0.005:
        #        continue
        if 'reqprt' not in ed_log:
            continue 
        reqprt = ed_log['reqprt'] 
        timestamp = time.strftime('%Y%m%d%H%M%S', 
            time.localtime(float(reqprt[:10])))

        fea_dict['date'] = timestamp[0:8]
        if int(fea_dict['date']) < 20191001:
            continue
        fea_dict['timestamp'] = timestamp 
        #user features
        if 'ifa' in ed_log:
            ifa = ed_log['ifa']
        else:
            ifa = UNK_FEA
      
        fea_dict['ifa'] = ifa
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
        fea_dict['req_style'] = format_str(ed_log['req_style'])
        track_type = format_str(ed_log.get('track_type', UNK_FEA))
        track_id = format_str(ed_log.get('track_id', UNK_FEA))
        if track_id == UNK_FEA and track_type != '2':
            continue
        #applist_len = ed_log.get('applist_len', UNK_FEA)
        #applist_len = UNK_FEA if applist_len == '0' else applist_len
        #if applist_len != UNK_FEA:
        #    applist_len = int(int(applist_len)/5)
        #fea_dict['applist_len'] = format_str(applist_len)

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
        app_series = ed_log.get('app_series', '')
        if app_series == '':
            continue
        fea_dict['app_series'] = format_str(app_series)
        fea_dict['sdk_version'] = format_str(ed_log.get('sdk_version', UNK_FEA))
        fea_dict['adid'] = format_str(ed_log.get('adid', UNK_FEA))
        if fea_dict['adid'] in ad_dict:
            (ad_level, industry) = ad_dict[fea_dict['adid']]
            fea_dict['ad_level'] = ad_level
            fea_dict['industry'] = industry
        #ad featyre
        fea_dict['adstyle'] = format_str(ed_log.get('ad_style', UNK_FEA))
        pkg = format_str(ed_log.get('promoted_app', UNK_FEA))
        fea_dict['promoted_app'] = pkg
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

        user_edclk = ed_log.get('user_edclk', '')
        if user_edclk == '':
            continue
        ed, clk = user_edclk.split('_')
        isnewuser = 0
        if int(ed) < 10:
            isnewuser = 1
        elif int(ed) == -1:
            isnewuser = -1
        fea_dict['isnewuser'] = str(isnewuser)

        is_trans = 0 
        if mode == 'online': 
            if 'trans_log' in ld:
                trans_log = ld['trans_log']
                events = ld['trans_log']['events']
                if '0' in events:
                    is_trans = 1
        else:
            is_trans = ld['trans_log']

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
        print "%s\002ins\t%s\t%s" % (fea_dict['ifa'], ' '.join(str(x) for x in ins_encode_list), str(is_trans))
        print "%s\002shitulog\t%s" % (ifa, json.dumps(ld))
 
        if promoted_app not in promotedapp_clktrans:
            promotedapp_clktrans[promoted_app] = {'click': 0, 'trans': 0}
        promotedapp_clktrans[promoted_app]['click'] = 1
        promotedapp_clktrans[promoted_app]['trans'] += int(is_trans)
    except Exception as e:
        sys.stderr.write("parse log failed, err_msg[%s]\n" % (e))
        traceback.print_exc()
        pass

for k,v in stat_info.iteritems():
    print "%s\002stat\t%s\t%s" % (k, v.pv, v.click)

for key, value in promotedapp_clktrans.items():
    print "%s\002appclktrans\t%s\t%s" % (key, value['click'], value['trans'])
