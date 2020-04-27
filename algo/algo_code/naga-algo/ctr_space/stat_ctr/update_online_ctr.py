import json
import sys
from collections import defaultdict
import time
import os
update_time = sys.argv[1] #time.strftime("%Y%m%d%H", time.localtime())
log_path = "/home/ling.fang/ctr_space/hourly_update/log/shitu_tmp/"
print "update time: %s" % update_time

#loads shitu
ed_click = {'ed': 0, 'click': 0, 'pctr': 0}
plid_ed_click = dict()
plid_adid_ed_click = dict()

for f in os.listdir(log_path):
    print 'process %s file' % f
    with open(log_path + f) as f_in:
        for raw_line in f_in:
            line_json = json.loads(raw_line.strip("\n\r "))
            click = 0
            if 'ed_log' not in line_json:
                continue
            pctr = float(line_json['ed_log']['pctr_cal'])

            if 'click_log' in line_json:
                click = 1
            ed_click['ed'] += 1
            ed_click['click'] += click
            ed_click['pctr'] += float(pctr)

            ed_log = line_json['ed_log']
            if 'plid' in ed_log:
                plid = ed_log['plid']

                if plid not in plid_ed_click:
                    plid_ed_click[plid] = {'ed': 0, 'click': 0, 'pctr': 0}
                plid_ed_click[plid]['ed'] += 1
                plid_ed_click[plid]['click'] += click
                plid_ed_click[plid]['pctr'] += pctr
            else:
                continue

            if 'adid' in ed_log and 'plid' in ed_log:
                adid = ed_log['adid'] 
                plid = ed_log['plid']
                if plid not in plid_adid_ed_click:
                    plid_adid_ed_click[plid] = {}
                if adid not in plid_adid_ed_click[plid]:
                    plid_adid_ed_click[plid][adid] = {'ed': 0, 'click': 0, 'pctr': 0}
                plid_adid_ed_click[plid][adid]['ed'] += 1
                plid_adid_ed_click[plid][adid]['click'] += click
                plid_adid_ed_click[plid][adid]['pctr'] += pctr

out_file = "stat_ctr_%s.csv" % update_time
print out_file
with open(out_file, 'w') as f_out:
    global_ctr = ed_click['click'] * 1.0 / ed_click['ed']
    print global_ctr
    f_out.write('global_ctr,%s\n' % global_ctr)
    for key, value in plid_ed_click.items():
        plid_ctr = value['click'] * 1.0 / value['ed']
        if plid_ctr == 0.0 or \
            plid_ctr == 1.0 and value['click'] < 20:
            continue
        f_out.write('%s,%s\n' % (key, plid_ctr))
    for key, value in plid_adid_ed_click.items():
        for k, v in value.items():
            plid_adid_ctr = v['click'] * 1.0 / v['ed'] 
            w_key = '%s_%s' % (key, k)
            if plid_adid_ctr == 0.0 or\
                plid_adid_ctr == 1.0 and v['click'] < 20:
                continue
            f_out.write('%s,%s\n' % (w_key, plid_adid_ctr))
