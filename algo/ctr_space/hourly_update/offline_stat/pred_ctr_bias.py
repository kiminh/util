import json
import sys
from collections import defaultdict

#loads shitu
ed_click = {'ed': 0, 'click': 0, 'pctr': 0}
plid_ed_click = dict()
plan_ed_click = dict()
for raw_line in sys.stdin:
    line_json = json.loads(raw_line.strip("\n\r "))
    if line_json['ed_log']['plid'] == '6f34be9660f7b93cdb9127f966d233a7' or \
        line_json['ed_log']['plid'] == 'c5d76445221436372d166799be822140':
        continue
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

    if 'planid' in ed_log:
        planid = ed_log['planid'] 
        if planid not in plan_ed_click:
            plan_ed_click[planid] = {'ed': 0, 'click': 0, 'pctr': 0}
        plan_ed_click[planid]['ed'] += 1
        plan_ed_click[planid]['click'] += click
        plan_ed_click[planid]['pctr'] += pctr

ctr = ed_click['click'] * 1.0 / ed_click['ed']
diff = 0 if ed_click['click'] == 0 else (ed_click['pctr'] - ed_click['click']) * 1.0 / ed_click['click']
copc = 0 if ed_click['click'] == 0 else (ed_click['pctr']) * 1.0 / ed_click['click']
print "ed: %s, clk: %s, pclk: %s, diff: %s, copc: %s" % (ed_click['ed'], ed_click['click'], ed_click['pctr'], diff, copc) 

print "plid diff:"
for key, value in plid_ed_click.items():
    if value['click'] == 0:
        diff = 0
        ctr = 0
    else:
        diff = (value['pctr'] - value['click']) * 1.0 / value['click']
        ctr = value['click'] * 1.0 / value['ed']

    print "plid: %s, ed: %s, clk: %s, pclk: %s, ctr: %s, diff: %s" % (key, value['ed'], value['click'], value['pctr'], ctr, diff) 

print "planid diff:"
for key, value in plan_ed_click.items():
    if value['click'] == 0:
        diff = 0
        ctr = 0
    else:
        diff = (value['pctr'] - value['click']) * 1.0 / value['click']
        ctr = value['click'] * 1.0 / value['ed']

    print "planid: %s, ed: %s, clk: %s, pclk: %s, ctr: %s, diff: %s" % (key, value['ed'], value['click'], value['pctr'], ctr, diff) 
