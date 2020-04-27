#coding:utf-8
import json
import sys
from collections import defaultdict
from utils.email_sender import MailSender
from sklearn.metrics import roc_auc_score

new_plan_list = []
for raw in open('new_plan_list.txt'):
    new_plan_list.append(raw.strip("\n\r "))
print(new_plan_list)

seg_size = 1000

ed_click = {}
click_pclick = {}
plid_ed_click = {}
plid_click_pclick = {}
plan_ed_click = {}
hour_ed_click = {}
new_plan_ed_click = {}

model_verison_list = ['6401', '6402', '6403', '6404']
version_type = {
    '6401': 'exp',
    '6402': 'exp',
    '6403': 'exp',
    '6404': 'exp'
}
#model_verison_list = ['6501', '6502']
#version_type = {
#    '6501': 'base',
#    '6502': 'exp'
#}

type2ver = {}
for ver, type_ in version_type.items():
    if type_ not in type2ver:
        type2ver[type_] = []
    type2ver[type_].append(ver)

for raw_line in sys.stdin:
    line_json = json.loads(raw_line.strip("\n\r "))
    if 'ed_log' not in line_json:
        continue
    if isinstance(line_json['ed_log'], dict):
        ed_log = line_json['ed_log']
    else:
        ed_log = json.loads(line_json['ed_log'])
    if 'plid' not in ed_log:
        continue
    if 'planid' not in ed_log:
        continue
    ed_time = line_json['ed_time']
    plid = ed_log['plid']
    planid = ed_log['planid']
    time_flag = ed_time[0:10]     
    if 'triggerd_expids' not in ed_log:
        continue
    triggerd_expids = ed_log['triggerd_expids']
    req_style = ed_log.get('req_style', '')
    adid = ed_log.get('adid', '')

    #filter DNN
    if '6502' in triggerd_expids:
        continue

    use_ver = ''
    for ver in model_verison_list:
        if ver in triggerd_expids:
            use_ver = ver
    model_ver = 'exp'
    if use_ver in version_type:
        model_ver = version_type[use_ver]
    
    if req_style == "6":
        continue

    click = 0
    pctr = float(ed_log['pctr_cal'])
    ed_cost = float(ed_log['cost'])
    click_cost = 0.0

    if 'click_log' in line_json:
        click = 1
        click_log = line_json['click_log']
        if 'click_cost' in line_json:
            click_cost = float(line_json['click_cost'])
        if isinstance(click_log, dict) and 'cost' in click_log:
            click_cost = click_log['cost']
    
    cost = click_cost if ed_cost == 0 else ed_cost 

    if model_ver not in click_pclick:
        click_pclick[model_ver] = {}
        click_pclick[model_ver]['y_true'] = []
        click_pclick[model_ver]['y_pred'] = []
    click_pclick[model_ver]['y_true'].append(click)
    click_pclick[model_ver]['y_pred'].append(pctr)

    if model_ver not in ed_click:
        ed_click[model_ver] = {'ed': 0, 'click': 0, 'pctr': 0, 'cost': 0}
    ed_click[model_ver]['ed'] += 1
    ed_click[model_ver]['click'] += click
    ed_click[model_ver]['pctr'] += float(pctr)
    ed_click[model_ver]['cost'] += cost

    if plid not in plid_ed_click:
        plid_ed_click[plid] = {}
    if model_ver not in plid_ed_click[plid]:
        plid_ed_click[plid][model_ver] = {'ed': 0, 'click': 0, 'pctr': 0, 'cost': 0}
    plid_ed_click[plid][model_ver]['ed'] += 1
    plid_ed_click[plid][model_ver]['click'] += click
    plid_ed_click[plid][model_ver]['pctr'] += pctr
    plid_ed_click[plid][model_ver]['cost'] += cost

    if plid not in plid_click_pclick:
        plid_click_pclick[plid] = {}
    if model_ver not in plid_click_pclick[plid]:
        plid_click_pclick[plid][model_ver] = {'y_true': [], 'y_pred': []}
    plid_click_pclick[plid][model_ver]['y_true'].append(click)
    plid_click_pclick[plid][model_ver]['y_pred'].append(pctr)

    if planid not in plan_ed_click:
        plan_ed_click[planid] = {}
    if model_ver not in plan_ed_click[planid]:
        plan_ed_click[planid][model_ver] = {'ed': 0, 'click': 0, 'pctr': 0, 'cost': 0}
    plan_ed_click[planid][model_ver]['ed'] += 1
    plan_ed_click[planid][model_ver]['click'] += click
    plan_ed_click[planid][model_ver]['pctr'] += pctr
    plan_ed_click[planid][model_ver]['cost'] += cost

    if model_ver not in hour_ed_click:
        hour_ed_click[model_ver] = {}
    if time_flag not in hour_ed_click[model_ver]:
        hour_ed_click[model_ver][time_flag] = {'ed': 0, 'click': 0, 'pctr': 0}
    hour_ed_click[model_ver][time_flag]['ed'] += 1
    hour_ed_click[model_ver][time_flag]['click'] += click
    hour_ed_click[model_ver][time_flag]['pctr'] += pctr

    if planid in new_plan_list:
        if planid not in new_plan_ed_click:
            new_plan_ed_click[planid] = {}
        if model_ver not in new_plan_ed_click[planid]:
            new_plan_ed_click[planid][model_ver] = {'ed': 0, 'click': 0, 'pctr': 0, 'cost': 0}
        new_plan_ed_click[planid][model_ver]['ed'] += 1
        new_plan_ed_click[planid][model_ver]['click'] += click
        new_plan_ed_click[planid][model_ver]['pctr'] += pctr
        new_plan_ed_click[planid][model_ver]['cost'] += cost

#分桶偏差
qq_plot = {}
for model_ver in click_pclick:
    if model_ver not in qq_plot:
        qq_plot[model_ver] = [[0 for x in range(3)] for y in range(int(seg_size / 10) + 1)]
    for y_true, y_pred in zip(click_pclick[model_ver]['y_true'], click_pclick[model_ver]['y_pred']):
        label = int(y_true)
        pred_score = float(y_pred)
        slot = int(pred_score * seg_size)
        if slot > seg_size / 10:
            slot = int(seg_size / 10)
        qq_plot[model_ver][slot][0] += 1
        qq_plot[model_ver][slot][2] += pred_score
        if label == 1:
            qq_plot[model_ver][slot][1] += 1

html_table = "<h3>总体模型指标情况: </h3><table border=\"2\", width=\"100%\">\
<tr>\
<td>模型</td>\
<td>展现</td>\
<td>点击</td>\
<td>预测点击</td>\
<td>点击率</td>\
<td>偏差</td>\
<td>扣费</td>\
<td>COPC</td>\
<td>AUC</td>\
</tr>"

for key, value in ed_click.items():
    
    y_true = click_pclick[key]['y_true']
    y_pred = click_pclick[key]['y_pred']
    try:
        auc = roc_auc_score(y_true, y_pred)
    except Exception as e:
        auc = 0.0
    ctr = value['click'] * 1.0 / value['ed']
    diff = 0 if value['click'] == 0 else (value['pctr'] - value['click']) * 1.0 / value['click']
    copc = 0 if value['click'] == 0 else value['pctr'] * 1.0 / value['click']
    html_table += "<tr><td>%s%s</td>\
                    <td>%s</td>\
                    <td>%s</td>\
                    <td>%.2f</td>\
                    <td>%.3f%%</td>\
                    <td>%.2f%%</td>\
                    <td>%.3f</td>\
                    <td>%.4f</td>\
                    <td>%.4f</td>\
                  </tr>" % \
            (key, '', value['ed'], value['click'], value['pctr'], ctr * 100, diff * 100, value['cost'], copc, auc)
html_table += "</table>"

plid_html_table = "<h3>分广告位指标情况: </h3><table border=\"2\", width=\"100%\">\
<tr>\
<td>广告位</td>\
<td>模型</td>\
<td>展现</td>\
<td>点击</td>\
<td>预测点击</td>\
<td>点击率</td>\
<td>偏差</td>\
<td>扣费</td>\
<td>AUC</td>\
</tr>"
## 
## {plid: {model_ver_1: {'ed': xx, 'click': xx, 'pctr': xx}}}
##
plid_ed_click_sort = sorted(plid_ed_click.items(), \
    key=lambda d: sum([ value['ed'] for key, value in d[1].items() ]), reverse=True)
for plid, value in plid_ed_click_sort: #plid_ed_click.items():
    line_num = len(value)
    plid_html_table += "<tr><td rowspan=\"%s\">%s</td>\
                  </tr>" % \
                (line_num+1, plid)
        
    for model_ver, val in value.items():
        y_true = plid_click_pclick[plid][model_ver]['y_true']
        y_pred = plid_click_pclick[plid][model_ver]['y_pred']
        try:
            auc = roc_auc_score(y_true, y_pred)
        except Exception as e:
            auc = 0.0

        if val['click'] == 0:
            diff = 0
            ctr = 0
        else:
            diff = (val['pctr'] - val['click']) * 1.0 / val['click']
            ctr = val['click'] * 1.0 / val['ed']
    
        plid_html_table += "<tr><td>%s</td>\
                    <td>%s</td>\
                    <td>%s</td>\
                    <td>%.2f</td>\
                    <td>%.2f%%</td>\
                    <td>%.2f%%</td>\
                    <td>%.3f</td>\
                    <td>%.4f</td>\
                  </tr>" % \
                (model_ver, val['ed'], val['click'], val['pctr'], ctr * 100, diff * 100, val['cost'], auc)
plid_html_table += "</table>"

plan_html_table = "<h3>分plan指标情况: </h3><table border=\"2\", width=\"100%\">\
<tr>\
<td>planid</td>\
<td>模型</td>\
<td>展现</td>\
<td>点击</td>\
<td>预测点击</td>\
<td>点击率</td>\
<td>偏差</td>\
<td>扣费</td>\
</tr>"
plan_ed_click_sort = sorted(plan_ed_click.items(), \
    key=lambda d: sum([ value['ed'] for key, value in d[1].items() ]), reverse=True)
for key, value in plan_ed_click_sort[:50]: #plan_ed_click.items():
    line_num = len(value)
    plan_html_table += "<tr><td rowspan=\"%s\">%s</td>\
                  </tr>" % \
                (line_num+1, key)
 
    for model_ver, val in value.items():
        if val['click'] == 0:
            diff = 0
            ctr = 0
        else:
            diff = (val['pctr'] - val['click']) * 1.0 / val['click']
            ctr = val['click'] * 1.0 / val['ed']
         
        plan_html_table += "<tr><td>%s</td>\
                    <td>%s</td>\
                    <td>%s</td>\
                    <td>%.2f</td>\
                    <td>%.2f%%</td>\
                    <td>%.2f%%</td>\
                    <td>%.3f</td>\
                  </tr>" % \
                (model_ver, val['ed'], val['click'], val['pctr'], ctr * 100, diff * 100, val['cost'])
plan_html_table += "</table>"

new_plan_html_table = "<h3>新plan指标情况: </h3><table border=\"2\", width=\"100%\">\
<tr>\
<td>planid</td>\
<td>模型</td>\
<td>展现</td>\
<td>点击</td>\
<td>预测点击</td>\
<td>点击率</td>\
<td>偏差</td>\
<td>扣费</td>\
</tr>"
new_plan_ed_click_sort = sorted(new_plan_ed_click.items(), \
    key=lambda d: sum([ value['ed'] for key, value in d[1].items() ]), reverse=True)
for key, value in new_plan_ed_click_sort: #plan_ed_click.items():
    line_num = len(value)
    new_plan_html_table += "<tr><td rowspan=\"%s\">%s</td>\
                  </tr>" % \
                (line_num+1, key)
 
    for model_ver, val in value.items():
        if val['click'] == 0:
            diff = 0
            ctr = 0
        else:
            diff = (val['pctr'] - val['click']) * 1.0 / val['click']
            ctr = val['click'] * 1.0 / val['ed']
    
        new_plan_html_table += "<tr><td>%s</td>\
                    <td>%s</td>\
                    <td>%s</td>\
                    <td>%.2f</td>\
                    <td>%.2f%%</td>\
                    <td>%.2f%%</td>\
                    <td>%.3f</td>\
                  </tr>" % \
                (model_ver, val['ed'], val['click'], val['pctr'], ctr * 100, diff * 100, val['cost'])
new_plan_html_table += "</table>"


qqplot_table = "<h3>模型分桶偏差情况: </h3><table border=\"2\", width=\"100%\">\
<tr>\
<td>模型</td>\
<td>桶号</td>\
<td>展现</td>\
<td>预测点击</td>\
<td>实际点击</td>\
<td>偏差</td>\
</tr>"

for model_ver, value in qq_plot.items():
    line_num = len(value)
    qqplot_table += "<tr><td rowspan=\"%s\">%s</td>\
                  </tr>" % (line_num+1, model_ver)
    
    for i, item in enumerate(value):
        diff = 0 if item[1] == 0 else (item[2] - item[1]) * 1.0 / item[1]
        qqplot_table += "<tr><td>%s</td>\
                    <td>%s</td>\
                    <td>%.2f</td>\
                    <td>%s</td>\
                    <td>%.2f%%</td>\
                  </tr>" % \
            (i, item[0], item[2], item[1], diff * 100)
qqplot_table += "</table>"


hour_html_table = "<h3>分小时指标情况: </h3><table border=\"2\", width=\"100%\">\
<tr>\
<td>模型</td>\
<td>时间</td>\
<td>展现</td>\
<td>点击</td>\
<td>预测点击</td>\
<td>点击率</td>\
<td>偏差</td>\
<td>COPC</td>\
</tr>"

for key, value in hour_ed_click.items():
    line_num = len(value)
    hour_html_table += "<tr><td rowspan=\"%s\">%s</td>\
                  </tr>" % \
                (line_num+1, key)
    value_sort = sorted(value.items(), key=lambda d: int(d[0])) 
    for hour, val in value_sort:
        if val['click'] == 0:
            diff = 0
            ctr = 0
            copc = 0
        else:
            diff = (val['pctr'] - val['click']) * 1.0 / val['click']
            copc = val['pctr'] * 1.0 / val['click']
            ctr = val['click'] * 1.0 / val['ed']
    
        hour_html_table += "<tr><td>%s</td>\
                    <td>%s</td>\
                    <td>%s</td>\
                    <td>%.2f</td>\
                    <td>%.2f%%</td>\
                    <td>%.2f%%</td>\
                    <td>%.4f</td>\
                  </tr>" % \
                (hour, val['ed'], val['click'], val['pctr'], ctr * 100, diff * 100, copc)
hour_html_table += "</table>"

table = html_table + plid_html_table + plan_html_table + new_plan_html_table + qqplot_table  + hour_html_table
#print(table)
mailsender = MailSender()
mailsender.init()
mailsender.send_email(subject="%s Naga DSP CTR Model Online Abtest Report" % sys.argv[1], msg=table)
