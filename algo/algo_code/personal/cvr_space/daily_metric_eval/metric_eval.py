#coding:utf-8
import json
import sys
from collections import defaultdict
from utils.email_sender import MailSender
from sklearn.metrics import roc_auc_score

seg_size = 1000

if len(sys.argv) < 2:
    print("Usage: py metric_eval.py date")
    exit(1)
date = sys.argv[1]

click_trans = {}
trans_ptrans = {}
plid_click_trans = {}
plid_trans_ptrans = {}
plan_click_trans = {}
hour_click_trans = {}

model_verison_list = ['6301', '6302', '6303']
version_type = {
    '6301': 'exp',
    '6302': 'exp',
    '6303': 'exp'
}
type2ver = {}
for ver, type_ in version_type.items():
    if type_ not in type2ver:
        type2ver[type_] = []
    type2ver[type_].append(ver)

for raw_line in sys.stdin:
    line_json = json.loads(raw_line.strip("\n\r "))
    if 'click_log' not in line_json:
        continue
    click_log = line_json['click_log']
    if 'plid' not in click_log:
        continue
    if 'planid' not in click_log:
        continue
    if click_log.get("promoted_app", "") == "":
        continue

    click_time = line_json['click_time']
    plid = click_log['plid']
    planid = click_log['planid']
    time_flag = click_time[0:8]
    #if time_flag != date:
    #    continue 
    triggerd_expids = click_log['triggerd_expids']
    req_style = click_log.get('req_style', '')
    use_ver = ''
    for ver in model_verison_list:
        if ver in triggerd_expids:
            use_ver = ver
    if use_ver == '':
        user_ver = "NULL"
        continue
    model_ver = version_type.get(use_ver, "None")

    if req_style == "6":
        continue

    trans = 0
    pcvr = float(click_log['adpcvr'])
    cost = float(click_log['cost'])

    if 'trans_log' in line_json:
        trans = 1

    if model_ver not in trans_ptrans:
        trans_ptrans[model_ver] = {}
        trans_ptrans[model_ver]['y_true'] = []
        trans_ptrans[model_ver]['y_pred'] = []
    trans_ptrans[model_ver]['y_true'].append(trans)
    trans_ptrans[model_ver]['y_pred'].append(pcvr)

    if model_ver not in click_trans:
        click_trans[model_ver] = {'click': 0, 'trans': 0, 'pcvr': 0, 'cost': 0}
    click_trans[model_ver]['click'] += 1
    click_trans[model_ver]['trans'] += trans
    click_trans[model_ver]['pcvr'] += float(pcvr)
    click_trans[model_ver]['cost'] += cost

    if plid not in plid_click_trans:
        plid_click_trans[plid] = {}
    if model_ver not in plid_click_trans[plid]:
        plid_click_trans[plid][model_ver] = {'click': 0, 'trans': 0, 'pcvr': 0, 'cost': 0}
    plid_click_trans[plid][model_ver]['click'] += 1
    plid_click_trans[plid][model_ver]['trans'] += trans
    plid_click_trans[plid][model_ver]['pcvr'] += pcvr
    plid_click_trans[plid][model_ver]['cost'] += cost

    if plid not in plid_trans_ptrans:
        plid_trans_ptrans[plid] = {}
    if model_ver not in plid_trans_ptrans[plid]:
        plid_trans_ptrans[plid][model_ver] = {'y_true': [], 'y_pred': []}
    plid_trans_ptrans[plid][model_ver]['y_true'].append(trans)
    plid_trans_ptrans[plid][model_ver]['y_pred'].append(pcvr)

    if planid not in plan_click_trans:
        plan_click_trans[planid] = {}
    if model_ver not in plan_click_trans[planid]:
        plan_click_trans[planid][model_ver] = {'click': 0, 'trans': 0, 'pcvr': 0, 'cost': 0}
    plan_click_trans[planid][model_ver]['click'] += 1
    plan_click_trans[planid][model_ver]['trans'] += trans
    plan_click_trans[planid][model_ver]['pcvr'] += pcvr
    plan_click_trans[planid][model_ver]['cost'] += cost

    if model_ver not in hour_click_trans:
        hour_click_trans[model_ver] = {}
    if time_flag not in hour_click_trans[model_ver]:
        hour_click_trans[model_ver][time_flag] = {'click': 0, 'trans': 0, 'pcvr': 0}
    hour_click_trans[model_ver][time_flag]['click'] += 1
    hour_click_trans[model_ver][time_flag]['trans'] += trans
    hour_click_trans[model_ver][time_flag]['pcvr'] += pcvr

#分桶偏差
qq_plot = {}
for model_ver in trans_ptrans:
    if model_ver not in qq_plot:
        qq_plot[model_ver] = [[0 for x in range(3)] for y in range(int(seg_size / 10) + 1)]
    for y_true, y_pred in zip(trans_ptrans[model_ver]['y_true'], trans_ptrans[model_ver]['y_pred']):
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
<td>点击</td>\
<td>转化</td>\
<td>预测转化</td>\
<td>转化率</td>\
<td>偏差</td>\
<td>扣费</td>\
<td>COPC</td>\
<td>AUC</td>\
</tr>"

for key, value in click_trans.items():
    
    y_true = trans_ptrans[key]['y_true']
    y_pred = trans_ptrans[key]['y_pred']
    try:
        auc = roc_auc_score(y_true, y_pred)
    except Exception as e:
        auc = 0.0
    cvr = value['trans'] * 1.0 / value['click']
    diff = 0 if value['trans'] == 0 else (value['pcvr'] - value['trans']) * 1.0 / value['trans']
    copc = 0 if value['trans'] == 0 else value['pcvr'] * 1.0 / value['trans']
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
            (key, type2ver[key], value['click'], value['trans'], value['pcvr'], cvr * 100, diff * 100, value['cost'], copc, auc)
html_table += "</table>"

plid_html_table = "<h3>分广告位指标情况: </h3><table border=\"2\", width=\"100%\">\
<tr>\
<td>广告位</td>\
<td>模型</td>\
<td>点击</td>\
<td>转化</td>\
<td>预测转化</td>\
<td>转化率</td>\
<td>偏差</td>\
<td>扣费</td>\
<td>AUC</td>\
</tr>"
## 
## {plid: {model_ver_1: {'ed': xx, 'click': xx, 'pcvr': xx}}}
##
plid_click_trans_sort = sorted(plid_click_trans.items(), \
    key=lambda d: sum([ value['click'] for key, value in d[1].items() ]), reverse=True)
for plid, value in plid_click_trans_sort: #plid_click_trans.items():
    line_num = len(value)
    plid_html_table += "<tr><td rowspan=\"%s\">%s</td>\
                  </tr>" % \
                (line_num+1, plid)
        
    for model_ver, val in value.items():
        y_true = plid_trans_ptrans[plid][model_ver]['y_true']
        y_pred = plid_trans_ptrans[plid][model_ver]['y_pred']
        try:
            auc = roc_auc_score(y_true, y_pred)
        except Exception as e:
            auc = 0.0

        if val['trans'] == 0:
            diff = 0
            cvr = 0
        else:
            diff = (val['pcvr'] - val['trans']) * 1.0 / val['trans']
            cvr = val['trans'] * 1.0 / val['click']
    
        plid_html_table += "<tr><td>%s</td>\
                    <td>%s</td>\
                    <td>%s</td>\
                    <td>%.2f</td>\
                    <td>%.2f%%</td>\
                    <td>%.2f%%</td>\
                    <td>%.3f</td>\
                    <td>%.4f</td>\
                  </tr>" % \
                (model_ver, val['click'], val['trans'], val['pcvr'], cvr * 100, diff * 100, val['cost'], auc)
plid_html_table += "</table>"

plan_html_table = "<h3>分plan指标情况: </h3><table border=\"2\", width=\"100%\">\
<tr>\
<td>planid</td>\
<td>模型</td>\
<td>点击</td>\
<td>转化</td>\
<td>预测转化</td>\
<td>转化率</td>\
<td>偏差</td>\
<td>扣费</td>\
</tr>"
plan_click_trans_sort = sorted(plan_click_trans.items(), \
    key=lambda d: sum([ value['click'] for key, value in d[1].items() ]), reverse=True)
for key, value in plan_click_trans_sort: #plan_click_trans.items():
    line_num = len(value)
    plan_html_table += "<tr><td rowspan=\"%s\">%s</td>\
                  </tr>" % \
                (line_num+1, key)
 
    for model_ver, val in value.items():
        if val['trans'] == 0:
            diff = 0
            cvr = 0
        else:
            diff = (val['pcvr'] - val['trans']) * 1.0 / val['trans']
            cvr = val['trans'] * 1.0 / val['click']
    
        plan_html_table += "<tr><td>%s</td>\
                    <td>%s</td>\
                    <td>%s</td>\
                    <td>%.2f</td>\
                    <td>%.2f%%</td>\
                    <td>%.2f%%</td>\
                    <td>%.3f</td>\
                  </tr>" % \
                (model_ver, val['click'], val['trans'], val['pcvr'], cvr * 100, diff * 100, val['cost'])
plan_html_table += "</table>"

qqplot_table = "<h3>模型分桶偏差情况: </h3><table border=\"2\", width=\"100%\">\
<tr>\
<td>模型</td>\
<td>桶号</td>\
<td>点击</td>\
<td>预测转化</td>\
<td>实际转化</td>\
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

hour_html_table = "<h3>分天指标情况: </h3><table border=\"2\", width=\"100%\">\
<tr>\
<td>模型</td>\
<td>时间</td>\
<td>点击</td>\
<td>转化</td>\
<td>预测转化</td>\
<td>转化率</td>\
<td>偏差</td>\
<td>COPC</td>\
</tr>"

for key, value in hour_click_trans.items():
    line_num = len(value)
    hour_html_table += "<tr><td rowspan=\"%s\">%s</td>\
                  </tr>" % \
                (line_num+1, key)
    value_sort = sorted(value.items(), key=lambda d: int(d[0])) 
    for hour, val in value_sort:
        if val['trans'] == 0:
            diff = 0
            ctr = 0
            copc = 0
        else:
            diff = (val['pcvr'] - val['trans']) * 1.0 / val['trans']
            copc = val['pcvr'] * 1.0 / val['trans']
            ctr = val['trans'] * 1.0 / val['click']
    
        hour_html_table += "<tr><td>%s</td>\
                    <td>%s</td>\
                    <td>%s</td>\
                    <td>%.2f</td>\
                    <td>%.2f%%</td>\
                    <td>%.2f%%</td>\
                    <td>%.4f</td>\
                  </tr>" % \
                (hour, val['click'], val['trans'], val['pcvr'], ctr * 100, diff * 100, copc)
hour_html_table += "</table>"
table = html_table + plid_html_table + plan_html_table + hour_html_table
mailsender = MailSender()
mailsender.init()
mailsender.send_email(subject="%s Naga DSP CVR Model Online Abtest Report" % sys.argv[1], msg=table)
