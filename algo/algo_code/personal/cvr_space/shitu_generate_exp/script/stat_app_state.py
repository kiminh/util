import json
import sys

stat_dict = {}
for raw in sys.stdin:
    line_json = json.loads(raw.strip("\n\r "))
    promoted_app = line_json['promoted_app']
    time_ = int(line_json['time'])
    click = line_json['click']
    trans = line_json['trans']

    if promoted_app not in stat_dict:
        stat_dict[promoted_app] = {}
    if time_ not in stat_dict[promoted_app]:
        stat_dict[promoted_app][time_] = {}
    stat_dict[promoted_app][time_]['click'] = click
    stat_dict[promoted_app][time_]['trans'] = trans

res_dict = {}
for app, value in stat_dict.items():
    value_sort = sorted(value.items(), key=lambda d:d[0], reverse=True)
    if app not in res_dict:
        res_dict[app] = {}
    for i, item in enumerate(value_sort):
        before7days_click = sum([ x[1]['click'] for x in value_sort[i+1:i+8] ])
        before3days_click = sum([ x[1]['click'] for x in value_sort[i+1:i+4] ])
        before1days_click = sum([ x[1]['click'] for x in value_sort[i+1:i+2] ])
        before7days_trans = sum([ x[1]['trans'] for x in value_sort[i+1:i+8] ])
        before3days_trans = sum([ x[1]['trans'] for x in value_sort[i+1:i+4] ])
        before1days_trans = sum([ x[1]['trans'] for x in value_sort[i+1:i+2] ])
        time_ = item[0]
        if time_ not in res_dict:
            res_dict[app][time_] = {}
        res_dict[app][time_]['before7days_click'] = before7days_click
        res_dict[app][time_]['before3days_click'] = before3days_click
        res_dict[app][time_]['before1days_click'] = before1days_click
        res_dict[app][time_]['before7days_trans'] = before7days_trans
        res_dict[app][time_]['before3days_trans'] = before3days_trans
        res_dict[app][time_]['before1days_trans'] = before1days_trans

json.dump(res_dict, open('app_stat.json', 'w'), indent=4)
