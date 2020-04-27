import json
import sys
import time

data_dict = {}
for raw in open(sys.argv[1]):
    line_json = json.loads(raw.strip("\n\r "))
    click_time = line_json['click_time']
    if 'trans_log' not in line_json:
        continue
    rawlog = line_json['trans_log']['rawlog']
    event_type = ''
    date_ = ''
    time_ = ''
    for rl in rawlog:
        if rl['event_type'] != '0':
            continue
        event_type = rl['event_type']
        date_ = rl['date']
        time_ = rl['time']

    if event_type == '':
        continue
    dl = date_.split('-')
    for i, d in enumerate(dl):
        if len(d) == 1:
            dl[i] = '%s%s' % (0, dl[i])
    date_ = ''.join(dl)

    tl = time_.split(":")
    for i, t in enumerate(tl):
        if len(t) == 1:
            tl[i] = '%s%s' % (0, tl[i])
    time_ = ''.join(tl)
        
    trans_time = '%s%s' % (date_, time_) #line_json['trans_time']
    promoted_app = line_json['click_log']['promoted_app']
    click_ta = time.strptime(click_time, "%Y%m%d%H%M%S")
    trans_ta = time.strptime(trans_time, "%Y%m%d%H%M%S")
    delay_time = int(time.mktime(trans_ta)) - int(time.mktime(click_ta))
    delay_time = int(delay_time / 60)
    if delay_time < 0:
        continue
    if promoted_app not in data_dict:
        data_dict[promoted_app] = {}
        data_dict[promoted_app][delay_time] = 0
        data_dict[promoted_app]['all'] = 0
    if delay_time not in data_dict[promoted_app]:
        data_dict[promoted_app][delay_time] = 0
    data_dict[promoted_app][delay_time] += 1
    data_dict[promoted_app]['all'] += 1

f_out = open(sys.argv[2], 'w')

for key, value in data_dict.items():
    value_sort = sorted(value.items(), key=lambda d: d[0])
    for k, v in value_sort[:5]:
        if k == 'all':
            continue
        f_out.write("%s %s %s %4.3f\n" % (key, k, v, float(v)/float(data_dict[key]['all'])))
f_out.close()
