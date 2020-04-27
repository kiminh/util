import json
import sys

if len(sys.argv) < 5:
    print("Usage: python join_click.py show_log pre_click_log click_log shitu_log")
    exit(1)

click_dict = dict()
for raw_line in open(sys.argv[2]):
    try:
        line_json = json.loads(raw_line.strip("\n\r "))
        if 'reqid' in line_json and 'adid' in line_json:
            reqid = line_json['reqid']
            adid = line_json['adid']
            key = "%s_%s" % (reqid, adid)
            if key not in click_dict:
                click_dict[key] = {}
            click_dict[key]['cost'] = line_json['cost']
    except Exception as e:
        print raw_line
        print("get click data error [%s]" % e)
        continue

for raw_line in open(sys.argv[3]):
    try:
        line_json = json.loads(raw_line.strip("\n\r "))
        if 'reqid' in line_json and 'adid' in line_json:
            reqid = line_json['reqid']
            adid = line_json['adid']
            key = "%s_%s" % (reqid, adid)
            if key not in click_dict:
                click_dict[key] = {}
            click_dict[key]['cost'] = line_json['cost']
    except Exception as e:
        print raw_line
        print("get click data error [%s]" % e)
        continue

ed_reqid = set()
with open(sys.argv[4], 'w') as f_out:
    for raw_line in open(sys.argv[1]):
        try:
            line_json = json.loads(raw_line.strip("\n\r "))
            if 'reqid' not in line_json:
                continue
            reqid = line_json['reqid']
            if 'adid' not in line_json:
                continue
            adid = line_json['adid']
            key = "%s_%s" % (reqid, adid)

            if key in ed_reqid:
                continue
            ed_reqid.add(key)
            
            label = 0
            cost = 0.0
            if key in click_dict:
                label = 1
                cost = click_dict[key]['cost']

            log = {}
            log['click_cost'] = cost
            log['ed_log'] = line_json
            if label == 1:
                log['click_log'] = label
            log['ed_time'] = line_json['log_time']

            f_out.write("%s\n" % json.dumps(log))
        except Exception as e:
            print(raw_line)
            print("join click error [%s]" % e)
            continue
