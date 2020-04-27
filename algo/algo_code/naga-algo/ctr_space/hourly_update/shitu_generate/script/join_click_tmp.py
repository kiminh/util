import json
import sys
if len(sys.argv) < 4:
    print("Usage: python join_click.py pre_shitu_log click_log shitu_log")
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
        print(raw_line)
        print("get click data error [%s]" % e)
        continue

with open(sys.argv[3], 'w') as f_out:
    for raw_line in open(sys.argv[1]):
        try:
            line_json = json.loads(raw_line.strip("\n\r "))
            if 'reqid' not in line_json['ed_log']:
                continue
            if 'adid' not in line_json['ed_log']:
                continue
            adid = line_json['ed_log']['adid']
            reqid = line_json['ed_log']['reqid']
            key = '%s_%s' % (reqid, adid)

            pre_label = 1 if 'click_log' in line_json else 0
            label = 0
            cost = 0.0
            if key in click_dict:
                label = 1
                cost = click_dict[key]['cost']
        
            if pre_label == 0 and label == 1:
                line_json['click_log'] = 1
                line_json['click_cost'] = cost
                print("hit %s!" % reqid)
            f_out.write("%s\n" % json.dumps(line_json))
        except Exception as e:
            print("join click error [%s]" % e)
            continue
