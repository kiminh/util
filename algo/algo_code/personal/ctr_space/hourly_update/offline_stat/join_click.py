import json
import sys

if len(sys.argv) < 4:
    print("Usage: python join_click.py show_log click_log shitu_log")
    exit(1)

click_set = set()
for raw_line in open(sys.argv[2]):
    try:
        line_json = json.loads(raw_line.strip("\n\r "))
        if 'reqid' in line_json:
            click_set.add(line_json['reqid'])
    except Exception as e:
        print("get click data error [%s]" % e)
        print(raw_line)
        continue

ed_reqid = set()
with open(sys.argv[3], 'w') as f_out:
    for raw_line in open(sys.argv[1]):
        try:
            line_json = json.loads(raw_line.strip("\n\r "))
            if 'reqid' not in line_json:
                continue
            reqid = line_json['reqid']
            if reqid in ed_reqid:
                continue
            ed_reqid.add(reqid)
            label = 0
            if reqid in click_set:
                label = 1
            log = {}
            log['ed_log'] = line_json
            if label == 1:
                log['click_log'] = label
            log['ed_time'] = line_json['log_time']

            f_out.write("%s\n" % json.dumps(log))
        except Exception as e:
            print("join click error [%s]" % e)
            print(raw_line)
            continue
