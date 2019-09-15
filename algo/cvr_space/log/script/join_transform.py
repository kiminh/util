import json
import sys
import base64

if len(sys.argv) < 4:
    print("Usage: python join_transform.py click_log transform_log shitu_log")
    exit(1)

reqid_dict = dict()
with open(sys.argv[2]) as f_in:
    for raw in f_in:
        try:
            line_json = json.loads(raw.strip("\n\r "))        
            if 'request' in line_json:
                line_json = line_json['request']['value']['DSPTRANSFORM_LOG']
            event_type = ''
            if 'event_type' in line_json:
                event_type = line_json['event_type']
            if event_type != '0':
                continue
            if 'reqid' not in line_json:
                if 'clickid' not in line_json:
                    continue
                clickid = line_json['clickid']
                decode_str = base64.b64decode(clickid)
                reqid = decode_str.split('_')[0]
            else:
                reqid = line_json['reqid']
            cost = line_json.get('cost', 0)
            reqid_dict[reqid] = {}
            reqid_dict[reqid]['cost'] = cost
        except Exception as e:
            continue

click_reqid = set()
with open(sys.argv[3], 'w') as f_out:
    for raw in open(sys.argv[1]):
        try:
            line_json = json.loads(raw.strip("\n\r "))

            reqid = line_json.get('reqid', '')
            if reqid in click_reqid:
                continue
            click_reqid.add(reqid)
            trans_log = 1 if reqid in reqid_dict else 0
            trans_cost = 0 if trans_log == 0 else reqid_dict[reqid]['cost']
            click_cost = line_json.get('cost', 0)
            log = {}
            log['click_log'] = line_json
            log['clict_cost'] = click_cost
            log['trans_log'] = trans_log
            log['trans_cost'] = trans_cost

            f_out.write("%s\n" % json.dumps(log))
        except Exception as e:
            #print(raw)
            continue
