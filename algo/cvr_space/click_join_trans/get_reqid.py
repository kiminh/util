import base64
import json
import sys

reqid_set = set()
for raw in open(sys.argv[1]):
    line_json = json.loads(raw.strip("\n\r "))
    log = line_json['request']['value']['DSPTRANSFORM_LOG']
    spam = log['spam']
    if spam != 0:
        continue
    reqid = ''
    if 'reqid' not in log:
        try:
            decode_str = base64.b64decode(log['clickid'])
        except:
            print line_json
            continue
        reqid = decode_str.split('_')[0]
    else:
        reqid = log['reqid']
    if reqid not in reqid_set:
        reqid_set.add(reqid)
    else:
        print line_json

print reqid_set
print len(reqid_set)
