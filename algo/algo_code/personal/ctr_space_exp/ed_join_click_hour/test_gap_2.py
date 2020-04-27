import json
import sys

src_reqid = set()
for raw_line in open(sys.argv[1]):
    line_json = json.loads(raw_line.strip("\n\r "))
    #dsp_log = line_json['request']['value']['DSPED_LOG']
    #reqid = dsp_log['reqid']
    reqid = line_json['ed_log']['reqid']
    #spam = dsp_log['spam']
    #if spam != 0:
    #    continue
    src_reqid.add(reqid)

des_reqid = set()
for raw_line in open(sys.argv[2]):
    line_json = json.loads(raw_line.strip("\n\r "))
    reqid = line_json['ed_log']['reqid']
    if reqid in des_reqid:
        print reqid
    des_reqid.add(reqid)

print len(src_reqid), len(des_reqid), len(src_reqid & des_reqid)
print src_reqid - des_reqid 
