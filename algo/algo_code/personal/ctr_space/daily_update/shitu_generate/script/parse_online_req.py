import json
import sys

for raw_line in open(sys.argv[1]):
    d = {}
    d['ed_log'] = {}
    line = raw_line.strip("\n\r ").split()
    for item in line:
        it_sp = item.split("=")
        if len(it_sp) != 2:
            continue
        if it_sp[0] == 'prt':
            it_sp[0] = 'reqprt' 
        if it_sp[0] == 'org_id':
            it_sp[0] = 'orgid'
        if it_sp[0] == 'app_id':
            it_sp[0] = 'appid'
        d['ed_log'][it_sp[0]] = it_sp[1] 
    print json.dumps(d)
