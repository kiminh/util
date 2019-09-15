import json
import sys

for raw in open(sys.argv[1]):
    line_json = json.loads(raw.strip("\n\r "))
    lg = line_json['request']['value']['DSPCLICK_LOG']
    promoted_app = lg.get('promoted_app', '')
    if promoted_app != '':    
        print "%s" % json.dumps(lg)
