import json
import sys

for raw in sys.stdin:
    try:
        line_json = json.loads(raw.strip("\n\r "))
        promoted_app = ''
        if 'promoted_app' in line_json:
            promoted_app = line_json['promoted_app']
        if promoted_app == '':
            continue
    
        print("%s" % json.dumps(line_json))
    except Exception as e:
        print raw
        continue
