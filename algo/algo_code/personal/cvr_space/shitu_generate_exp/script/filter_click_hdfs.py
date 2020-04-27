import json
import sys

for raw in sys.stdin:
    try:
        line_json = json.loads(raw.strip("\n\r "))
        if 'click_log' not in line_json:
            continue
        click_log = line_json['click_log']
        promoted_app = ''
        if 'promoted_app' in click_log:
            promoted_app = click_log['promoted_app']
        if promoted_app == '':
            continue
    
        print("%s" % json.dumps(line_json))
    except Exception as e:
        print raw
        continue
