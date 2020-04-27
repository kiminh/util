import sys
import json
from collections import defaultdict

stat_info = defaultdict(lambda:0)
for line in sys.stdin:
    if line.find("DSPCLICK") != -1:
        input_src = 'click'
    elif line.find("DSPED") != -1:
        input_src =  'ed'
    elif line.find("DSPREQUEST") != -1:
        input_src =  'request'
    else:
        continue

    try:
        line = line.strip('\r\n')
        ld = json.loads(line)
        if input_src == 'click':
            log = ld['request']['value']['DSPCLICK_LOG']
        elif input_src == 'ed':
            log = ld['request']['value']['DSPED_LOG']
        elif input_src == 'request':
            log = ld['request']['value']['DSPREQUEST_LOG']
        time = ld['time']
        reqid = log.get('reqid', '')
        if reqid == '':
            continue
        adid = log.get('adid', '')
        spam = log.get('spam', 0)
        if input_src in ['click', 'ed'] and spam != 0:
            continue
        value_str = json.dumps(log)
        key = '%s_%s' % (reqid, adid)
        print "%s\t%s\t%s\t%s" % (key, input_src, value_str, time)
    except Exception as e:
        pass
