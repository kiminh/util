import sys
import json
from collections import defaultdict
import base64

stat_info = defaultdict(lambda:0)
for line in sys.stdin:
    if line.find("DSPCLICK") != -1:
        input_src = 'click'
    elif line.find("DSPTRANSFORM") != -1:
        input_src =  'trans'
    elif line.find("DSPREQUEST") != -1:
        input_src =  'request'
    else:
        continue

    try:
        line = line.strip('\r\n')
        ld = json.loads(line)

        if input_src == 'click':
            log = ld['request']['value']['DSPCLICK_LOG']
        elif input_src == 'trans':
            log = ld['request']['value']['DSPTRANSFORM_LOG']
        elif input_src == 'request':
            log = ld['request']['value']['DSPREQUEST_LOG']
        
        time = ld['time']
        date = time[0:8]
        
        #transform handler unjoin transform, maybe click miss
        #clickid: sniper:1562357835086-ec990fc6-696f-499b-82d5-360b7f70da29
        #if input_src == 'trans' and 'clkid_resolved_error' in log:
        if input_src == 'trans' and 'reqid' not in log:
            clickid = log.get('clickid', '')
            decode_str = base64.b64decode(clickid)
            reqid = decode_str.split('_')[0]
            log['reqid'] = reqid
        else:
            reqid = log.get('reqid', '')
        spam = log.get('spam', '')
        value_str = json.dumps(log)
        if not reqid: continue
        
        print "%s\t%s\t%s\t%s\t%s" % (reqid, input_src,value_str, time, spam)
    except:
        pass
