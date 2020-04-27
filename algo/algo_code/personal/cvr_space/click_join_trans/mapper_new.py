import sys
import json
from collections import defaultdict
import base64

stat_info = defaultdict(lambda:0)
for line in sys.stdin:
    if line.find("DSPCLICK") != -1:
        input_src = 'click'
    elif line.find("DSPTRANSFORM") != -1:
        input_src = 'trans'
    elif line.find("DSPREQUEST") != -1:
        input_src = 'request'
    elif line.find("sspstat") != -1:
        input_src = 'sspstat'
    elif line.find("DSPBUILDSTATION_LOG") != -1:
        input_src = 'action'
    elif line.find("DSPSDKACTION_LOG") != -1:
        input_src = 'sdkaction'
    else:
        continue

    try:
        if input_src == 'sspstat':
            felds = line.strip('\r\n').split('\001')
            reqid = felds[13]
            if reqid == '\\N' or reqid == '':
                continue
            event_type = int(felds[15])
            #DOWNSTART_URL=11, DOWNSUCC_URL=12, 
            #INSTALLSTART_URL=13, INSTALLSUCC_URL=14
            if event_type < 11 or event_type > 14:
                continue
            spam = felds[19]
            time = felds[4]
            plid = felds[24]
            bundle = felds[31]
            reqprt = felds[30]
            idfa = felds[33]
            imei = felds[35]
            adid = felds[-1]
            #if adid == '\\N':
            #    continue
            print reqid, adid
            log = {
                'reqid': reqid,
                'event_type': str(event_type),
                'spam': spam,
                'time': time,
                'plid': plid,
                'bundle': bundle,
                'reqprt': reqprt,
                'idfa': idfa,
                'imei': imei,
                'adid': adid
            }
            key = '%s_%s' % (reqid, adid)
            value_str = json.dumps(log)
            print "%s\t%s\t%s\t%s\t%s" % (key, input_src, value_str, time, spam)
        elif input_src == 'action':
            line = line.strip('\r\n')
            log = json.loads(line)
            reqid = log.get('reqid', '')
            if reqid == '':
                continue
            adid = log.get('adid', '')
            time = log.get('time', '')
            value_str = json.dumps(log)
            key = '%s_%s' % (reqid, adid)
            spam = 0
            print "%s\t%s\t%s\t%s\t%s" % (key, input_src, value_str, time, spam)
        elif input_src == 'sdkaction':
            line = line.strip('\r\n')
            log = json.loads(line)
            reqid = log.get('reqid', '')
            if reqid == '':
                continue
            adid = log.get('adid', '')
            time = log.get('time', '')
            if time == '':
                time = '0'
            value_str = json.dumps(log)
            key = '%s_%s' % (reqid, adid)
            spam = 0
            print "%s\t%s\t%s\t%s\t%s" % (key, input_src, value_str, time, spam)
        else:
            line = line.strip('\r\n')
            print line
            ld = json.loads(line)
            print ld
            if input_src == 'click':
                log = json.loads(ld['extra']) #ld['request']['value']['DSPCLICK_LOG']
            elif input_src == 'trans':
                log = json.loads(ld['extra']) #ld['request']['value']['DSPTRANSFORM_LOG']
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
            adid = log.get('adid', '')
            spam = log.get('spam', '')
            key = '%s_%s' % (reqid, adid)
            value_str = json.dumps(log)
            if not reqid: continue

            print "%s\t%s\t%s\t%s\t%s" % (key, input_src,value_str, time, spam)
    except:
        pass
