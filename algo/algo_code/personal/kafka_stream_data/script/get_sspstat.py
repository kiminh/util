import json
import sys

for line in sys.stdin:
    felds = line.strip('\r\n').split('\001')
    reqid = felds[13]
    if reqid == '\\N' or reqid == '':
        continue
    event_type = felds[15]
    #DOWNSTART_URL=11, DOWNSUCC_URL=12, 
    #INSTALLSTART_URL=13, INSTALLSUCC_URL=14
    if event_type != '14': 
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
    #  continue
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
    print json.dumps(log)
