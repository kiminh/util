import sys
import json
import re

def get_pkgsize(rawlog):
    try:
        rawlog = rawlog.lower() 
        match_obj = re.search("[\d]+m|[\d]+k|[\d]+g", rawlog, re.I)
        if match_obj:
            suffix = rawlog[-1].lower()
            if suffix == 'k':
                return '1'
            elif suffix == 'g':
                return '3'
            elif suffix == 'm':
                pkgsize = int(rawlog[0:-1])
                if pkgsize < 20: return '1'
                elif pkgsize < 60: return '2'
                else: return '3'
        return 'unk' 
    except:
        return 'unk'

def format_str(rawlog):
    return rawlog.strip().lower().replace(' ', '#')

stat_info = {}
for line in sys.stdin:
    json_dict = json.loads(line.strip())
    if 'package_name' not in json_dict:
        continue
    pkg = format_str(json_dict['package_name'])
    trade_str = json_dict.get('category', '-')
    pkgtrade = format_str(trade_str)
    size_str = json_dict.get('size', '-')
    pkgsize = get_pkgsize(size_str)

    stat_info[pkg] = "%s#%s" % (pkgsize, pkgtrade)

for k,v in stat_info.iteritems():
    print "%s\t%s" % (k, v)
