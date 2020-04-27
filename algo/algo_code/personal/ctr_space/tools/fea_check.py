import sys
import json

spe_type = 'pctr'
adid_slot = '1043'

reqid_set = set()
fea_dict = {}
with open(sys.argv[1]) as f_in:
    for raw in f_in:
        felds = raw.strip("\n\r ").split()
        ocpc_log = felds[-1]
        ocpc_json = json.loads(ocpc_log)
        type_ = ocpc_json['type']

        if type_ != spe_type:
            continue    
        
        ad_model_log = ocpc_json['ad_model_log']
        winad_model_info = ad_model_log['winad_model_info']
        win_adid = ad_model_log['win_adid'].lower()
        
        reqid = ocpc_json['reqid']
        reqid_set.add(reqid)

        for log in ad_model_log.keys():
            if adid_slot in log and win_adid not in log:
                continue
            if log not in fea_dict:
                fea_dict[log] = 0
            fea_dict[log] += 1

f_out = open('fea_dict.txt', 'w')
fea_dict_sort = sorted(fea_dict.items(), key=lambda d:d[0], reverse=False)
for key, value in fea_dict_sort:
    f_out.write('%s\t%s\n' % (key, value))
f_out.close()

with open(sys.argv[2]) as f_in, with open(sys.argv[3], 'w') as f_out:
    for raw in f_in:
        lj = json.loads(raw.strip("\n\r "))
        ed_log = lj['ed_log']
        reqid = ed_log['reqid']
        if reqid not in reqid_set:
            continue
        f_out.write(raw)

