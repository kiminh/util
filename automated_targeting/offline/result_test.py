import json
import random
import sys
res_dict = {}
clc_score = {}
clc_num = 0
for i, raw_line in enumerate(open(sys.argv[1]), start=1):
    line_json = json.loads(raw_line.strip("\n\r "))
    isclick = int(line_json['isclick'])
    isconv = int(line_json['isconv'])
    score = float(line_json['rating']) 
    gaid = line_json['gaid']
    if gaid not in clc_score:
        clc_score[gaid] = {}
        clc_score[gaid]['clc'] = 0
        clc_score[gaid]['ed'] = 0
        clc_score[gaid]['conv'] = 0
        clc_score[gaid]['score'] = score
    clc_score[gaid]['ed'] += 1
    clc_score[gaid]['clc'] += isclick
    clc_score[gaid]['conv'] += isconv
    #if i % 10000 == 0:
    #    print i

res_sorted = sorted(clc_score.items(), key=lambda x:x[1]['score'], reverse=True)
ed = 0
conv = 0
click = 0
i = 0
clc_num = sum([ 1 if val['clc'] > 0 else 0 for idx, val in res_sorted ])#len(res_sorted)
user_num = len(res_sorted)
#print "clc num: %d, all user num %d" % (clc_num, user_num) 
recall_clc_num = 0
#print "precent,sel_top_uv,clck,ed,recall_clc_num,CTR,recall"
for idx, val in res_sorted:
    ed += val['ed']
    click += val['clc']
    conv += val['conv']
    score = val['score']
    i += 1
    if val['clc'] != 0:
        recall_clc_num += 1
    
    #print "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s" % (idx,i*1.0/user_num, score, click, ed, recall_clc_num, click*1.0/ed, recall_clc_num*1.0/clc_num, val['ed'], conv * 1.0 / (click + 0.01))
    print "%s,%s,%s,%s,%s" % (idx, i*1.0/user_num, score, click*1.0/ed, conv * 1.0 / (click + 0.01))
    #print "%s %s" % (idx, score)
