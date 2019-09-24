import sys
import json
import random

gaid_list = []
for i, raw_line in enumerate(open(sys.argv[1])):
    line_json = json.loads(raw_line.strip("\n\r "))
    idx = line_json['idx']
    gaid = line_json['gaid']
    gaid_list.append(gaid)
    if i % 10000 == 0:
        print i

size = len(gaid_list)
fetch_size = int(size * 0.16)
print size, fetch_size
base_user = random.sample(gaid_list, fetch_size)
exp_user = gaid_list[:fetch_size]

with open('exp.txt', 'w') as f_exp, open('base.txt', 'w') as f_base:
    for gaid in base_user:
        f_base.write("%s\n" % gaid)
    for gaid in exp_user:
        f_exp.write("%s\n" % gaid)
