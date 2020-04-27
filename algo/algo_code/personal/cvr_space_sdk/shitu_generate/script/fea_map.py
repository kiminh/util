import json
import sys

if len(sys.argv) < 3:
    print("Usage: python fea_map.py train_file")
    exit(1)

has_valid = False
if len(sys.argv) == 3:
    has_valid = True
token2idx = {}
cnt = 0
for raw_line in open(sys.argv[1]):
    felds = raw_line.strip().split(" ")
    for token in felds[1:]:
        if token not in token2idx:
            token2idx[token] = cnt
            cnt+=1
if has_valid:
    for raw_line in open(sys.argv[2]):
        felds = raw_line.strip().split(" ")
        for token in felds[1:]:
            if token not in token2idx:
                token2idx[token] = cnt
                cnt+=1

train_file = '%s.map' % (sys.argv[1])
with open(train_file, 'w') as f_out_tr:
    for raw_line in open(sys.argv[1]):
        felds = raw_line.strip().split(" ")
        f_out_tr.write("%s " % felds[0])
        for token in felds[1:]:
            f_out_tr.write("%s " % token2idx[token])
        f_out_tr.write("\n")

if has_valid:
    test_file = '%s.map' % (sys.argv[2])
    with open(test_file, 'w') as f_out_te:
        for raw_line in open(sys.argv[2]):
            felds = raw_line.strip().split(" ")
            f_out_te.write("%s " % felds[0])  
            for token in felds[1:]:
                f_out_te.write("%s " % token2idx[token])  
            f_out_te.write("\n")

json.dump(token2idx, open('token2idx.json', 'w'), indent=4)
