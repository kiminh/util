import sys

train_token_dict = dict()
for raw in open(sys.argv[1]):
    felds = raw.strip().split()
    is_trans = int(felds[0])
    for token in felds[1:]:
        if token not in train_token_dict:
            train_token_dict[token] = {}
            train_token_dict[token]['click'] = 0
            train_token_dict[token]['trans'] = 0
        train_token_dict[token]['click'] += 1
        train_token_dict[token]['trans'] += is_trans

test_token_dict = dict()
for raw in open(sys.argv[2]):
    felds = raw.strip().split()
    is_trans = int(felds[0])
    for token in felds[1:]:
        if token not in test_token_dict:
            test_token_dict[token] = {}
            test_token_dict[token]['click'] = 0
            test_token_dict[token]['trans'] = 0
        test_token_dict[token]['click'] += 1
        test_token_dict[token]['trans'] += is_trans

for key, value in test_token_dict.items():
    if key in train_token_dict:
        train_click = train_token_dict[key]['click']
        train_trans = train_token_dict[key]['trans']
        test_click = value['click']
        test_trans = value['trans']

        train_cvr = train_trans * 1.0 / train_click
        test_cvr = test_trans * 1.0 / test_click

        print("train_click=%s, train_trans=%s, test_click=%s, test_trans=%s, train_cvr=%s, test_cvr=%s" \
            % (train_click, train_trans, test_click, test_trans, train_cvr, test_cvr))
    
