import sys 
import json
from sklearn.metrics import roc_auc_score, log_loss
from collections import defaultdict
import numpy as np

def cal_group_auc(labels, preds, user_id_list):
    """Calculate group auc"""

    print('*' * 50)
    if len(user_id_list) != len(labels):
        raise ValueError(
            "impression id num should equal to the sample num," \
            "impression id num is {0}".format(len(user_id_list)))
    group_score = defaultdict(lambda: [])
    group_truth = defaultdict(lambda: [])
    for idx, truth in enumerate(labels):
        user_id = user_id_list[idx]
        score = preds[idx]
        truth = labels[idx]
        group_score[user_id].append(score)
        group_truth[user_id].append(truth)

    group_flag = defaultdict(lambda: False)
    for user_id in set(user_id_list):
        truths = group_truth[user_id]
        flag = False
        for i in range(len(truths) - 1):
            if truths[i] != truths[i + 1]:
                flag = True
                break
        group_flag[user_id] = flag

    impression_total = 0
    total_auc = 0
    #
    for user_id in group_flag:
        if group_flag[user_id]:
            auc = roc_auc_score(np.asarray(group_truth[user_id]), np.asarray(group_score[user_id]))
            total_auc += auc * len(group_truth[user_id])
            impression_total += len(group_truth[user_id])
    group_auc = float(total_auc) / impression_total
    group_auc = round(group_auc, 4)
    return group_auc

y_true = []
y_score = []
user_id_list = []
ed_cnt = 0
clk_cnt = 0
for raw_line in sys.stdin:
    line = raw_line.strip("\n\r ")
    ld = json.loads(line)
    ed_log = ld['ed_log']
    spam = ed_log['spam']
    if spam != 0:
        continue

    label = 1 if 'click_log' in ld else 0
    adpctr = float(ed_log['adpctr'])
    identifier = ed_log['identifier']
    user_id_list.append(identifier)
    y_true.append(label)
    y_score.append(adpctr)
    ed_cnt += 1
    clk_cnt += label

print(sum(y_true), sum(y_score))
print("model copc: %g" % (sum(y_score) / sum(y_true)))
print("model diff: %g" % ((sum(y_score) - sum(y_true)) / sum(y_true)))
print("model auc: %g, gauc: %g" % (roc_auc_score(y_true, y_score), cal_group_auc(y_true, y_score, user_id_list)))
print("ed: %g, clk: %g, ctr : %g" % (ed_cnt, clk_cnt, clk_cnt*1.0/ed_cnt))
print("log loss: %s" % log_loss(y_true, y_score))
