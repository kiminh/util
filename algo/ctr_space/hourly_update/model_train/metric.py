import json
import sys

y_true = []
y_pred = []

for raw in open(sys.argv[1]):
    line_json = json.loads(raw.strip("\n\r "))
    if 'click_log' in raw:
        y_true.append(1)
    else:
        y_true.append(0)

    pctr = line_json['ed_log']['pctr_cal']
    y_pred.append(float(pctr))

from sklearn.metrics import roc_auc_score
print(roc_auc_score(y_true, y_pred))
