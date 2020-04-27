import csv
import json
import sys
from sklearn.metrics import roc_auc_score

if len(sys.argv) < 2:
    print("Usage: get_metric.py pred.csv")
    exit(1)
labels = []
preds = []
with open(sys.argv[1], 'r') as f:
    reader = csv.reader(f, quotechar='"')
    for row in reader:
        labels.append(float(row[0]))
        pred = json.loads(row[1])
        preds.append(float(pred["1"]))

print("click: %s, pclick: %s " % (sum(labels), sum(preds)))
print("model copc: %g" % (sum(labels) / sum(preds)))
print("model diff: %g" % ((sum(preds) - sum(labels)) / sum(labels)))
print("model auc: %g" % (roc_auc_score(labels, preds)))
