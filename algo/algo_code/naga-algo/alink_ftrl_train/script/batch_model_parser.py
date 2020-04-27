import csv
import json
import sys
from sklearn.metrics import roc_auc_score

if len(sys.argv) < 3:
    print("Usage: get_metric.py model.csv model.dat")
    exit(1)

model_info = ""
with open(sys.argv[1], 'r') as f:
    reader = csv.reader(f, quotechar='"')
    for index, row in enumerate(reader):
        if index == 0:
            continue
        model_info += row[1]
        if index % 100 == 0:
            print("parser %s lines" % index)

model_json = json.loads(model_info)
data = model_json['coefVector']['data']
with open(sys.argv[2], 'w') as f_out:
    for key, weight in enumerate(data):
        f_out.write('%s %s\n' % (key, weight))
