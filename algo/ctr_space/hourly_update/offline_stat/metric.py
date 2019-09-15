import json
import sys
import math
class AUC:
    def __init__(self, bs=10000):
        self.bucket_size = bs
        self.ed_bucket = [0] * self.bucket_size
        self.click_bucket = [0] * self.bucket_size

    def get_pos(self, pctr):
        bs = self.bucket_size
        p = float(pctr)
        p = min(1 - 1.0 / bs, max(0, p))
        pos = int(math.floor(p * bs))
        return pos

    def add_join_sample(self, pctr, label):
        pos = self.get_pos(pctr)
        if label == 1:
            self.click_bucket[pos] += 1
        self.ed_bucket[pos] += 1
        return

    def add_unjoin_sample(self, pctr, label):
        pos = self.get_pos(pctr)
        if label == 1:
            self.click_bucket[pos] += 1
        else:
            self.ed_bucket[pos] += 1
        return

    def cal_auc(self):
        pN = 0
        nN = 0
        area = 0.0
        r = 10000
        bs = self.bucket_size
        for i in range(bs):
            click = self.click_bucket[bs - 1 - i]
            noclick = self.ed_bucket[bs - 1 - i] - click
            area += (pN + 0.5 * click) * noclick / r
            pN += click
            nN += noclick
        if pN == 0 or nN == 0:
            return -1
        return area / pN * r / nN

auc = AUC()

plid_ed_clk = {}
with open('../model_train/script/plid_edclk.json') as f_in:
    for raw in f_in:
        line_json = json.loads(raw.strip("\n\r "))
        if 'plid' not in line_json:
            continue
        plid = line_json['plid']
        ed = line_json['ed']
        clk = line_json['click']
        
        if clk * 1.0 / ed > 0.5:
            if plid not in plid_ed_clk:
                plid_ed_clk[plid] = {}
            plid_ed_clk[plid]['ed'] = ed
            plid_ed_clk[plid]['clk'] = clk
print(plid_ed_clk)
y_true = []
y_pred = []

for raw in sys.stdin:
    line_json = json.loads(raw.strip("\n\r "))
    if 'plid' not in line_json['ed_log']:
        continue
    plid = line_json['ed_log']['plid']
    if plid in plid_ed_clk:
        continue
    if 'click_log' in raw:
        y_true.append(1)
    else:
        y_true.append(0)

    pctr = line_json['ed_log']['pctr_cal']
    y_pred.append(float(pctr))

from sklearn.metrics import roc_auc_score
print(roc_auc_score(y_true, y_pred))
