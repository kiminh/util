import sys
import json
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from scipy.sparse import csr_matrix

if len(sys.argv) < 2:
    print("Usage:python py train_data")
    exit(1)

has_valid = False
if len(sys.argv) == 3:
    has_valid = True

idx2token = {}
token2idx = {}

X_data = []
X_row = []
X_col = []
train_size = 0
y_train = []
cnt = 0

for i, raw in enumerate(open(sys.argv[1])):
    field = raw.strip().split()
    y_train.append(int(field[0]))
    ins = []
    for token in field[1:]:
        if token in token2idx:
            idx_ = token2idx[token]
        else:
            idx_ = cnt
            token2idx[token] = idx_
            cnt += 1
        X_col.append(idx_) 
    X_row.extend([i] * len(field[1:]))
    train_size += 1

Xv_row = []
Xv_col = []
y_valid = []
valid_size = 0
if has_valid:
   for i, raw in enumerate(open(sys.argv[2])):
        field = raw.strip().split()
        y_valid.append(int(field[0]))
        for token in field[1:]:
            if token in token2idx:
                idx_ = token2idx[token]
            else:
                idx_ = cnt
                token2idx[token] = idx_
                cnt += 1
            Xv_col.append(idx_)
        Xv_row.extend([i] * len(field[1:]))
        valid_size += 1

fea_num = max(X_col) + 1
if has_valid:
    fea_num = max(max(Xv_col), fea_num) + 1

X_train = csr_matrix(([1]*len(X_row), (X_row, X_col)), shape=(train_size, fea_num))
y_train = np.array(y_train)
if has_valid:
    X_valid = csr_matrix(([1]*len(Xv_row), (Xv_row, Xv_col)), shape=(valid_size, fea_num))
    y_valid = np.array(y_valid)

idx2token = dict((value, key) for key, value in token2idx.items())
clf = LogisticRegression(penalty='l1',
                         random_state=2019, 
                         solver='liblinear',
                         max_iter=300,
                         multi_class='ovr',
                         verbose=1,
                         fit_intercept=False,
                         tol=1e-4,
                         C=0.1).fit(X_train, y_train)
if has_valid:
    y_pred = clf.predict_proba(X_valid)[:,1]
    #for i in y_pred.tolist():
    #    print(i)
    auc = roc_auc_score(y_valid, y_pred)
    copc = np.sum(y_pred) / np.sum(y_valid)
    print("\nvalid: COPC=%s, auc=%s" % (copc, auc))

y_train_pred = clf.predict_proba(X_train)[:,1]
train_auc = roc_auc_score(y_train, y_train_pred)
train_copc = np.sum(y_train_pred) / np.sum(y_train)
print("train: COPC=%s, auc=%s" % (train_copc, train_auc))

with open('./model/lr_model.dat', 'w') as f_out:
    for idx, weight in enumerate(clf.coef_[0]):
        if weight == 0.0:
            continue
        f_out.write('%s %s\n' % (idx2token[idx], weight))
