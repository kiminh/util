import sys
import numpy as np
import xgboost as xgb
from xgboost import XGBClassifier
from sklearn.feature_extraction.text import CountVectorizer
import pandas as pd
features = []
for raw_line in open(sys.argv[1]):
    flds = raw_line.strip("\n\r ").split("\001")
    features.append(flds[0])
print(features)

data_x = []
data_y = []
for raw_line in open(sys.argv[2]):
    line_sp = raw_line.strip("\n\r ").split()
    data_y.append(int(line_sp[0]))
    data_x.append([val for val in line_sp[1:]])

df = pd.DataFrame(np.array(data_x))
for col in df.columns:
    uniq_val = df[col].unique().tolist()
    val_map = dict(zip(uniq_val, range(0, len(uniq_val))))
    df[col] = df[col].map(val_map)
m, n = df.shape
X = df.values
y = np.array(data_y)

train_size = int(m * 0.8)
X_train, X_valid = X[:train_size], X[train_size+1:]
y_train, y_valid = y[:train_size], y[train_size+1:]
model = XGBClassifier(learning_rate=0.1,
                      n_estimators=100,
                      max_depth=5,
                      min_child_weight = 1,
                      gamma=0.,
                      #subsample=0.8,
                      #colsample_btree=0.8,
                      objective='binary:logistic',
                      random_state=27)
model.fit(X_train,
          y_train,
          eval_set = [(X_valid, y_valid)],
          eval_metric = "logloss", 
          early_stopping_rounds = 10, 
          verbose = True)

fea_imp = dict(zip(features, model.feature_importances_))
fea_imp_sort = sorted(fea_imp.items(), key=lambda x: x[1], reverse=True)
with open('feature_importances.csv', 'w') as f_out:
    for fea, imp_val in fea_imp_sort:
        f_out.write("%s,%s\n" % (fea, imp_val))
