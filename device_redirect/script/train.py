#coding:utf-8
import pandas as pd
import numpy as np

from sklearn import linear_model
from sklearn.model_selection import train_test_split
from sklearn import preprocessing
from sklearn.preprocessing import OneHotEncoder,LabelEncoder
from sklearn.linear_model import LogisticRegression, SGDClassifier
from sklearn.ensemble import GradientBoostingClassifier
import jieba, gensim
from sklearn import metrics
from scipy import sparse
import sys
from sklearn.utils import shuffle

filter_chars = [" ", "「", "」", "》", "\r", "\n", "\t", "。", "，", ",", "“", "！", "？", "！", "《", ":", "；", "：", "（", "）", "、"]
base_feature = ["screenhight", "screenheight", "carrier", "connectiontype", "model", 'model1', "osv", "devicetype", "ip18", "ip20", "ip24", "ip30", "channel", "cs"]
topk = 50000
threshold = 0.1

def word_segment(s):
    for c in filter_chars:
        s = s.replace(c, "")
    it = jieba.cut(s, cut_all=False)

    _ = []
    for w in it:
        _.append(w)
    result = ' '.join(_)
    try:
        result = result.encode("gbk")
        return result
    except:
        print result
        return ""

def addr2bin(addr): 
    items = [int(x) for x in addr.split(".")]  
    return sum([items[i] << [24, 16, 8, 0][i] for i in range(4)])

def ip_function(ip, l):
  #print ip
  bin_ip = addr2bin(ip.strip("\r\n "))
  ip_ = (bin_ip >> l)
  return ip_

def read_data(train_file, test_file):
    train_data = pd.read_csv(sys.argv[1])
    test_data = pd.read_csv(sys.argv[2])
    print train_data.shape, test_data.shape
    train_data = train_data.fillna("0")
    test_data = test_data.fillna("0")
    #label
    train_data["is_seek"] = 1
    test_data["is_seek"] = 0
    
    data = pd.concat([train_data, test_data])
    data = data.reset_index(drop=True)
    #ip feature
    data["ip18"] = data.apply(lambda x: ip_function(x["ip"], 18), axis=1)
    data["ip20"] = data.apply(lambda x: ip_function(x["ip"], 20), axis=1)
    data["ip24"] = data.apply(lambda x: ip_function(x["ip"], 24), axis=1)
    data["ip30"] = data.apply(lambda x: ip_function(x["ip"], 30), axis=1)
    
    print "Ip Feature Complete..."
    
    #title feature
    #data["title"] = train_data.apply(lambda x: word_segment(x["title"]), axis=1)
    #print "Word Segment Complete..."
    #model = Doc2Vec.load("model/model.txt")  
    #inferred_vector_dm = model.infer_vector(test_text)  

    return data

"""
one hot encoding
"""
def get_train_ins(data):
    le = LabelEncoder()
    for fea in base_feature:
        try:
            data[fea] = le.fit_transform(data[fea].apply(int))
        except:
            data[fea] = le.fit_transform(data[fea])
    print "label encoding complete"
    
    enc = OneHotEncoder()
    enc.fit(data['screenhight'].values.reshape(-1, 1))
    data_x = enc.transform(data['screenhight'].values.reshape(-1, 1))
    
    for fea in base_feature[1:]:
        enc.fit(data[fea].values.reshape(-1, 1))
        data_a = enc.transform(data[fea].values.reshape(-1, 1))
        data_x= sparse.hstack((data_x, data_a))
    print "one hot encoding complete"
    
    data_y = data["is_seek"]
    return data_x, data_y

def lr_train(X, y):
    print "LR Train ..."
    X_train, X_test, y_train, y_test = train_test_split(X, y)
    LR_model = LogisticRegression()
    LR_model.fit(X_train, y_train)
    pred = LR_model.predict_proba(X_test)[:,1]
    print "test auc is {}".format(metrics.roc_auc_score(y_test, pred))
    print "LR Train Success ..."

    X_train, y_train = shuffle(X, y, random_state=0)
    LR_model.fit(X_train, y_train)
    return LR_model

def get_target_audience(model, data, X, y, fout):
    pred = model.predict_proba(X)[:,1]
    score = []
    for i, line in enumerate(pred):
        if y[i] == 0:
            score.append(line)
    candicate_idfa = data[data.is_seek == 0]['idfa']

    result_dict = dict(zip(candicate_idfa, score))
    result_dict = sorted(result_dict.items(), key=lambda d: d[1], reverse=True)

    with open(fout, 'w') as fp_w:
        for key, val in result_dict:
            if val > threshold:
                fp_w.write(str(key) + "\n")

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print "Usage : python train.py seek.txt target.txt output"
        exit()
    print "read data ..."
    data = read_data(sys.argv[1], sys.argv[2])
    print "get train instance ..."
    X, y = get_train_ins(data)
    print "model training ..."
    model = lr_train(X, y)
    print "get target audience ..."
    get_target_audience(model, data, X, y, sys.argv[3])
