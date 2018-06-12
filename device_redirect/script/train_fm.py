#coding:utf-8
import pandas as pd
import socket
import chardet
import numpy as np
from sklearn import linear_model
from sklearn.model_selection import train_test_split
from sklearn import preprocessing
from sklearn.preprocessing import OneHotEncoder,LabelEncoder
from sklearn.linear_model import LogisticRegression, SGDClassifier
from sklearn.ensemble import GradientBoostingClassifier
import jieba,gensim
from sklearn import metrics
from scipy import sparse
import sys
from sklearn.utils import shuffle
import xlearn as xl

doc2vec_size = 150
filter_chars = [" ", "「", "」", "》", "\r", "\n", "\t", "。", "，", ",", "“", "！", "？", "！", "《", ":", "；", "：", "（", "）", "、"]
 
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

header = ["idfa", "screenhight", "screenheight", "carrier", "connectiontype", "model", "model1", "osv", "devicetype", "ip", "title", "keywords", "channel", "cs"]
base_feature = ["screenhight", "screenheight", "carrier", "connectiontype", "model", 'model1', "osv", "devicetype", "ip18", "ip20", "ip24", "ip30", "channel", "cs"]
title_feature = [ 'title_{}'.format(i) for i in range(doc2vec_size) ]

train_data = pd.read_csv(sys.argv[1])
test_data = pd.read_csv(sys.argv[2])
#train_data = train_data.fillna("-1")
#test_data = test_data.fillna("-1")
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
"""
data["title"] = train_data.apply(lambda x: word_segment(x["title"]), axis=1)
print "Word Segment Complete..."
def read_corpus():
    for i, line in enumerate(data["title"]):
        # split with space to isolate each word
        # the words list are tagged with a label as its identity
        try:
            yield gensim.models.doc2vec.TaggedDocument(gensim.utils.to_unicode(line.decode("GB2312")).split(), ['%s' % i])
        except:
            print line
            yield gensim.models.doc2vec.TaggedDocument(gensim.utils.to_unicode("this is default").split(), ['%s' % i])

#生成一个train corpus
train_corpus = list(read_corpus())
model = gensim.models.doc2vec.Doc2Vec(vector_size=150, min_count=1, epochs=50, workers=7)
model.build_vocab(train_corpus)
model.train(train_corpus, total_examples=model.corpus_count, epochs=model.iter)
print "Doc2Vec Train Finished..."

docvec = []
for i in range(data.shape[0]):
    row = []
    for idx in range(150):
        row.append(model[i][idx])
    docvec.append(row)
data_t = pd.DataFrame(docvec, columns=title_feature)
"""
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
###LR train###
X_train, X_test, y_train, y_test = train_test_split(data_x, data_y)
print "FM Train ..."
fm_model = xl.FMModel(task='binary', init=0.1, epoch=30, k=3, lr=0.1, reg_lambda=0.01, opt='sgd')
fm_model.fit(X_train, y_train, eval_set=[X_test, y_test])
pred = fm_model.predict(X_test)
print pred
print "FM test auc is {}".format(metrics.roc_auc_score(y_test, pred))
print "FM Train Success ..."

score = []
for i, line in enumerate(pred):
    if data_y[i] == 1:
        score.append(line)
candicate_idfa = data[data.is_seek == 0]['idfa']

result_dict = dict(zip(candicate_idfa, score))
result_dict = sorted(result_dict.items(), key=lambda d: d[1], reverse=True)

with open('idfa.txt', 'w') as fp_w:
    for key, val in result_dict:
        fp_w.write(str(key) + "," + str(val) + "\n")
