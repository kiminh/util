#coding:utf-8
import os
import sys
import math
import json
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd

from scipy import sparse
from sklearn.metrics import confusion_matrix, mean_squared_error
from sklearn.metrics import precision_score, accuracy_score, recall_score
import tensorflow as tf
import xgboost as xgb

from tensorflow.contrib.layers.python.layers import batch_norm as batch_norm

def load_data(filename):
    uids = []
    pkgs = []
    ratings = []
    with open(filename) as f_in:
        for i, raw_line in enumerate(f_in, 1):
            line_json = json.loads(raw_line.strip("\n\r"))
            uids.append(int(line_json['uid_index']))
            pkgs.append(int(line_json['pkg_index']))
            ratings.append(float(line_json['rating']))
            if i % 10000 == 0:
                print i

    return pd.DataFrame(data={
                "uids": uids,
                "pkgs": pkgs,
                "ratings": ratings
            })

def shuffle_and_split(n_rows, test_size=0.3):
    ind = np.arange(n_rows)
    for i in range(7):
        np.random.shuffle(ind)
    test_set_size = int(n_rows * test_size)
    train_set_size = n_rows - test_set_size
    return ind[:train_set_size], ind[train_set_size:]

def get_train_test_split(data_list, label, test_size=0.3):
    n_rows = (data_list[0]).shape[0]
    train_ind, test_ind = shuffle_and_split(n_rows, test_size)
    train_list = []
    test_list = []
    y = []
    for data in data_list:
        train_list.append(data[train_ind])
        test_list.append(data[test_ind])
    y_train, y_test = label[train_ind], label[test_ind]
    return train_list, test_list, y_train, y_test

def shuffle_in_unison_scary(data_list):
    rng_state = np.random.get_state()
    for data in data_list:
        np.random.shuffle(data)
        np.random.set_state(rng_state)

def batch_norm_layer(x, train_phase, scope_bn):
    bn_train = batch_norm(x, decay=0.995, center=True, scale=True, updates_collections=None,
                          is_training=True, reuse=None, trainable=True, scope=scope_bn)
    bn_inference = batch_norm(x, decay=0.995, center=True, scale=True, updates_collections=None,
                          is_training=False, reuse=True, trainable=True, scope=scope_bn)
    z = tf.cond(train_phase, lambda: bn_train, lambda: bn_inference)
    return z

def DNN(df):
    X_uids = df['uids'].values
    X_pkgs = df['pkgs'].values
    y = df['ratings'].values
    uids_num = np.unique(X_uids).shape[0]
    pkgs_num = np.unique(X_pkgs).shape[0]
    print uids_num, pkgs_num

    [X_uid_tr, X_pkg_tr], [X_uid_te, X_pkg_te], y_tr, y_te = \
        get_train_test_split([X_uids, X_pkgs], y, test_size=0.3)

    uid_input = tf.placeholder(tf.int32, shape=[None],
                                                 name="uid_input")
    pkg_input = tf.placeholder(tf.int32, shape=[None],
                                                 name="pkg_input")

    label = tf.placeholder(tf.float32, shape=[None], name="label")
    keep_prob = tf.placeholder(tf.float32) 
    train_phase = tf.placeholder(tf.bool)
 
    #uid embeddings    
    uid_mat = tf.Variable(
            tf.random_normal([uids_num, 50], 0, 0.01),
            name="uid_embeddings", trainable=True)
    pkg_mat = tf.Variable(
            tf.random_normal([pkgs_num, 50], 0, 0.01),
            name="pkg_embeddings", trainable=True)

    uid_embed = tf.nn.embedding_lookup(uid_mat, uid_input)
    pkg_embed = tf.nn.embedding_lookup(pkg_mat, pkg_input)

    output = tf.concat([uid_embed, pkg_embed], axis=1)
    
    deep_layers = [256, 128, 64] 
    weights = {}
    input_size = 100
    glorot = np.sqrt(2.0 / (input_size+deep_layers[0]))
    weights["layer_0"] = tf.Variable(
            np.random.normal(loc=0, scale=glorot, size=(input_size, deep_layers[0])), dtype=np.float32)
    weights["bias_0"] = tf.Variable(np.random.normal(loc=0, scale=glorot, size=(1, deep_layers[0])),
                                                        dtype=np.float32)  # 1 * layers[0]
    for i in range(1, len(deep_layers)):
        glorot = np.sqrt(2.0 / (deep_layers[i-1] + deep_layers[i]))
        weights["layer_%d" % i] = tf.Variable(
                np.random.normal(loc=0, scale=glorot, size=(deep_layers[i-1], deep_layers[i])),
                dtype=np.float32)  # layers[i-1] * layers[i]
        weights["bias_%d" % i] = tf.Variable(
                np.random.normal(loc=0, scale=glorot, size=(1, deep_layers[i])),
                dtype=np.float32)  # 1 * layer[i]    

    output = batch_norm_layer(output, train_phase=train_phase, scope_bn="bn_-1")    
    for i in range(len(deep_layers)):
        output = tf.add(tf.matmul(output, weights["layer_%d" %i]), weights["bias_%d"%i]) 
        output = batch_norm_layer(output, train_phase=train_phase, scope_bn="bn_%d" %i)
        output = tf.nn.relu(output)
        output = tf.nn.dropout(output, keep_prob)

    prediction = tf.layers.dense(output, 1)
    #print prediction.shape, label.shape
    prediction = tf.squeeze(prediction)
    loss = tf.reduce_mean(tf.square(label - prediction))

    train_op = tf.train.AdamOptimizer(learning_rate=0.001).minimize(loss)

    sess = tf.Session()
    sess.run(tf.global_variables_initializer())

    total_loss = 0.0
    total_batch_num = 0

    data_num = len(X_uid_tr)
    epoch = 5
    batch_size = 64
    predict = None
    for i in range(epoch):
        start, end = 0, 0
        shuffle_in_unison_scary([X_uid_tr, X_pkg_tr, y_tr])
        while end < data_num:
            end = start + batch_size
            if end > data_num:
                end = data_num
            X_uid_slice = X_uid_tr[start:end]
            X_pkg_slice = X_pkg_tr[start:end]
            y_slice = y_tr[start:end]

            feed_dict = {uid_input: X_uid_slice,
                        pkg_input: X_pkg_slice,
                         label: y_slice,
                         keep_prob: 0.5,
                         train_phase: True}
            _, each_loss = sess.run([train_op, loss], feed_dict)
            total_loss += each_loss
            total_batch_num += 1
            start = end
            if total_batch_num % 100 == 0:
                feed_dict = {uid_input: X_uid_te,
                             pkg_input: X_pkg_te,
                             label: y_te,
                             keep_prob: 1,
                             train_phase: False}
                test_loss, predict  = sess.run([loss, prediction], feed_dict)
                train_loss = total_loss * 1.0 / total_batch_num
                print "epoch %d, train loss %g, test loss %g" %(i, math.sqrt(train_loss), math.sqrt(test_loss))
    
    #f_out = open("result.txt", 'w')
    #for uid, pkg, y, pred in zip(X_uid_te.tolist(), X_pkg_te.tolist(), y_te.tolist(), predict.tolist()):
    #    f_out.write("%s %s %s %s\n" % (uid, pkg, y, pred))
  
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: age_predict.py data.json"
        exit()

    df = load_data(sys.argv[1])
    #apps_vector = load_app_vector("app_vector.json")
    DNN(df)
