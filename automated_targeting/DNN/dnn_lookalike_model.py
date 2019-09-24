#coding:utf-8
import os
import sys
import json
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd

from scipy import sparse
from sklearn.metrics import confusion_matrix, mean_squared_error
from sklearn.metrics import precision_score, accuracy_score, recall_score, roc_auc_score
import tensorflow as tf
import xgboost as xgb

from tensorflow.contrib.layers.python.layers import batch_norm as batch_norm

def gen_data_frame(filename):
    """gen data frame from json"""
    applist_list = []
    gaid_list = []
    label_list = []
    with open(filename) as fp_r:
        for i, raw_line in enumerate(fp_r):
            line_json = json.loads(raw_line.strip("\n\r"))
            if len(line_json['applist']) > 200:
                continue
            gaid_list.append(line_json['gaid'])
            applist_list.append(line_json['applist'])
            label_list.append(line_json['label']) 
            if i % 10000 == 0:
                print i
    data_dict = {
        'gaid': gaid_list,
        'label': label_list,
        'applist': applist_list
    }
    return pd.DataFrame(data=data_dict)

def load_app_vector(filename):
    app_vector_dict = {}
    with open(filename) as f_in:
        for raw_line in f_in:
            line_json = json.loads(raw_line.strip("\n\r")) 
            pkg_name = line_json['pkg_name']
            vector = list(line_json['features'])  

            app_vector_dict[pkg_name] = vector
    print "load %d apps vector %d dim" % (len(app_vector_dict), len(vector))
    return app_vector_dict

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

def get_applist(df):
    apps_dict = {}
    apps_dict['<pad>'] = 0 
    num = 1 
    max_app_len = -1
    for index, row in df.iterrows():
        applist = row['applist']
        for app in applist:
            if app not in apps_dict:
                apps_dict[app] = num 
                num += 1
    
        if len(applist) > max_app_len:
            max_app_len = len(applist)
    print max_app_len    
    data = []
    for index, row in df.iterrows():
        applist = row['applist']
        instance = []
        for app in applist:
            instance.append(apps_dict[app])
        instance.sort()
        if len(applist) < max_app_len:
            instance.extend([0] * (max_app_len - len(applist)))
        if len(instance)==0:
            continue
        data.append(instance)

    return np.array(data), len(apps_dict), max_app_len

def batch_norm_layer(x, train_phase, scope_bn):
    bn_train = batch_norm(x, decay=0.995, center=True, scale=True, updates_collections=None,
                          is_training=True, reuse=None, trainable=True, scope=scope_bn)
    bn_inference = batch_norm(x, decay=0.995, center=True, scale=True, updates_collections=None,
                          is_training=False, reuse=True, trainable=True, scope=scope_bn)
    z = tf.cond(train_phase, lambda: bn_train, lambda: bn_inference)
    return z

def DNN(df):
    X_applist, APP_NUM, max_app_len = get_applist(df)
    y = df['label'].values
    
    [X_app_tr], [X_app_te], y_tr, y_te = get_train_test_split([X_applist], y, test_size=0.3)
    input_dim = X_applist.shape[1]

    applist_input = tf.placeholder(tf.int32, shape=[None, input_dim],
                                                 name="applist_input")

    label = tf.placeholder(tf.int32, shape=[None], name="label")
    keep_prob = tf.placeholder(tf.float32) 
    train_phase = tf.placeholder(tf.bool)
 
    #applist embeddings    
    applist_mat = tf.Variable(
            tf.random_normal([APP_NUM, 100], 0, 0.01),
            name="applist_embeddings", trainable=True)

    s_mask = tf.cast(tf.cast(applist_input, tf.bool), tf.float32)
    s_mask = tf.reshape(s_mask, [-1, input_dim, 1])
    #embeddings
    applist_embed = tf.nn.embedding_lookup(applist_mat, applist_input)
    applist_embed = tf.multiply(applist_embed, s_mask)
    output = tf.reduce_mean(applist_embed, axis=1)
    
    deep_layers = [128, 64, 32] 
    weights = {}
    input_size = 100
    #input_size = 50 + len(cate_dict)
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

    logit = tf.layers.dense(output, 2)
    loss = tf.nn.sparse_softmax_cross_entropy_with_logits(logits=logit, labels=label)
    loss = tf.reduce_mean(loss)
    for i in range(len(deep_layers)):
        loss += tf.contrib.layers.l2_regularizer(0.01)(weights["layer_%d"%i])

    predict = tf.argmax(tf.nn.softmax(logit), axis=1)
    probs = tf.nn.top_k(tf.nn.softmax(logit), 1)[0]
    true_pred = tf.equal(tf.cast(label, tf.int64), predict)
    accu = tf.reduce_mean(tf.cast(true_pred, tf.float32))

    train_op = tf.train.AdamOptimizer(learning_rate=0.001).minimize(loss)

    sess = tf.Session()
    sess.run(tf.global_variables_initializer())

    total_loss, total_accu = 0.0, 0.0
    total_batch_num = 0

    data_num = len(X_app_tr)
    epoch = 100
    batch_size = 64
    for i in range(epoch):
        start, end = 0, 0
        shuffle_in_unison_scary([X_applist, y])
        while end < data_num:
            end = start + batch_size
            if end > data_num:
                end = data_num
            X_app_slice = X_applist[start:end, :]
            y_slice = y[start:end]

            feed_dict = {applist_input: X_app_slice,
                         label: y_slice,
                         keep_prob: 0.5,
                         train_phase: True}
            _  = sess.run(train_op, feed_dict)
            total_batch_num += 1
            start = end
            if total_batch_num % 10 == 0:
                feed_dict = {applist_input: X_app_te,
                             label: y_te,
                             keep_prob: 1,
                             train_phase: False}
                test_probs = sess.run(probs, feed_dict)
                print "test AUC: %g" % roc_auc_score(y_te, test_probs) 
        
        feed_dict = {applist_input: X_app_te,
                    label: y_te,
                    keep_prob: 1,
                    train_phase: False}
        print "Epoch %d " % i
        test_probs = sess.run(probs, feed_dict)
        print "test AUC: %g" % roc_auc_score(y_te, test_probs) 

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: age_predict.py data.json"
        exit()

    df = gen_data_frame(sys.argv[1])
    #apps_vector = load_app_vector("app_vector.json")
    DNN(df)
