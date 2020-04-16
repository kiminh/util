import json
import numpy as np
import tensorflow as tf

_HASH_BUCKET_SIZE = 10000
_CSV_COLUMNS = ['is_trans']

for line in open('./data_generate/script/features.list'):
    fea_name, fea_code = line.strip('\n\r').split('\t')
    _CSV_COLUMNS.append(fea_name)

_CSV_COLUMN_DEFAULTS = [ ['unk'] for i in range(len(_CSV_COLUMNS)) ]

def build_model_columns():
    """Builds a set of wide and deep feature columns."""
    wide_columns = []
    deep_columns = []
    
    for column in _CSV_COLUMNS[1:]:
        column_hash = tf.feature_column.categorical_column_with_hash_bucket(
            column, hash_bucket_size=_HASH_BUCKET_SIZE)
        wide_columns.append(column_hash)
        deep_columns.append(
            tf.feature_column.embedding_column(column_hash, dimension=8))
    return wide_columns, deep_columns

def input_fn(data_file, num_epochs, shuffle, batch_size):
    def parse_csv(value):
        columns = tf.io.decode_csv(value, record_defaults=_CSV_COLUMN_DEFAULTS)
        features = dict(list(zip(_CSV_COLUMNS, columns)))
        labels = features.pop('is_trans')
        labels = tf.reshape(
            tf.dtypes.cast(tf.equal(labels, '1'), tf.float32), [-1])
        return features, labels

    dataset = tf.data.TextLineDataset(data_file)
    dataset = dataset.map(parse_csv, num_parallel_calls=5)
    
    if shuffle:
        dataset = dataset.shuffle(buffer_size=256)
    return dataset.repeat(num_epochs).batch(batch_size)

my_feature_columns = []
def create_feature_columns():
    global my_feature_columns
    for column in _CSV_COLUMNS[1:]:
        if column == 'promoted_app' or column == 'applist':
            continue
        column_hash = tf.feature_column.categorical_column_with_hash_bucket(
            column, hash_bucket_size=_HASH_BUCKET_SIZE)
        my_feature_columns.append(
            tf.feature_column.embedding_column(column_hash, dimension=8))
    return my_feature_columns
       
def parse_exmp(serial_exmp):
    label = tf.feature_column.numeric_column("label", default_value=0, dtype=tf.float32) 
    fea_columns = [label]
    fea_columns += my_feature_columns
    feature_spec = tf.feature_column.make_parse_example_spec(fea_columns)
    other_feature_spec = {
        "applist": tf.VarLenFeature(tf.string),
        "promoted_app": tf.FixedLenFeature([], tf.string)
    }
    feature_spec.update(other_feature_spec)
    feats = tf.parse_single_example(serial_exmp, features=feature_spec)
    label = feats.pop('label')
    return feats, tf.to_float(label)

def input_fn_tfrecord(filenames, num_epochs, shuffle, batch_size):
    files = tf.data.Dataset.list_files(filenames)
    dataset = files.apply(tf.contrib.data.parallel_interleave(tf.data.TFRecordDataset, cycle_length=1))
    if shuffle:
        dataset = dataset.shuffle(buffer_size=256)
    dataset = dataset.map(parse_exmp, num_parallel_calls=8)
    dataset = dataset.repeat(num_epochs).batch(batch_size)
    return dataset
"""
#create_feature_columns()
#print(my_feature_columns)
dataset = input_fn_tfrecord('./data_generate/train.tfrecord', 1, False, 10)

iterator = dataset.make_one_shot_iterator()
one_element = iterator.get_next()
with tf.Session() as sess:
    for i in range(1):
        print(sess.run(one_element))
"""
