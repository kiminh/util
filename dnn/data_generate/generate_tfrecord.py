import sys
import tensorflow as tf

_CSV_COLUMNS = ['is_trans']

for line in open('./script/features.list'):
    fea_name, fea_code = line.strip('\n\r').split('\t')
    _CSV_COLUMNS.append(fea_name)


def get_tfrecords_example(rawlog, label):
    tfrecords_features = {}
    for key, value in rawlog.items():
        if not isinstance(value, list):
            value = bytes(value, encoding='utf8')
            value = [value]
        else:
            value = [ bytes(v, encoding='utf8') for v in value]
        tfrecords_features[key] = tf.train.Feature(bytes_list=tf.train.BytesList(value=value))
    tfrecords_features['label'] = tf.train.Feature(float_list=tf.train.FloatList(value=[label]))
    return tf.train.Example(features=tf.train.Features(feature=tfrecords_features))


def make_tfrecord(datafile, output='train'):
    output += '.tfrecord'
    tfrecord_writer = tf.python_io.TFRecordWriter(output)

    for i, raw in enumerate(open(datafile)):
        flds = raw.split(',')
        feats = [ f.strip('\n\r\t') for f in flds ]
        feats_dict = dict(zip(_CSV_COLUMNS, feats))
        label = float(feats_dict.pop('is_trans'))
        applist = feats_dict['applist'].split('_')
        feats_dict['applist'] = applist

        exmp = get_tfrecords_example(feats_dict, label)
        exmp_serial = exmp.SerializeToString()
        tfrecord_writer.write(exmp_serial)
        if i % 100000 == 0:
            print("process %s datas" % i)

    tfrecord_writer.close()

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python xx.py input output")
    make_tfrecord(sys.argv[1], sys.argv[2])
