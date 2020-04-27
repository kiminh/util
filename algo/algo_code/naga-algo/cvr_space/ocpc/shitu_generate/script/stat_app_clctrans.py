#coding:utf-8
import sys
import json
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import IDF

from pyspark.ml.linalg import Vectors
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.ml.feature import Bucketizer
from pyspark.ml.feature import VectorIndexer

reload(sys)
sys.setdefaultencoding('utf-8')

def get_label(events):
    if events is None:
        return -1
    if '14' in events:
        return 1
    else:
        return 0

if __name__ == '__main__':
    if len(sys.argv) < 3:
        sys.exit(0)

    data_path = sys.argv[1]
    des_path = sys.argv[2]

    sc = SparkContext(
        appName='naga dsp: stat user state'
    )
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)
    
    get_label_udf = F.udf(get_label, T.IntegerType())

    click_join_trans = spark.read.json(data_path)
    click_join_trans = click_join_trans.select(
        F.col('click_log.promoted_app').alias('promoted_app'),
        F.col('trans_log.events').alias('events'),
        F.col('click_log.plid').alias('plid'),
        F.col('click_time').substr(1, 8).alias('time')
    )
    click_join_trans.show(10, False)

    click_join_trans = click_join_trans.filter(
        (F.col('promoted_app') <> '') & (F.col('time') >= 20191001)
    ).withColumn('label', get_label_udf('events'))
    click_join_trans.show(10, False)
    
    click_join_trans = click_join_trans.filter(
        (click_join_trans.label == 1) | (click_join_trans.label == 0)
    )
    click_join_trans.show(10, False)
        
    gb_df = click_join_trans.groupBy(['promoted_app']).agg(
        F.count('*').alias('click'),
        F.sum('label').alias('trans')
    )
    gb_df.write.json(des_path, 'overwrite')
