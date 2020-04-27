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
sys.setdefaultencoding("utf-8")

if __name__ == '__main__':
    if len(sys.argv) < 3:
        sys.exit(0)

    ed_data_path = sys.argv[1]
    des_path = sys.argv[2]

    sc = SparkContext(
        appName='naga dsp: stat user state'
    )
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)
    
    ed_df = spark.read.json(ed_data_path)
    ed_df.show(10, False)
    ed_df = ed_df.select(
        F.col("request.value.DSPCLICK_LOG.did").alias('did'),
        F.col("time").alias('time')
    ).filter(
        F.col('did') <> ''
    ).groupBy('did').agg(F.min('time').alias('time')).write.mode('overwrite').json(des_path)
