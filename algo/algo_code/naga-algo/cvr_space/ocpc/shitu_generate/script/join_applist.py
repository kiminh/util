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

def parse_applist(value):
    raw = json.loads(value)['mobile']['applist']
    raw = raw.split('@@')
    res_list = [x.split('$$')[0] for x in raw]
    res = [appname_dict.get(x, x) for x in res_list]
    return res

if __name__ == '__main__':
    if len(sys.argv) < 3:
        sys.exit(0)

    data_path = sys.argv[1]
    applist_path = sys.argv[2]
    des_path = sys.argv[3]

    sc = SparkContext(
        appName='naga dsp: stat user state'
    )
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)
    parse_applist_udf = F.udf(parse_applist, T.ArrayType(T.StringType()))
    
    click_join_trans = spark.read.json(data_path)
    applist_df = spark.read.parquet(applist_path)
    click_join_trans.show(10, False)
    applist_df.show(10, False)
    
    click_join_trans = click_join_trans.withColumn('identifier', 
        click_join_trans.click_log.identifier)
    click_join_trans.show(10, False)

    applist_df = applist_df.withColumn('applist', parse_applist_udf('value')) \
        .select('identifier', 'applist')

    click_join_trans = click_join_trans.join(applist_df, 
        click_join_trans.identifier == applist_df.identifier, 'left')
    click_join_trans.drop('identifier').write.json(des_path, 'overwrite')
