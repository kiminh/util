#coding:utf-8
import sys
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import StringIndexer
from pyspark import SparkContext
from pyspark.sql import Row, SparkSession
from pyspark import SparkFiles
import json
import datetime
import time
import math

reload(sys)
sys.setdefaultencoding('utf-8') 

def gen_install_apps(end_request):
    end_request_json = json.loads(end_request)
    end_data = end_request_json['data']
    end_set = set()
    
    for item in end_data:
        try:
            end_set.add(item['package_name'])
        except:
            print item
            continue
    return list(end_set)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print "args lens is less than 5"
        exit(0)

    appdb_path = sys.argv[1]
    des_path = sys.argv[2]
    start_time = sys.argv[3]
    end_time = sys.argv[4]

    sc = SparkContext(appName="DMP.pangu:appusage data")

    sc.setLogLevel('ERROR')
    spark = SparkSession \
        .builder \
        .appName('hive_test') \
        .enableHiveSupport() \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("hive.exec.max.dynamic.partitions", "30000") \
        .config("hive.exec.max.dynamic.partitions.pernode", "30000") \
        .config("spark.rdd.compress", "true") \
        .getOrCreate()

    gen_install_apps_udf = F.udf(gen_install_apps, T.ArrayType(T.StringType()))

    import pyspark
    sqlContext = pyspark.sql.SQLContext(sc)
    user_df = sqlContext.sql("select * from dw.dw_usage_noah_info_d where usage_type='usage_apps' and (dt<=%s and dt>=%s)" % (end_time, start_time)) 
    user_df.show()
    print "usage apps %s~%s(uv) [gaid]: %s" % \
        (start_time, end_time, user_df.dropDuplicates(['gaid']).count())
    print "usage apps %s~%s(uv) [identifier] %s" % \
        (start_time, end_time, user_df.dropDuplicates(['identifier']).count())

    #get matrix user
    user_df = user_df.filter(((~user_df.package_name.like('%keyboard%')) 
        & (~user_df.package_name.like('%smartinput%'))) & (user_df.gaid.isNotNull())) \
        .select('identifier', 'gaid', 'request', 'dt', 'region')

    print "matrix exchange applist uv [gaid]: %s" % user_df.dropDuplicates(['gaid']).count()
    user_df.show() 
    user_last_df = user_df.groupBy('gaid').agg(
        F.max('dt').alias('last_day'))
    user_df.show()

    user_df = user_df.withColumn('key', F.concat_ws('#', user_df.gaid, user_df.dt)) 
    user_last_df = user_last_df.withColumn('l_key', 
        F.concat_ws('#', user_last_df.gaid, user_last_df.last_day)).drop('gaid')
  
    user_df = user_df.dropDuplicates(['key']) 
    user_last_df = user_last_df.dropDuplicates(['l_key'])

    user_last_df.show()
    user_df = user_df.join(user_last_df, user_df.key == user_last_df.l_key)
    user_df.show()

    user_df = user_df.withColumn('applist', gen_install_apps_udf('request'))
    print "After get install apps(uv): %s" % user_df.dropDuplicates(['gaid']).count()
    idf2gaid = spark.read.parquet("/user/james.jiang/1/2/3/4/5/all_ids/matrix/{ap,us,eu}/latest/")
    idf2gaid = idf2gaid.dropDuplicates(['gaid'])

    idf2gaid = idf2gaid.withColumnRenamed('gaid', 'tgaid')
    user_df = user_df.join(idf2gaid, user_df.gaid == idf2gaid.tgaid, 'left')
    
    user_df = user_df.select('gaid', 'applist', 'region', 'ip_city')
    user_df.show()
    user_df.write.json(des_path + '/json', 'overwrite')
