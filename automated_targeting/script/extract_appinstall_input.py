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

def gen_install_apps(start_request, end_request):
    start_request_json = json.loads(start_request)
    end_request_json = json.loads(end_request)
    
    start_data = start_request_json['data']
    end_data = end_request_json['data']
    start_set = set()
    end_set = set()
    
    for item in start_data:
        try:
            start_set.add(item['package_name'])
        except:
            print item
            continue
    for item in end_data:
        try:
            end_set.add(item['package_name'])
        except:
            print item
            continue
    diff = end_set - start_set
    return list(diff)

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
    
    #get input user
    #user_df = user_df.filter(((user_df.package_name.like('%keyboard%')) 
    #    | (user_df.package_name.like('%smartinput%'))) & (user_df.gaid.isNotNull())) \
    #    .select('identifier', 'gaid', 'request', 'dt', 'region')
    
    user_df = user_df.filter((user_df.package_name == "com.emoji.keyboard.touchpal")
        | (user_df.package_name == "com.cootek.smartinputv5"))

    print "After filter smartinput %s~%s(uv) [gaid]: %s" % \
        (start_time, end_time, user_df.dropDuplicates(['gaid']).count())
    print "After filter smartinput %s~%s(uv) [identifier] %s" % \
        (start_time, end_time, user_df.dropDuplicates(['identifier']).count())
    
    user_df.show() 
    user_early_df = user_df.groupBy('gaid').agg(
        F.min('dt').alias('early_day'))
    user_last_df = user_df.groupBy('gaid').agg(
        F.max('dt').alias('last_day'))
    user_df.show()

    user_df = user_df.withColumn('key', F.concat_ws('#', user_df.gaid, user_df.dt)) 
    user_early_df = user_early_df.withColumn('e_key', 
        F.concat_ws('#', user_early_df.gaid, user_early_df.early_day)).drop('gaid')
    user_last_df = user_last_df.withColumn('l_key', 
        F.concat_ws('#', user_last_df.gaid, user_last_df.last_day)).drop('gaid')
  
    user_df = user_df.dropDuplicates(['key']) 
    user_early_df = user_early_df.dropDuplicates(['e_key']) 
    user_last_df = user_last_df.dropDuplicates(['l_key'])

    user_early_df = user_df.join(user_early_df, user_df.key == user_early_df.e_key)
    user_last_df = user_df.join(user_last_df, user_df.key == user_last_df.l_key)
    user_early_df.show()
    user_last_df.show()

    user_early_df = user_early_df.select('gaid', 'request', 'region') \
        .withColumnRenamed('gaid', 'egaid') \
        .withColumnRenamed('request', 'start_request')

    user_last_df = user_last_df.select('gaid', 'request') \
        .withColumnRenamed('gaid', 'lgaid') \
        .withColumnRenamed('request', 'end_request')
    
    user_early_df.show()
    user_last_df.show()
     
    user_df = user_last_df.join(user_early_df, user_early_df.egaid == user_last_df.lgaid)
    user_df.show()

    user_df = user_df.withColumn('install_apps', gen_install_apps_udf('start_request', 'end_request')) \
        .withColumnRenamed('egaid', 'gaid').drop('lgaid')
    print "After get install apps(uv): %s" % user_df.dropDuplicates(['gaid']).count()

    user_df = user_df.filter(F.size(user_df.install_apps) <> 0)
    print "After filter no install apps(uv): %s" % user_df.dropDuplicates(['gaid']).count()
        
    user_df = user_df.select(F.explode(user_df.install_apps).alias('app_name'), 'gaid', 'region')

    appdb_df = spark.read.json(appdb_path) \
        .select('package_name', 'category') \
        .withColumnRenamed('package_name', 'pkg_name')
    appdb_df.show()

    user_df = user_df.join(appdb_df, user_df.app_name == appdb_df.pkg_name)
    idf2gaid = spark.read.parquet("/user/james.jiang/1/2/3/4/5/all_ids/smartinput/{ap,us,eu}/latest/")
    idf2gaid = idf2gaid.dropDuplicates(['gaid'])

    idf2gaid = idf2gaid.withColumnRenamed('gaid', 'tgaid')
    user_df = user_df.join(idf2gaid, user_df.gaid == idf2gaid.tgaid, 'left')
    user_df.select('gaid', 'ip_city', 'app_name', 'category').write.json(des_path + '/json', 'overwrite')
