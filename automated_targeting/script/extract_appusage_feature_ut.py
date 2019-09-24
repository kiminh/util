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

def get_use_cnt(action_time_list):
    return len(action_time_list)

def get_use_time(action_time_list):
    diff_time = 0.0
    for item in action_time_list:
        start_time = float(item[1][:10])
        end_time = float(item[0][:10])
        diff = math.fabs(end_time - start_time)
        diff_time += diff

    return diff_time

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print "args lens is less than 5"
        exit(0)

    user_applist_path = sys.argv[1]
    appusage_path = sys.argv[2]
    appdb_path = sys.argv[3]
    idf2gaid_path = sys.argv[4]
    des_path = sys.argv[5]

    sc = SparkContext(appName="DMP.pangu:appusage data")

    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)

    get_use_time_udf = F.udf(get_use_time, T.FloatType())
    get_use_cnt_udf = F.udf(get_use_cnt, T.IntegerType())
 
    user_appusage_df = spark.read.parquet(appusage_path) \
        .select('identifier', 'user_id', 'recommend_channel', 'channel_code', 'use_app_name', 'action_time_list', 'package_name') \
        .withColumnRenamed('package_name', 'package_src')
    user_appusage_df.show()
    user_appusage_df = user_appusage_df.dropDuplicates(['identifier', 'use_app_name'])
    print "user appusage count: %d" % user_appusage_df.count()

    appdb_df = spark.read.json(appdb_path) \
        .select('package_name', 'category')
    appdb_df.show()

    idf2gaid_df = spark.read.parquet(idf2gaid_path) \
        .withColumnRenamed('identifier', 'idf')
    idf2gaid_df = idf2gaid_df.dropDuplicates(['idf'])    
    ## filter OEM exchange
    user_appusage_df = user_appusage_df.filter((user_appusage_df.package_src.like('%input%')) | 
        (user_appusage_df.package_src.like("%keyboard%")))
    user_appusage_df = user_appusage_df.filter((~(user_appusage_df.recommend_channel.like("%OEM%"))) & 
        (~(user_appusage_df.channel_code.like("%OEM%"))))
    print "After filter OEM exchange(pv): %d" % user_appusage_df.count()
    print "After filter OEM exchange(uv): %d" % user_appusage_df.dropDuplicates(['identifier']).count()

    user_appusage_df = user_appusage_df.join(idf2gaid_df,
        user_appusage_df.identifier == idf2gaid_df.idf)
    print "After join idf2gaid table(pv): %d" % user_appusage_df.count()
    print "After join idf2gaid table(uv): %d" % user_appusage_df.dropDuplicates(['identifier']).count()
    #user_appusage_df = user_appusage_df.filter(user_appusage_df.ip_city == 'US')
    #print "After filter US: %d" % user_appusage_df.dropDuplicates(['identifier']).count()
    user_appusage_df = user_appusage_df.join(F.broadcast(appdb_df), 
        user_appusage_df.use_app_name == appdb_df.package_name, 'left')
    print "After user appusage join appdb count(pv): %d" % user_appusage_df.count()
    print "After user appusage join appdb count(uv): %d" % user_appusage_df.dropDuplicates(['identifier']).count()
    
    user_appusage_df = user_appusage_df.filter(user_appusage_df.category <> "Personalization[APPLICATION]") \
        .filter(user_appusage_df.category <> "Communication[APPLICATION]") \
        .filter(user_appusage_df.category <> "Maps & Navigation[APPLICATION]") \
        .filter(~user_appusage_df.use_app_name.like("%lock%")) \
        .filter(~user_appusage_df.use_app_name.like("%Lock%")) \
        .filter(~user_appusage_df.use_app_name.like("%screen%")) \
        .filter(~user_appusage_df.use_app_name.like("%Screen%")) \
        .filter(~user_appusage_df.use_app_name.like("%Keyword%")) \
        .filter(~user_appusage_df.use_app_name.like("%keyword%")) \
        .filter(~user_appusage_df.use_app_name.like("%virus%")) \
        .filter(~user_appusage_df.use_app_name.like("%Virus%")) \
        .filter(~user_appusage_df.use_app_name.like("%Theme%")) \
        .filter(~user_appusage_df.use_app_name.like("%theme%")) \
        .filter(~user_appusage_df.use_app_name.like("%Security%")) \
        .filter(~user_appusage_df.use_app_name.like("%security%")) \
        #.filter(~user_appusage_df.use_app_name.like("%facebook%")) \
        #.filter(~user_appusage_df.use_app_name.like("%samsung%")) \
        #.filter(~user_appusage_df.use_app_name.like("%vivo%")) \
        #.filter(~user_appusage_df.use_app_name.like("%huawei%")) \
        .filter(~user_appusage_df.use_app_name.like("%Map%")) \
        .filter(~user_appusage_df.use_app_name.like("%map%")) \
        .filter(~user_appusage_df.use_app_name.like("%packageinstaller%")) \
        .filter(~user_appusage_df.use_app_name.like("%Packageinstaller%")) \
        .filter(~user_appusage_df.use_app_name.like("%Calculator%")) \
        .filter(~user_appusage_df.use_app_name.like("%calculator%")) \
        .filter(~user_appusage_df.use_app_name.like("%input%")) \
        .filter(~user_appusage_df.use_app_name.like("%Input%"))
    
    #user_appusage_df.show(10)
    user_appusage_df = user_appusage_df.withColumn('use_cnt',
        get_use_cnt_udf(user_appusage_df.action_time_list))
    user_appusage_df = user_appusage_df.withColumn('use_time',
        get_use_time_udf(user_appusage_df.action_time_list))
    
    user_appusage_df = user_appusage_df.drop('action_time_list', 'recommend_channel', 'channel_code')
    user_appusage_df.show()
    user_appusage_df.write.json(des_path + 'json', "overwrite")
