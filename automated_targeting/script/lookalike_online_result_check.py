#coding:utf-8
import sys
import json
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.sql import Window
 
from pyspark.ml.feature import Bucketizer
from pyspark.ml.feature import StringIndexer, IndexToString, StringIndexerModel

import math

if __name__ == '__main__':
    test_data_path = sys.argv[1]
    res_path = sys.argv[2]
    des_path = sys.argv[3]

    sc = SparkContext(
        appName='ling.fang-dmp2.0-normal-temporary-als_model_online_evaluate'
    )
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)

    test_df = spark.read.json(test_data_path)
    test_df = test_df.filter(((test_df.time > 20190601000000) &
        (test_df.time < 20190604000000)) & 
        ((~test_df.bundle_id.like("%input%")) &
        (~test_df.bundle_id.like("%keyboard%"))) &
        (test_df.promoted_app == "com.particlenews.newsbreak"))
    
    #test_df = test_df.filter(((test_df.time < 20190606000000) &
    #    (test_df.time > 20190604000000)) &
    #    ((~test_df.bundle_id.like("%input%")) &
    #    (~test_df.bundle_id.like("%keyboard%"))) &
    #    (test_df.promoted_app == "com.machsystem.gawii"))
    
    test_df.show()
    print "Before join lookalike data test data pv: %s" % test_df.count()
    print "Before join lookalike data test data uv: %s" % test_df.dropDuplicates(['gaid']).count()
    clc_num = test_df.filter(test_df.isclick == 1).count()
    ed_num = test_df.count()
    conv_num = test_df.filter(test_df.isconv == 1).count()

    print "Before join lookalike data ctr: %s" % (clc_num * 1.0 / ed_num)
    print "Before join lookalike data cvr: %s" % (conv_num * 1.0 / clc_num)
   
    lookalike_res_df = spark.read.json(res_path)
    lookalike_res_df = lookalike_res_df.withColumnRenamed('gaid', 'tgaid')
    lookalike_res_df.show()

    test_df = test_df.join(lookalike_res_df, test_df.gaid == lookalike_res_df.tgaid)
    test_df = test_df.drop('tgaid')

    print "After join lookalike data test data pv: %s" % test_df.count()
    print "After join lookalike data test data uv: %s" % test_df.dropDuplicates(['gaid']).count()
    test_df = test_df.withColumnRenamed('prob', 'rating')
    test_df.show()
    test_df = test_df.select("gaid", "rating", "isclick", "isconv")
    clc_num = test_df.filter(test_df.isclick == 1).count()
    ed_num = test_df.count()
    conv_num = test_df.filter(test_df.isconv == 1).count()

    print "After join lookalike data ctr: %s" % (clc_num * 1.0 / ed_num)
    print "After join lookalike data cvr: %s" % (conv_num * 1.0 / clc_num)
    test_df.write.json(des_path + '/json', 'overwrite')
