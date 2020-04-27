#coding:utf-8
import sys
import json
import os
import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.ml.linalg import Vectors
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import SparkContext

reload(sys)
sys.setdefaultencoding("utf-8")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: py stat_info.py" \
              " ed_log click_log des_path"
        sys.exit(0)

    ed_path = sys.argv[1]
    sc = SparkContext(
        appName='naga DSP: ed_join_click'
    )
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)

    df = spark.read.json("/user/ad_user/ctr_space/ed_join_click/join/{20191018,20191019}/")
    df1 = spark.read.json("/user/ad_user/ctr_space/ed_join_click/join/temp/{20191018,20191019}")

    df = df.select(
        F.col('ed_log').alias('ed_log'),    
        F.col('ed_log.reqid').alias('reqid'),
        F.col('ed_log.adid').alias('adid')
    )
    df1 = df1.select(
        F.col('ed_log.reqid').alias('reqid'),
        F.col('ed_log.adid').alias('adid')
    )

    print df.count(), df1.count()
    
    df = df.withColumn('key', F.concat_ws('_', 'reqid', 'adid'))
    df1 = df1.withColumn('tkey', F.concat_ws('_', 'reqid', 'adid'))

    """
    join_ret = df1.join(df, df1.tkey == df.key, 'left')
    join_ret = join_ret.filter(join_ret.key.isNull()).drop('reqid', 'adid')
    join_ret.write.json('/user/ad_user/ling.fang/temp/json', 'overwrite')

    join_ret_t = df.join(df1, df1.tkey == df.key, 'inner')
    print join_ret_t.count()
    """

    join_ret_x = df.join(df1, df1.tkey == df.key, 'left')
    join_ret_x = join_ret_x.filter(join_ret_x.tkey.isNull()).drop('reqid', 'adid')
    join_ret_x.write.json('/user/ad_user/ling.fang/temp1/json', 'overwrite')

    """
    spark.sql("select count(*) from df").show()
    spark.sql("select count(*) from df1").show()
    spark.sql("select count(*) from df where click_log is Not Null").show()
    spark.sql("select count(*) from df1 where click_log is Not Null").show()

    spark.sql("select * from (select ed_log, ed_log.reqid as reqid, ed_log.adid as adid from df) tab1 left join (select ed_log.reqid as reqid, ed_log.adid as adid from df1) tab2 on (tab1.reqid = tab2.reqid and tab1.adid = tab2.adid) where tab2.reqid is Null").repartition(100).write.mode('overwrite').json('/user/ad_user/ling.fang/temp/json')
    
    #spark.sql("select * from df3").show(10, False)
    #spark.sql("select count(*) from df3").show(10, False)

    #spark.sql("select * from df2 where ed_log.reqid = 'd72cc26b-284d-415f-b709-c33e1c85c417'").show(100, False)
    """
