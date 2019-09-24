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

def clear_data(data):
    none_list = ["", " ", "unknown", "null"]
    if data in none_list:
        return "none"
    return data

"""
{"url":"dsp/click","request":{"path":"DSPCLICK","value":{"DSPCLICK_LOG":{"ip":"181.68.5.250","spam":"0","dv":"1","cost":"0","cmod":"0","pubid":"","reqid":"1537282849414-bf82d8f2-003b-4fd4-9b46-518c1785a713","adxsrc":"sniper","advid":"U8839433952979h9","slotid":"0","dpid":"808f7ea23c22ad9b","cc2":"CO","planid":"P78462846wxmxem7","plid":"2c22acd7151b7f2a2def1e77b8230742","bid":"0.0","reqprt":"1537282849505","adid":"A64212021dem0igr","reqtype":"2","bundle_id":"com.fast.messages.social.messenger.free","impid":"1","lan_t":"2","adsrc":"ct_com","campaignid":"C107555855we83vs","userid":"274b8352-f94a-4eac-a0e9-4603e015371c","platform_id":"","appid":"9f9ffe72f6a2d9dc4b7954b9ed1eddc6","os":"1","log_ts":"4"}}},"ip":"usa-ad-dsp01.uscasv2.cootek.com-dsp-http_dsp-44","user_id":0,"time":"20180918150116"}
"""
def deal_click_data(iter):
    for raw in iter:
        try:
            info = json.loads(raw[1].lower())
        except:
            continue    
        request = info.get("request", "")
        value = request.get("value", "")
        dspclick_json = value.get("dspclick_log", "")
        gaid = clear_data(dspclick_json.get("userid", "none")) 
        reqid = clear_data(dspclick_json.get("reqid", "none"))
        
        yield Row(reqid=reqid, click_gaid=gaid)
"""
{"url":"dsp/transform","request":{"path":"DSPTRANSFORM","value":{"DSPTRANSFORM_LOG":{"ip":"8.37.236.187","spam":"0","dv":"1","cost":"0","publisher":"","lau_t":"2","user_id":"56180cb3-1391-4591-a1f0-ae56d776f999","adsrc":"display","advid":"U5159659lnm4fn9m","ip":"42.110.192.101","bid":"0.0","appid":"4f3b564a0217c347558f16b643da6f1b","reqprt":"1538529077534","adxsrc":"sniper","cc2":"IN","planid":"P5935435okqvz3bk","reqid":"1538529077518-2721fa3c-81c1-462a-8e67-c720106cd1ce","campaignid":"C534524582b2nw2q","adid":"A5935435ymk42e6u","plid":"fb1f75497746ef08e128f700923b1d91","bundle_id":"com.emoji.keyboard.touchpal","post_back_url_type":"5","os":"1","clickid":"VEJBWmVZcTVnNTJKNCtkYUcvc2JobGdwSUJydDREank5VVkzWUlpV0NkaVR1YzZVYVJ5UjVKRzRuYm5NKzF1K1FsTWw5cE9STmtBR0E4a1NBTW8xNXdrbHFaNU1vcVJLWDFBdW9nQXEzRFB5YUgvanR6NVRBTWJiNitlWkl4bzRDbmxFS2tTaW5pN2x0dWRJL2xjVFA4STZJUVhnZXRLeGp1NzVuaVFYY2hldUhULzdFNXRseS9haWo5YmZCY2grREhRNVlsRTVWNFhKbTN1K2wxOVBCSTRScTROaHN2SVhZZThsMHA2aW5UUVROcWlTa2VQVnNwcVFIVHMybXhmUWRWaDlwbERNTHoyQVRtL3Ntb25iTDJpTkVIY2ZVYUVtMkJpM3NZWTc2T3lIL2d2ZVgwcTdVZ1A4bkRDanRtN2pGZWZGN3VhdUFJcVRhT2drdzZsb2FzSFJTVWRFdkkwSlJpWWJ1RlMzVnB2dFl2UWNDMUhjYzRGMkxmblJXVUNpTjcydi9NNzFuMnUyYjNTTHQ0eWluNkZYK2x4OHRmOFhROXBtWFJNTXdhVT0x","log_ts":"2"}}},"ip":"usa-ad-dsp01.uscasv2.cootek.com-dsp-http_dsp-1","user_id":0,"time":"20181009140108"}
"""
def deal_conv_data(iter):
    for raw in iter:
        try:
            info = json.loads(raw[1].lower())
        except:
            continue    
        request = info.get("request", "")
        value = request.get("value", "")
        dsptransform_json = value.get("dsptransform_log", "")
        gaid = clear_data(dsptransform_json.get("user_id", "none")) 
        reqid = clear_data(dsptransform_json.get("reqid", "none"))

        yield Row(reqid=reqid, conv_gaid=gaid)
"""
{"url":"dsp/ed","request":{"path":"DSPED","value":{"DSPED_LOG":{"ip":"107.77.241.8","spam":"0","dv":"1","cost":"0","cmod":"0","pubid":"","reqid":"1539125987871-c1e4bc1d-4418-4711-99f3-4b6be975b89c","adxsrc":"sniper","advid":"U8839433952979h9","slotid":"0","dpid":"b4c168454013518","cc2":"US","planid":"P77182718rt748wm","plid":"fb1f75497746ef08e128f700923b1d91","bid":"0.0","reqprt":"1539125987885","adid":"A771827184bcci74","reqtype":"2","bundle_id":"com.deepsleep.sleep.soft.music.sounds","impid":"1","lan_t":"2","adsrc":"ct_com","campaignid":"C107555855we83vs","triggerd_expids":"6202_9900","userid":"735f69be-7a0e-4c07-ba73-60affc591ca2","platform_id":"","appid":"4f3b564a0217c347558f16b643da6f1b","os":"1","log_ts":"4"}}},"ip":"usa-ad-dsp01.uscasv2.cootek.com-dsp-http_dsp-48","user_id":0,"time":"20181009230001"}
"""
def deal_ed_data(iter):
    for raw in iter:
        try:
            info = json.loads(raw[1].lower())
        except:
            continue    
        request = info.get("request", "")
        value = request.get("value", "")
        try:
            dsped_json = value.get("dsped_log", "")
            gaid = clear_data(dsped_json.get("userid", "none")) 
        except:
            print dsped_json
            continue
        reqid = clear_data(dsped_json.get("reqid", "none"))
        time = clear_data(info.get("time", "none")) 
        appid = clear_data(dsped_json.get("appid", "none"))
        dpid = clear_data(dsped_json.get("dpid", "none"))
        planid = clear_data(dsped_json.get("planid", "none"))
        spam = clear_data(dsped_json.get("spam", "0"))
        bundle_id = clear_data(dsped_json.get("bundle_id", "none"))
        promoted_app = clear_data(dsped_json.get("promoted_app", "none"))
        cc2 = clear_data(dsped_json.get("cc2", "none"))
        if int(spam) == 1:
            continue

        yield Row(ed_gaid=(gaid), promoted_app=promoted_app, bundle_id=bundle_id, ctry=cc2, reqid=reqid, dpid=dpid, planid=planid, appid=appid, time=int(time))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "args lens is less than 2"
        exit(0)
    ed_path = sys.argv[1]
    click_path = sys.argv[2]
    conv_path = sys.argv[3]
    des_path = sys.argv[4]

    sc = SparkContext(appName="DMP:user_show_click_conv")
    sc.addFile('hdfs:///user/dmp/libs/GeoIP2-City.mmdb')
    sc.addPyFile('hdfs:///user/dmp/libs/maxminddb.zip')
    sc.addPyFile('hdfs:///user/dmp/libs/geoip2.zip')

    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)

    ed_rdd = sc.newAPIHadoopFile(ed_path,
                    "com.hadoop.mapreduce.LzoTextInputFormat",
                    "org.apache.hadoop.io.LongWritable",
                    "org.apache.hadoop.io.Text")
    click_rdd = sc.newAPIHadoopFile(click_path,
                    "com.hadoop.mapreduce.LzoTextInputFormat",
                    "org.apache.hadoop.io.LongWritable",
                    "org.apache.hadoop.io.Text")
    conv_rdd = sc.newAPIHadoopFile(conv_path,
                    "com.hadoop.mapreduce.LzoTextInputFormat",
                    "org.apache.hadoop.io.LongWritable",
                    "org.apache.hadoop.io.Text")

    # extract user info from src_path
    ed_data = ed_rdd.mapPartitions(deal_ed_data).toDF()
    click_data = click_rdd.mapPartitions(deal_click_data).toDF() \
        .withColumnRenamed('reqid', 'treqid')
    conv_data = conv_rdd.mapPartitions(deal_conv_data).toDF() \
        .withColumnRenamed('reqid', 'treqid')
    
    ed_data = ed_data.dropDuplicates(['reqid'])
    click_data = click_data.dropDuplicates(['treqid'])
    conv_data = conv_data.dropDuplicates(['treqid'])

    #ed_data = ed_data.filter(
    #    (ed_data.ed_gaid <> "none") &
    #    ((ed_data.bundle_id.like('%input%')) |
    #    (ed_data.bundle_id.like('%keyboard%'))) &
    #    ((ed_data.promoted_app == "com.particlenews.newsbreak") |
    #    (ed_data.promoted_app == "com.machsystem.gawii")))
    
    ed_data = ed_data.filter(
        (ed_data.ed_gaid <> "none") &
        ((ed_data.promoted_app == "com.particlenews.newsbreak") |
        (ed_data.promoted_app == "com.machsystem.gawii")))

    ed_join_click = ed_data.join(click_data, \
        ed_data.reqid == click_data.treqid, 'left')
    ed_join_click.show()
  
    ed_join_click = ed_join_click.withColumn('isclick', 
        F.when(ed_join_click.treqid.isNull(), 0).otherwise(1)) \
        .drop('treqid')

    ed_join_click_conv = ed_join_click.join(conv_data, 
        ed_join_click.reqid == conv_data.treqid, 'left')
    
    ed_join_click_conv = ed_join_click_conv.withColumn('isconv', 
        F.when(ed_join_click_conv.treqid.isNull(), 0).otherwise(1)) \
        .drop('treqid')
    
    #uid_udf = F.udf(lambda x, y: x if x != "none" else y, T.StringType())
    #ed_join_click_conv = ed_join_click_conv.withColumn('gaid', 
    #    uid_udf(ed_join_click_conv.ed_gaid, ed_join_click_conv.dpid))
    ed_join_click_conv = ed_join_click_conv.withColumnRenamed('ed_gaid', 'gaid')
    #print ed_join_click_conv.filter(ed_join_click_conv.isconv == 1).dropDuplicates(['gaid']).count()

    #ed_join_click_conv = ed_join_click_conv.filter(
    #    (ed_join_click_conv.gaid <> "none") &
    #    (ed_join_click_conv.bundle_id.like('%input%')) &
    #    (ed_join_click_conv.bundle_id.like('%keyboard%')) &
    #    (ed_join_click_conv.promoted_app == "com.particlenews.newsbreak"))
    
    #print ed_join_click_conv.count() 
    print ed_join_click_conv.take(10)
    ed_join_click_conv.write.json(des_path + '/json', 'overwrite') 
