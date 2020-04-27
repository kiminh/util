#coding:utf-8
import redis
from pyalink.alink import *
from feature_engineer import *

#useLocalEnv(3)
env, btenv, senv, stenv = getMLEnv()
stenv.register_java_function('json_parse_online','com.cootek.streaming.udf.ApdJsonParseOnline')

BROKER_LIST = "cn-bdpkafka01.corp.cootek.com:9092,cn-bdpkafka02.corp.cootek.com:9092,cn-bdpkafka03.corp.cootek.com:9092,cn-bdpkafka04.corp.cootek.com:9092,cn-bdpkafka05.corp.cootek.com:9092,cn-bdpkafka06.corp.cootek.com:9092,cn-bdpkafka07.corp.cootek.com:9092,cn-bdpkafka08.corp.cootek.com:9092"

from datetime import datetime, date
import time

UNK_FEA = 'unk'
EMPTY_FEA = ""
COMBINE_FEA_SEP = '_'

def format_str(rawlog):
    if rawlog in ["", None]:
        rawlog = UNK_FEA
    return str(rawlog).strip().lower().replace(' ', '#')

def usertype_fea(fac_ts, reqprt):
    if fac_ts == '':
        fac_ts = 0
    if reqprt == '':
        reqprt = 0
    fac_ts = int(fac_ts)
    prt = int(int(reqprt)*1.0/1000)
    if fac_ts == 0 or reqprt == 0:
        user_act = -1
    else:
        user_act = int((prt - fac_ts)*1.0/(60*60))
        if user_act > 168: 
            user_act = 168
    return user_act

def week_ed_fea(industry, user_frequency):
    freq = user_frequency.strip().split('_')
    if len(freq) != 4:
        return '%s_%s' % (industry, '0')
    else:
        return '%s_%s' % (industry, freq[0])

def week_clk_fea(industry, user_frequency):
    freq = user_frequency.strip().split('_')
    if len(freq) != 4:
        return '%s_%s' % (industry, '0')
    else:
        return '%s_%s' % (industry, freq[1])

def day_ed_fea(industry, user_frequency):
    freq = user_frequency.strip().split('_')
    if len(freq) != 4:
        return '%s_%s' % (industry, '0')
    else:
        return '%s_%s' % (industry, freq[1])
    return '%s_%s' % (industry, freq[2])

def day_clk_fea(industry, user_frequency):
    freq = user_frequency.strip().split('_')
    if len(freq) != 4:
        return '%s_%s' % (industry, '0')
    else:
        return '%s_%s' % (industry, freq[3])

def hour_fea(reqprt):
    try:
        timestamp = time.strftime('%Y%m%d%H%M%S', 
            time.localtime(float(str(reqprt)[:10])))
        return datetime.strptime(timestamp, "%Y%m%d%H%M%S").strftime("%H")
    except:
        return UNK_FEA 

def dayofweek_fea(reqprt):
    try:
        timestamp = time.strftime('%Y%m%d%H%M%S', 
            time.localtime(float(str(reqprt)[:10])))
        return str(datetime.strptime(timestamp, "%Y%m%d%H%M%S").strftime("%w"))
    except:
        return UNK_FEA 

def isholiday_fea(reqprt):
    try:
        timestamp = time.strftime('%Y%m%d%H%M%S',
            time.localtime(float(str(reqprt)[:10]))) 
        wk = str(datetime.strptime(timestamp, "%Y%m%d%H%M%S").strftime("%w"))
        if wk in ['0', '6']:
            return '1'
        else:
            return '0'
    except:
        return UNK_FEA 

def iswifi_fea(rawlog):
    if rawlog == '1':
        return '1'
    else:
        return '0'

def combined_fea(seg1, seg2): 
    return '%s%s%s' % (seg1, COMBINE_FEA_SEP, seg2) 

def json_parse(data_str):
    data_json = json.loads(data_str)
    cols = label + pattern
    ret_vals = []
    for c in cols:
        if c not in data_json:
            ret_vals.append('')
        else:
            ret_vals.append(format_str(data_json[c]))
    yield ret_vals

def json_parse1(data_str):
    data_json = json.loads(data_str)
    ed_log = json.loads(data_json['ed_log'])

    cols = label + pattern
    ret_vals = []
    for c in cols:
        if c == label[0]:
            is_clk = data_json['is_click'] #"1" if 'click_log' in data_json else "0"
            ret_vals.append(is_clk) 
        elif c not in ed_log:
            ret_vals.append('')
        else:
            ret_vals.append(format_str(ed_log[c]))
    yield ret_vals

udf_map = { 
    'combined_fea': (combined_fea, 'udf', [DataTypes.STRING(), DataTypes.STRING()], DataTypes.STRING()),
    'format_str': (format_str, 'udf', [DataTypes.STRING()], DataTypes.STRING()),
    'hour_fea': (hour_fea, 'udf', [DataTypes.STRING()], DataTypes.STRING()),
    'dayofweek_fea': (dayofweek_fea, 'udf', [DataTypes.STRING()], DataTypes.STRING()),
    'iswifi_fea': (iswifi_fea, 'udf', [DataTypes.STRING()], DataTypes.STRING()),
    'isholiday_fea': (isholiday_fea, 'udf', [DataTypes.STRING()], DataTypes.STRING()),
    'usertype_fea': (usertype_fea, 'udf', [DataTypes.STRING(), DataTypes.STRING()], DataTypes.INT()),
    'week_ed_fea': (week_ed_fea, 'udf', [DataTypes.STRING(), DataTypes.STRING()], DataTypes.STRING()),
    'week_clk_fea': (week_clk_fea, 'udf', [DataTypes.STRING(), DataTypes.STRING()], DataTypes.STRING()),
    'day_ed_fea': (day_ed_fea, 'udf', [DataTypes.STRING(), DataTypes.STRING()], DataTypes.STRING()),
    'day_clk_fea': (day_clk_fea, 'udf', [DataTypes.STRING(), DataTypes.STRING()], DataTypes.STRING()),
    'json_parse': (json_parse, 'udtf', [DataTypes.STRING()], [DataTypes.STRING()] * (len(pattern)+1)),
    'json_parse1': (json_parse1, 'udtf', [DataTypes.STRING()], [DataTypes.STRING()] * (len(pattern)+1))
}

def udfs_register(Operator):
    for func_name, item in udf_map.items():
        func_addr, udf_type, input_types, result_type = item
        if udf_type == 'udf':
            func_udf = udf(func_addr, input_types=input_types, result_type=result_type)
        else:
            func_udf = udtf(func_addr, input_types=input_types, result_types=result_type)
        Operator.registerFunction(func_name, func_udf)

def batch_model_train(operator, train_data_path, model_save_path):
    if not train_data_path:
        print("Error: train data path is NULL!")
        exit(1)
    if not model_save_path:
        print("Error: model save path is NULL!")
        exit(1)

    udfs_register(operator)
    data_batch = CsvSourceBatchOp()\
        .setFilePath(train_data_path)\
        .setSchemaStr('rawlog string')\
        .setFieldDelimiter("\001")
   
    data_batch = batch_model_feature_engineer(BatchOperator, data_batch)
    feature_pipeline = Pipeline() \
        .add(FeatureHasher() \
                .setSelectedCols(featureCols) \
                .setOutputCol('vec') \
                .setNumFeatures(numFea))
    data_batch = feature_pipeline.fit(data_batch).transform(data_batch)
    
    batch_model = LogisticRegressionTrainBatchOp()\
        .setL2(0.001)\
        .setWithIntercept(True)\
        .setVectorCol('vec')\
        .setEpsilon(1.0e-7)\
        .setLabelCol(label[0])\
        .setMaxIter(100).linkFrom(data_batch)
    
    csvSink = CsvSinkBatchOp()\
        .setFilePath(model_save_path) \
        .setOverwriteSink(True)

    batch_model.link(csvSink)
    operator.execute() 
    return batch_model

def online_learning(operator, batch_model, model_save_path):
    if not batch_model:
        print("Error: batch model is NULL!")
        exit(1)
    if not model_save_path:
        print("Error: model save path is NULL!")
        exit(1)

    udfs_register(operator)
    data_stream = KafkaSourceStreamOp() \
        .setBootstrapServers(BROKER_LIST) \
        .setTopic("Cootek_bdp_dsp_ed_clk") \
        .setStartupMode("GROUP_OFFSETS")\
        .setGroupId("flink_kafka_test")
    #.setStartupMode("earliest") \
    data_stream = online_model_feature_engineer(StreamOperator, data_stream) 
    hasher = FeatureHasherStreamOp()\
        .setSelectedCols(featureCols)\
        .setOutputCol("vec")\
        .setNumFeatures(numFea)
    data_stream = hasher.linkFrom(data_stream)
    models = FtrlTrainStreamOp(batch_model) \
            .setVectorCol('vec') \
            .setLabelCol('is_clk') \
            .setTimeInterval(1) \
            .setAlpha(0.1) \
            .setBeta(0.1) \
            .setL1(0.1) \
            .setL2(0.1)\
            .setVectorSize(numFea)\
            .setWithIntercept(True) \
            .linkFrom(data_stream)
    
    csvSink = CsvSinkStreamOp()\
        .setFilePath(online_model_save_path) \
        .setOverwriteSink(True)
 
    models.link(csvSink)
    operator.execute()  

def batch_model_predict(operator, model_in=None, valid_data_path=None, pred_out=None):
    if not operator:
        print("Error: BatchOperator is NULL!")
        exit(1)
    udfs_register(BatchOperator)
    if not model_in:
        print("Error: batch model is NULL!")
        exit(1)
    if not valid_data_path:
        print("Error: valid data path is NULL!")
        exit(1)
    if not pred_out:
        print("Error: valid data path is NULL!")
        exit(1)

    print("use batch model path: ", model_in)
    print("use valid data path: ", valid_data_path)
    schema = "model_id BIGINT, model_info VARCHAR, label_value VARCHAR"
    batch_model = CsvSourceBatchOp()\
        .setFilePath(model_in)\
        .setSchemaStr(schema)\
        .setFieldDelimiter(",") 

    valid_data = CsvSourceBatchOp()\
        .setFilePath(valid_data_path)\
        .setSchemaStr('rawlog string')\
        .setFieldDelimiter("\001")

    valid_data = batch_model_feature_engineer(operator, valid_data)

    feature_pipeline = Pipeline() \
        .add(FeatureHasher() \
                .setSelectedCols(featureCols) \
                .setOutputCol('vec') \
                .setNumFeatures(numFea))

    valid_data = feature_pipeline.fit(valid_data).transform(valid_data)

    predictor = LogisticRegressionPredictBatchOp()\
                    .setPredictionCol("pred") \
                    .setPredictionDetailCol("details")
    
    res_op = predictor.linkFrom(batch_model, valid_data)
     
    metrics = EvalBinaryClassBatchOp() \
                .setLabelCol(label[0]) \
                .setPredictionDetailCol("details")\
                .linkFrom(res_op)
    sink_res = res_op.link(SelectBatchOp().setClause("%s, details" % label[0]))
    csvSink = CsvSinkBatchOp()\
        .setFilePath(pred_out) \
        .setOverwriteSink(True)

    sink_res.link(csvSink)
    operator.execute()    
 
    
train_mode = 'only_batch'
job_mode = 'train'

if __name__ == '__main__':
    train_data_path = "hdfs:///user/ad_user/alink_train_data_week_sample/"
    batch_model_out = 'hdfs:///user/ad_user/alink_test_model/lr_model_batch.csv'
    model_in = "hdfs:///user/ad_user/alink_test_model/lr_model_batch.csv"

    #batch_model_in_path = "hdfs:///user/ad_user/alink_test_model/lr_model_batch.csv"
    #online_model_save_path = "hdfs:///user/ad_user/alink_test_model/lr_model.csv"
    
    valid_data_path = "hdfs:///user/ad_user/alink_test_data"
    pred_out = "hdfs:///user/ad_user/alink_test_result/result.csv"
    
    if job_mode == 'train':
        if train_mode == 'only_batch':
            batch_model = batch_model_train(BatchOperator, train_data_path, batch_model_out)
        elif train_mode == 'only_online':
            schema = "model_id BIGINT, model_info VARCHAR, label_value VARCHAR"
            batch_model = CsvSourceBatchOp()\
                .setFilePath(model_in)\
                .setSchemaStr(schema)\
                .setFieldDelimiter(",") 
            print('load batch model %s sucessfully, begin online learning...' % model_in)
            online_learning(StreamOperator, batch_model, model_out)
        else:
            batch_model = batch_model_train(BatchOperator, train_data_path, batch_model_out)
            online_learning(StreamOperator, batch_model, online_model_out)
    else:
        print("predict...")
        batch_model_predict(BatchOperator, model_in, valid_data_path, pred_out) 
