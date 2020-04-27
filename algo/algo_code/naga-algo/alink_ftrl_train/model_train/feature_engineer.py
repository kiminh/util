#!/usr/bin/python3.5
pattern = [
    'did',
    'reqprt',
    'nt',
    'adid', 
    'planid', 
    'make', 
    'osv',
    'model',
    'ad_style',
    'campaignid',
    'orgid',
    'appid',
    'plid',
    'city_id',
    'adw',
    'adh',
    'pw',
    'ph',
    'app_series',
    'industry',
    'fac_ts',
    'user_frequency'
]
label = ['is_clk']

NO_USE=False
USE=True
# fea_name, (org_seg, udf, use or not)
direct_fea_map = {
    'did': (['did'], None, USE),
    'hr': (['reqprt'], 'hour_fea', USE),
    'wk': (['reqprt'], 'dayofweek_fea', USE),
    'daytp': (['reqprt'], 'isholiday_fea', USE),
    'nt': (['nt'], None, USE),
    'iswifi': (['nt'], 'iswifi_fea', USE),
    'city': (['city_id'], None, USE),
    'osv': (['osv'], None, USE),
    'adid': (['adid'], None, USE), 
    'planid': (['planid'], None, USE), 
    'make': (['make'], None, USE), 
    'model': (['model'], None, USE),
    'make_model': (['make', 'model'], 'combined_fea', USE),
    'adstyle': (['ad_style'], None, USE),
    'cmpid': (['campaignid'], None, USE),
    'orgid': (['orgid'], None, USE),
    'appid': (['appid'], None, USE),
    'plid': (['plid'], None, USE),
    'adwh': (['adw', 'adh'], 'combined_fea', USE),
    'pwh': (['pw', 'ph'], 'combined_fea', USE),
    'app_series': (['app_series'], None, USE),
    'industry': (['industry'], None, USE),
    'useracttime': (['fac_ts', 'reqprt'], 'usertype_fea', NO_USE),
    'week_ed': (['industry', 'user_frequency'], 'week_ed_fea', USE),
    'week_clk': (['industry', 'user_frequency'], 'week_clk_fea', USE),
    'day_ed': (['industry', 'user_frequency'], 'day_ed_fea', USE),
    'day_clk': (['industry', 'user_frequency'], 'day_clk_fea', USE)
}

combined_fea_map = {
    'adwh_pwh': (['adwh', 'pwh'], 'combined_fea'),
    'pwh_appid': (['pwh', 'appid'], 'combined_fea'),
    'pwh_plid': (['pwh', 'plid'], 'combined_fea'),
    'make_model_appid': (['make_model', 'appid'], 'combined_fea'),
    'make_model_plid': (['make_model', 'plid'], 'combined_fea'),
    'make_model_adid': (['make_model', 'adid'], 'combined_fea'),
    'appid_planid': (['appid', 'planid'], 'combined_fea'),
    'appid_cmpid': (['appid', 'cmpid'], 'combined_fea'),
    'appid_hour': (['appid', 'hr'], 'combined_fea'),
    'appid_adid': (['appid', 'adid'], 'combined_fea'),
    'plid_hour': (['plid', 'hr'], 'combined_fea'),
    'plid_adid': (['plid', 'adid'], 'combined_fea'),
    'plid_planid': (['plid', 'planid'], 'combined_fea'),
    'plid_cmpid': (['plid', 'cmpid'], 'combined_fea'),
    'adid_hour': (['adid', 'hr'], 'combined_fea'),
    'city_adid': (['city', 'adid'], 'combined_fea'),
    'city_plid': (['city', 'plid'], 'combined_fea'),
    'city_appid': (['city', 'appid'], 'combined_fea'),
    'iswifi_adid': (['iswifi', 'adid'], 'combined_fea'),
    'orgid_plid': (['orgid', 'plid'], 'combined_fea'),
    'orgid_iswifi': (['orgid', 'iswifi'], 'combined_fea'),
    'orgid_hour': (['orgid', 'hr'], 'combined_fea'),
    'nt_adid': (['nt', 'adid'], 'combined_fea'),
    'nt_plid': (['nt', 'plid'], 'combined_fea'),
    'appid_orgid': (['appid', 'orgid'], 'combined_fea'),
    'appseries_industry': (['app_series', 'industry'], 'combined_fea'),
    'useracttime_planid': (['useracttime', 'planid'], 'combined_fea'),
    'useracttime_adstyle': (['useracttime', 'ad_style'], 'combined_fea')
}

featureCols = [ fea for fea, item in direct_fea_map.items() if item[2] ] + list(combined_fea_map.keys())
print("use number of features: %s" % len(set(featureCols)))
print(featureCols)
numFea = 10000000

def generate_feature_engineer_sql(fea_map, table_name):
    sql = "select *, "
    for key, value in fea_map.items():
        fea_name = key 
        fea_udf = value[1]
        dep_segs = value[0]
            
        if fea_udf:
            sql += "%s(%s) as %s," % (fea_udf, ','.join(dep_segs), fea_name)
        else:
            sql += "%s as %s," % (','.join(dep_segs), fea_name)
    sql = sql.strip(",") + " from %s" % table_name
    return sql

def generate_batch_json_parse_sql(table_name):
    """
        batch data json parse sql
    """
    extract_cols = ','.join(label+pattern) 
    sql = "select * from %s, lateral table(json_parse(rawlog)) as T(%s)" % (table_name, extract_cols)
    return sql

def generate_online_json_parse_sql(table_name):
    extract_cols = ','.join(pattern)
    sql = "select seqs from %s, lateral table(json_parse_online(message, '%s')) as T(seqs)" % (table_name, extract_cols)
    return sql

def generate_online_cols_extract_sql(table_name):
    extract_cols = label+pattern
    sql = "select "
    for index, seg in enumerate(extract_cols):
        sql += "seqs[%s] as %s," % (index+1, seg)
    sql = sql.strip(',') + ' from %s' % table_name
    return sql

def batch_model_feature_engineer(operator, data_batch):
    data_batch.registerTableName("batch_t_a")
    sql = generate_batch_json_parse_sql("batch_t_a")
    operator.sqlQuery(sql).registerTableName("batch_t_b")
    sql = generate_feature_engineer_sql(direct_fea_map, "batch_t_b")
    operator.sqlQuery(sql).registerTableName("batch_t_c")
    sql = generate_feature_engineer_sql(combined_fea_map, "batch_t_c")
    data_batch = operator.sqlQuery(sql)
    return data_batch

def online_model_feature_engineer(operator, data_stream):
    data_stream.registerTableName("stream_t_a")
    sql = generate_online_json_parse_sql("stream_t_a")
    operator.sqlQuery(sql).registerTableName("stream_t_b")
    sql = generate_online_cols_extract_sql("stream_t_b")
    operator.sqlQuery(sql).registerTableName("stream_t_c")
    sql = generate_feature_engineer_sql(direct_fea_map, "stream_t_c")
    operator.sqlQuery(sql).registerTableName("stream_t_d")
    sql = generate_feature_engineer_sql(combined_fea_map, "stream_t_d")
    data_stream = operator.sqlQuery(sql)
    return data_stream 

