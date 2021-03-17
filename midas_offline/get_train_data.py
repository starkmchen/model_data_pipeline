from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf
import sys
import json
import datetime
import copy
from collections import defaultdict, Counter, namedtuple
from pyspark.sql.functions import to_json
from feature_conf import user_count_feature_conf, ad_count_feature_conf

def get_pb_key(key):
    small_key = '_'.join(key.split('#'))
    ele_names = small_key.split('_')
    li = [ele_names[0]]
    li.extend(map(lambda x:x.capitalize(), ele_names[1:]))
    return ''.join(li)


def build_train_data(data, ad_counter_dict):
    Sample = namedtuple('Sample', 'user_id,nation,package_name,pos_id,client_ip,server_time,os_version,brand,model, \
        system_language, app_version_code, ad_package_name, ad_package_category, ad_id, c_id, is_auto_download, \
        is_click, is_install, counter_pb, profile_pb')
    sample = Sample(*data)
    sample_json = {'feature': {}, 'label': {'click': sample.is_click, 'install': sample.is_install}}

    if sample.profile_pb is not None:
        user_profile_pb = json.loads(sample.profile_pb)
    else:
        user_profile_pb = {'userBase': {}, 'userBehavior': {}}
    if sample.counter_pb is not None:
        user_counter_dict = json.loads(sample.counter_pb)
    else:
        user_counter_dict = {'storeUserCounter': {}}

    sample_json['feature']['userProfile'] = {'userId': sample.user_id, \
        'userBase': copy.deepcopy(user_profile_pb['userBase']), \
        'userBehavior': copy.deepcopy(user_profile_pb['userBehavior']), 'userCounter': {}}

    sample_json['feature']['userAdFeature'] = {'userAdCount': {}}
    user_counter_json = sample_json['feature']['userAdFeature']['userAdCount']

    for conf in user_count_feature_conf:
        try:
          ele_names = map(lambda x: str(getattr(sample, x)), conf.split('#'))
        except:
          continue
        key = conf + '#' + '#'.join(ele_names)
        pb_key = get_pb_key(conf)
        if conf == 'user_id':
            sample_json['feature']['userProfile']['userCounter'][pb_key] = \
                copy.deepcopy(user_counter_dict['storeUserCounter'].get(key, {}))
        else:
            user_counter_json[pb_key] = copy.deepcopy(user_counter_dict['storeUserCounter'].get(key, {}))

    sample_json['feature']['context'] = {'posId': sample.pos_id, 'appName': sample.package_name, 'nation': sample.nation, \
        'osVersion': sample.os_version, 'brand': sample.brand, 'model': sample.model, 'language': sample.system_language, \
        'appVersionCode': sample.app_version_code, 'clientIp': sample.client_ip, 'reqTime': sample.server_time}

    sample_json['feature']['adData'] = {'adCounter': {}}
    sample_json['feature']['adData']['adInfo'] = {'adId': sample.ad_id, 'appId': sample.ad_package_name, \
            'category': sample.ad_package_category, 'isAutoDownload': sample.is_auto_download, 'creativeId': str(sample.c_id)}

    ad_counter_json = sample_json['feature']['adData']['adCounter']
    for conf in ad_count_feature_conf:
        ele_names = map(lambda x: str(getattr(sample, x)), conf.split('#'))
        key = sample.nation + '#' + conf + '#' + '#'.join(ele_names)
        pb_key = get_pb_key(conf)
        ad_counter_json[pb_key] = copy.deepcopy(ad_counter_dict['storeAdCounter'].get(key, {}))

    return json.dumps(sample_json)

def parse_user_info(x):
    j = json.loads(x)
    return {'user_id': j['user_id'], 'pb': json.dumps(j['pb'])}

if __name__ == '__main__':
  spark = SparkSession.builder.appName('midas_offline_get_train_feature').enableHiveSupport().getOrCreate()
  feature_date = sys.argv[1]
  label_date = sys.argv[2]

  df_user_counter = spark.read.text("s3://sprs.push.us-east-1.prod/data/warehouse/midas_offline_model/user_counter/%s" % (feature_date))
  df_user_counter = spark.createDataFrame(df_user_counter.rdd.map(lambda x: parse_user_info(x[0])))
  df_user_counter.createOrReplaceTempView("user_counter")

  df_user_profile = spark.read.text("s3://sprs.push.us-east-1.prod/data/warehouse/midas_offline_model/user_profile/%s" % (feature_date))
  df_user_profile = spark.createDataFrame(df_user_profile.rdd.map(lambda x: parse_user_info(x[0])))
  df_user_profile.createOrReplaceTempView("user_profile")

  sql = "select beyla_id, req_country, package_name, pos_id, client_ip,server_time,os_version,brand,model, \
      system_language, app_version_code, ad_package_name, ad_package_category, ad_id, c_id, is_auto_download, \
      is_click, is_install, user_counter.pb as counter_pb, user_profile.pb as profile_pb \
      from (select * from sprs_ad_dws.dws_beyla_midas_offline_inc_daily where dt = '%s' and req_country = 'EG') label_t \
      left join user_counter on label_t.beyla_id = user_counter.user_id \
      left join user_profile on label_t.beyla_id = user_profile.user_id" % (label_date)

  df_ad_counter = spark.read.text("s3://sprs.push.us-east-1.prod/data/warehouse/midas_offline_model/ad_counter/%s" % (feature_date))
  bv = df_ad_counter.rdd.map(lambda x: json.loads(x[0])).collect()[0]
  ad_counter_dict = spark.sparkContext.broadcast(bv)

  rdd = spark.sql(sql).rdd.map(lambda x: build_train_data(x, ad_counter_dict.value))
  rdd.saveAsTextFile('s3://sprs.push.us-east-1.prod/data/warehouse/midas_offline_model/train_data/%s' % (label_date))
