from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf
import sys
import json
import datetime
import copy
from collections import defaultdict, Counter, namedtuple

def build_train_data(x):
    is_click, is_attr_install, feature = x
    output = {'label': {'click': is_click, 'attrInstall': is_attr_install}}
    output['feature'] = json.loads(feature)
    return json.dumps(output)

if __name__ == '__main__':
  spark = SparkSession.builder.appName('join_train_feature').enableHiveSupport().getOrCreate()
  feature_date = sys.argv[1]

  df_label = spark.sql("select rid, ad_id, c_id, is_click, is_attr_install from sprs_ad_dwd.imp_click_install_temp \
      where dt = '%s' and package_name in ('video.likeit', 'video.watchit', 'video.likeit.lite')" % feature_date)

  df_feature = spark.read.json('s3://sprs.push.us-east-1.prod/data/warehouse/model/nt_train_feature_hourly/dt=%s' % feature_date)

  cond = [df_label.rid == df_feature.request_id, df_label.ad_id == df_feature.ad_id, df_label.c_id == df_feature.creative_id]
  result = df_label.join(df_feature, cond).select(df_label.is_click, df_label.is_attr_install, df_feature.feature)

  rdd = result.rdd.map(lambda x: build_train_data(x))
  rdd.saveAsTextFile('s3://sprs.push.us-east-1.prod/data/warehouse/model/train_data/%s' % (feature_date))
