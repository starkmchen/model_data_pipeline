from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf
import sys
import json
import datetime
import copy
from collections import defaultdict, Counter, namedtuple

def build_user_profile(data, app_white_list):
    beyla_id = data[0]
    user_profile = {'userBase': {}, 'userBehavior': {}}
    for item in data[1]:
        d_type = item[0]
        if d_type == "base":
            user_profile['userBase'].update({'age': str(item[1][0]),
                'gender': str(item[1][1]), 'country': str(item[1][2])})
            try:
              user_profile['userBase']['province'] = str(item[1][3])
            except:
              user_profile['userBase']['province'] = ''
            try:
              user_profile['userBase']['city'] = str(item[1][4])
            except:
              user_profile['userBase']['city'] = ''
        elif d_type == 'behavior':
            beha_type = item[1][0]
            app_list = set(item[1][1])
            if beha_type == 'app_install':
                app_list = app_list.intersection(app_white_list)
                user_profile['userBehavior']['installAppList'] = copy.copy(list(app_list))
            elif beha_type == 'app_use':
                app_list = app_list.intersection(app_white_list)
                user_profile['userBehavior']['useAppList'] = copy.copy(list(app_list))
            elif beha_type == 'ad_app_click':
                user_profile['userBehavior']['clickAdAppList'] = copy.copy(list(app_list))
            elif beha_type == 'ad_app_install':
                user_profile['userBehavior']['installAdAppList'] = copy.copy(list(app_list))
    output = {'user_id': beyla_id, 'pb': user_profile}
    return json.dumps(output)

if __name__ == '__main__':
  spark = SparkSession.builder.appName('midas_offline_get_train_feature').enableHiveSupport().getOrCreate()
  date_str = sys.argv[1]
  #df_app_sql1 = "select beyla_id, action_type, app_list from sprs_ad_dws.dws_midas_offline_user_app_list_inc_daily where dt = '%s'" % (date_str)
  #rdd1 = spark.sql(df_app_sql1).rdd.map(lambda x:(x.beyla_id, ("behavior", (x.action_type, x.app_list))))

  df_base_sql = "select beyla_id, coalesce(age, '') as age, coalesce(gender, '') as gender, \
      coalesce(country, '') as country, coalesce(province, '') as province, coalesce(city, '') as city \
      from sprs_ad_dws.dws_user_profile_main"
  df = spark.sql(df_base_sql)
  rdd2 = spark.sql(df_base_sql).rdd.map(lambda x: (x.beyla_id, ("base", (x.age, x.gender, x.country, x.province, x.city))))

  app_white_list_df = spark.read.text("s3://sprs.push.us-east-1.prod/data/warehouse/model/app_white_list")
  bv = set(app_white_list_df.rdd.map(lambda x:x[0]).collect())
  app_white_list = spark.sparkContext.broadcast(bv)

  #rdd = spark.sparkContext.union([rdd1, rdd2])
  rdd = rdd2
  rdd = rdd.groupByKey().mapValues(list).map(lambda x:build_user_profile(x, app_white_list.value))
  rdd.saveAsTextFile("s3://sprs.push.us-east-1.prod/data/warehouse/midas_offline_model/user_profile/%s" % (date_str))
