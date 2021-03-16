from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf
import sys
import json
import datetime

def parse_rec_ads(data):
  rsp = json.loads(data.value)
  output = []
  hit_exp = rsp.get('hit_exp', [])
  exp_ids = ','.join(hit_exp)
  ad_li = rsp.get('model_results', {}).get('res_model', [])
  for ad in ad_li:
    ad_json = {}
    ad_json['request_id'] = rsp['request_id']
    ad_json['exp_ids'] = exp_ids
    ad_json['user_id'] = rsp['user_id']
    ad_json['pos_id'] = rsp['pos_id']
    ad_json['timestamp'] = rsp['timestamp']
    ad_json['app_id'] = ad['app_id']
    ad_json['camp_id'] = ad['camp_id']
    ad_json['creative_id'] = ad['creative_id']
    ad_json['ecpm'] = ad['ecpm']
    ad_json['pctcvr'] = ad['model_spec']
    output.append(json.dumps(ad_json))
  return output


if __name__ == '__main__':
  spark = SparkSession.builder.appName('get_ad_counter').enableHiveSupport().getOrCreate()
  out_date = sys.argv[1]
  out_day = datetime.datetime.strptime(out_date, "%Y%m%d%H").strftime("%Y%m%d")
  out_hour = datetime.datetime.strptime(out_date, "%Y%m%d%H").strftime("%H")
  in_day = (datetime.datetime.strptime(out_date, "%Y%m%d%H") + datetime.timedelta(hours = 8)).strftime("%Y%m%d")
  in_hour = (datetime.datetime.strptime(out_date, "%Y%m%d%H") + datetime.timedelta(hours = 8)).strftime("%H")
  data = spark.read.text('s3://sprs.push.us-east-1.prod/data/warehouse/sprs_ad_ods/ods_modelserver_log_inc_hourly/datepart=%s/hour=%s' % (in_day, in_hour))
  rdd = data.rdd.map(lambda x: parse_rec_ads(x)).filter(lambda data: bool(data)).flatMap(lambda x:x)
  rdd.saveAsTextFile('s3://sprs.push.us-east-1.prod/data/warehouse/model/nt_mixer_log_inc_hourly/dt=%s/hour=%s' % (out_day, out_hour))
