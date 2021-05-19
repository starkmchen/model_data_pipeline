from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf
import sys
import json
import datetime

def parse_rec_ads(data):
  req_ads = json.loads(data.value)
  output = []
  for ad in req_ads.get('req_ads', []):
    output.append(json.dumps(ad))
  return output


if __name__ == '__main__':
  spark = SparkSession.builder.appName('nt_hour_log').enableHiveSupport().getOrCreate()
  out_date = sys.argv[1]
  out_day = datetime.datetime.strptime(out_date, "%Y%m%d%H").strftime("%Y%m%d")
  out_hour = datetime.datetime.strptime(out_date, "%Y%m%d%H").strftime("%H")
  in_date = (datetime.datetime.strptime(out_date, "%Y%m%d%H") + datetime.timedelta(hours = 8)).strftime("%Y/%m/%d/%H")
  data = spark.read.text('s3://cbs.metis.ap-southeast-1/sprs/ad-model-server-basic/sprs_ad-model-server-basic_stat_req-ads/prod/%s' % in_date)
  rdd = data.rdd.map(lambda x: parse_rec_ads(x)).filter(lambda data: bool(data)).flatMap(lambda x:x)
  rdd.saveAsTextFile('s3://sprs.push.us-east-1.prod/data/warehouse/model/nt_req_ads_log_inc_hourly/dt=%s/hour=%s' % (out_day, out_hour))
