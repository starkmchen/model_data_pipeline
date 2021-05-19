from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf
import sys
import json
import datetime
from collections import defaultdict, Counter, namedtuple


def build_data(data):
  output = []
  requestid, beyla_id, country, pos_id,  package_name, event_name, succ_camp = data
  succ_camp = json.loads(succ_camp)
  c_id = succ_camp['c_id']
  ad_id = succ_camp['campaign_id']
  ad_pkg_name = succ_camp['ad_package_name']
  try:
    output = '\t'.join(map(str, [requestid, beyla_id, country, pos_id,  package_name, event_name, ad_pkg_name, ad_id, c_id]))
  except:
    return ''
  return output


if __name__ == '__main__':
  spark = SparkSession.builder.appName('parse_imp_click').enableHiveSupport().getOrCreate()
  out_date = sys.argv[1]
  out_day = datetime.datetime.strptime(out_date, "%Y%m%d%H").strftime("%Y%m%d")
  out_hour = datetime.datetime.strptime(out_date, "%Y%m%d%H").strftime("%H")
  df = spark.read.parquet("s3://shareit.ads.ap-southeast-1/dw/table/dwb/dwb_adcs_cpi_logs_hour/dt=%s/hour=%s" % (out_day, out_hour))
  df.createOrReplaceTempView("imp_click")
  sql = "select requestid, beyla_id, country, pos_id, package_name, event_name, succ_camp from imp_click where \
      sub_platform = 'cpi' and package_name in ('video.watchit', 'video.likeit', 'video.likeit.lite') and event_name in ('AD_CpiShow', 'AD_CpiClick')"
  rdd = spark.sql(sql).rdd.map(lambda x: build_data(x)).filter(lambda data: bool(data))
  rdd.saveAsTextFile('s3://sprs.push.us-east-1.prod/data/warehouse/model/nt_cpi_imp_click_hourly/%s' % (out_date))
