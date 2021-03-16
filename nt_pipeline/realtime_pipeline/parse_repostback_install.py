from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf
import sys
import json
import datetime
from collections import defaultdict, Counter, namedtuple


def build_data(data):
  output = '\t'.join(map(str, data))
  return output


if __name__ == '__main__':
  spark = SparkSession.builder.appName('parse_repostback_install').enableHiveSupport().getOrCreate()
  out_date = sys.argv[1]
  out_day = datetime.datetime.strptime(out_date, "%Y%m%d%H").strftime("%Y%m%d")
  out_hour = datetime.datetime.strptime(out_date, "%Y%m%d%H").strftime("%H")
  df = spark.read.parquet("s3://shareit.ads.ap-southeast-1/dw/table/dwd/dwd_adcs_adping_logs_detail_hour/dt=%s/hour=%s/flag=repostback" % (out_day, out_hour))
  df.createOrReplaceTempView("repostback")
  sql = "select requestid,beyla_id, nation, placement, channel_pkg, pkg_name, cpi_camp_id, c_id from repostback \
      where source = 'cpi' and channel_pkg in ('video.watchit', 'video.likeit')"
  rdd = spark.sql(sql).rdd.map(lambda x: build_data(x))
  rdd.saveAsTextFile('s3://sprs.push.us-east-1.prod/data/warehouse/model/repostback_install/%s' % (out_date))
