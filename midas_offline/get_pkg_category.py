from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf
import sys
import json
import datetime
from collections import defaultdict, Counter, namedtuple


def build_info(data):
    field_key = 'adInfos'
    ad_infos = {field_key: {}}
    for pkg, category in data:
        ad_infos[field_key][pkg] = {'category': category}
    return json.dumps(ad_infos)


if __name__ == '__main__':
  spark = SparkSession.builder.appName('midas_offline_get_pkg_info').enableHiveSupport().getOrCreate()
  date_str = sys.argv[1]
  sql = "select package_name, category from sprs_ad_dwd.app_category_temp"
  data = spark.sql(sql).rdd.collect()
  data = build_info(data)
  rdd = spark.sparkContext.parallelize([data])
  rdd.saveAsTextFile('s3://sprs.push.us-east-1.prod/data/warehouse/midas_offline_model/pkg_info/%s' % (date_str))
