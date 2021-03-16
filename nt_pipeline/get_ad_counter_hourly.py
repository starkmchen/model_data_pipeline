from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf
import sys
import json
import datetime
from collections import defaultdict, Counter, namedtuple
from feature_conf import ad_count_feature_conf


def build_ad_counter(data):
  ad_counter = defaultdict(Counter)
  Sample = namedtuple('Sample', 'tag, pos_id, ad_id, c_id, \
      ad_package_name, ad_package_category, imp, click')
  for item in data:
    sample = Sample(*item)
    for conf in ad_count_feature_conf:
      ele_names = map(lambda x: str(getattr(sample, x)), conf.split('#'))
      key = '#'.join([sample.tag, conf] + ele_names)
      ad_counter[key] +=  Counter({'imp': sample.imp, 'click': sample.click})
  field_name = 'storeAdCounter'
  output = {field_name : {}}
  output_counter = output[field_name]
  for ele_name, v in ad_counter.items():
    li = ele_name.split('#')
    tag = li[0]
    ele_name = '#'.join(li[1:])
    if ele_name not in output_counter:
      output_counter[ele_name] = {}
    output_counter[ele_name]['countFeatures%s' % (tag)] = dict(v)
  return json.dumps(output)


if __name__ == '__main__':
  spark = SparkSession.builder.appName('get_ad_counter').enableHiveSupport().getOrCreate()
  date_str = sys.argv[1]
  hour_str = sys.argv[2]
  sql = "select '30d' as tag, pos_id,ad_id,c_id, ad_package_name,ad_package_category, sum(1) as imp_sum, \
      sum(is_click) as click_sum from sprs_ad_dwd.imp_click_install_cpi_hourly_temp \
      where dt = '%s' and package_name in ('video.likeit', 'video.watchit') \
      group by pos_id,ad_id,ad_package_name,ad_package_category" % (date_str)
  data = spark.sql(sql).rdd.collect()
  data = build_ad_counter(data)
  rdd = spark.sparkContext.parallelize([data])
  rdd.saveAsTextFile('s3://sprs.push.us-east-1.prod/data/warehouse/model/ad_counter_hourly/%s' % (hour_str))
