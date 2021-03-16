from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf
import sys
import json
import datetime
from collections import defaultdict, Counter, namedtuple
from feature_conf import user_count_feature_conf


def build_user_counter(data):
  user_id = data[0]
  user_counter = defaultdict(Counter)
  Sample = namedtuple('Sample', 'user_id, tag, package_name, pos_id, \
      ad_package_name, ad_id, ad_package_category, c_id, imp, click, attr_install')
  for item in data[1]:
    sample = Sample(user_id, *item)
    for conf in user_count_feature_conf:
      ele_names = map(lambda x: str(getattr(sample, x)), conf.split('#'))
      key = '#'.join([sample.tag, conf] + ele_names)
      user_counter[key] +=  Counter({'imp': sample.imp, 'click': sample.click, 'attrInstall': sample.attr_install})
  field_name = 'storeUserCounter'
  output = {'user_id': user_id, 'pb': {field_name: {}}}
  output_counter = output['pb'][field_name]
  for ele_name, v in user_counter.items():
    li = ele_name.split('#')
    tag = li[0]
    ele_name = '#'.join(li[1:])
    if ele_name not in output_counter:
      output_counter[ele_name] = {}
    output_counter[ele_name]['countFeatures%s' % (tag)] = dict(v)
  return json.dumps(output)


if __name__ == '__main__':
  spark = SparkSession.builder.appName('get_user_counter').enableHiveSupport().getOrCreate()
  date_str = sys.argv[1]
  sql = "select * from sprs_ad_dwd.user_feature_accu_temp where dt = '%s'" % (date_str)
  rdd = spark.sql(sql).rdd.map(lambda x:(x.beyla_id, (x.tag, x.package_name, x.pos_id,
       x.ad_package_name, x.ad_id, x.ad_package_category, x.c_id,
       x.imp_sum, x.click_sum, x.attr_install_sum))).groupByKey().mapValues(list).map(lambda x:build_user_counter(x))
  rdd.saveAsTextFile('s3://sprs.push.us-east-1.prod/data/warehouse/model/user_counter/%s' % (date_str))
