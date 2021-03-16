from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf
import sys
import json
import datetime
import copy
import os
from collections import defaultdict, Counter, namedtuple
from base64 import b64encode
from operator import add


class Partitioner:
  def __init__(self):
    pass
  def callPartitionSetup(self):
    sys.path.append('lib')
    import libpyfeature_extract
    self.fe_lib = libpyfeature_extract.PyFeatureExtract('')
  def processPartition(self, partition):
    self.callPartitionSetup()
    for data in partition:
      output = []
      sample = data.value
      obj = json.loads(self.fe_lib.extract(sample))
      click = obj['label']['click']
      attr_install = obj['label']['attr_install']
      for k, v in obj.get('int_features', {}).items():
        output.append(((k,v, click, attr_install), 1))
      yield output


if __name__ == '__main__':
  spark = SparkSession.builder.appName('tf_feature_analysis').enableHiveSupport().getOrCreate()
  date_str = sys.argv[1]
  train_data = spark.read.text('s3://sprs.push.us-east-1.prod/data/warehouse/model/train_data/%s' % (date_str))
  p = Partitioner()
  rdd = train_data.rdd.mapPartitions(p.processPartition).flatMap(lambda x:x).reduceByKey(add). \
      map(lambda x: '\t'.join(map(str, (x[0][0], x[0][1], x[0][2], x[0][3], x[1]))))
  rdd.saveAsTextFile('s3://sprs.push.us-east-1.prod/data/warehouse/model/model_analysis/%s' % (date_str))
