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
      sample = self.fe_lib.extract_tf_example(data.value)
      yield b64encode(sample)
      #yield sample


if __name__ == '__main__':
  spark = SparkSession.builder.appName('tf_feature_extract').enableHiveSupport().getOrCreate()
  date_str = sys.argv[1]
  train_data = spark.read.text('s3://sprs.push.us-east-1.prod/data/warehouse/model/train_data/%s' % (date_str))
  p = Partitioner()
  rdd = train_data.rdd.mapPartitions(p.processPartition)
  rdd.saveAsTextFile('s3://sprs.push.us-east-1.prod/data/warehouse/model/train_tf_record/%s' % (date_str))
