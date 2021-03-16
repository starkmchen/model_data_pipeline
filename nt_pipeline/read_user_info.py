import json
from google.protobuf import json_format
import socket
import sys
import time
import os
from multiprocessing import Process
from sharestore_lib import SharestorePool
from store_table_pb2 import StoreUserProfile, StoreUserCounter

options = {'host': 'NLB-CBS-sharestore-server-test-9316364d543dd2d6.elb.ap-southeast-1.amazonaws.com', 'port': 9090}
options = {'host': 'prod.main.cbs.sg1.sharestore', 'port': 9090}
table = 'sprs_ads'
dname = 'user_counter'

pool = SharestorePool(max_size=10, options=options)
pool.SetTimeout(timeout_ms=1000)

def read_user_counter(user_id):
    pb = StoreUserCounter()
    key = "nt:ads:user_counter:%s" % (user_id)
    value, ret = pool.GetValue(table, key)
    pb.ParseFromString(value)
    print(pb)

def read_user_profile(user_id):
    pb = StoreUserProfile()
    key = "nt:ads:user_profile:%s" % (user_id)
    value, ret = pool.GetValue(table, key)
    pb.ParseFromString(value)
    print(pb)


user_id = "ec8c298042194ae38711d545bbf48232"
read_user_profile(user_id)
read_user_counter(user_id)

