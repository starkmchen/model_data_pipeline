from store_table_pb2 import StoreUserProfile, StoreUserCounter
import json
from google.protobuf import json_format
import socket
import sys
import time
import os
from multiprocessing import Process
from sharestore_lib import SharestorePool

options = {'host': 'NLB-CBS-sharestore-server-test-9316364d543dd2d6.elb.ap-southeast-1.amazonaws.com', 'port': 9090}
options = {'host': 'prod.main.cbs.sg1.sharestore', 'port': 9090}
table = 'sprs_ads'
dname = 'user_counter'

def process_func(fname_list):
  pool = SharestorePool(max_size=10, options=options)
  pool.SetTimeout(timeout_ms=1000)
  for fname in fname_list:
    full_name = "%s/%s" % (dname, fname)
    f = open(full_name)
    for line in f:
      j = json.loads(line)
      user_id = j['user_id']
      pb = StoreUserCounter()
      json_format.Parse(json.dumps(j['pb']), pb)
      key = "nt:ads:user_counter:%s" % (user_id)
      pool.SetValue(table, key, pb.SerializeToString(), 24 * 3600 * 3)

def write_user_counter():
  li = os.listdir(dname)
  p_num = 20
  batch = int((len(li) + p_num) / p_num)
  p_li = []
  for i in range(0, len(li), batch):
    p = Process(target = process_func, args = (li[i:i+batch],))
    p.start()
    p_li.append(p)
  for p in p_li:
    p.join()

def main():
  print(time.time())
  write_user_counter()
  print(time.time())


if __name__ == '__main__':
  main()
