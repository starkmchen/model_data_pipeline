from store_table_pb2 import StoreAdCounter, StoreAdInfo
import json
from google.protobuf import json_format


def write_ad_counter():
  f = open('ad_counter')
  j = json.load(f)
  a = StoreAdCounter()
  json_format.Parse(json.dumps(j), a)
  f = open('ad_counter.pb', 'wb')
  f.write(a.SerializeToString())

write_ad_counter()
