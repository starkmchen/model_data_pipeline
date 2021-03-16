from store_table_pb2 import StoreAdCounter, StoreAdInfo
import json
from google.protobuf import json_format

def write_ad_info():
  f = open('ad_info')
  j = json.load(f)
  a = StoreAdInfo()
  json_format.Parse(json.dumps(j), a)
  f = open('ad_info.pb', 'wb')
  f.write(a.SerializeToString())

write_ad_info()

