import mysql.connector
import datetime
import sys
import json
from google.protobuf import json_format
from store_table_pb2 import StoreAdInfo
from model_ad_pb2 import AdInfo

def get_mysql_result(host, sql, user, passwd):
    import mysql.connector
    db = mysql.connector.connect(
        host = host,
        user = user,
        passwd = passwd)
    cursor = db.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    return result

def update_creative_info(store_ad_info):
  host = "prod.cpi-platform-ro.ads.sg1.mysql"
  user = "cpi_platform_reader"
  passwd = "mEAYkZeJVQ7KjsMv8WKqWXumiehAhKCD"
  before_day = (datetime.datetime.now() - datetime.timedelta(days = 7)).strftime("%Y-%m-%d %H:%M:%S")
  sql = "select id,create_time from cpi_platform.creatives where create_time > STR_TO_DATE('%s', '%%Y-%%m-%%d %%H:%%i:%%s')" % before_day
  result = get_mysql_result(host, sql, user, passwd)
  for item in result:
      c_id = item[0]
      create_time = int(item[1].strftime("%s"))
      ad_info = AdInfo()
      ad_info.creative_create_time = create_time
      key = "c_id#%s" % c_id
      store_ad_info.ad_infos[key].CopyFrom(ad_info)


def load_pkg_category(fname):
  f = open(fname)
  j = json.load(f)
  a = StoreAdInfo()
  json_format.Parse(json.dumps(j), a)
  return a

def main(argv):
    store_ad_info = load_pkg_category(argv[0])
    update_creative_info(store_ad_info)
    f = open(argv[1], 'wb')
    f.write(store_ad_info.SerializeToString())
    f.close()

if __name__ == '__main__':
    main(sys.argv[1:])
