import kafka
import json
import os
from collections import defaultdict
import datetime

servers = 'prod.cpi-report.ads-data.sprs.sg1.mq:9092'
update_interval=30000
topic_cpi = 'sprs_ads-data_cpi-report'
consumer = kafka.KafkaConsumer(topic_cpi, group_id = 'ad_rt_counter',\
        auto_offset_reset="latest", auto_commit_interval_ms = update_interval, bootstrap_servers = [servers])

def instanceHandler(instance):
    try:
        m = json.loads(instance).get('message', '')
        ad_msg = json.loads(m)
    except:
        return []
    sub_pf = ad_msg.get('sub_pf', '')
    if sub_pf != 'cpi':
      return []
    event_name = ad_msg.get('event_name', '')
    package_name = ad_msg.get('package_name', '')
    output = []
    if package_name in ('video.watchit', 'video.likeit') and event_name in ('AD_CpiShow', 'AD_CpiClick'):
        pos_id = ad_msg.get('pos_id', '')
        beyla_id = ad_msg.get('beyla_id', '')
        if len(beyla_id) == 0:
            return []
        for item in ad_msg.get('params', []):
            ad_package_name = item.get('ad_package_name', '')
            if len(ad_package_name) == 0:
                continue
            ad_id = item.get('campaign_id', 0)
            c_id = item.get('c_id', 0)
            if ad_id == 0 or c_id == 0:
                continue
            elem = [event_name, beyla_id, package_name, pos_id, ad_package_name, ad_id, c_id]
            output.append('\t'.join(map(str, elem)) + '\n')
    return output

def write_log(log_data, fname):
    dname = 'rt_ad_data/%s/' % fname
    os.mkdir(dname)
    f = open('%s/part-00000' % dname, 'w')
    f.writelines(log_data)
    f.close()

log_data = []
for msg in consumer:
    output = instanceHandler(msg.value)
    log_data.extend(output)
    now_time = datetime.datetime.now()
    now_min = int(now_time.strftime("%M"))
    if now_min % 5 == 0:
        write_log(log_data, now_time.strftime("%Y%m%d%H%M"))
        log_data = []
