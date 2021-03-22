from store_table_pb2 import StoreAdCounter, StoreAdInfo
import json
import sys
from collections import defaultdict, Counter, namedtuple
import datetime
from google.protobuf import json_format
from feature_conf import ad_count_feature_conf


def get_rt_counter(rt_counter, sample, event_name, tag = "normal"):
    for conf in ad_count_feature_conf:
        if 'category' in conf:
            continue
        if tag == 'bj' and conf != 'ad_id':
            continue
        ele_names = map(lambda x: str(getattr(sample, x)), conf.split('#'))
        ele_name = '#'.join([conf] + ele_names)
        if ele_name not in rt_counter:
            rt_counter[ele_name] = {}
        if tag == 'bj':
            counter_key = 'countFeaturesBj1d'
        else:
            counter_key = 'countFeatures1d'
        if counter_key not in rt_counter[ele_name]:
            rt_counter[ele_name][counter_key] = {}
        if event_name not in rt_counter[ele_name][counter_key]:
            rt_counter[ele_name][counter_key][event_name] = 1
        else:
            rt_counter[ele_name][counter_key][event_name] += 1


def load_hourly_imp_click(fname, rt_counter):
    Sample = namedtuple('Sample', 'requestid, beyla_id, country, pos_id, package_name, event_name, ad_package_name, ad_id, c_id')
    f = open(fname)
    for line in f:
        li = line.strip().split('\t')
        sample = Sample(*li)
        if 'Show' in sample.event_name:
            event_name = 'imp'
        elif 'Click' in sample.event_name:
            event_name = 'click'
        else:
            continue
        get_rt_counter(rt_counter, sample, event_name)

def load_hourly_install(fname, rt_counter, tag = 'normal'):
    Sample = namedtuple('Sample', 'requestid,beyla_id, country, pos_id, package_name, ad_package_name, ad_id, c_id')
    f = open(fname)
    for line in f:
        li = line.strip().split('\t')
        sample = Sample(*li)
        get_rt_counter(rt_counter, sample, 'attrInstall', tag)

def merge_feature_counter(counter_name, new_fea_counter, old_counter):
    if counter_name not in old_counter:
        old_counter[counter_name] = {}
    for event_key, num in new_fea_counter.iteritems():
        if event_key not in old_counter[counter_name]:
            old_counter[counter_name][event_key] = num
        else:
            old_counter[counter_name][event_key] += num

def merge_counter(rt_counter, day_counter):
    for feature_key, feature_counter in rt_counter.iteritems():
        counter_key_1d = 'countFeatures1d'
        new_1d_counter = feature_counter[counter_key_1d]
        if feature_key not in day_counter:
            day_counter[feature_key] = {counter_key_1d: new_1d_counter}
        else:
            day_counter[feature_key][counter_key_1d] = new_1d_counter
        merge_feature_counter('countFeatures3d', new_1d_counter, day_counter[feature_key])
        merge_feature_counter('countFeatures7d', new_1d_counter, day_counter[feature_key])
        counter_bj_key = 'countFeaturesBj1d'
        day_counter[feature_key][counter_bj_key] = feature_counter[counter_bj_key]


def main(argv):
    field_name = 'storeAdCounter'
    rt_counter = {field_name : {}}
    load_hourly_imp_click(argv[1], rt_counter[field_name])
    load_hourly_install(argv[2], rt_counter[field_name])
    load_hourly_install(argv[3], rt_counter[field_name], 'bj')

    f = open(argv[0])
    day_counter = json.load(f)
    merge_counter(rt_counter[field_name], day_counter[field_name])

    a = StoreAdCounter()
    json_format.Parse(json.dumps(day_counter), a)
    f = open(argv[4], 'wb')
    f.write(a.SerializeToString())
    f.close()


if __name__ == '__main__':
    main(sys.argv[1:])
