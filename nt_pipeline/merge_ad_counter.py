from store_table_pb2 import StoreAdCounter, StoreAdInfo
import json
from google.protobuf import json_format
import sys
from collections import Counter, defaultdict
import numpy as np

hourly_counter = defaultdict(Counter)
day_counter = defaultdict(Counter)
output_counter = defaultdict(Counter)
all_imp = 0
all_install = 0


def wilson_score(pos, total, p_z=2.0):
    rate = pos * 1.0 / total
    score = (rate + (np.square(p_z) / (2. * total))
             - ((p_z / (2. * total)) * np.sqrt(4. * total * (1. - rate) * rate + np.square(p_z)))) / \
            (1. + np.square(p_z) / total)
    return score


def load_counter_file(fname, is_day = True):
    global all_imp, all_install
    f = open(fname)
    j = json.load(f)
    ad_counter = j['storeAdCounter']
    for k,v in ad_counter.iteritems():
        if k.find("ad_package_name") == 0 or k.find("ad_package_category") == 0:
            if is_day:
                counter_key = "countFeatures7d"
                if counter_key not in v:
                    continue
                day_counter[k] += Counter(v[counter_key])
                if k.find("ad_package_category") == 0:
                    all_imp += v[counter_key]['imp']
                    all_install += v[counter_key].get('attrInstall', 0)
            else:
                counter_key = "countFeatures30d"
                hourly_counter[k] += Counter(v[counter_key])


def merge_day_and_hour():
    for k,v in day_counter.iteritems():
        output_counter[k] = day_counter[k] + hourly_counter[k]
    for k,v in hourly_counter.iteritems():
        if k not in output_counter:
            output_counter[k] += v

    for k,v in output_counter.iteritems():
        if k.find("ad_package_category") == 0 and v['imp'] < 10000:
            v['imp'] = all_imp
            v['attrInstall'] = all_install
            continue
        if k.find("ad_package_name") == 0 and v['imp'] < 10000:
            v['imp'] = 50
            continue
        imp = v.get('imp', 0)
        click = v.get('click', 0)
        tmp_counter = day_counter[k]
        day_click = tmp_counter.get('click', 0)
        day_install = tmp_counter.get('attrInstall', 0)
        if day_click > 0:
            day_cvr = min(day_install * 1.0 / day_click, 0.01)
            #v['attrInstall'] = int(click * day_cvr)
        if day_install == 0:
            v['attrInstall'] = int(imp * 1.0 / 10000)
            continue
        #if imp > 100000:
        #    w_score = wilson_score(v.get('attrInstall', 0), imp)
        #    v['attrInstall'] = int(w_score * imp)




def generate_output(fname):
    field_name = "storeAdCounter"
    output = {field_name: {}}
    json_output = {}
    for k,v in output_counter.iteritems():
        output[field_name][k] = {"countFeatures30d": dict(v)}
        json_output[k] = dict(v)

    pb = StoreAdCounter()
    json_format.Parse(json.dumps(output), pb)

    f = open("%s.pb" % fname, 'wb')
    f.write(pb.SerializeToString())
    f.close()

    f = open("%s.json" % fname, 'w')
    f.write(json.dumps(json_output))

def main(argv):
    load_counter_file(argv[0])
    for fname in argv[1:]:
        load_counter_file(fname, is_day = False)
    merge_day_and_hour()
    generate_output("ad_counter")

if __name__ == '__main__':
    main(sys.argv[1:])
