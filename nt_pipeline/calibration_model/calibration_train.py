import json
import sys
from collections import defaultdict
import numpy as np


def isotonic_regression(ctr_bucket):
  new_bucket = []
  ctr_bucket.sort(key = lambda x:x[0])
  for item in ctr_bucket:
    new_bucket.append(item)
    while True:
      idx = len(new_bucket) - 1
      if idx == 0:
        break
      new_ctr = new_bucket[idx][1][1] / new_bucket[idx][1][0]
      old_ctr = new_bucket[idx-1][1][1] / new_bucket[idx-1][1][0]
      if new_ctr < old_ctr:
        new_bucket[idx-1][1][0] += new_bucket[idx][1][0]
        new_bucket[idx-1][1][1] += new_bucket[idx][1][1]
        new_bucket.pop()
      else:
        break
  for k,v in new_bucket:
    print k,v[0], v[1], k / 10000.0, v[1]/v[0]

def main(argv):
  ctr_bucket = defaultdict(lambda: np.zeros(2))
  f = open(argv[0])
  for line in f:
    li = line.strip().split('\t')
    beyla_id, ad_package_name, ad_id, c_id, pctr, pcvr, bid, is_click, is_attr_install = li
    pctr = float(pctr)
    pcvr = float(pcvr)
    bid = float(bid)
    click = int(is_click)
    install = int(is_attr_install)
    bucket = int(pctr * 10000)
    ctr_bucket[bucket] += np.array([1, click])
  isotonic_regression(ctr_bucket.items())

  #for k,v in ctr_bucket.iteritems():
  #  print "ori", k,v[0], v[1]
    
    
    

if __name__ == '__main__':
  main(sys.argv[1:])
