import sys
import json
from collections import defaultdict
import numpy as np


def get_ecpm_auc(score_vec):
  score_vec.sort(key = lambda x:x[0])
  lv = len(score_vec)
  pnum = 0
  nnum = 0
  for item in score_vec:
    if item[1] > 0:
      pnum += 1
    else:
      nnum += 1
  pidx = pnum
  nidx = nnum
  neg = pos = 0.0
  for idx, item in enumerate(score_vec[-1:None:-1]):
    if item[1] > 0:
      pos += (lv - idx - pidx) * item[2]
      pidx -= 1
    else:
      neg += (lv - idx - nidx) * item[2]
      nidx -= 1
  auc = pos / (pos + neg)
  print auc
   
    
  
def main(argv):
  score_vec = []
  f = open(argv[0])
  for line in f:
    li = line.strip().split('\t')
    beyla_id, ad_package_name, ad_id, c_id, pctcvr, is_click, is_attr_install, bid_price = li
    if pctcvr == "NULL":
      continue
    pctcvr = float(pctcvr)
    bid = float(bid_price)
    click = int(is_click)
    install = int(is_attr_install)
    ecpm = pctcvr * bid * 1000
    score_vec.append((ecpm, install, bid))
  get_ecpm_auc(score_vec)

    
    
    

if __name__ == '__main__':
  main(sys.argv[1:])
