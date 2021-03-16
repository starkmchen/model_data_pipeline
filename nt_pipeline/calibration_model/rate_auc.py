import sys
import json
from collections import defaultdict
import numpy as np
import bisect


def get_auc(score_vec):
  score_vec.sort(key = lambda x:x[0], reverse= True)
  x = 0.0
  y = 0.0
  area = 0.0
  for item in score_vec:
    if item[1] == 1:
      y += 1
    else:
      x += 1
      area += y
  auc = area / (x * y)
  return auc
   
def load_calibration_model(fname):
  model = []
  m_index = []
  f = open(fname)
  for line in f:
    _,_,_,prate, rate = line.strip().split(' ')
    prate = float(prate)
    rate = float(rate)
    model.append([prate, rate])
    m_index.append(prate)
  return model, m_index

def calibrate(pctr, calc_model, m_index):
  idx = bisect.bisect_left(m_index, pctr)
  if idx == len(calc_model) or (idx != 0 and calc_model[idx] != pctr):
    idx -= 1
  return calc_model[idx][1]

def main(argv):
  calc_model, m_index = load_calibration_model('ctr_calibration_model')
  score_vec = []
  f = open(argv[0])
  score_sum = 0.0
  label_sum = 0.0
  all_sum = 0.0
  for line in f:
    li = line.strip().split('\t')
    beyla_id, ad_package_name, ad_id, c_id, pctr, pcvr, bid, is_click, is_attr_install = li
    pctr = float(pctr)
    pctr = calibrate(pctr, calc_model, m_index)
    pcvr = float(pcvr)
    bid = float(bid)
    click = int(is_click)
    install = int(is_attr_install)
    score = pctr * pcvr
    label = install
    score_sum += score
    label_sum += label
    all_sum += 1
    score_vec.append((score, label))
  #print get_auc(score_vec)
  print score_sum / all_sum, label_sum / all_sum

if __name__ == '__main__':
  main(sys.argv[1:])
