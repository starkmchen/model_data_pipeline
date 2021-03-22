import json
import sys
from collections import defaultdict
import numpy as np
from scipy.stats import chi2_contingency
from scipy.stats import chi2

def mutual_info(name, feature):
  all_num = 0.0
  pos = 0.0
  neg = 0.0
  for k,v in feature.iteritems():
    fea_pos = v[1]
    fea_neg = v[0]
    pos += fea_pos
    neg += fea_neg
    all_num = all_num + fea_pos + fea_neg
  pos_ratio = pos / all_num
  neg_ratio = neg / all_num
  y_ent = pos_ratio * np.log2(pos_ratio) + neg_ratio * np.log2(neg_ratio)
  x_ent = 0.0
  xy_ent = 0.0
  for k,v in feature.iteritems():
    fea_pos = v[1]
    fea_neg = v[0]
    v_ratio = (fea_pos + fea_neg) * 1.0 / all_num
    x_ent += v_ratio * np.log2(v_ratio)
    if fea_pos > 0:
      v_ratio = fea_pos / all_num
      xy_ent += v_ratio * np.log2(v_ratio)
    if fea_neg > 0:
      v_ratio = fea_neg / all_num
      xy_ent += v_ratio * np.log2(v_ratio)
  mi_value = xy_ent - x_ent - y_ent
  print name, len(feature), mi_value


def chi_analysis(name, feature):
  all_num = 0.0
  pos = 0.0
  neg = 0.0
  fea_count = defaultdict(float)
  chi_li = []
  for k,v in feature.iteritems():
    chi_li.append([v[1], v[0]])
    fea_pos = v[1]
    fea_neg = v[0]
    pos += fea_pos
    neg += fea_neg
    all_num = all_num + fea_pos + fea_neg
    fea_count[k] = fea_pos + fea_neg
  if pos == 0 or neg == 0 or len(feature) == 1:
    print name, len(feature), -1
    return
  stat,p,dof,expected = chi2_contingency(chi_li)
  prob = 0.8
  critical = chi2.ppf(prob,dof)
  print name, len(feature), stat, critical, p
  return
  pos_ratio = pos / all_num
  neg_ratio = neg / all_num
  chi_sum = 0.0
  for k,v in feature.iteritems():
    fea_ratio = fea_count[k] / all_num
    pos_exp = all_num * fea_ratio * pos_ratio
    neg_exp = all_num * fea_ratio * neg_ratio
    chi_sum += np.square(v[1] - pos_exp) / pos_exp
    chi_sum += np.square(v[0] - neg_exp) / neg_exp
  print len(feature), chi_sum

def main(argv):
  feature_count = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
  f = open(argv[0])
  for line in f:
    fea_k, fea_v, click, install, imp = line.strip().split('\t')
    click = int(click)
    install = int(install)
    imp = int(imp)
    feature_count[fea_k][fea_v][click] += imp
  for fea_k, v in feature_count.iteritems():
    chi_analysis(fea_k, v)
    mutual_info(fea_k, v)


if __name__ == '__main__':
  main(sys.argv[1:])
