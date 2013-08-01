#!/usr/bin/python
import os
import numpy as np
import sys
import subprocess

N = 6001215

# Each file in path is a feature;
def load_features(path, n):
  all_cols = []
  
  for f in os.listdir(path):
    print f
    if f.startswith('.'):
      continue
    cur_col = [False] * n
    fw = open(path + '/' + f, 'r')
    for line in fw:
      cur_col[int(line) - 1] = True
    fw.close()
    print cur_col.count(1)
    all_cols.append(cur_col)

  return np.transpose(np.array(all_cols))

def output_features(filename, features, delim = ','):
  fw = open(filename, 'w')
  for i, row in enumerate(features):
    fw.write( str(i) + delim + delim.join('1' if x else '0' for x in row) + '\n')
  fw.close()

def reform_labels(in_file, out_file):
  fr = open(in_file, 'r')
  fw = open(out_file, 'w')
  
  buf = {}
  for line in fr:
    t = line.split()
    buf[int(t[0])] = t[1]
  
  fr.close()

  for k in sorted(buf.keys()):
    fw.write(buf[k] + '\n')

  fw.close()

def main():
  query_res_path = sys.argv[1]
  features = load_features(query_res_path, N)
  output_features('feat.txt', features)
  
#  k = 10

#  cmd = 'kmeans --data=feat.txt --clusters=%d --output-data=clus.out --output-clusters=clus.res --id 1' % k
#  proc = subprocess.check_call(cmd.split())
  
#  reform_labels('clus.out_1_of_1', 'labels.txt')

if __name__ == '__main__':
  main()
