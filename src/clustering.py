#!/usr/bin/python
import sys
import mixture as mx
import numpy as np
import random
import divergence as dv
import value_counts as vc
import preprocess as prep
import copy
import os

def read_domains(cols, domain_file='.domains'):
  domains = {}
  for c in cols:
    domains[c] = {}
  f = open(domain_file, 'r')
  for line in f:
    row = line.split('\t')
    col_id = int(row[0])
    if col_id in domains:
      domains[col_id][row[1]] = float(row[2])

  return domains

def get_col_type(col_idx, header):
  desc = header[col_idx][1]
  if 'text' in desc or 'char' in desc:
    return 'cat'
  elif 'integer' in desc or 'decimal' in desc or 'date' in desc:
    return 'num'
  else:
    print 'unknown column type: %s' % header[col_idx][1]
    return None

# Main clustering procedure
# return a list of labels, one for each row
def clustering(k, feature_cols, feature_domains, header, table, result_file):
  best_loglike = None
  best_model = None
  # Giant randome seeding loop, 

  data = mx.DataSet()
  data.fromArray(table)
  
  for r in range(10):
    weights = np.random.random_sample(k)
    weights_norm = weights / sum(weights)
    components = []
    for i in range(k):
      products = []
      for j in range(table.shape[1]):
        col_type = get_col_type(feature_cols[j], header)
        col_id = feature_cols[j]

        if col_type == 'cat':
          vals = feature_domains[col_id].keys()
          cnt_vals = len(vals)
          rand_dist = np.random.random_sample(cnt_vals)

          dist = mx.DiscreteDistribution(cnt_vals, rand_dist / sum(rand_dist), mx.Alphabet(vals))
        
        elif col_type == 'num':
          min_val = feature_domains[col_id]['min']
          max_val = feature_domains[col_id]['max']
          rand_mean = random.uniform(min_val, max_val)
          stdev = (max_val - min_val) / k
          
          dist = mx.NormalDistribution(rand_mean, stdev)

        else:
          sys.exit(1)
        products.append(dist)

      comp = mx.ProductDistribution(products)
      components.append(comp)
    
    mix_table = mx.MixtureModel(k, weights_norm, components)
    loglike = mix_table.randMaxEM(data,10,30,100)
    print mix_table
    if not best_loglike or loglike > best_loglike:
      best_loglike = loglike
      best_model = copy.copy(mix_table)
    

#data.internalInit(mix)
# mix_table.modelInitialization(data)
  print best_loglike
  print best_model

  labels = best_model.classify(data, None, None, 1) 

  ## output clustering results
  
  # count cluster sizes on sampled data
  f = open(result_file + '.stats', 'w')
  cnt = {}
  for l in labels:
    cnt[l] = 1 if l not in cnt else cnt[l] + 1

  for l in cnt:
    f.write('%s %d %f%%\n' % ( l, cnt[l], cnt[l] * 100.0 / sum(cnt.values())))
  f.close()

  mx.writeMixture(best_model, result_file + '.model')
  return best_model

def assign_labels(model, data_file, cols):
  table = []
  labels = []
 
  cnter = 0
  for row in prep.gen_file_stream(data_file, cols):
    table.append(row)
    cnter += 1
    if cnter % 100000 == 0:
      print cnter
      data = mx.DataSet()
      print 'table size: %d' % len(table)
      data.fromArray(np.array(table))
      labels += list(model.classify(data, None, None, 1))
      del data
      table[:] = []
    #  print len(list(labels))

  # process the trailing entries
  if len(table) > 0:
    data = mx.DataSet()
    data.fromArray(np.array(table))
    labels += list(model.classify(data, None, None, 1))

  return labels

# Assign labels to all the '*.train' data in [path] using 'model'.
# The labels will be written into file .'k'.labels in the same folder.
# So put at most one train file in each folder :(
def classify_data(k, cols, model, path):
  for f in prep.gen_file_list(path):
    if f.endswith('.train'):
      print 'classifying %s' % f
      labels = assign_labels(model, f, cols)

      fw = open(f[:f.rfind('/')] + '/.' + str(k) + '.labels', 'w')
      fw.write('\n'.join(str(x) for x in labels) + '\n')
      fw.close()

def output(filename, arr):
  f = open(filename, 'w')
  f.write(header + ',class class\n')
  for i, val in enumerate(arr):
    f.write(','.join(list(arr[i,:])) + '\n')
  f.close()

# cluster all tables
# tailored for tpch
def cluster_all_tables(data_path):
  for d in os.listdir(data_path):
    if not os.path.isdir(data_path + '/' + d):
      continue
    
    print 'processing %s' % d
    full_path = data_path.rstrip('/') + '/' + d.rstrip('/') + '/' 
    sample_ratio = int(open(full_path + '.ratio').read())
    data_file = '%s%s.train.%d.sample' % (full_path, d, sample_ratio)
    
    k = int(open(full_path + '.k').read())
    if k > 1:
      feat_cols = prep.get_feature_columns(full_path + '.columns')
      table = prep.load_file(data_file, feat_cols)
      feat_doms = read_domains(feat_cols, full_path + '.domains')
      header = prep.get_header(full_path + '.header')

      print 'start clustering %s' % data_file
      model = clustering(k, feat_cols, feat_doms, header, table, data_file + '.res')
      classify_data(k, feat_cols, model, full_path)

def main():
  #cols = [schema["dIndustry nominal"], schema["iYearsch nominal"], schema["iMeans nominal"]]
  #cols = sorted([11, 24, 28, 37, 33, 51, 45, 49, 53, 32, 86, 103, 101, 104, 114, 118, 135])
  #cols = sorted([11, 24, 28, 37, 33, 51, 32, 86, 103, 101, 104, 114, 135])
  #all_cols = range(table.shape[1])

#  if len(sys.argv) < 4:
#    print 'Usage: python clustering.py [train_file] [test_data_path] [k]'
#    return

  data_path = sys.argv[1]
  cluster_all_tables(data_path)

#  table = np.hstack((table, np.array(labels)[np.newaxis].T))
#  output(sys.argv[3], table)
#  dv.calculate_divergence(table, all_cols)

if __name__ == '__main__':
  main()
