#!/usr/local/homebrew/bin/python2.6
import mixture as mx
import sys
import numpy as np
import divergence as dv
import preprocess as prep
fields = []
schema = {}
header = ""

def count_distinct(arr, cols):
  distinct_vals = []
  for idx in cols:
    distinct_vals.append(len(set(arr[:,idx])))

  for idx, val in enumerate(fields):
    print val, distinct_vals[idx]

  cols = []
  for key, val in schema.items():
    if distinct_vals[val] > 10 and distinct_vals[val] < 50:
      cols.append(val)
  print cols

def clustering(k, arr, cols):
  weights = [1.0/k] * k
  components = []
  for i in range(k):
    products = []
    for j in cols:
      vals = list(set(arr[:,j]))
      # print vals
      cnt_vals = len(vals)
      dist = mx.DiscreteDistribution(cnt_vals, [1.0/cnt_vals] * cnt_vals, mx.Alphabet(vals))
      products.append(dist)

    comp = mx.ProductDistribution(products)
    components.append(comp)

  mix_table = mx.MixtureModel(k, weights, components)

  data = mx.DataSet()
  data.fromArray(np.array(arr[:,cols]))

  #data.internalInit(mix)
  mix_table.modelInitialization(data)
  print mix_table

  mix_table.randMaxEM(data,1,10,0.1)
  print mix_table

  print mix_table.components[0]
  return mix_table.classify(data, None, None, 1)

def output(filename, arr):
  f = open(filename, 'w')
  f.write(header + ',class class\n')
  for i, val in enumerate(arr):
    f.write(','.join(list(arr[i,:])) + '\n')
  f.close()

def main():
  cols = [3,7]
  cols = [1,2]

 # count_distinct(table, all_cols)

  #cols = [schema["dIndustry nominal"], schema["iYearsch nominal"], schema["iMeans nominal"]]
#  cols = [16, 25, 31, 50, 66]
#  cols = [86, 101, 104, 114, 118]
  cols = [11, 24, 28, 37, 33, 51, 45, 49, 53, 32, 86, 103, 101, 104, 114, 118, 135]
  cols = [11, 24, 28, 37, 33, 51, 32, 86, 103, 101, 104, 114, 135]
#  print list(fields[x] for x in cols)
#  sparse_cols = [45, 50, 43, 3, 12, 31, 2, 16, 25, 66, 53]
#  num_sparse = len(sparse_cols)

  table = prep.load_file(sys.argv[1], cols)
  all_cols = range(table.shape[1])

# print schema
  #print table[:, schema['fixed acidity']]
  #print table[:, schema['citric acid']]
  
#  print dv.calc_divergence(table[:,cols])

#  for i in range(num_sparse):
#    for j in range(num_sparse):
#      if j == i:
#        continue
#      ii = sparse_cols[i]
#      jj = sparse_cols[j]
#      print fields[ii], fields[jj]
#      print dv.calc_divergence(table[:, [ii, jj]])

  labels = clustering(int(sys.argv[2]), table, all_cols)
  table = np.hstack((table, np.array(labels)[np.newaxis].T))

  output(sys.argv[3], table)
  dv.calculate_divergence(table, all_cols)

if __name__ == '__main__':
  main()
