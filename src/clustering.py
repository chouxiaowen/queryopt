#!/usr/local/homebrew/bin/python2.5
import mixture as mx
import sys
import numpy as np
import divergence as dv

fields = []
schema = {}
header = ""

def loadTable(filename):
  print "start loading table..."
  f = open(filename, 'r')
  global header
  global schema
  global fields

  header = f.next().strip()

  for idx, val in enumerate(header.strip().split(',')):
    fields.append(val.strip('"'))
    schema[fields[-1]] = idx

  lists = []
  for line in f:
    lists.append(line.strip().split(','))

#  print lists
  print "Done"
  return np.array(lists)

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

  mix_table.randMaxEM(data,1,20,0.0001)
  print mix_table

  return mix_table.classify(data, None, None, 1)

def output(filename, arr):
  f = open(filename, 'w')
  f.write(header + ',class class\n')
  for i, val in enumerate(arr):
    f.write(','.join(list(arr[i,:])) + '\n')
  f.close()

def main():
  table = loadTable(sys.argv[1])
  print schema
  #print table[:, schema['fixed acidity']]
  #print table[:, schema['citric acid']]
  all_cols = range(table.shape[1])
  cols = [3,7]
  cols = [1,2]

 # count_distinct(table, all_cols)

  #cols = [schema["dIndustry nominal"], schema["iYearsch nominal"], schema["iMeans nominal"]]
  cols = [16, 25, 31, 50, 66]
  print list(fields[x] for x in cols)
#  sparse_cols = [45, 50, 43, 3, 12, 31, 2, 16, 25, 66, 53]
#  num_sparse = len(sparse_cols)


#  print dv.calc_divergence(table[:,cols])

#  for i in range(num_sparse):
#    for j in range(num_sparse):
#      if j == i:
#        continue
#      ii = sparse_cols[i]
#      jj = sparse_cols[j]
#      print fields[ii], fields[jj]
#      print dv.calc_divergence(table[:, [ii, jj]])

  labels = clustering(int(sys.argv[2]), table, cols)
  table = np.hstack((table, np.array(labels)[np.newaxis].T))

  output(sys.argv[3], table)
#  dv.calculate_divergence(table, cols)

if __name__ == '__main__':
  main()
