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

# main clustering procedure
# return a list of labels, one for each row
def clustering(k, data_file, cols):
  table = prep.load_file(data_file, cols)

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

  return mix_table

def assign_labels(model, data_file, cols):
  table = prep.load_file(data_file, cols)

  data = mx.Dataset()
  data.fromArray(np.array(table))

  labels = model.classify(data)

  return labels

def classify_data(model, path, cols):
  for f in prep.gen_file_list(path):
    labels = assign_labels(model, f, cols)

    fw = open(f+'.labels', 'w')
    for x in labels:
      fw.write(x+'\n')
    fw.close()

def output(filename, arr):
  f = open(filename, 'w')
  f.write(header + ',class class\n')
  for i, val in enumerate(arr):
    f.write(','.join(list(arr[i,:])) + '\n')
  f.close()

def main():
  cols = [3,7]
  cols = [1,2]

  #cols = [schema["dIndustry nominal"], schema["iYearsch nominal"], schema["iMeans nominal"]]
  cols = [11, 24, 28, 37, 33, 51, 45, 49, 53, 32, 86, 103, 101, 104, 114, 118, 135]
  cols = [11, 24, 28, 37, 33, 51, 32, 86, 103, 101, 104, 114, 135]
  
#  all_cols = range(table.shape[1])

  model = clustering(int(sys.argv[2]), sys.argv[1], cols)
  classify_data(model, path, cols)

#  table = np.hstack((table, np.array(labels)[np.newaxis].T))
#  output(sys.argv[3], table)
#  dv.calculate_divergence(table, all_cols)

if __name__ == '__main__':
  main()
