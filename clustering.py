#!/usr/local/homebrew/bin/python2.5
import mixture as mx
import sys
import numpy as np
import divergence as dv


schema = {}
header = ""

def loadTable(filename):
  f = open(filename, 'r')
  global header
  header = f.next().strip()

  for idx, val in enumerate(header.strip().split(',')):
    schema[val.strip('"')] = idx

  lists = []
  for line in f:
    lists.append(line.strip().split(','))

#  print lists
  return np.array(lists)

def clustering(k, arr, cols):
  distinct_vals = []
  for idx in cols:
    distinct_vals.append(set(arr[:,idx]))

  print distinct_vals

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

  mix_table.randMaxEM(data,2,20,0.1)
  print mix_table

  return mix_table.classify(data)


def output(filename, arr, labels):
  f = open(filename, 'w')
  f.write(header + ',class class\n')
  for i, val in enumerate(arr):
    f.write(','.join(list(arr[i,:]) + [str(labels[i])]) + '\n')
  f.close()

def main():
  table = loadTable(sys.argv[1])
  print schema
  #print table[:, schema['fixed acidity']]
  #print table[:, schema['citric acid']]
  all_cols = range(table.shape[1])
  cols = [0,2]
  labels = clustering(int(sys.argv[2]), table, cols)
  #print labels
  
 # print table[0]
  table = np.hstack((table, np.array(labels)[np.newaxis].T))
 # print table[0]

  #output('c_%s_%s' % (sys.argv[2], sys.argv[1]), table, labels)
  dv.calculate_divergence(table, cols)

if __name__ == '__main__':
  main()
