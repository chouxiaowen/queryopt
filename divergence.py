import sys
import discretize as disc
import numpy as np
import math

def calc_marginals(vals):
  margins = {}

  for v in vals:
    if v in margins:
      margins[v] += 1
    else:
      margins[v] = 1

  n = len(vals)
  for k, v in margins.items():
    margins[k] = v * 1.0 / n

  return margins

def calculate_divergence(data, cols):
  labels = list(set(data[:,-1]))

  for x in labels:
    subset = np.array(list(list(y) for y in data if y[-1] == x))
   # print subset
    print x, subset.shape[0], calc_divergence(subset[:,cols])
  
  print data.shape[0], calc_divergence(data[:,cols])

def calc_divergence(data):
  marginals = calc_all_marginals(data)
  #data[data[:,0].argsort()]
  sort_data = sorted(list(list(x) for x in data)) #np.array(sorted(list(data)))
  pre = []
  cnt_pre = 0
  res = 0.0
  n = data.shape[0]

  for row in sort_data:
    if row == pre:
      cnt_pre += 1
      continue

    if pre:
      p = cnt_pre * 1.0 / n # joint probability
      q = 1.0 # product probability
      for i, x in enumerate(pre):
        q *= marginals[i][x]
      res += p * (math.log(p) -  math.log(q))

    cnt_pre = 1
    pre = row
  return res

def calc_all_marginals(data):
  all_marginals = []

  for i in range(data.shape[1]):
    all_marginals.append(calc_marginals(data[:,i]))

  return all_marginals
#  print all_marginals

def main():
  data = disc.loadFile(sys.argv[1])
  #calc_all_marginals(data)
  
  calculate_divergence(data, [0,2])
  calculate_divergence(data, range(data.shape[1]))
 # calc_divergence(data[:,[0,2]])

if __name__ == '__main__':
  main()
