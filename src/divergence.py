#!/usr/local/homebrew/bin/python2.5
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

def calc_histogram(data, cols):
  print "Calculating histogram"
  histos = {}
  for c in cols:
    histos[c] = {}
    for r in data[:,c]:
      histos[c][r] = 1 if not r in histos[c] else histos[c][r] + 1

  print "Done"

  for c in cols:
    print c, histos[c]

  calc_pwmi(data, cols, histos)

  return histos

def calculate_histogram(data, cols):
  labels = list(set(data[:,-1]))

  for x in labels:
    subset = np.array(list(list(y) for y in data if y[-1] == x))
    print "cluster %s, size %d" % (str(x), data.shape[0])
    calc_histogram(subset, cols)

  print "total"
  calc_histogram(data, cols)
  
def calc_pwmi(data, cols, histos):
  print "Calculating point-wise mutual information"
  mis = []
  n = data.shape[0]
  for i in cols:
    for j in cols:
      if j <= i:
        continue
      for x in histos[i]:
        for y in histos[j]:
          if y == x:
            continue
          pxy = ((data[:,i] == x) & (data[:,j] == y)).sum() * 1.0 / n
          px = histos[i][x] * 1.0 / n
          py = histos[j][y] * 1.0 / n

          #print pxy, px, py
          if pxy < 1e-6:
            cur = -1
          else: #cur = math.log(pxy) - math.log(px) - math.log(py)
            cur = (math.log(pxy) - math.log(px) - math.log(py)) / math.log(pxy)

          mis.append((cur, -px*py, i, x, j, y))
  print "Done!"
  print '\n'.join(str(x) for x in sorted(mis))

def calculate_divergence(data, cols):
  labels = list(set(data[:,-1]))

  tot = 0
  for x in labels:
    subset = np.array(list(list(y) for y in data if y[-1] == x))
    # print subset
    div = calc_divergence(subset[:,cols])
    print x, subset.shape[0], div
    tot += subset.shape[0] * div

  print "weighted_average: %f" %  (tot * 1.0 / data.shape[0])

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
  [header, data] = disc.load_file(sys.argv[1])
  #calc_all_marginals(data)

  cols = [16,25,31,50,66]
#  cols = [0,2]  
  calculate_histogram(data, cols)
#  calc_histogram(data, cols)

  #  calculate_divergence(data, [3,7])
#  calculate_divergence(data, range(data.shape[1]))
 # calc_divergence(data[:,[0,2]])

if __name__ == '__main__':
  main()
