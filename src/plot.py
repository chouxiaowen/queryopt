#!/usr/bin/python
from matplotlib import *
import matplotlib.pyplot as plt
import numpy as np
import clustering as clus

def load_wine_data(filename):
  header = {}
  f = open(filename, 'r')
  for i, field in enumerate(f.next().split(',')):
    pair = field.strip().split(' ')
    header[pair[0]] = i
   
  cols = [header['density'], header['alcohol']]
  
  table = []
  for line in f:
    table.append([float(x) for i, x in enumerate(line.split(',')) if i in cols])
  print table[0]
  print table[1]
  print table[2]
  

  minv = [1000000] * len(table[0])
  maxv = [-1] * len(table[0])

  for row in table:
    for i, cell in enumerate(row):
      if cell < minv[i]:
        minv[i] = cell
      if cell > maxv[i]:
        maxv[i] = cell

  print minv
  print maxv

  rescaled = []
  for i, row in enumerate(table):
      rescaled.append([(cell - minv[j]) / (maxv[j] - minv[j]) for j, cell in enumerate(row)])
  
  print rescaled[0]
  print rescaled[1]
  print rescaled[2]
  
  return rescaled

def cluster_wine(k):
  table = load_wine_data('redwine.csv')
  labels = clus.kmeans(k, table)
  plot_scatter(table, labels, k)

def plot_scatter(table, labels, k):
  markers = ['o', 'x', '^', '*', 's']

  print len(table), len(labels)

  tab = np.column_stack((np.array(table), np.array(labels)))
#  np.random.shuffle(tab)
#  tab = tab[:50000]
#  plt.ioff()
#
#  plt.plot(tab[:,0], tab[:,1], markers[0])
#  
#
#  y1 = sorted([x[1] for x in tab if x[0] < 0.5])
#  y2 = sorted([x[1] for x in tab if x[0] >= 0.5])
# 
#  n = len(y1)
#  
#  plt.axhspan(0, y2[n/3], 0.5, 1, hold=None, fill=False)
#  plt.axhspan(y2[n/3], y2[2*n/3], 0.5, 1, hold=None, fill=False)
#  plt.axhspan(y2[2*n/3], 1.0, 0.5, 1, hold=None, fill=False)
#  
#  plt.axhspan(0, y1[n/3], 0, 0.5, hold=None, fill=False)
#  plt.axhspan(y1[n/3], y1[2*n/3], 0, 0.5, hold=None, fill=False)
#  plt.axhspan(y1[2*n/3], 1.0, 0, 0.5, hold=None, fill=False)
#  
#



#  for x in range(l):
#    for y in range(w):
#      plt.axhspan(y*1.0/w, (y+1.0)/w, (x*1.0)/l, (x+1.0)/l, hold=None, fill=False)


   
  for it in range(k):
    sub_tab = np.array([np.array(row) for row in tab if int(row[-1]) == int(it)])
    plt.plot(sub_tab[:,0], sub_tab[:,1], markers[it%5])
    xmin = 2
    ymin = 2
    xmax = -1
    ymax = -1

    for row in sub_tab:
      if row[0] < xmin:
        xmin = row[0]
      if row[0] > xmax:
        xmax = row[0]
      if row[1] < ymin:
        ymin = row[1]
      if row[1] > ymax:
        ymax = row[1]
    
    plt.axhspan(ymin, ymax, xmin, xmax, hold=None, fill=False)

  plt.show()

def main():
  cluster_wine(25)
if __name__ == '__main__':
  main()
