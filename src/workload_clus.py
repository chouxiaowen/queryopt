#!/usr/bin/python
import os
import itertools as itt
import numpy as np
import heapq as hq

class Cluster():
  def __init__(self, vector, ids):
    self.vector = vector
    self.ids = ids

  def query(self, idx):
    return self.vector[idx]

  def add(self, vid, vector):
    self.ids.append(vid)
    self.vector = [x or y for x, y in itt.izip(self.vector, vector)]

  def union(self, v1, v2):
    v = []
    for i, val in enumerate(v1):
      v.append(v1[i] or v2[i])

    return v

  def size(self):
    return len(self.ids)

class MyClustering():
  def __init__(self, clusters, k):
    self.k = k
    self.clusters = {}
    self.heap = []
    self.ids = 0
    self.label_mapper = None

    for cl in clusters:
      self.clusters[self.ids] = cl
      self.ids += 1

  def calc_distance(self, cl1, cl2):
    v1 = sum(1 for x in cl1.vector if x) * cl1.size()
    v2 = sum(1 for y in cl2.vector if y) * cl2.size()

    new_v = sum(1 for x, y in itt.izip(cl1.vector, cl2.vector) if x or y) * (cl1.size() + cl2.size())

    return new_v - v1 - v2

  def merge_cluster(self, cl1, cl2):
    m = cl1.size()
    v = [x or y for x, y in itt.izip(cl1.vector, cl2.vector)]
    ids = cl1.ids + cl2.ids

    return Cluster(v, ids)

  def form_cluster(self, cl):
    self.clusters[self.ids] = cl
    new_id = self.ids
    self.ids += 1

    return new_id
    
  def init_heap(self):
    self.heap = []
    for i in self.clusters.keys():
      for j in self.clusters.keys():
        if i == j:
          continue        
        hq.heappush(self.heap, (self.calc_distance(self.clusters[i], self.clusters[j]), i, j))

  def find_cluster(self):
    cnter = len(self.clusters)
    while cnter > self.k:
      victim = hq.heappop(self.heap)

      if victim[1] not in self.clusters or victim[2] not in self.clusters:
        continue

      print victim[1], victim[2]
      cl1 = self.clusters[victim[1]]
      cl2 = self.clusters[victim[2]]
      new_cl = self.merge_cluster(cl1, cl2)
      new_id = self.form_cluster(new_cl)
      del self.clusters[victim[1]]
      del self.clusters[victim[2]]

      for i in self.clusters.keys():
        if i == new_id:
          continue
        hq.heappush(self.heap, (self.calc_distance(self.clusters[i], new_cl), i, new_id))
      
      cnter -= 1

    # map the k labels into the new space: {0, 1, ..., k-1}
    tmp_id = 0
    new_clusters = {}
    for v in self.clusters.values():
      new_clusters[tmp_id] = v
      tmp_id += 1
    self.clusters = new_clusters

    # calculate and store the intra distances of all clusters
   # self.calc_intra_distances()

  def get_cluster(self):
  #  self.calc_intra_distances()
    for k, v in self.clusters.items():
      print 'cluster %d' % k
 #     for kk, vv in v.points.items():
 #       print kk, vv
      print k, v.vector, v.size()

  def output_label(self):
    labels = [-1] * 10000000
    for k, v in self.clusters.items():
      for l in v.ids:
        labels[l] = k

    fw = open('.%d.labels' % self.k, 'w')
    for l in labels: 
      if l == -1:
        break
      fw.write(str(l) + '\n')
      
    fw.close()

  def hierarchy_cluster(self):
    self.init_heap()
    self.find_cluster()
    self.get_cluster()
    self.output_label()

  def assign_label(self, pt):
    cl = Cluster({-1:pt})
    min_delta = None
    min_cl = None
    for k, v in self.clusters.items():
      cur_dist = self.calc_distance(cl, v)
      cur_delta = cur_dist / self.intra_distances[k]
      if not min_delta or cur_delta < min_delta:
        min_delta = cur_delta
        min_cl = k

    return min_cl

  def clear_sample_points(self):
    for v in self.clusters.values():
      v.remove_points()

  def add_point(self, pid, pt):
    label = self.assign_label(pt)
    self.clusters[label].add_points({pid:pt})

class DataLoader():
  def __init__(self, filename):
    self.clusters = {}
    self.index = {}

    self.shuffle(filename)

  def get_index(self, vector):
    idx = sum(int(v << i) for i, v in enumerate(vector) if v)
    if idx not in self.index:
      self.index[idx] = vector

    return idx

  def shuffle(self, filename):
    f = open(filename, 'r')
    for line in f:
      t = line.strip().split(',')
      idx = self.get_index([True if x == '1' else False for x in t[1:]])
      
      if idx in self.clusters:
        self.clusters[idx].append(int(t[0]))
      else:
        self.clusters[idx] = [int(t[0])]
    f.close()
    print len(self.clusters)

def main():
  data = DataLoader('feat.txt')
  clusters = [Cluster(data.index[k], v) for k, v in data.clusters.items()] 
  mycluster = MyClustering(clusters, 50)
  mycluster.hierarchy_cluster()

if __name__ == '__main__':
  main()

