#!/usr/bin/python
import os
import itertools
import numpy as np
import heapq as hq
#class Cluster():
#  def __init__(self, points, min_bound = None, max_bound = None):
#    self.points = points
#    self.min_bound = min_bound
#    self.max_bound = max_bound
#
#    if not self.max_bound or not self.min_bound:
#      for pt in points.values():
#        self.update_bound_box(pt)
#  
#  def add_points(self, points):
#    for k, v in points.items():
#      if self.get_num_dim() and len(v) != self.get_num_dim():
#        print 'error: unmatched number of dimensions: %d %d' % (self.num_dim, len(p))
#      self.points[k] = v
#      self.update_bound_box(v)
#
#  def update_bound_box(self, pt):
#    if not self.min_bound:
#      self.min_bound = pt
#    if not self.max_bound:
#      self.max_bound = pt
#
#    for idx, d in enumerate(pt):
#      if d < self.min_bound[idx]:
#        self.min_bound[idx] = d
#      if d > self.max_bound[idx]:
#        self.max_bound[idx] = d
#
#  def get_num_dim(self):
#    if not self.points.values():
#      return None
#    return len(self.points.values()[0])
#
#  def get_points(self):
#    return self.points
#
#  def remove_points(self):
#    self.points.clear()
#
#  def size():
#    return len(points)


class Cluster():
  def __init__(self, vector，ids):
    self.vector = vector
    self.ids = ids

  def query(self, idx):
    return self.vector[idx]

  def add(self, vid, vector):
    self.ids.append(vid)
    self.vector = union(self.vector, vector)

  def union(self, v1, v2):
    v = []
    for i, val in enumerate(v1):
      v.append(v1[i] or v2[i])

    return v

  def size(self):
    return len(ids)

class MyClustering():
  def __init__(self, data, k):
    self.data = data
    self.k = k
    self.clusters = {}
    self.intra_distances = {}
    self.heap = []
    self.ids = 0
    self.label_mapper = None

    for i, pt in enumerate(data):
      cl = Cluster({i: pt})
      self.clusters[self.ids] = cl
      self.ids += 1

# Only used for evaluating clustering results, not for finding clusters
  def get_intra_distance(self, cl):
    max_dist = -1
    dists = []
    for i in range(cl.get_num_dim()):
      cur_dist = cl.max_bound[i] - cl.min_bound[i]
      dists.append(cur_dist)
    return np.average(dists)

  def calc_intra_distances(self):
    for k, v in self.clusters.items():
      self.intra_distances[k] = self.get_intra_distance(v)

  def calc_hamwei(self, cl1, cl2):
    num_dim = cl1.get_num_dim()

  def calc_distance(self, cl1, cl2):
    num_dim = cl1.get_num_dim()
    
    max_dist = -1
    dists = []
    for i in range(num_dim):
      new_min_bound = min(cl1.min_bound[i], cl2.min_bound[i])
      new_max_bound = max(cl1.max_bound[i], cl2.max_bound[i])
      cur_dist = new_max_bound - new_min_bound
      dists.append(cur_dist)
      
    return np.average(dists)
 #     if cur_dist > max_dist:
 #       max_dist = cur_dist
 #   return max_dist

#  def pairwise_distance(self, pt1, pt2):
#    max_dist = -1
#    for xi, xj in itertools.izip(pt1, pt2):
#      cur_dist = math.fabs(xi, xj)
#      if cur_dist > max_dist:
#        max_dist = cur_dist
#    return max_dist

  def merge_cluster(self, cl1, cl2):
    num_dim = cl1.get_num_dim()
    new_min_bounds = []
    new_max_bounds = []
    for i in range(num_dim):
      new_min_bounds.append(min(cl1.min_bound[i], cl2.min_bound[i]))
      new_max_bounds.append(max(cl1.max_bound[i], cl2.max_bound[i]))

    new_cl = Cluster(dict(cl1.points.items() + cl2.points.items()), new_min_bounds, new_max_bounds)
    return new_cl
    
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
    cnter = len(self.data)
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
    self.calc_intra_distances()

  def get_cluster(self):
    self.calc_intra_distances()
    for k, v in self.clusters.items():
      print 'cluster %d' % k
 #     for kk, vv in v.points.items():
 #       print kk, vv
      print k, len(v.points), self.intra_distances[k]
      for i in range(v.get_num_dim()):
        print v.min_bound[i], v.max_bound[i]
      print '------------------'

  def hierarchy_cluster(self):
    self.init_heap()
    self.find_cluster()
    self.get_cluster()

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

def main():
  data = [[1,2], [2,3], [5,7], [7,8]]
  mycluster = MyClustering(data, 2)
  mycluster.init_heap()
  mycluster.find_cluster()
  mycluster.get_cluster()

if __name__ == '__main__':
  main()
