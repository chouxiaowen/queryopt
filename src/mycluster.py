import os
import itertools
import heapq as hq

class Cluster():
  def __init__(self, points, min_bound = None, max_bound = None):
    self.points = points
    self.max_bound = min_bound
    self.min_bound = max_bound

    if not self.max_bound or not self.min_bound:
      for pt in points:
        self.update_bound_box(pt)
  
  def add_points(self, points):
    for p in points:
      if len(p) != self.num_dim:
        print 'error: unmatched number of dimensions: %d %d' % (self.num_dim, len(p))
      self.points.append(p)
      self.update_bound_box(p)

  def update_bound_box(self, pt):
    if not self.min_bound:
      self.min_bound = pt
    if not self.max_bound:
      self.max_bound = pt

    for idx, d in enumerate(pt):
      if d < self.min_bound[idx]:
        self.min_bound[idx] = d
      if d > self.max_bound[idx]:
        self.max_bound[idx] = d

  def get_num_dim(self):
    return len(self.points[0])

  def get_points(self):
    return self.points


class MyClustering():
  
  def __init__(self, data, k):
    self.data = data
    self.k = k
    self.clusters = {}
    self.heap = []
    self.ids = 0

    for pt in data:
      cl = Cluster([pt])
      self.clusters[self.ids] = cl
      self.ids += 1

  def calc_distance(self, cl1, cl2):
    num_dim = cl1.get_num_dim()

    max_dist = -1
    for i in range(num_dim):
      new_min_bound = min(cl1.min_bound[i], cl2.min_bound[i])
      new_max_bound = max(cl1.max_bound[i], cl2.max_bound[i])
      cur_dist = new_max_bound - new_min_bound
      if cur_dist > max_dist:
        max_dist = cur_dist
    return max_dist

  def pairwise_distance(self, pt1, pt2):
    max_dist = -1
    for xi, xj in itertools.izip(pt1, pt2):
      cur_dist = math.fabs(xi, xj)
      if cur_dist > max_dist:
        max_dist = cur_dist
    return max_dist

  def merge_cluster(self, cl1, cl2):
    num_dim = cl1.get_num_dim()
    new_min_bounds = []
    new_max_bounds = []
    for i in range(num_dim):
      new_min_bounds.append(min(cl1.min_bound[i], cl2.min_bound[i]))
      new_max_bounds.append(max(cl1.max_bound[i], cl2.max_bound[i]))

    new_cl = Cluster(cl1.points + cl2.points, new_min_bounds, new_max_bounds)
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
      
      print victim[1], victim[2]

      if victim[1] not in self.clusters or victim[2] not in self.clusters:
        print 'ignored'
        continue

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

  def get_clusters(self):
    for v in self.clusters.values():
      print v.get_points()

  def hierarcy_cluster(self):
    self.init_heap()
    self.find_clusters()
    self.get_clusters()

def main():
  data = [[1,2], [2,3], [5,7], [7,8]]
  mycluster = MyClustering(data, 1)
  mycluster.init_heap()
  mycluster.find_cluster()
  mycluster.get_clusters()

if __name__ == '__main__':
  main()
