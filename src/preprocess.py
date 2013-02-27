import sys
import math
import os
import numpy as np

schema = []
header = ""

def gen_file_list(path):
  if not os.path.exists(path):
    print 'path not exist! - \'%s\'' % path
    sys.exit(1)

  if os.path.isdir(path):
    for f in os.listdir(path):
      for x in gen_file_list(path.rstrip('/') + '/' + f):
        yield x
  else:
    yield path

def get_header(filename):  
  f = open(filename, 'r')
  header = f.next().strip()
  f.close()
  return list(x.split()[0] for x in header.split(','))
  
# load a header-less tsv file into a numpy matrix
def load_file(filename, cols = None):
  print "start loading file: %s..." % filename 

  f = open(filename, 'r')

  table = []
  uni_len = -1
  for i, line in enumerate(f):
    t = line.strip().split('\t')
    if uni_len >= 0 and len(t) != uni_len:
      print "unmatched length found at line: %d" % i
      sys.exit(1)

    uni_len = len(t)
    if not cols:
      table.append(t)
    else:
      sub_t = []
      for c in cols:
        sub_t.append(t[c])
      table.append(sub_t)
  f.close()
  print "Done!"
  return np.array(table)

# load all files from a path, and output 
# samp_ratio% rows into a single file


def sample_files(in_path, out_file, sample_ratio):
  f = open(out_file, 'w')
  sample_file(in_path, f, sample_ratio)
  f.close()

def sample_file(in_path, f_out, sample_ratio):
  if not os.path.exists(in_path):
    print '%s doesn\'t exist' % in_path
    sys.exit(1)

  if os.path.isdir(in_path):
    for f in os.listdir(in_path):
      sample_file(in_path + '/' + f, f_out, sample_ratio)
    return

  fr = open(in_path, 'r')
  for line in fr:
    if np.random.randint(sample_ratio) == 0:
      f_out.write(line)

def discretize_table(table, p):
  dtable = []
  for i in range(len(schema)):
    if schema[i][1] == "numerical":
      col = list(float(x) for x in table[:,i])
      dtable.append(discretizeColumn(col, p))

  return np.array(dtable).T

# Discritize the numerical values into equal-width histogram
def discretize_column(col, num_buckets):
  max_val = max(col)
  min_val = min(col)

  bucket_width = (max_val - min_val) / num_buckets
  labels = []

  for val in col:
    labels.append(int( (val - min_val) / bucket_width))

  return labels

def output_table(table, filename):
  print header
  f = open(filename, 'w')
  f.write(','.join( '%s %s' % (x[0], x[1]) for x in schema)+'\n')
  for row in table:
    f.write(','.join(str(x) for x in row)+'\n')

  f.close()

def main():
#  table = loadFile(sys.argv[1])
#  dtable = discretizeTable(table, float(sys.argv[2]))
# sample_files(sys.argv[1], sys.argv[2], int(sys.argv[3]))

  for f in gen_file_list(sys.argv[1]):
    print f

#  outputTable(dtable, "d_%s_%s" % (sys.argv[2], sys.argv[1]))

if __name__ == '__main__':
  main()
