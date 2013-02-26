import sys
import math
import numpy as np

schema = []
header = ""

def get_header(filename):  
  f = open(filename, 'r')
  header = f.next().strip()
  f.close()
  return list(x.split()[0] for x in header.split(','))
  
# load a header-less tsv file into a numpy matrix
def load_file(filename, cols = None, sample_ratio = None):
  print "start loading file: %s..." % filename 

  f = open(filename, 'r')

  table = []
  uni_len = -1
  for i, line in enumerate(f):
    t = line.strip().split('\t')
    if uni_len >= 0 and len(t) != uni_len:
      print "unmatched length found at line: %d" % i
      sys.exit(1)

    if np.randint(0, sample_ratio) != 0:
        continue

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
  table = loadFile(sys.argv[1])
#  dtable = discretizeTable(table, float(sys.argv[2]))

  outputTable(dtable, "d_%s_%s" % (sys.argv[2], sys.argv[1]))

if __name__ == '__main__':
  main()
