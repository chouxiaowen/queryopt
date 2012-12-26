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
  

def load_file(filename):
  print "start loading file: %s..." % filename 
  global schema
  global header

  f = open(filename, 'r')

  header = f.next().strip()
  schema = list(x.split() for x in header.strip().split(','))

  for field in schema:
    if len(field) != 2:
      print "invalid header: " + str(field)
      sys.exit(1)
    if (field[1] != 'numerical' and field[1] != 'nominal'
      and field[1] != 'class'):
      print "unknown field type: %s" % field[1]
      sys.exit(1)

  table = []
  for i, line in enumerate(f):
    t = line.strip().split(',')
    if len(t) != len(schema):
      print "unmatched length found at line: %d" % i
      sys.exit(1)
    table.append(t)

  f.close()
  print "Done!"
  fields = list(x.split()[0] for x in header.split(','))

  return [fields, np.array(table)]

def discretizeTable(table, p):
  dtable = []
  for i in range(len(schema)):
    if schema[i][1] == "numerical":
      col = list(float(x) for x in table[:,i])
      dtable.append(discretizeColumn(col, p))

  return np.array(dtable).T

# Discritize the numerical values into equal-width histogram
def discretizeColumn(col, num_buckets):
  max_val = max(col)
  min_val = min(col)

  bucket_width = (max_val - min_val) / num_buckets
  labels = []

  for val in col:
    labels.append(int( (val - min_val) / bucket_width))

  return labels

def outputTable(table, filename):
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
