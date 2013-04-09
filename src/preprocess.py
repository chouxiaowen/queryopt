#!/usr/bin/python
import sys
import math
import os
import numpy as np
import fnmatch

# Get table header from file
# return a list of column entries
def get_header(filename):
  header = []
  f = open(filename, 'r')
  for line in f:
    header.append([x.strip() for x in line.lower().split('\t')])
  f.close()
  return header 

# Recursively enumerate files in a directory
# Ignore files starting with '.'
def gen_file_list(path):
  if not os.path.exists(path):
    print 'path not exist! - \'%s\'' % path

  for root, dirs, files in os.walk(path):
    for f in files:
      if not fnmatch.fnmatch(f, '.*'):
        yield root + '/' + f

# Load a header-less tsv file into a numpy matrix
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

def gen_file_stream(filename, cols = None):
  print "start streaming-in file: %s..." % filename 

  f = open(filename, 'r')

  table = []
  for i, line in enumerate(f):
    t = line.strip().split('\t')

    if not cols:
      yield t
    else:
      sub_t = []
      for c in cols:
        sub_t.append(t[c])
      yield sub_t

  f.close()
  print "End of stream!"

def get_feature_columns(column_file):
  cols = []
  f = open(column_file, 'r')
  for line in f:
    cols.append(int(line.strip()))

  return cols

def get_col_type(col_idx, header):
  name = header[col_idx][0]
  desc = header[col_idx][1]

  if 'comment' in name:
    return 'ignore'
  if 'text' in desc or 'char' in desc:
    return 'cat'
  if 'integer' in desc or 'decimal' in desc:
    return 'num'
  if 'date' in desc:
    return 'num'
  
  print 'unknown column type: %s' % header[col_idx][1]
  return None

def compute_domains(path, suffix, header):
  all_domains = {}

  for f in gen_file_list(path):
    if not f.endswith(suffix):
      continue

    print 'computing domains for %s' % f
    f = open(f, 'r')
    for line in f:
      row = line.strip().split('\t')
      for i, cell in enumerate(row):
        if i not in all_domains:
          all_domains[i] = {}
         
        col_type = get_col_type(i, header) 

        if col_type == 'cat':
          all_domains[i][cell] = 1 if cell not in all_domains[i] else all_domains[i][cell] + 1
        elif col_type == 'num':
          val = float(cell)
          if 'min' not in all_domains[i] or val < all_domains[i]['min']:
            all_domains[i]['min'] = val
          if 'max' not in all_domains[i] or val > all_domains[i]['max']:
            all_domains[i]['max'] = val
        elif col_type == 'ignore':
          continue
        else: 
          print 'unregonized type %s' % col_type
          sys.exit(1)

  return all_domains

def write_domain_file(filename, all_domains):
  fout = open(filename, 'w')
  for d in all_domains:
    for k, v in all_domains[d].items():
      fout.write('%s\t%s\t%s\n' % (str(d), str(k), str(v)))

  fout.close()

def read_domains(cols, domain_file):
  domains = {}
  for c in cols:
    domains[c] = {}
  f = open(domain_file, 'r')
  for line in f:
    row = line.split('\t')
    col_id = int(row[0])
    if col_id in domains:
      domains[col_id][row[1]] = float(row[2])

  return domains


# Load all *.train files from 'in_path', and sample 'samp_ratio' percent 
# of rows and write them into 'out_file'
def sample_train_files(in_path, out_file, sample_ratio):
  fw = open(out_file, 'w')

  for f in gen_file_list(in_path):
    if not f.endswith('.train'):
      continue
    fr = open(f, 'r')
    for line in fr:
      if np.random.randint(sample_ratio) == 0:
        fw.write(line)

    fr.close()
  fw.close()

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
    if not f.endswith('.prescale'):
      continue
    path = f.rsplit('/', 1)[0]
    header = get_header(path + '/.header')
    all_domains = compute_domains(path, '.prescale', header)
    write_domain_file(path + '/.prescale.domains', all_domains)

#  outputTable(dtable, "d_%s_%s" % (sys.argv[2], sys.argv[1]))

if __name__ == '__main__':
  main()
