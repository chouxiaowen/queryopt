import sys
import os

all_values = {}

def meta_count(file_path):
  if os.path.isdir(file_path):
    if file_path[-1] != '/':
        file_path += '/'
    for f in os.listdir(file_path):
      meta_count(file_path + f)
  else:
    count_values(file_path)

def get_header(filename):
  header = []
  f = open(filename)

  for line in f:
    header.append(list(x.strip() for x in line.lower().split('\t')))
  return header 

def count_values(filename):
  print 'computing histograms for file %s' % filename
  f = open(filename)
  num_cols = 0
  for lineid, line in enumerate(f):
    row = line.split('\t')
    # check if the num_col match the previous rows
    if num_cols != 0 and len(row) != num_cols:
      print 'lineid %d: unmatched length: previous: %d, current: %d' % (lineid, num_cols, len(row))
      sys.exit(1)
    
    num_cols = len(row)

    for i, val in enumerate(row):
      if i not in all_values:
        all_values[i] = {}
      if val not in all_values[i]:
        all_values[i][val] = 1
      else:
        all_values[i][val] += 1

def output_all_distinct_count():
  for i in all_values:
    print i, len(all_values[i].keys())

def output_distinct_count(idx):
  print len(all_values[idx].keys())

def output_histogram(idx):
  for k, v in all_values[idx].items():
    print k, v

def output_subset(dict_header):
  varnames = ['sex', 'race1', 'marstat', 'educ', 'enroll', 
        'citizen', 'lang5', 'pob5', 'yr2us', 'sfrel',
        'miltary', 'esp', 'esr', 'powst5', 'trvmns', 
        'lvtime', 'clwkr']
  for v in varnames: 
    print v, str(dict_header[v]) + ',',
    output_distinct_count(dict_header[v])
  print ''
def run_stats():
  meta_count(sys.argv[1])
  header = get_header(sys.argv[2])

  dict_header = {}
  for idx, field in enumerate(header):
    dict_header[field[0]] = idx

  print dict_header
  output_subset(dict_header)

def main():
  run_stats()

if __name__ == '__main__':
  main()
