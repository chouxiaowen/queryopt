import sys
import os
import get_header

all_values = {}

def meta_count(file_path):
  if os.path.isdir(file_path):
    for f in os.listdir(file_path):
      count_values(f)
  else:
    count_values(file_path)

def get_header(filename):
  header = []
  f = open(filename)

  for line in f:
    header.append(line.lower().split('\t'))
  return header 

def count_values(filename):
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

def run_stats():
  meta_count(sys.argv[1])
  header = get_header(sys.argv[2])

  dict_header = {}
  for idx, field in enumerate(header):
    dict_header[field[0]] = idx

#  output_histogram(dict_header['connectiontype'])
  print 'conntype'
  output_histogram(dict_header['conntype'])
  print 'isp'
  output_histogram(dict_header['isp'])
  print 'city'
  output_histogram(dict_header['city'])

#  print dict_header['joinTimeMs']
#  print dict_header['playTimeMs']

def main():
  run_stats()

if __name__ == '__main__':
  main()
