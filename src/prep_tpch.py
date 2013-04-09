#!/usr/bin/python
import sys
import os
import preprocess as prep
import value_counts as vc
import datetime as dt
import numpy as np
import math

# Given a list of files in a flat directory in path [old_path], 
# for each x.tbl file, output two files into path [new_path/x]: 
# - x.txt, for loading into db, delimited by '\t'
# - x.train, for clustering, having converted dates into integers.
def place_data_file(old_path, new_path):
  for path, dirs, files in os.walk(old_path):
    for f in files:
      if f.endswith('.tbl'):
        print 'placing %s' % f
        p = new_path.rstrip('/') + '/' + f.rsplit('.', 1)[0]
        if not os.path.exists(p):
          os.mkdir(p)
        fr = open(path + '/' + f, 'r')
        fw = open(p + '/' + f.rsplit('.', 1)[0] + '.txt', 'w')
        for line in fr:
          fw.write(line.replace('|\n', '\n').replace('|', '\t'))
        fr.close()
        fw.close()

# For each *.txt files from 'in_path', and sample 'samp_ratio' percent of rows 
# Write out the sampled data, e.g., 'x.train.10.sample'
def sample_train_files(in_path):
  for f in prep.gen_file_list(in_path):
    if not f.endswith('.train'):
      continue

    print 'sampling %s' % f
    fpath = f.rsplit('/', 1)[0] 
    sample_ratio = int(open(fpath + '/.ratio').read())

    fr = open(f, 'r')
    fw = open('%s.%d.sample' % (f, sample_ratio), 'w')
    
    for line in fr:
      if np.random.randint(sample_ratio) == 0:
        fw.write(line)

    fr.close()
    fw.close()

# Process 'dss.ddl'
def redistribute_header(cur_path='./', des_path='./train/'):
  whole_str = open(cur_path + 'dss.ddl', 'r').read()
  ls = whole_str.split(';')

  for schema in ls[:-1]:
    table_name = schema.split('(', 1)[0].strip().split(' ')[2].lower()
    fw = open(des_path + table_name + '/.header', 'w') 
    for field in schema.split('(', 1)[1].rsplit(')', 1)[0].split(',\n'):
      parsed_field  = field.strip().split()
      print parsed_field
      fw.write('\t'.join([parsed_field[0], ' '.join(parsed_field[1:])]) + '\n')
    fw.close()

def convert_row(row, header):
  new_row = []
  for i, cell in enumerate(row):
    col_name = header[i][0]
    col_desc = header[i][1]
    
    val = row[i]
    if 'date' in col_desc:
      date = val.split('-')
      val = dt.date(int(date[0]), int(date[1]), int(date[2])).toordinal()

    # a naive way to scale up 
#    if 'l_discount' in col_name:
#      val = float(val) * 1000000

#    if 'key' in col_name:       
#      val = int(val) % 11

#    if 'date' in col_name:
#      mark = -1
#      for i, x in enumerate(date_mark):
#        if val <= x:
#          mark = i
#          break
#
#      if mark < 0:
#        print 'error in discretizing date: %s' % str(val)
#        print dt.date(1900,1,1).fromordinal(val)
#        print date_mark
#        sys.exit(1)
#      val = mark
#
    new_row.append(str(val))

  return new_row

def convert_data_file(path):
  # get date mark
#  mindate = dt.date(1992,1,1).toordinal()
#  maxdate = dt.date(1998,12,31).toordinal()
#  weight = [10.0, 4.0, 3.0]
#  weight = [(99.0 * x + 1.0) for x in np.random.sample(5)]
#  date_mark = []
#  acc = 0
#  for x in weight:
#    acc += x
#    date_mark.append(acc/sum(weight) * (maxdate - mindate) + mindate)

  for f in prep.gen_file_list(path):
    if not f.endswith('.txt'):
      continue

    print 'converting file: %s' % f
  
    header_file = f.rsplit('/', 1)[0] + '/.header'
    header = prep.get_header(header_file)
  
    fin = open(f, 'r')
    fout = open(f.replace('.txt', '.prescale'), 'w')

    for line in fin:
      row = line.strip().split('\t')
      new_row = convert_row(row, header)
  
      fout.write('\t'.join(new_row) + '\n')

    fin.close()
    fout.close()

def rescale(val, min_val, max_val, scale):
  return (val - min_val) * 1.0 / (max_val - min_val) * scale

def rescale_data_file(path):
  for f in prep.gen_file_list(path):
    if not f.endswith('.prescale'):
      continue
   
    print 'rescaling file: %s' % f
    fpath = f.rsplit('/', 1)[0]
    cols = prep.get_feature_columns(fpath + '/.columns')
    domains = prep.read_domains(cols, fpath + '/.prescale.domains')
    header = prep.get_header(fpath + '/.header')

    scaled_file = f.replace('.prescale', '.train')

    fin = open(f, 'r')
    fout = open(scaled_file, 'w')

    for line in fin:
      row = line.strip().split('\t')
      for c in cols:
        if prep.get_col_type(c, header) == 'num':
          min_val = float(domains[c]['min'])
          max_val = float(domains[c]['max'])
          new_val = rescale(float(row[c]), min_val, max_val, 1e6)
    #      log_val = math.log(new_val + 1)
          row[c] = str(new_val)
      fout.write('\t'.join(row) + '\n')
    fin.close()
    fout.close()
    #    all_domains = prep.compute_domains(fpath, '.train', header)
   # prep.write_domain_file(fpath + '/.domains', all_domains)

def count(path):
  vc.meta_count(path)
  vc.output_all_distinct_count()
  vc.output_domain('hehe.txt')

def main():
#  place_data_file('./raw', './train')
#  redistribute_header() 
#  convert_data_file('./train/lineitem')
#  rescale_data_file('./train/lineitem')
#  sample_train_files('./train/lineitem')
#  convert_data_file('./train/lineitem')  
#  vc.meta_count(sys.argv[1])
#  vc.output_domain(sys.argv[2])


  header = prep.get_header('./train/lineitem/.header')
  all_domains = prep.compute_domains('./train/lineitem', '.train', header)
  prep.write_domain_file('./train/lineitem/.domains', all_domains)




if __name__ == '__main__':
  main()
