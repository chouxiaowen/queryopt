#!/usr/bin/python
import os
import preprocess as prep
import sys
import random
import shutil as shu

# Generate labels for files in in_path
def gen_labels(in_path, mode):
  for f in prep.gen_file_list(in_path):
    if f.endswith('.txt'):
      path = f[:f.rfind('/')]
      line_count = sum(1 for line in open(f, 'r'))
      k = int(open(path+'/.k').read())
      part_len = line_count / k
      fr = open(f, 'r')
      fw = open('%s/.%d.%s.labels' % (path, k, mode), 'w')
      print fw
      for i, line in enumerate(fr):
        if mode == 'random':
          label = random.randint(0, int(k)-1)
        elif mode == 'keyrange':
          label = i / part_len  
        else:
          print 'not implemented for %s' % mode
          return
        fw.write(str(label)+'\n')
      fr.close()
      fw.close()

def shuffle_data(in_path, out_path, mode = 'learn'):
  out_path = out_path.strip('/')

  if not os.path.exists(out_path):
    os.mkdir(out_path)
    
  files = {}
  for f in prep.gen_file_list(in_path):
    if not f.endswith('.txt'):
       continue

    fpath = f[:f.rfind('/')]
    k = int(open(fpath + '/.k', 'r').read())
    fout_path = out_path + '/' + mode 
    if not os.path.exists(fout_path):
      os.makedirs(fout_path)
    
    print 'shuffling file %s into %s' % (f, fout_path)
    
    if not os.path.exists(fpath + '/.header'):
      print 'header file missing: %s' % fpath + '/.header'
      sys.exit(1)

    shu.copy(fpath + '/.header', fout_path)

    if k == 1:
      shu.copy(f, fout_path + '/whole')
      continue

    if mode == 'learn':
      flabel = '%s/.%s.labels' % (fpath, k)
    
    else:
      flabel = '%s/.%s.%s.labels' % (fpath, k, mode)

    if not os.path.exists(flabel):
      if mode == 'learn':
        print 'ERROR: no label file found for %s' % f
        sys.exit(1)
      else:
        gen_random_labels(in_path, k, mode)

    fd = open(f, 'r')
    fl = open(flabel, 'r')
    for line in fd:
      label = fl.next().strip()
      if label not in files:
        fw = open('%s/%s' % (fout_path, label), 'w')
        files[label] = fw

      files[label].write(line)
  
  for w in files.values():
    w.close()

def shuffle_all_tables(in_path, out_path, mode):
  for d in os.listdir(in_path):
    if not os.path.isdir(in_path.rstrip('/') + '/' + d):
      continue
    full_in_path = in_path.rstrip('/') + '/' + d
    full_out_path = out_path.rstrip('/') + '/' + d
    shuffle_data(full_in_path, full_out_path, mode)

def main():
#  if len(sys.argv) != 4:
#    print 'Usage: postprocess.py [in_path] [out_path] [mode]'
#    return 
#  gen_labels(sys.argv[1], 'keyrange')
  shuffle_all_tables(sys.argv[1], sys.argv[2], 'keyrange')

if __name__ == '__main__':
  main()
