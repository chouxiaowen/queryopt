import os
import preprocess as prep

def shuffle_data(in_path, out_path, label_file):
  out_path = out_path.strip('/')

  if not os.path.exists(out_path):
    os.mkdir(out_path)

  files = {}
  for f in prep.gen_file_list(in_path):
    print 'shuffling file %s' % f

    fpath = f[:f.rfind('/')]
    flabel = fpath + '/' + label_file
    if not os.path.exists(flabel):
      print 'ERROR: no label file found for %s' % f
      sys.exit(1)

    fd = open(f, 'r')
    fl = open(flabel, 'r')


    for line in fd:
      label = fl.next().strip()
      if label not in files:
        fw = open(out_path + '/' + label)
        files[label] = fw

      files[label].write(line)
  
  for w in files.values():
    w.close()

def main():
  shuffle_data(sys.argv[1], sys.argv[2], sys.argv[3])
