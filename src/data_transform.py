import sys
import os

class VariableEntity:
  "describes a variable entity"
  
  def __init__(self, name, descr, length):
    self.name = name
    self.descr = descr
    self.length = length
    self.domain = {}
  
  def add_value(self, lo, hi, descr):
    if lo in self.domain:
      print 'value already existed: ' + lo
    self.domain[lo] = (hi, descr)

  def values(self):
    return self.domain

  def if_valid_value(self, val):
    if self.name == 'INDNAICS' or self.name == 'OCCSOC5':
      return True

    if val.strip() == '':
      return 'blank' in self.domain

    if val == 'P':
      return 'P' in self.domain

    for k, v in self.domain.items():
      if k == 'blank' or k == 'P':
        continue
      
      if k[-1] == '+':
        try:
          if ((int(k[:-1]) >= 0 and int(val) >= int(k[:-1])) or 
              (int(k[:-1]) < 0 and int(val) <= int(k[:-1]))):
            return True
        except ValueError:
          print k
          print v[0]
          print val
     
      else:
        try:
          if ((int(val) >= int(k) and int(val) <= int(v[0])) or
            (int(val) <= int(k) and int(val) >= int(v[0]))):
            return True
        except ValueError:
          print k
          print v[0]
          print val

    print 'actual value: %s' % val
    print 'expected value %s' % str(self.domain)

    return False

class DataDictionary:
  "describes the fields of a raw data file"

  def __init__(self, dict_file):
    self.filename = dict_file
    self.fields = []
    self.rec_type = ''
    self.variables = {}

    self.parse_file()

  def parse_file(self):
    f = open(self.filename)
    for i, r in enumerate(f):
      if i < 2:
        continue
      if len(r.strip().split('\t')) <= 1:
        continue
      self.parse_line(r)

  def parse_line(self, line):
    ls = line.split('\t')
    self.rec_type = ls[0]
    beg_idx = int(ls[1])
    end_idx = int(ls[2])
    length = int(ls[3])
    an = ls[4]
    var_name = ls[5]
    var_desc = ls[6]
    
    if var_name not in self.variables:
      self.variables[var_name] = VariableEntity(var_name, var_desc, length)
      self.fields.append(var_name)
   
    if 'FILLER' in var_name:
      return

    val_low = ls[7]
    if val_low == '':
      return

    val_high = ls[8] if ls[8].strip() != '' else val_low
    val_desc = ls[9]

    self.variables[var_name].add_value(val_low, val_high, val_desc)
    
  def output_fields(self):
    for f in self.fields:
      print f

class DataTransformer:
  'tranform the raw data into tsv format'

  def __init__(self, idir, odir, dict_file):
    if odir == idir:
      print 'cannot write back to the same dir'
      return 

    self.input_dir = self.concat_path([idir], True)
    self.output_dir = self.concat_path([odir], True)
    self.dict_file = dict_file

  def load_data_dict(self):
    return DataDictionary(self.dict_file)


  def concat_path(self, path_list, is_path):
    full_path = ''
    for p in path_list:
      full_path += (p if p[-1] == '/' else (p + '/'))

    return full_path if is_path else full_path[:-1]

  def transform_data_dir(self):
    if not os.path.isdir(self.input_dir):
      print '%s is not a directory' % self.input_dir
      return

    file_list = os.listdir(self.input_dir)
    if not os.path.exists(self.output_dir):
      os.mkdir(self.output_dir)

    data_dict = self.load_data_dict()

    for d in file_list:
      in_d = self.concat_path([self.input_dir, d], True)
      out_d = self.concat_path([self.output_dir, d], False)

      if d != 'Idaho':
        continue

      if not os.path.isdir(in_d):
        continue
      if not os.path.isdir(out_d):
        os.mkdir(out_d)
        
      in_files = list(x for x in os.listdir(in_d) if 'PUMS' in x)

      if len(in_files) > 2:
        print 'unrecognized files in %s' % in_d
        return

      cnt = 0
      for f in in_files:
        if ((len(in_files) == 1 and 'PUMS' in f) or
            (len(in_files) == 2 and 'REVISED' in f)): 
          self.trans_file(self.concat_path([in_d, f], False),
              self.concat_path([out_d, f], False),
              data_dict)
          cnt += 1
      if cnt != 1:
        print 'didn\'t write exactly 1 file'
        return
          
  def trans_file(self, in_file, out_file, data_dict):
    
    print 'transforming %s' % in_file 
    if os.path.exists(out_file):
      print 'file exists; aborted'
      return 

    fr = open(in_file, 'r')
    fw = open(out_file, 'w')

    for line in fr:
      if line[0] != 'P':
        continue
      cur = 0
      for field in data_dict.fields[:-1]:
        if cur != 0:
          fw.write('\t')

        var = data_dict.variables[field]
        fw.write(line[cur:cur+var.length])
        cur += var.length
      fw.write('\n')

    fr.close()
    fw.close()

    self.validate_file(out_file, data_dict)

  def validate_file(self, data_file, data_dict):
    print 'validating file: %s' % data_file
    fr = open(data_file, 'r')

    for i, line in enumerate(fr):
      row = line[:-1].split('\t') # ignore the last '\n' char
      char_col = 0
      for j, cell in enumerate(row):
        var = data_dict.variables[data_dict.fields[j]]
        char_col += var.length
        if not var.if_valid_value(cell):
          print 'invalid value found in line %d, field %d' % (i, j) 
          print char_col
          return False
    print 'all values valid'
    return True

def main():
  trans = DataTransformer(sys.argv[1], sys.argv[2], sys.argv[3])
  trans.transform_data_dir()

if __name__ == '__main__':
  main()
