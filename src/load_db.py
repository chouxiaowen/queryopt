#!/usr/bin/python
import sys
import os
import preprocess as prep
import psycopg2

def get_primary_keys():
  keys = {}
  keys['part'] = ['p_partkey']
  keys['supplier'] = ['s_suppkey']
  keys['partsupp'] = ['ps_partkey', 'ps_suppkey']
  keys['customer'] = ['c_custkey']
  keys['orders'] = ['o_orderkey']
  keys['lineitem'] = ['l_orderkey', 'l_linenumber']
  keys['nation'] = ['n_nationkey']
  keys['region'] = ['r_regionkey']

  return keys

# Create a schema string by parsing 'header_file'
def create_schema(header_file='.header'):
  schema = []
  f = open(header_file, 'r')
  for line in f:
    schema.append(' '.join(line.strip().split('\t')))
    
  return ', '.join(schema)

def create_all_table(in_path, db_name, part_mode, load_mode):
  for f in os.listdir(in_path):
    if not os.path.isdir(in_path.rstrip('/') + '/' + f):
      continue

    full_path = '/'.join([in_path.rstrip('/'), f, part_mode])
    print 'creating tables for %s' % full_path
    create_table(full_path, f, db_name, load_mode)

# Populate the data from 'data_path' into database 'db_name'.
# Each single file will correspond to one table
# If mode='P', or 'partitioned', create a single table and load files as its partitions
# If mode='I', or 'individual', create one table for each file
def create_table(data_path, table_name, db_name, mode):
  # connect to db
  conn = psycopg2.connect("dbname=%s port=11111" % db_name)
  conn.set_isolation_level(0)
  cur = conn.cursor()

  schema = create_schema(data_path.strip('/') + '/.header')

  if mode == 'individual' or mode == 'I':
    cur.execute("DROP TABLE IF EXISTS %s CASCADE;" % table_name)
    for f in prep.gen_file_list(data_path):
      file_name = f[f.rfind('/')+1:]
      cur_path = f[:f.rfind('/')]
      full_path = os.path.abspath(cur_path)

      t_name = table_name + '_' + file_name 
      cur.execute("DROP TABLE IF EXISTS %s;" % t_name)
      cur.execute("CREATE TABLE %s (%s);" % (t_name, schema))
      cur.execute("COPY %s FROM '%s';" % (t_name, full_path + '/' + file_name))

  elif mode == 'partitioned' or mode == 'P':
  # create master table
    cur.execute("DROP TABLE IF EXISTS %s CASCADE;" % table_name)
    cur.execute("CREATE TABLE %s (%s);" % (table_name, schema))
    
    for f in prep.gen_file_list(data_path):
      file_name = f[f.rfind('/')+1:]
      cur_path = f[:f.rfind('/')]
      full_path = os.path.abspath(cur_path)

      t_name = table_name + '_' + file_name
      print t_name, table_name
      cur.execute("DROP TABLE IF EXISTS %s;" % t_name)
      cur.execute("CREATE TABLE %s () INHERITS (%s);" % (t_name, table_name))
      cur.execute("COPY %s FROM '%s' ;" % (t_name, full_path + '/' + file_name))

  else:
    print 'unknown mode: %s' % mode
    sys.exit(1)

  cur.close()
  conn.close()

def create_all_clustered_indexes(in_path, db_name, part_mode, keys):
  for f in os.listdir(in_path):
    if not os.path.isdir(in_path.rstrip('/') + '/' + f):
      continue

    full_path = '/'.join([in_path.rstrip('/'), f, part_mode])
    print 'creating index for %s' % full_path
    create_clustered_index(full_path, db_name, f, keys)

def create_clustered_index(data_path, db_name, table_name, keys):
# create master table
  
  conn = psycopg2.connect("dbname=%s port=11111" % db_name)
  conn.set_isolation_level(0)
  cur = conn.cursor()

  for f in prep.gen_file_list(data_path):
    file_name = f[f.rfind('/')+1:]
    cur_path = f[:f.rfind('/')]
    full_path = os.path.abspath(cur_path)
    t_name = table_name + '_' + file_name
    print t_name, table_name
    col_names = ','.join(keys[table_name])
    idx_name = t_name + '_' + col_names.replace(',', '_')

    cur.execute('DROP INDEX IF EXISTS %s' % idx_name)
    print 'creating index %s %s' % (t_name, col_names)
    cur.execute('CREATE INDEX %s ON %s (%s)' % (idx_name, t_name, col_names))
    print 'clustering index %s' % idx_name
    cur.execute('CLUSTER %s USING %s' % (t_name, idx_name))
  
  cur.close()

def create_index(dbname, tblname, colname):
  conn = psycopg2.connect("dbname=%s port=11111" % dbname)
  conn.set_isolation_level(0)
  cur = conn.cursor()

  idxname = tblname + colname
  cur.execute("DROP INDEX IF EXISTS %s" % idxname)
  cur.execute("CREATE INDEX %s ON %s (%s)" % (idxname, tblname, colname))

  cur.close()
  conn.close()

# Create database 'dbname' in postgres  
def create_db(dbname):
  conn = psycopg2.connect("dbname=postgres port=11111")
  conn.set_isolation_level(0)

  cur = conn.cursor()
  cur.execute("DROP DATABASE IF EXISTS %s;" % dbname)
  cur.execute("CREATE DATABASE %s;" % dbname)
  cur.close()
  conn.close()

def main():
  if len(sys.argv) < 5:
    print 'Usage: load_db.py [data_path] [db_name] [partitioned_mode: "learn", "keyrange" or "random"] [load_mode:I or P]'
    return
  
  data_path = sys.argv[1]
  db_name = sys.argv[2]
  part_mode = sys.argv[3]
  load_mode = sys.argv[4]
 
  create_all_clustered_indexes(data_path, db_name, part_mode, get_primary_keys())
#  create_db(db_name)
#  create_all_table(data_path, db_name, part_mode, load_mode)

if __name__ == '__main__':
  main()
