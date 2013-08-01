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
  keys['denorm'] = ['id']

  return keys

# Create a schema string by parsing 'header_file'
def create_schema(header_file='.header'):
  schema = []
  f = open(header_file, 'r')
  for line in f:
    schema.append(' '.join(line.strip().split('\t')))
    
  return ', '.join(schema)

# Create a schema without table prefix, e.g., l_orderkey is converted to orderkey
def create_denorm_schema(header_file = '.header'):
  schema = []
  f = open(header_file, 'r')
  for line in f:
    schema.append(' '.join(line.split('_', 1)[1].strip().split('\t')))

  return ', '.join(schema)

def create_all_table(in_path, db_name, part_mode, load_mode):
  for f in os.listdir(in_path):
    if not os.path.isdir(in_path.rstrip('/') + '/' + f):
      continue
#    if f != 'lineitem':
#      continue
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

      t_name = table_name 
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

def create_all_clustered_index(in_path, db_name, part_mode, keys):
  for f in os.listdir(in_path):
    if not os.path.isdir(in_path.rstrip('/') + '/' + f):
      continue

    full_path = '/'.join([in_path.rstrip('/'), f, part_mode])
    print 'creating index for %s' % full_path
    create_clustered_index(full_path, db_name, f, keys)

def create_clustered_index(data_path, db_name, table_name, keys):
  conn = psycopg2.connect("dbname=%s port=11111" % db_name)
  conn.set_isolation_level(0)
  cur = conn.cursor()

  for f in prep.gen_file_list(data_path):
    file_name = f[f.rfind('/')+1:]
    cur_path = f[:f.rfind('/')]
    full_path = os.path.abspath(cur_path)
#    t_name = table_name + '_' + file_name

    # this change is for denomalization
    t_name = table_name
    print t_name, table_name
    col_names = ','.join(keys[table_name])
    idx_name = t_name + '_' + col_names.replace(',', '_')

    cur.execute('DROP INDEX IF EXISTS %s' % idx_name)
    print 'creating index %s %s' % (t_name, col_names)
    cur.execute('CREATE INDEX %s ON %s (%s)' % (idx_name, t_name, col_names))
    print 'clustering index %s' % idx_name
    cur.execute('CLUSTER %s USING %s' % (t_name, idx_name))
  
  cur.close()

def denormalize(db_name):
  print 'start denormalizing...'
  conn = psycopg2.connect("dbname=%s port=11111" % db_name)
  conn.set_isolation_level(0)
  cur = conn.cursor()

#  cur.execute("DROP TABLE IF EXISTS denorm;")

  query = 'CREATE TABLE denorm AS \
           SELECT row_number() over (order by l_orderkey, l_linenumber) as id, \
                  lineitem.*, customer.*, orders.*, part.*, partsupp.*, supplier.*, \
                  nc.n_nationkey nc_nationkey, nc.n_name nc_name, nc.n_regionkey nc_regionkey, nc.n_comment nc_comment, \
                  ns.n_nationkey ns_nationkey, ns.n_name ns_name, ns.n_regionkey ns_regionkey, ns.n_comment ns_comment, \
                  rc.r_regionkey rc_regionkey, rc.r_name rc_name, rc.r_comment rc_comment, \
                  rs.r_regionkey rs_regionkey, rs.r_name rs_name, rs.r_comment rs_comment \
           FROM lineitem, customer, nation nc, nation ns, orders, part, partsupp, region rc, region rs, supplier \
           WHERE p_partkey = ps_partkey AND \
                 s_suppkey = ps_suppkey AND \
                 ps_partkey = l_partkey AND ps_suppkey = l_suppkey AND \
                 c_custkey = o_custkey AND \
                 ns.n_nationkey = s_nationkey AND nc.n_nationkey = c_nationkey AND \
                 rs.r_regionkey = ns.n_regionkey AND rc.r_regionkey = nc.n_regionkey AND \
                 o_orderkey = l_orderkey \
            ;'

  cur.execute(query)
  abspath = os.path.abspath('.')
  query2 = "copy denorm to '%s/denorm.txt' (delimiter E'\t')" % abspath;
  cur.execute(query2)

  cur.close()
  print 'done'

def create_all_unclustered_index(in_path, db_name, part_mode, load_mode):
  for f in os.listdir(in_path):
    if not os.path.isdir(in_path.rstrip('/') + '/' + f):
      continue

    full_path = '/'.join([in_path.rstrip('/'), f, part_mode])
    print 'creating unclustered index for %s' % full_path
    create_unclustered_index(full_path, db_name, f, load_mode)

def create_unclustered_index(data_path, db_name, table_name, mode = 'P'):
  conn = psycopg2.connect("dbname=%s port=11111" % db_name)
  conn.set_isolation_level(0)
  cur = conn.cursor()

  for f in prep.gen_file_list(data_path):
    file_name = f[f.rfind('/')+1:]
    cur_path = f[:f.rfind('/')]
    full_path = os.path.abspath(cur_path)

    if mode == 'P':
      t_name = table_name + '_' + file_name
    elif mode == 'I':
      t_name = table_name
    else:
      print 'unrecognized mode: %s' % mode
      sys.exit(1)

    print t_name

#    if not os.path.exists(cur_path + '/.k'):
#      k = -1
#    else:
#      k = int(open(cur_path + '/.k').read())
#    if k == 1:
#      continue
#   cols = prep.get_feature_columns(cur_path + '/.columns')
    header = prep.get_header(cur_path + '/.header')
    cols = range(len(header))[1:]

    for col in cols:
      field_name = header[col][0]
      if 'comment' in field_name:
        continue
      idx_name = '%s_%s_%s' % (t_name, str(col), field_name)
      cur.execute('DROP INDEX IF EXISTS %s' % idx_name)
      print 'creating (unclustered) index %s' % idx_name
      cur.execute('CREATE INDEX %s ON %s (%s)' % (idx_name, t_name, field_name))
  
  cur.close()

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
 
  create_db(db_name)
  create_all_table(data_path, db_name, part_mode, load_mode)
#  create_all_clustered_index(data_path, db_name, part_mode, get_primary_keys())
#  create_all_unclustered_index(data_path, db_name, part_mode, load_mode)

#  denormalize(db_name)
 
if __name__ == '__main__':
  main()

#########
#def create_index(dbname, tblname, colname):
#  conn = psycopg2.connect("dbname=%s port=11111" % dbname)
#  conn.set_isolation_level(0)
#  cur = conn.cursor()
#
#  idxname = tblname + colname
#  cur.execute("DROP INDEX IF EXISTS %s" % idxname)
#  cur.execute("CREATE INDEX %s ON %s (%s)" % (idxname, tblname, colname))
#
#  cur.close()
#  conn.close()
