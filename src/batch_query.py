#!/usr/bin/python
import psycopg2
import os
import sys
import numpy as np
import random
import load_db
import time
import platform
import commands


QUERY_PATH = '/home/ubuntu/queryopt/data/tpch_2_15_0/tpch_2_15.0/dbgen/'
ROOT_PATH = "/home/ubuntu/queryopt/" if platform.system() == 'Linux' else "/Users/liwen/work/queryopt/"
PGDATA_PATH = ROOT_PATH+"pgdata/"

def load_domains(domain_file):
  domains = []
  f = open(domain_file, 'r')
  for line in f:
    domains.append(line.strip('\n').split('\t'))

  return domains

def execute_status(command):
   print command
   status, output = commands.getstatusoutput(command)
   time.sleep(5)
   if status != 0:
      print " ===== FATAL: " + command + " FAILED.  Output: ====="
      print output
      return False
   return True

# Generate a file of random queries.
# Each line is format ready to be in a where clause, e.g.,
# Age = 01 AND Sex = 02
# 'col_count' determines # of columns participating the query 
# schema is from 'header_file' and values are in 'domain_file'
def gen_random_clauses(clause_file, domain_file, header_file, queriable_cols, 
    num_clause, num_query):
  
  schema = load_db.create_schema(header_file)
  attr_list = [x.split(' ')[0] for x in schema.split(', ')]
  domains = load_domains(domain_file)

  fw = open(clause_file, 'w')

  for i in xrange(num_query):
    q_cols = random.sample(queriable_cols, num_clause)

    q_str = []
    for col in q_cols:
      val = random.sample(domains[col], 1)[0]
      q_str.append('%s = %s' % (attr_list[col], val))
    fw.write(' AND '.join(q_str) + '\n')
  fw.close()

# Given a clause file, write a file of queries. 
def gen_select_count_queries(clause_file, query_file, table_name):
  fr = open(clause_file, 'r')
  fw = open(query_file, 'w')

  for line in fr:
    fw.write('SELECT count(*) FROM %s WHERE %s;' % (table_name, line))
  fw.close()
  fr.close()

def read_query_file(filename):
  return open(filename, 'r').read()

def parse_query_file(filename):
  query = ''
  f = open(filename, 'r')
  sub = {}
  lim = -1

  for line in f:  
    if line.startswith('--:'):
      t = line.lstrip('-').strip().split('=')
      sub[t[0]] = t[1]
      continue

    if line.startswith(':n'):
      lim = int(line.split()[1])

    if line.startswith('--') or line.startswith(':'):
      continue
    query += line

  for k, v in sub.items():
    query = query.replace(k, v)

  if lim > 0:
    query = query.rstrip().rstrip(';') + '\n limit %d;' % lim 

  return query

def run_tpch_query(db_name, query_path, stats_file, result_path, k, clearcache=True, restart=True, write=True):
#  cur.execute("set statement_timeout to '10min';")
  queries = {}
  times = {}
  tup = os.walk(query_path).next()

  print tup
  if not os.path.isdir(result_path):
    os.mkdir(result_path)

  fres = open(stats_file, 'w')
  for f in tup[2]:
    print f
    f_path = tup[0] + '/' + f
#    query = parse_query_file(f_path) 
    
    query = read_query_file(f_path)

    if f in ['17.sql','15.sql', 
              '13.sql', 
              '21.sql', 
              '20.sql', 
              '2.sql', 
              '22.sql', 
              '11.sql',
              '16.sql',
              '4.sql']:
      continue

    forbid = ['17_', '15_', '13_', '21_', '20_', '2_', '22_', '11_', '16_', '4_']

    flag = False
    for forb in forbid:
      if f.startswith(forb): 
        flag = True
    
    if flag:
      continue

    qid = f.split('.')[0]
    queries[qid] = query

####### test to skip partitions
    if restart: 
      if not execute_status('pg_ctl start -D %s -l pg.log -o "-p 11111"' % PGDATA_PATH):          
        print "databaes failed to start..."
        sys.exit(1)
    
    conn = psycopg2.connect("dbname=%s port=11111" % db_name)
    conn.set_isolation_level(0)
    cur = conn.cursor()
   


    if k > 0: 
      parts = []
    
      for i in range(int(k)):
        print 'querying %d' % i
        part = 'denorm_%d' % i
        cur.execute(query.replace('denorm', part))
        res = cur.fetchone()
        print res
        if res and res[0]:
          parts.append(part)

      view_query = 'create view view1 as ' +  ' UNION ALL '.join( 'select * from %s' % part for part in parts) + ';'
      print view_query
      cur.execute('drop view if exists view1;')
      cur.execute(view_query)

      if restart:
        if not execute_status('pg_ctl stop -m fast -D ' + PGDATA_PATH):
          print "database failed to stop..."
          sys.exit(1)

      cur.close()
#######

    if clearcache:
      if platform.system() == 'Linux' and not execute_status('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches'):
        print "failed to clear cache"
        sys.exit(1)

    if restart:
      if not execute_status('pg_ctl start -D %s -l pg.log -o "-p 11111"' % PGDATA_PATH):          
        print "databaes failed to start..."
        sys.exit(1)

    conn = psycopg2.connect("dbname=%s port=11111" % db_name)
    conn.set_isolation_level(0)
    cur = conn.cursor()

# create/drop view for q15
# count timing for them
    print 'running query %s' % f
#    if f == '15.sql':
#      c_view = query.split(';')[0] + ';'
#      q_body = query.split(';')[1] + ';'
#      d_view = query.split(';')[2] + ';'
      
#      start = time.time()
#      cur.execute(c_view)
#      cur.execute(q_body)
#      cur.execute(d_view)
#      t = time.time() - start

#    else:
    start = time.time()
#    cur.execute(query)
    if k > 0:
      cur.execute(query.replace('denorm', 'view1'))
    else:
      cur.execute(query)
    t = time.time() - start   

    if write:
      fw = open(result_path + f, 'w')
      for res_tup in cur.fetchall():
        fw.write('\t'.join(str(x) for x in res_tup) + '\n')
      fw.close()
    else:
      print cur.fetchone()
    
    t = t * 1000
    times[qid] = t 
   
    fres.write('%s\t%s\t%d\n' % (db_name, qid, t))
    print '%s\t%s\t%d' % (db_name, qid, t)
    fres.flush() 
     
    if k > 0:
      cur.execute('drop view view1 if exists;')

    conn.close()
    if restart:
      if not execute_status('pg_ctl stop -m fast -D ' + PGDATA_PATH):
        print "database failed to stop..."
        sys.exit(1)

  fres.close()

# Run a set of queries on databaes 'db_name', and 
# get the timing information back
# Each line in 'query_file' is a stand-alone sql query
def run_query(db_name, query_file, result_file):
  conn = psycopg2.connect("dbname=%s port=11111" % db_name)
  conn.set_isolation_level(0)
  cur = conn.cursor()
#  cur.execute("set statement_timeout to '10min';")

  times = []
  for line in query_file:
    start = time.time()
    cur.execute(line)
    t = time.time() - start
  #  print cur.fetchone()
    times.append(t)
    print line
    print '%d seconds' % t

  conn.close()

  avg = np.average(times)
  std = np.std(times)

  fw = open(result_file, 'w')
  fw.write('\t'.join(times) + '\n')
  fw.write('%f\t%f\n' % (avg, std))
  fw.close()

def main():
#  cols = sorted([11, 24, 28, 37, 33, 51, 32, 86, 103, 101, 104, 114, 135]) 
#  gen_random_clauses('rand_clause.txt', 'domain.txt', 'header.txt', cols, 3, 100)

#  gen_select_count_queries('rand_clause.txt', 'rand_query.txt', 'shuf5')  

  db_name = sys.argv[1]
  query_path = sys.argv[2]
  stats_file = sys.argv[3]
  k = int(sys.argv[4])

#  run_tpch_query(db_name, QUERY_PATH + query_path, stats_file, query_path.replace('/', '_') + 'res/', True, True, False)
  run_tpch_query(db_name, QUERY_PATH + query_path, stats_file, query_path.replace('/', '_') + 'res/', k, False, False, True)

if __name__ == '__main__':
  main()
