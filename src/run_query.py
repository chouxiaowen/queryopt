#!/opt/local/bin/python
import subprocess
import commands
import os
import sys
from time import time, sleep
import random
import signal
import shlex
import numpy as np
import discretize
import psycopg2
import math
import string
import platform

# linux machine
ROOT_PATH = "/home/ubuntu/queryopt/" if platform.system() == 'Linux' else "/Users/liwen/work/queryopt/"
PGDATA_PATH = ROOT_PATH+"pgdata/"
RAWDATA_PATH = "data/census/"
POSTGRES_BIN_PATH = "/Library/PostgreSQL/9.2/bin/"

query_cols = []
join_cols = []

DB_SCHEMA = "(caseid varchar(255), dAge varchar(255), dAncstry1 varchar(255), \
          dAncstry2 varchar(255), iAvail varchar(255), iCitizen varchar(255), \
          iClass varchar(255), dDepart varchar(255), iDisabl1 varchar(255), \
          iDisabl2 varchar(255), iEnglish varchar(255), iFeb55 varchar(255), \
          iFertil varchar(255), dHispanic varchar(255), dHour89 varchar(255), \
          dHours varchar(255), iImmigr varchar(255), dIncome1 varchar(255), \
          dIncome2 varchar(255), dIncome3 varchar(255), dIncome4 varchar(255), \
          dIncome5 varchar(255), dIncome6 varchar(255), dIncome7 varchar(255), \
          dIncome8 varchar(255), dIndustry varchar(255), iKorean varchar(255), \
          iLang1 varchar(255), iLooking varchar(255), iMarital varchar(255), \
          iMay75880 varchar(255), iMeans varchar(255), iMilitary varchar(255), \
          iMobility varchar(255), iMobillim varchar(255), dOccup varchar(255), \
          iOthrserv varchar(255), iPerscare varchar(255), dPOB varchar(255), \
          dPoverty varchar(255), dPwgt1 varchar(255), iRagechld varchar(255), \
          dRearning varchar(255), iRelat1 varchar(255), iRelat2 varchar(255), \
          iRemplpar varchar(255), iRiders varchar(255), iRlabor varchar(255), \
          iRownchld varchar(255), dRpincome varchar(255), iRPOB varchar(255), \
          iRrelchld varchar(255), iRspouse varchar(255), iRvetserv varchar(255), \
          iSchool varchar(255), iSept80 varchar(255), iSex varchar(255), \
          iSubfam1 varchar(255), iSubfam2 varchar(255), iTmpabsnt varchar(255), \
          dTravtime varchar(255), iVietnam varchar(255), dWeek89 varchar(255), \
          iWork89 varchar(255), iWorklwk varchar(255), iWWII varchar(255), \
          iYearsch varchar(255), iYearwrk varchar(255), dYrsserv varchar(255), class varchar(255))"

def get_query_cols(filename):
  global query_cols
  f = open(filename, 'r')
  query_cols = list(x.split(',') for x in f.read().split())
  f.close()

def get_join_cols(filename):
  global join_cols
  f = open(filename, 'r')
  join_cols = f.read().split()
  f.close()

def gen_join_cols(cols):
  global join_cols
  global query_cols

  cols = list(int(x) for x in cols)

  for q in query_cols:
    q = list(int(x) for x in q)
    diff = list(set(cols) - set(q))
    join_cols.append(random.sample(diff, 1))

    print join_cols[-1][0]

def execute_or_die(command):
   if not execute_status(command):
      exit()

def execute_status(command):
   print command
   status, output = commands.getstatusoutput(command)
   sleep(2)
   if status != 0:
      print " ===== FATAL: " + command + " FAILED.  Output: ====="
      print output
      return False
   return True

def connect_to_db():
# Connect to an existing database
  conn = psycopg2.connect("dbname=postgres port=11111")

  cur = conn.cursor()
  cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
# Pass data to fill a query placeholders and let Psycopg perform
# the correct conversion (no more SQL injections)
  cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (100, "abc'def"))

# Query the database and obtain data as Python objects
  cur.execute("SELECT * FROM test;")
  cur.fetchone()

# Make the changes to the database persistent
  conn.commit()

# Close communication with the database
  cur.close()
  conn.close()

# Given a raw data file with class labels
# create a database for this file, and
# load each class as a separate partition
def create_db(file_name, force_split):
  # split this big into separate files
  print 'populating data to db'
  fname = ROOT_PATH + RAWDATA_PATH + file_name
  [header, data] = discretize.load_file(fname)

  labels = list(set(data[:,-1]))
  force_name = ("_sp_" + str(force_split)) if force_split > 0 else ""
  data_dir = file_name + force_name + ".splits"
  print data_dir
  if os.path.exists(data_dir):
    print "seems the directory: %s exists already..." % data_dir
    print "skipping this step"

  else:
    os.mkdir(data_dir)

    if force_split > 0:
      interval = int(math.ceil(data.shape[0] * 1.0 / force_split))
      for i in range(force_split):
        subset = np.array(data[i*interval:min((i+1)*interval, data.shape[0]), :])
        print subset.shape
        fw = open(data_dir + "/" + file_name + "_" + str(i), 'w')
        for row in subset:
          if len(row) != len(DB_SCHEMA.split(",")):
            print "unmatched length!"
          fw.write(','.join(row) + '\n')
        fw.close()

    else:
      for x in labels:
        subset = np.array(list(list(y) for y in data if y[-1] == x))
        fw = open(data_dir + "/" + file_name + "_" + str(x), 'w')
        for row in subset:
          if len(row) != len(DB_SCHEMA.split(",")):
            print "unmatched length!"
          fw.write(','.join(row) + '\n')
        fw.close()

  # load each file into db as a table
  dbname = file_name + force_name 

  conn = psycopg2.connect("dbname=postgres port=11111")
  conn.set_isolation_level(0)

# psql_proc = subprocess.Popen([POSTGRES_BIN_PATH+"psql", "-p 11111", "postgres"], 
#      stdin=subprocess.PIPE, 
#      stdout=subprocess.PIPE,
#      universal_newlines=True)

  cur = conn.cursor()
  cur.execute("DROP DATABASE IF EXISTS %s;" % dbname)
  cur.execute("CREATE DATABASE %s;" % dbname)
  cur.close()
  conn.close()

  conn = psycopg2.connect("dbname=%s port=11111" % dbname)
  conn.set_isolation_level(0)
  cur = conn.cursor()
  tables = os.listdir(data_dir)
  for data_file in tables:
    cur.execute("DROP TABLE IF EXISTS %s;" % data_file)
    cur.execute("CREATE TABLE %s %s;" % (data_file, DB_SCHEMA))
    cur.execute("COPY %s FROM '%s' WITH (FORMAT CSV);" %
        (data_file, ROOT_PATH + RAWDATA_PATH + data_dir + "/" + data_file))

  cur.close()
  conn.close()

  return [header, data, dbname, tables]

# Given a raw data file with class labels
# create a database for this file, and
# load each class as a separate partition
def create_partitioned_db(file_name, force_split):
  # split this big into separate files
  print 'populating data to db'
  fname = ROOT_PATH + RAWDATA_PATH + file_name
  [header, data] = discretize.load_file(fname)

  labels = list(set(data[:,-1]))
  force_name = ("_sp_" + str(force_split)) if force_split > 0 else ""
  data_dir = file_name + force_name + ".splits"
  print data_dir
  if os.path.exists(data_dir):
    print "seems the directory: %s exists already..." % data_dir
    print "skipping this step"

  else:
    os.mkdir(data_dir)

    if force_split > 0:
      interval = int(math.ceil(data.shape[0] * 1.0 / force_split))
      for i in range(force_split):
        subset = np.array(data[i*interval:min((i+1)*interval, data.shape[0]), :])
        print subset.shape
        fw = open(data_dir + "/" + file_name + "_" + str(i), 'w')
        for row in subset:
          if len(row) != len(DB_SCHEMA.split(",")):
            print "unmatched length!"
          fw.write(','.join(row) + '\n')
        fw.close()

    else:
      print 'partition based on labels'
      for x in labels:
        print 'creating partition %s' % x
       # subset = np.array(list(list(y) for y in data if y[-1] == x))
        print 'created subset'
        fw = open(data_dir + "/" + file_name + "_" + str(x), 'w')
        print 'opened file'
        for row in data:
          if row[-1] != x:
            continue
          if len(row) != len(DB_SCHEMA.split(",")):
            print "unmatched length!"
          fw.write(','.join(row) + '\n')
        fw.close()

  # load each file into db as a table
  dbname = file_name + force_name 

  if not execute_status('pg_ctl start -D %s -l pg.log -o "-p 11111"' % PGDATA_PATH):         
    print "databaes failed to start..."
    sys.exit(1)
 
  sleep(2)

  conn = psycopg2.connect("dbname=postgres port=11111")
  conn.set_isolation_level(0)

# psql_proc = subprocess.Popen([POSTGRES_BIN_PATH+"psql", "-p 11111", "postgres"], 
#      stdin=subprocess.PIPE, 
#      stdout=subprocess.PIPE,
#      universal_newlines=True)

  cur = conn.cursor()
  cur.execute("DROP DATABASE IF EXISTS %s;" % dbname)
  cur.execute("CREATE DATABASE %s;" % dbname)
  cur.close()
  conn.close()

  conn = psycopg2.connect("dbname=%s port=11111" % dbname)
  conn.set_isolation_level(0)
  cur = conn.cursor()
  tables = os.listdir(data_dir)

  # create master table
  master_table = dbname
  cur.execute("DROP TABLE IF EXISTS %s;" % master_table)
  cur.execute("CREATE TABLE %s %s;" % (master_table, DB_SCHEMA))

  for data_file in tables:
    cur.execute("DROP TABLE IF EXISTS %s;" % data_file)
    cur.execute("CREATE TABLE %s () INHERITS (%s);" % (data_file,  master_table))
    cur.execute("COPY %s FROM '%s' WITH (FORMAT CSV);" %
        (data_file, ROOT_PATH + RAWDATA_PATH + data_dir + "/" + data_file))

  cur.close()
  conn.close()

  if not execute_status('pg_ctl stop -D %s' % PGDATA_PATH):         
    print "databaes failed to stop..."
    sys.exit(1)

  return [header, data, dbname, tables]

def gen_queries_uniform_row(data, query_file, num_query):
  if os.path.exists(query_file):
    print "file %s exists! Aborted. " % query_file
    return

  values = []

  for j in xrange(data.shape[1]):
    values.append(random.sample(xrange(len(data[:,j])), num_query))

  values = np.array(values).T
  print values.shape

  f = open(query_file, "w")

  for row in values:
    f.write(','.join(str(data[x][j]) for j, x in enumerate(row)) + '\n')
  f.close()

def gen_queries_uniform_key(data, query_file, num_query):
  if os.path.exists(query_file):
    print "file %s exists! Aborted. " % query_file
    return

  print "Calculating histogram"
  histos = {}
  for c in xrange(data.shape[1]):
    histos[c] = {}
    for r in data[:,c]:
      histos[c][r] = 1 if not r in histos[c] else histos[c][r] + 1

  print "Done"

  values = []
  for j in xrange(data.shape[1]):
    values.append([])
    for k in xrange(num_query):
      values[-1].append(random.sample(histos[j],1)[0])

  values = np.array(values).T
  print values.shape

  f = open(query_file, "w")
  for row in values:
    f.write(','.join(str(x) for x in row) + '\n')
  f.close()

def create_indexes(header, dbname, tables, cols):
  for table in tables:
    create_indexes_single_table(header, dbname, table, cols)

def create_indexes_single_table(header, dbname, table, cols):
  print 'creating indexes for %s' % table
  
  if not execute_status('pg_ctl start -D %s -l pg.log -o "-p 11111"' % PGDATA_PATH):         
    print "databaes failed to start..."
    sys.exit(1)

  sleep(2)

  conn = psycopg2.connect("dbname=%s port=11111" % dbname)
  conn.set_isolation_level(0)
  cur = conn.cursor()
  
  for col in cols:
    cur.execute("DROP INDEX IF EXISTS %s" % (table + str(col)))
    cur.execute("CREATE INDEX %s ON %s (%s)" % (table + str(col), table, header[col]))

  cur.execute("DROP INDEX IF EXISTS %s" % (table + str(0)))
  cur.execute("CREATE INDEX %s ON %s (%s)" % (table + str(0), table, header[0]))
  cur.execute("CLUSTER %s USING %s" % (table, table + str(0)))

  cur.close()
  conn.close()

  if not execute_status('pg_ctl stop -D ' + PGDATA_PATH):
    print "database failed to stop..."
    sys.exit(1)

def run_query(dbname, query): 
#  if platform.system() == 'Linux' and not execute_status('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches'):
#    print "failed to clear cache"
#    sys.exit(1)

#  if not execute_status('pg_ctl start -D %s -l pg.log -o "-p 11111"' % PGDATA_PATH):          
#    print "databaes failed to start..."
#    sys.exit(1)

  conn = psycopg2.connect("dbname=%s port=11111" % dbname)
  conn.set_isolation_level(0)
  cur = conn.cursor()

 # for query in queries:
 #   print query
  print query
  start = time()
  cur.execute(query)
  elapsed = time() - start

  # Selectivity estiamtion
  actual = 0
  tup = cur.fetchone()
  if not tup:
    actual = 0
  else:
    actual = int(tup[0])
      
  cur.execute("explain " + string.replace(query, 'count(*)', '*'))
  plan = cur.fetchone()[0].split()
  estimate = int(list(x for x in plan if "rows=" in x)[0].split("=")[1])

  cur.close()
  conn.close()
  
#  if not execute_status('pg_ctl stop -D ' + PGDATA_PATH):
#    print "database failed to stop..."
#    sys.exit(1)

  return [elapsed, actual, estimate]

def run_selection_queries(header, dbname, table, cols, query_file, perform_file):
  global query_cols

  f = open(query_file, 'r')

#  perform_file = perform_file + "_perform"
#  print perform_file
#  if os.path.exists(perform_file):
#    print "file-to-write: %s exists! aborted!" % perform_file
#    return

  fperf = open(perform_file, 'w')

#  conn = psycopg2.connect("dbname=%s port=11111" % dbname)
#  conn.set_isolation_level(0)
#  cur = conn.cursor()

#  cur.execute("set statement_timeout to '5min';")

  line_no = -1
  total_times = []
  total_diffs = []
  for line in f:
    if line[0] == '#':
      print "skipped this query"
      continue

    qrow = line.strip().split(',')
    line_no += 1

 #   queries = []
 #   for table_name in tables:
    query = "SELECT count(*) FROM %s WHERE" % dbname
    q_cols = query_cols[line_no]
#     q_cols = [25,31]

    for j, col in enumerate(q_cols):
      if j > 0:
        query += " AND "
      query += " %s = '%s'" % (header[int(col)], qrow[int(col)])
    query += ';'
  
    [elapsed, actual, estimate] = run_query(dbname,  query)
    
    total_times.append(elapsed)
    total_diffs.append(abs(actual-estimate))

  print "%s\ttime\t%f" % (dbname, sum(total_times) * 1.0 / len(total_times))
  print "%s\terror\t%f" % (dbname, sum(total_diffs) * 1.0 / len(total_diffs))
  return 

# unused code below
#    actuals = []
#    estimates = []
#    elapses = []
#    for query in queries:
#      print query
#      start = time()
#      cur.execute(query)
#      elapses.append(time() - start)
#
#      # Selectivity estiamtion
#      tup = cur.fetchone()
#      if not tup:
#        actual = 0
#      else:
#        actual = int(tup[0])
#      actuals.append(actual)
#      
#      cur.execute("explain " + string.replace(query, 'count(*)', '*'))
#    #  cur.execute("explain " + query)
#      plan = cur.fetchone()[0].split()
#      estimate = int(list(x for x in plan if "rows=" in x)[0].split("=")[1])
#      estimates.append(estimate)
#
#    total_diffs.append(math.fabs(sum(actuals)-sum(estimates)) * 1.0 / max(sum(actuals), sum(estimates)))
#
#    total_time.append(sum(elapses))
#
#    fperf.write('%.4f' % sum(elapses) + '\t' + 
#        '\t'.join('%.4f' % x for x in elapses) + '\n')
#
#  print ""
#  print "%s\ttime\t%f" % (dbname, sum(total_time) * 1.0 / len(total_time))
#  print "%s\testimate\t%f" %(dbname, sum(total_diffs) *1.0 / len(total_diffs))
#
  f.close()
  fperf.close()

def run_it():
  get_query_cols('query_cols')
#  get_join_cols('join_cols')
 
  if sys.argv[4] == 'create':
#  [header, data, dbname, tables] = create_db(sys.argv[1], int(sys.argv[3]))
    [header, data, dbname, tables] = create_partitioned_db(sys.argv[1], int(sys.argv[3]))
  header = discretize.get_header("header")
  cols = [16, 25, 31, 50, 66]

  force_split = int(sys.argv[3])
  file_name = sys.argv[1]
  force_name = ("_sp_" + str(force_split)) if force_split > 0 else ""
  data_dir = file_name + force_name + ".splits"

  tables = os.listdir(data_dir)

  if sys.argv[4] == 'create':
    create_indexes(header, file_name + force_name, tables, cols)
 
  query_file = sys.argv[2]
  run_selection_queries(header, file_name + force_name, tables, cols, 
      query_file, file_name + force_name + "_" + query_file)

def gen_query():
  [header, data] = discretize.load_file(sys.argv[1])
  gen_queries_uniform_row(data, sys.argv[2]+"row", int(sys.argv[3]))
  gen_queries_uniform_key(data, sys.argv[2]+"key", int(sys.argv[3]))

def main():
  run_it()

if __name__ == '__main__':
  main()
