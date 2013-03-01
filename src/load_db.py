import preprocess as prep
import psycopg2


# Create a schema string by parsing 'header_file'
def create_schema(header_file):
  schema = []
  f = open(header_file, 'r')
  for line in f:
    schema.append(' '.join(line.strip().split('\t')))
    
  return ', '.join(schema)

# Populate the data from 'data_path' into database 'db_name'. 
# if mode='partitioned', create a single table and load files as its partitions
# if mode='individual', create one table for each file
def create_table(data_path, header_file, table_name, db_name, mode):
  # connect to db
  conn = psycopg2.connect("dbname=%s port=11111" % dbname)
  conn.set_isolation_level(0)
  cur = conn.cursor()

  schema = create_schema(header_file)

  if mode == 'individual':
    for f in prep.gen_file_list(data_path):
      table_name = table_name + f[f.rfind('/')+1:]
      cur.execute("DROP TABLE IF EXISTS %s;" % table_name)
      cur.execute("CREATE TABLE %s %s;" % (table_name, schema))
      cur.execute("COPY %s FROM '%s' WITH (FORMAT CSV DELIMITER '\t');" % (table_name, f))

  elif mode == 'partitioned':
  # create master table
    master_table = table_name
    cur.execute("DROP TABLE IF EXISTS %s;" % master_table)
    cur.execute("CREATE TABLE %s %s;" % (master_table, schema))

    for f in prep.gen_file_list(data_path):
      table_name = f[f.rfind('/')+1:]
      cur.execute("DROP TABLE IF EXISTS %s;" % f)
      cur.execute("CREATE TABLE %s () INHERITS (%s);" % (table_name, master_table))
      cur.execute("COPY %s FROM '%s' WITH (FORMAT CSV DELIMITER '\t');" % (table_name, f))

  else:
    print 'unknown mode: %s' % mode
    sys.exit(1)

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
# create_db('toy')
  create_table(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])

if __name__ == '__main__':
  main()


