import subprocess
import commands
import os
import time
import signal
import shlex

ROOT_PATH = "/home/ubuntu/"
REPO_PATH = "/home/ubuntu/cs186-fa12/"
POSTGRES_BIN_PATH = "/home/ubuntu/pgsql/bin/"
PGDATA_PATH = ROOT_PATH + "pgsql/data/"
POSTGRES_TLD = REPO_PATH + "hw3/postgresql-8.4.2/"
BUFFER_C_PATH = POSTGRES_TLD + "src/backend/storage/buffer/"
BUFFER_H_PATH = POSTGRES_TLD + "src/include/storage/"

OUTPUT_LOG_PATH = ROOT_PATH + "output_logs/"
GRADE_LOG_PATH = ROOT_PATH + "grading_logs/"

BUFFER_STRAT = ['LRU', 'MRU', '2Q']
BUFFER_SIZE = [16, 32, 64, 96]

# commands to setup for testing
SETUP_COMMANDS = [ "CREATE DATABASE test;",
                   "\connect test;",
                   "CREATE TABLE raw_r_tuples (tname varchar(2), pkey int, num2 int, num3 int, num1 int, node varchar(16), inserttime float8, lifetime float8, testname varchar(64));",
                   "CREATE TABLE raw_s_tuples (tname varchar(2), pkey int, num2 int, num3 int, num1 int, node varchar(16), inserttime float8, lifetime float8, testname varchar(64));",
                   "COPY raw_r_tuples FROM '/home/ff/cs186/sp11_files/hw1/R' DELIMITERS ',';",
                   "COPY raw_s_tuples FROM '/home/ff/cs186/sp11_files/hw1/S' DELIMITERS ',';" ]
# the queries to run
BENCHMARK = [ "SELECT * FROM raw_r_tuples;",
              "SELECT * FROM raw_r_tuples;",
              "SELECT * FROM raw_r_tuples;",
              "SELECT * FROM raw_r_tuples r, raw_s_tuples s WHERE r.pkey = s.pkey;",
              "SELECT * FROM raw_r_tuples r, raw_s_tuples s WHERE r.pkey = s.pkey;",
              "SELECT * FROM raw_r_tuples r, raw_s_tuples s WHERE r.pkey = s.pkey;" ]

# An alarm timeout exception.
class Alarm(Exception):
  pass

def alarm_handler(signum, frame):
  raise Alarm

#execute shell command 'command', or exit
def execute_or_die(command):
   if not execute_status(command):
      exit()

def execute_status(command):
   print command
   status, output = commands.getstatusoutput(command)
   if status != 0:
      print " ===== FATAL: " + command + " FAILED.  Output: ====="
      print output
      return False
   return True

#sends something to a subprocess' stdin
def send_cmd_to_proc(proc, command):
   print command
   proc.stdin.write(command + '\n')
   # XXX: this is pretty horribile
   #out = proc.stdout.readline()
   #print out

run_time_log = {}

#runs a bunch of commands on psql, waits for it to terminate, etc.
def run_psql(dbname, commands, output_file):
  psql_proc = subprocess.Popen([POSTGRES_BIN_PATH+"psql", "-p 11111", dbname], stdin=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)
  logfile = open(output_file, "r")
  logtext = logfile.read()
  logfile.close()
  logtext = logtext.lower()
  if ("fatal" in logtext or
      "error" in logtext or
      "segmentation fault" in logtext):
    return 2

  for cmd in commands:
    send_cmd_to_proc(psql_proc, cmd)
  send_cmd_to_proc(psql_proc, "\q")

  signal.signal(signal.SIGALRM, alarm_handler)
  signal.alarm(POSTGRES_TIMEOUT)
  try:
    psql_proc.communicate()
    signal.alarm(0) # reset alarm
    return 0
  except Alarm:
    print 'Running toooooooo looooooooong!!!!!!!!!!'
    return 1

def run_it():
  count_total = 0
  count_compile = 0

  for login in os.listdir(SUBMISSIONS_PATH):
    freelist_file = SUBMISSIONS_PATH + login + "/freelist.c"
    bufinternals_file = SUBMISSIONS_PATH + login + "/buf_internals.h"

    print " ===== " + login + " ===== "

    try:
      f = open(GRADE_LOG_PATH + login + ".log")
      logtext = f.read()
      f.close()
      if ("2Q 96 successfully finished!" in logtext or
          "All completed!" in logtext):
        print "skipped; we have already run this login"
        continue
    except IOError:
      pass

    # remove unnecessary logs
#    fr = open(freelist_file, "r")
#    fw = open(BUFFER_C_PATH+freelist_file, "w")
#    for line in f:
#      if "elog" in line and "LOG" in line and "GRADING" not in line:
#        continue
#      fw.write(line + "\n")
#    fw.close()

    execute_or_die("cp %s %s" % (freelist_file, BUFFER_C_PATH))
    execute_or_die("cp %s %s" % (bufinternals_file, BUFFER_H_PATH))

    os.chdir(POSTGRES_TLD)
    print " === 0. Make === "
   # RUBRIC: 0 points if submission fails to make
    count_total += 1

    make_fail = False
    if not execute_status('make && make install'):
      print "make failed..."
      run_time_log[login] = ["make failed"]
      make_fail = True
    else:
      run_time_log[login] = ["make succeeded"]

    count_compile += 1

    print " === 1. Benchmarking === "
   # begin the benchmarking
    
    output_path = OUTPUT_LOG_PATH  + login + "/"

    if not os.path.exists(output_path):
      os.mkdir(output_path)

    os.chdir(output_path)

    for strat in BUFFER_STRAT:
      for size in BUFFER_SIZE:
        ssize = str(size)
        output_file = output_path + strat.lower() + "-" + ssize + ".log"
        if os.path.exists(output_file): # remove old cruft 
          os.remove(output_file)
        run_time_log[login].append("%s %s started!" % (strat, ssize))

        print " === " + strat + " " + ssize + " === "

        # Kill previous postgres process, if any
        if os.path.exists(PGDATA_PATH + "postmaster.pid"):
#  if not execute_status(POSTGRES_BIN_PATH + 'pg_ctl -D ' + PGDATA_PATH + ' stop'):
#if not execute_status("kill `head -1 %spostmaster.pid`" % PGDATA_PATH):
          os.remove(PGDATA_PATH + "postmaster.pid")
      
        p1 = subprocess.Popen(shlex.split('ps aux'), stdout = subprocess.PIPE)
        p2 = subprocess.Popen(['grep', 'postgres'], stdin = p1.stdout, stdout = subprocess.PIPE)
        p3 = subprocess.Popen(['grep', '-v', 'grep'], stdin = p2.stdout, stdout = subprocess.PIPE)
        output = p3.communicate()[0]
        proc_list = []
        tmplist = output.split()
        for i, word in enumerate(tmplist):
          if "saasbook" == word and tmplist[i+1].isdigit():
            proc_list.append(tmplist[i+1])

        if proc_list:
          print "killing existing postgres server"
          execute_status("kill -3 %s" % " ".join(proc_list))

        if make_fail:
          out = open(output_file, "w")
          out.write("make failed")
          out.close()
          print "make failed; don't bother start the server"
          continue

        if not execute_status(POSTGRES_BIN_PATH +  'pg_ctl start -D ' + PGDATA_PATH + ' -l ' + output_file + ' -o "--buffer-replacement-policy=' + strat + ' -B ' + ssize + ' -p 11111 -N 1 -o \'-te -fm -fh\' --autovacuum=off"'):
          print "FATAL (" + strat + "): pg_ctl failed to start..." + "\n"
          run_time_log[login].append("server start-up failed")
          continue

        time.sleep(5) # XXX: RACE CONDITION HERE
        logfile = open(output_file, "r")
        logtext = logfile.read()
        logfile.close()
        if logtext.find("FATAL") != -1 or logtext.find("ERROR") != -1:
          print "Found FATAL in log file..."
          run_time_log[login].append("server start-up failed")
          continue

        run_time_log[login].append("server start-up succeeded")

        
        sig = run_psql("postgres", BENCHMARK, output_file)
        if sig:
          if sig == 1:
            print "FATAL (" + strat + "): timeout..." + "\n"
            run_time_log[login].append("query processing failed: time limit exceeded")
          elif sig == 2:
            print "FATAL: psql connection failed"
            run_time_log[login].append("psql connection failed")
        run_time_log[login].append("query processed")

        if not execute_status(POSTGRES_BIN_PATH + 'pg_ctl -D ' + PGDATA_PATH + ' stop'):
          print "FATAL (" + strat + "): pg_ctl failed to stop..." + "\n"
          run_time_log[login].append("server shut-down failed")
          
          #execute_or_die("kill -9 `head -1 %spostmaster.pid`" % PGDATA_PATH)
          continue

        else:
          run_time_log[login].append("server shut-down succeeded")

        run_time_log[login].append("%s %s successfully finished!" % (strat, ssize))

    run_time_log[login].append('All completed!')

    write_grade_logs(login)

    os.chdir('/media/sf_hw3/')
    execute_or_die('python vm_parselog.py')

  print count_compile, count_total

def write_grade_logs(login):
  if not os.path.exists(GRADE_LOG_PATH):
    os.mkdir(GRADE_LOG_PATH)

# for key, val in run_time_log:  
  log_file = GRADE_LOG_PATH + login + '.log'
  log = open(log_file, 'w')
  for line in run_time_log[login]:
    log.write(line+"\n")

  log.close()

def main():
  run_it()

if __name__ == "__main__":
  main()

