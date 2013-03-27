import sys

f = open(sys.argv[1])
header = f.next().strip().split(',')

print 'CREATE TABLE census(' + ', '.join(("%s varchar(255)" % col.split(" ")[0]) for col in header) + ')'
