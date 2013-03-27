import sys
import datetime

f = open(sys.argv[1])
for line in f:
  ship = line.split(',')[0]
  rec = line.split(',')[1]
  
  shipdate = ship.split('-')
  recdate = rec.split('-')

  shipday = datetime.datetime(int(shipdate[0]), int(shipdate[1]), int(shipdate[2])).timetuple().tm_yday
  recday = datetime.datetime(int(recdate[0]), int(recdate[1]), int(recdate[2])).timetuple().tm_yday

  if recday < shipday:
    recday += 365

  print str(shipday) + ',' + str(recday)

