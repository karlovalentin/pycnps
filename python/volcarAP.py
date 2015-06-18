#!/usr/bin/python
import re
import requests
from netaddr import EUI
import MySQLdb
import random
import time
from termcolor import colored
import csv
import sys
import glob

##conexion con mysql
dbms = MySQLdb.connect("173.194.251.199","receptor","mofos2014","cinepolis" )

#cursor=dbms.cursor()

#cursor.execute("""SELECT * from prosfiles""")
#rows=cursor.fetchall()

#print len(rows)

#funcion para validar que sea numero
def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False

#creamos la lista de archivos del directorio
#listaarchivos= glob.glob("/var/www/csv/*.csv")

#creamoslistadesdemysql
cursor=dbms.cursor()
#cursor.execute("""SELECT * from prosfiles where pros = 1""")
cursor.execute("""SELECT * from prosfiles""")
lif=cursor.fetchall()


for archivocsv in lif:
	#print archivocsv
	prosfile='/var/www/csv/'+archivocsv[1]
	print  'estamos volcando el archivo: %r' % prosfile
	namegateway=prosfile.split('-')
	print namegateway[1]
	nameestacion=namegateway[1]

	data_initial = open(prosfile, "rU")
	data = csv.reader((line.replace('\0','') for line in data_initial), delimiter=",")
	arrelgos=[]

	for row in data:
		if row != []:
			arrelgos.append(row)

	longitudrows= len(arrelgos)

	#creamos el primer array de aps
	counter=0;
	aps = []
	
	while counter < longitudrows and arrelgos[counter][0] != 'Station MAC':	
		aps.append(arrelgos[counter])
		counter+=1

	longituaps = len(aps)
	print 'tenemos %r puntos de acceso' % longituaps
	miniflag=0
	for laps in aps:
		#print len(laps)
		if laps[0] != 'BSSID' and len(laps)>=13:
			mac= laps[0]
			pwr=laps[8]
			fts=laps[1]
			lts=laps[2]
			try:
				bssid=laps[13].replace("'","")
			except:
				bssid= '(NOT SET)'
			#essid=re.sub('\W+',' ',bssid)
			essid=bssid.strip()
			essid=essid.decode('latin-1').encode("utf-8")
			print 'estas son las variables mac %r pwr %r fts %r lts %r bssid %r' % (mac,pwr,fts,lts,essid)

			#revisamos primero que no exista

			
			cursor.execute("""SELECT * from aps where mac = %s and estacion = %s """,(mac,nameestacion))
			rows=cursor.fetchall()
			#print len(rows)
			#se hace el insert
			if len(rows) == 0:
				cursor.execute(""" INSERT into aps (mac,fts,lts,pwr,name,estacion) values(%s,%s,%s,%s,%s,%s)""",(mac,fts,lts,pwr,essid,nameestacion))
				dbms.commit()
				print colored('hacemos insert','green')
			else:
				#se hace update
				cursor.execute(""" UPDATE aps set lts = %s, pwr = %s where mac = %s and estacion = %s """,(lts,pwr,mac,nameestacion))
				dbms.commit()
				print colored('hacemos update','yellow')
	#hacemos update del pros
	reported_unix=  int(time.time())
	lastpros = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(reported_unix))
	print lastpros

	up = cursor.execute(""" UPDATE prosfiles set pros = 2 , lastpros = %s  where name_file = %s """,(lastpros,archivocsv[1]))
	
	dbms.commit()
	print colored('archivo processed','green')

			


