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

#funcion para validar que sea numero
def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False

killer=1

while killer ==1:
	

	#creamoslistadesdemysql
	cursor=dbms.cursor()
	cursor.execute("""SELECT * FROM prosfiles where pros = 0 and uploaded > 1431399982 order by rand() limit 1""")
	lif=cursor.fetchall()

	killer=len(lif)



	for archivocsv in lif:
		#print archivocsv
		prosfile='/var/www/csv2/'+archivocsv[1]
		print  'estamos volcando el archivo: %r' % prosfile

		#hacemos update de ese archivo para evitar condicion carrera
		cursor.execute(""" UPDATE prosfiles set pros = 1010 where name_file = %s """,(archivocsv[1]))
		dbms.commit()






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

		#creamos el array de las estaciones
		counter=longituaps
		estaciones = []

		while counter < longitudrows:
			estaciones.append(arrelgos[counter])
			counter+=1
			#print estaciones[counter]
		longitudestaciones = len(estaciones)
		print 'tenemos %r estaciones' % longitudestaciones

		#hacemos el volcado.
		for les in estaciones:
			if les[0]!='Station MAC' and len(les)>=7:
				forprobes = len(les)
				#print forprobes
				probes=[]
				maces=les[0]
				ftses=les[1]
				ftses=ftses.strip()
				ltses=les[2]
				ltses=ltses.strip()
				pwres=les[3]
				bssides=les[5]

				reported_unix=  int(time.time())
				reported = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(reported_unix))

				try:
					rssi=int(pwres)+95
				except:
					rssi=94

				pattern='%Y-%m-%d %H:%M:%S'
				print ftses

				try:
					ftunix=int(time.mktime(time.strptime(ftses, pattern)))
					ltunix=int(time.mktime(time.strptime(ltses, pattern)))
				except:
					ftunix='1010'
					ltsunix='1010'

				print 'mac %r fts %r lts %r pwr %r bssid %r reported %r reported_unix %r rssi %r ftunix %r ltunix %r device %r'  % (maces,ftses,ltses,pwres,bssides,reported,reported_unix,rssi,ftunix,ltunix,nameestacion)

				#checamos la existencia primero
				cursor.execute(""" SELECT id from estaciones where mac = %s and ftsunix = %s and ltsunix = %s and estacion = %s""",(maces,ftunix,ltunix,nameestacion))
				rows=cursor.fetchall()

				if len(rows)==0 and ftunix!='1010':
					cursor.execute(""" INSERT into estaciones (mac,fts,lts,ftsunix,ltsunix,pwr,rssi,asociado,reported,reportedunix,estacion) 
						VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(maces,ftses,ltses,ftunix,ltunix,pwres,rssi,bssides,reported,reported_unix,nameestacion))
					dbms.commit()

					print colored('hacemos insert','green')
				else:
					print colored('No hay cambio','blue')

		#hacemos update del pros
		reported_unix=  int(time.time())
		lastpros = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(reported_unix))

		cursor.execute(""" UPDATE prosfiles set pros = 1, lastpros = %s where name_file = %s """,(lastpros,archivocsv[1]))
		dbms.commit()
		print colored('archivo processed','magenta')
		
	else:
		print colored('ya no hay archivos que procesar','red')

else:
	print colored('ya no hay archivos que procesar','red')




			#print 'mac %r fts %r lts %r pwr %r bssid %r reported %r reported_unix %r rssi %r ftunix %r ltunix %r'  % (maces,ftses,ltses,pwres,bssides,reported,reported_unix,rssi,ftunix,ltunix)
			#localtime = time.localtime(time.time())
			#print "Local current time :", localtime
			#print iso_time