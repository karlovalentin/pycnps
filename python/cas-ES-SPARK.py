#!/usr/bin/python
import MySQLdb
import random
import time
from termcolor import colored
import csv
import sys
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.policies import TokenAwarePolicy
from cassandra.policies import RetryPolicy
import glob




#conexion con cassandra

#ap = PlainTextAuthProvider(username='sparkia', password='Mofos2014')

#c = Cluster(protocol_version=2, contact_points=['10.240.207.160','10.240.38.30','10.240.19.27'], auth_provider=ap, load_balancing_policy= TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc='us-central1')),default_retry_policy = RetryPolicy())
c = Cluster(contact_points=['10.240.176.173','10.240.166.206','10.240.248.131'])
s = c.connect()
s.set_keyspace('cinepolis')

#conexion con mysql
#dbms = MySQLdb.connect("173.194.251.199","receptor","mofos2014","cinepolis" )
#cursor=dbms.cursor()

#leemos los csv directo del folder
listaarchivos= glob.glob("/lamp/csvape/*.csv")

for archivocsv in listaarchivos:
	prosfile=archivocsv
	purename=prosfile.split('/')
	namegateway=purename[3].split('-')
	nameestacion=namegateway[1]
	nameestacion=nameestacion.strip()
	print  'estamos volcando el archivo: %r de la estacion %r' % (purename[3],namegateway[1])

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
			maces=maces.strip()
			ftses=les[1]
			ftses=ftses.strip()
			ltses=les[2]
			ltses=ltses.strip()
			pwres=les[3]
			bssides=les[5]
			bssides=bssides.strip()

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
				ltunix='1010'

			print 'mac %r fts %r lts %r pwr %r bssid %r reported %r reported_unix %r rssi %r ftunix %r ltunix %r device %r'  % (maces,ftses,ltses,pwres,bssides,reported,reported_unix,rssi,ftunix,ltunix,nameestacion)

			#hacemos insert de la estacion
			try:
				insertaes=s.execute("""INSERT INTO estaciones (mac, ftsunix , ltsunix , rssi , estacion , asociado , fts , lts , pwr , reported, reportedunix ) 
					VALUES ( '%s',%s,%s,%s, '%s','%s','%s','%s',%s,'%s',%s)""" % (maces,ftunix,ltunix,rssi,nameestacion,bssides,ftses,ltses,pwres,reported,reported_unix))
			
			except (KeyboardInterrupt, SystemExit):
				raise
			except:
				e = sys.exc_info()[0]
				print 'hubo un error: %r' % e








