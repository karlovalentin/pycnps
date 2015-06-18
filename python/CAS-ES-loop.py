#!/usr/bin/python
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
from cassandra.query import dict_factory
from random import randint

#conexion con cassandra

ap = PlainTextAuthProvider(username='sparkia', password='Mofos2014')

c = Cluster(protocol_version=2, contact_points=['10.240.207.160','10.240.38.30','10.240.19.27'], auth_provider=ap, load_balancing_policy= TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc='us-central1')),default_retry_policy = RetryPolicy())
s = c.connect()
s.default_timeout = 120
s.set_keyspace('cinepolis')
#s.row_factory = dict_factory

##limpiar variables



while True:


	reported_unix2=  int(time.time())
	reported2 = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(reported_unix2))

	#seleccionamos el archivo a procesar
	archivos=s.execute(""" select name_file from prosfiles where pros = 0 limit 30""")




	if len(archivos)>0:
		maxl= len(archivos)-1
		#print 'Estamos procesando el archivo %s' % archivos[randint(0,maxl)][0]

		prosfile=archivos[randint(0,maxl)][0]
		#prosfile='1431903671-spk1-4578.csv'
		
		namegateway=prosfile.split('-')
		nameestacion=namegateway[1]
		nameestacion=nameestacion.strip()

		print  'estamos volcando el archivo: %r de la estacion %r' % (prosfile,namegateway[1])

		#hacemos el update
		updata=s.execute(""" update prosfiles set pros = 1010, beginpros='%s' where name_file = '%s' """ % (reported2,prosfile))
		del reported_unix2,reported2,updata

		


		

		data_initial = open('/lamp/csv2/'+prosfile, "rU")
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
		del aps


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
				print ftses,ltses

				try:
					ftunix=int(time.mktime(time.strptime(ftses, pattern)))
					
				except:
					ftunix='1010'
					

				try:
					ltunix=int(time.mktime(time.strptime(ltses, pattern)))
				except:
					ltunix='1010'
				

				print 'mac %r fts %r lts %r pwr %r bssid %r reported %r reported_unix %r rssi %r ftunix %r ltunix %r device %r file %r'  % (maces,ftses,ltses,pwres,bssides,reported,reported_unix,rssi,ftunix,ltunix,nameestacion,prosfile)

				#hacemos insert de la estacion
				
				#print 'volcando'
				if ftunix!='1010' and ltunix != '1010':

					try:
						insertaes=s.execute("""INSERT INTO estaciones (mac, ftsunix , ltsunix , rssi , estacion , asociado  , fts , lts , pwr , reported, reportedunix) 
							VALUES ( '%s',%s,%s,%s,'%s','%s','%s','%s',%s,'%s',%s)""" % (maces,ftunix,ltunix,rssi,nameestacion,bssides,ftses,ltses,pwres,reported,reported_unix))
					except:
						e = sys.exc_info()[0]
						print colored('hubo un error: %r' % e,'red')
				else:
					print colored('EEEEEError fts 1010','magenta')
					#time.sleep(5)
					#break

				del maces,ftunix,ltunix,rssi,bssides,ftses,ltses,pwres,reported,reported_unix
					

				
				#except (KeyboardInterrupt, SystemExit):
					#raise
				
					#del insertaes,maces,ftunix,ltunix,rssi,bssides,ftses,ltses,pwres,reported,reported_unix
		

		#hacemos el update del status
		reported_unix3=  int(time.time())
		reported3 = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(reported_unix3))
		updata2=s.execute(""" update prosfiles set pros = 1, lastpros='%s' where name_file = '%s' """ % (reported3,prosfile))
		del reported3,reported_unix3,updata2,prosfile,estaciones,nameestacion
		print colored('PROCESADO','green')




	else:
		print colored('ya no hay archivos que procesar','red')
		time.sleep(30)
		