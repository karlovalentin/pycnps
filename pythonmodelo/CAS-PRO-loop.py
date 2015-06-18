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
s.set_keyspace('modelo')
#s.row_factory = dict_factory



#creamos lista
archivos=s.execute(""" select name_file from prosfiles where pros = 2 """)


for namefile in archivos:
	reported_unix2=  int(time.time())
	reported2 = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(reported_unix2))

	prosfile=namefile[0]
		
	namegateway=prosfile.split('-')
	nameestacion=namegateway[1]
	nameestacion=nameestacion.strip()
	print  'estamos volcando el archivo: %s de la estacion %s' % (prosfile,namegateway[1])

	#hacemos el update
	updata=s.execute(""" update prosfiles set pros = 3030, beginpros='%s' where name_file = '%s' """ % (reported2,prosfile))

	data_initial = open('/lamp/csvmodelo/'+prosfile, "rU")
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

	for les in estaciones:
		if les[0]!='Station MAC' and len(les)>=7:
			forprobes = len(les)
			probes=[]
			maces=les[0]
			#obtener las probes de cada estacion
			if les[6]!='':
				inicio=6
				while inicio < forprobes:
					probes.append(les[inicio])
					inicio+=1
				
				print 'mac %r mis probes %r' % (maces,probes)
				#print len(probes)
				for lopo in probes:
					caspo=lopo.strip()
					caspo=caspo.decode('latin-1').encode("utf-8")
					#hacemos el insert del probe
					if caspo == '':
						caspo='(not associated)'
						
					try:
						insprobe=s.execute(""" 
							INSERT into probes (mac,probe,file,reported) VALUES ('%s','%s','%s','%s') """ % (maces,caspo,prosfile,reported2) )
					except:
						eprobe = sys.exc_info()[0]
						print 'hubo un error en el probe: %r' % eprobe


	else:
		print colored('archivo processed','green')
		#hacemos el update del status
		reported_unix3=  int(time.time())
		reported3 = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(reported_unix3))
		updata2=s.execute(""" update prosfiles set pros = 3, lastpros='%s' where name_file = '%s' """ % (reported3,prosfile))
		#print 'aqui hubieramos hecho el update de pros'

else:
	print colored('sin archivos que procesar','blue')