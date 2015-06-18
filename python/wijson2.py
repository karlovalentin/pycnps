#!/usr/bin/python

import re
import requests
#import simplekml
#from bs4 import BeautifulSoup
from netaddr import EUI
import MySQLdb
import random
import time
from termcolor import colored
import json

dbms = MySQLdb.connect("173.194.226.212","mofos","mofos","geomap" )

ciclo=0

while True:
	flag=0
	matador=0
	lat1=str(random.uniform(19.6899985,19.7307226))
	lat2=str(random.uniform(19.6899985,19.7307226))
	lon1=str(random.uniform(-101.2095571,-101.2337614))
	lon2=str(random.uniform(-101.20955717,-101.2337614))
	print lat1,lat2,lon1,lon2
	last=0
	start=0 
	conocida=0
	#cookieA = dict(auth='auth=kavastudio:179052610:1426006693:iwqEwBE9Ck9kxK7dxSXpWg')
	#cookieB = dict(auth='karlovalen:465767407%3A1426015873:MiM9kFREG%2BpUrB/pWhq5Ng	')
	#cookie = dict(auth='mofojoy:481156531:1426014786:W+wpspSdEt/sZgMawEXMYg')
	#sc=random.randrange(1,3)
	
	#print sc
	#if sc==1:
	#	cookie=cookieA
	#else:
	#	cookie=cookieB
	#cookie1 = dict(auth='kavastudio:516044554:1432789974:b6v1LwSdxQDiTA0a+oJi/w')
	cookie1 = dict(auth='kavastudio:730342981:1426112081:cT8iELgfo/L0QirzSC5MhQ')
	cookie2 = dict(auth='mofojoy:468052184:1426134449:y4cDTLX5RFiCWFgieERpZg')
	sc=random.randrange(1,3)
	if sc==1:
		cookie=cookie1
	else:
		cookie=cookie1
	
	print cookie


	
	
	while matador != 10000:


		#headers={'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8','User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36','Accept-Encoding':'gzip, deflate, sdch','Accept-Language':'es-419,es;q=0.8'}
		headers={'Accept':'application/json, text/javascript, */*; q=0.01','User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36','Accept-Encoding':'gzip, deflate, sdch','Accept-Language':'es-419,es;q=0.8','Referer':'https://wigle.net/search','X-Requested-With': 'XMLHttpRequest','X-FirePHP-Version':' 0.0.6','Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8','Origin':'https://wigle.net','X-DevTools-Emulate-Network-Conditions-Client-Id':'D026D731-4DBC-475C-8173-9FEB6ABB4761'}
		payload={
		'Query':'Query',
		'addresscode':'',
		'lastupdt':'',
		'latrange1':lat1,
		'latrange2':lat2,
		'longrange1':lon1,
		'longrange2':lon2,
		'netid':'',
		'ssid':'',
		'statecode':'',
		'variance':0.010,
		'zipcode':''

		}

		if flag==0:
			urlarranque='https://wigle.net/api/v1/jsonSearch'
			r=requests.post(urlarranque,cookies=cookie,headers=headers,data=payload)
		else:
			start = flag*100
			last = start + 100
			print start
			print last
			urlarranque='https://wigle.net/api/v1/jsonSearch?latrange1='+lat1+'&latrange2='+lat2+'&longrange1='+lon1+'&longrange2='+lon2+'&variance=0.010&netid=&ssid=&lastupdt=&addresscode=&statecode=&zipcode=&Query=Query&first='+str(start)+'&last='+str(last)+''
			r=requests.get(urlarranque,cookies=cookie,headers=headers)




		
		#print r
		print r.text
		
		jsonx = json.loads(r.text)
		print len(jsonx)
		
		#matador=10000

		if jsonx['resultCount']==0:
			matador=10000

		for red in jsonx['results']:
			mac= red['netid']
			ssid= red['ssid']
			if ssid == None:
				ssid = 'NOT SET'

			lat= red['trilat']
			lon= red['trilong'] 
			try:
				macx=EUI(mac)
				manufe = macx.oui.records[0]['org']
			except:
				manufe = 'NOT SET'

			print mac,ssid,lat,lon,manufe

			cursor=dbms.cursor()
			try:
				cursor.execute(""" 
					SELECT * from morelia where mac = %s""",mac)
				rows=cursor.fetchall()
				#print 'se hace select'
			except:
				print colored('el select fallo','red')

			if len(rows) == 0:
				try:
					cursor.execute(""" 
						INSERT into morelia (mac, manuf, ssid,lat,lon)
						VALUES (%s,%s,%s,%s,%s)""",(mac,manufe,ssid,lat,lon))
					dbms.commit()
					print colored('hacemos insert','green')
				except:
					print colored('hubo error de caracters en insert','red')
			else:
				print colored('ya la conociamos','yellow')
				conocida+=1


		if last >= 5100:
			matador=10000

		if conocida>=1000:
			matador=10000

		flag+=1
		print 'Ya acabamos round %r' % flag
		time.sleep(2)

	ciclo+=1
	print 'vamos en el ciclo %r' % ciclo
	print lat1,lat2,lon1,lon2
	time.sleep(2)
	if ciclo>500:
		break


