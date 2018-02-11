# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup

import time
import json
import redis
import MySQLdb

pool = redis.ConnectionPool(host='localhost', port=6379)
r = redis.Redis(connection_pool = pool)

import sqlite3
from sqlite3 import Error
 
 
def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
 
    return None

def define_tasks_table(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS pagecount (url STRING PRIMARY KEY, title STRING, status INT, price REAL, Count INT)")

# scrap wab pages based on url in the csv file
def get_page(url,count):
	content = requests.get(url)
	content.encoding='utf-8'
	soup = BeautifulSoup(content.text,'lxml')
	titles = soup.select('#auction_content > div.house_page > div > div.house_page_main_left > h2 > a')
	status = soup.select('#bmStatusTd > input')
	price = soup.select('#_biddingDataInput')

	for t,s,p in zip(titles,status,price):
		data = {
			'url': url,
			'title': t.get_text(),
			'status': s.get('value'),
			'price': p.get('value'),
			'count': count
		}
		return data

database = "./page.db" 
# create a database connection
conn = create_connection(database)
with conn:
	define_tasks_table(conn)

	result = []
	with open('output.json', 'w', encoding = 'utf8') as fp:
		for k in r.keys():
			url = "http://www.chanjs.com"+str(k)[2:]
			url = url[:-1]
			v=int(r.get(k))	# v = count
			element = get_page(url, v)
			if element is not None: 
				result.append(element)
				print(element)
			time.sleep(2)
		json.dump(result,fp,ensure_ascii=False)
	fp.close()

