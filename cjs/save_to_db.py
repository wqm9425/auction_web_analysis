# -*- coding: utf-8 -*-
import requests
import http.client
from bs4 import BeautifulSoup
import re 

import json
import redis
import sqlite3
from sqlite3 import Error

pool = redis.ConnectionPool(host='localhost', port=6379)
r = redis.Redis(connection_pool = pool) 
 
def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
 
    return None

def define_table(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS pagecount (url STRING, title STRING, status STRING, price STRING, count INT)")

def query_url(conn): 
	output = []
	cur = conn.cursor()
	cur.execute("SELECT url FROM pagecount WHERE status != '正在进行'")
	rows = cur.fetchall()
	for row in rows:
		row = str(row)[2:][:-3]
		output.append(row)
	return output

def update_all(conn, url, count, title, price):
    cur = conn.cursor()
    price = "当前价："+str(price)
    cur.execute("UPDATE pagecount SET count = ?, url = ? , price = ? WHERE title = ?",(count, url, price, title))

def update_count_only(conn, url, count):
	cur = conn.cursor()
	cur.execute("UPDATE pagecount SET count = ? WHERE url = ?",(count, url))

def get_page(url):
	content = requests.get(url)
	content.encoding='utf-8'
	soup = BeautifulSoup(content.text,'lxml')

	titles = soup.select('#auction_content > div.house_page > div > div.house_page_main_left > h2 > a')

	script = soup.findAll('script')[6]
	pattern = re.compile("(\d+)")
	subject_id = re.findall(pattern, script.text)[0]

	for t in titles:
		data = {
			'url': url,
			'title': t.get_text(),
			'subject_id': subject_id,
		}
		return data

def get_updated_data(subject_id):
	conn = http.client.HTTPConnection("www.chanjs.com")

	payload = "[%s]" % subject_id

	headers = {
	    'content-type': "application/json",
	    'cache-control': "no-cache",
	    'postman-token': "37d739db-bcfe-2e91-aca6-5dd97a5c080f"
	    }

	conn.request("POST", "/project/indexSubject.do", payload, headers)

	res = conn.getresponse()
	data = res.read()

	return data.decode("utf-8")

def save_to_json(conn,fp):
	conn.row_factory=sqlite3.Row
	cur = conn.cursor()
	rows = cur.execute("SELECT * FROM pagecount").fetchall()
	return json.dump([dict(row) for row in rows],fp, ensure_ascii=False)

if __name__ == '__main__':
	database = "./page.db" 
	# create a database connection
	conn = create_connection(database)
	with conn:
		with open('output.json', 'w', encoding = 'utf8') as fp:
			define_table(conn)
			for k in r.keys():
				url = "http://10.100.122.231"+str(k)[2:][:-1]
				v = int(r.get(k))	# v = count
				url_list = query_url(conn)	

				if url not in url_list:		#包含没有记录的url和正在竞价的url
					element = get_page(url) #(url,title,subject_id)
					if element is not None: 
						new_data = get_updated_data(element['subject_id'])	#更新价格
						pattern = re.compile("bidData\":\"(\d+)")
						if re.findall(pattern, new_data) != []:
							price = re.findall(pattern, new_data)[0]	
						else:
							price = 0		#没有biddata设为0
						update_all(conn, element['url'], v, element['title'], int(price))
				else:
					update_count_only(conn, url , v)

			save_to_json(conn,fp)
