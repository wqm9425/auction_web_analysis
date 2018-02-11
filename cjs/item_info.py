# -*- coding: utf-8 -*-
from selenium import webdriver
from bs4 import BeautifulSoup
import time
import sqlite3
from sqlite3 import Error
from selenium.webdriver.common.action_chains import ActionChains

def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
 
    return None

def define_tasks_table(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS pagecount (url STRING, title STRING, status STRING, price STRING, count INT)")

def update_all(conn,title,status,price):
	cur = conn.cursor()
	cur.execute("INSERT INTO pagecount(title, status, price) VALUES (?, ?, ?)",(title, status, price))

#get total page nunmber
def get_page_num(html):
	soup=BeautifulSoup(html,'lxml')
	page_num=soup.select('#kkpager > div > span.infoTextAndGoPageBtnWrap > span.totalText > span.totalPageNum')
	return int(page_num[0].get_text())

def get_page(html):
	soup=BeautifulSoup(html,'lxml')

	title=soup.select('#underly > div > div > div > a > div > span')
	price=soup.select('#underly > div > div > div > p > span')[::3]
	status=soup.select('#underly > div > div > div > span > div')

	data_json=[]

	for t,s,p in zip(title,status,price):
		data = {
			'title':t.get_text(),
			'status':s.get_text(),
			'price':p.get_text()
		}
		data_json.append(data)
	return data_json


if __name__ == '__main__':
	database = "./page.db"
	conn = create_connection(database)
	browser = webdriver.Chrome()
	browser.get('http://10.100.122.231/project/subjectList.do?category=10')
	html = browser.page_source
	page_num = get_page_num(html)  # page_num定义需要爬取得页数
	
	with conn:
		define_tasks_table(conn)
		for i in range(page_num):  
			page_info = get_page(html)
			print(page_info)
			for item in page_info:
			 	update_all(conn,item['title'],item['status'],item['price'])

			element = browser.find_element_by_xpath("//*[@id='kkpager']/div[1]/span[1]/a[7]")
			actions = ActionChains(browser)
			actions.move_to_element(element).click().perform()
			#browser.find_element_by_link_text('下一页').click()	
			time.sleep(2)
			html = browser.page_source


