# coding: utf-8
# 接收消息队列中的链接，读取并保存到ElasticSearch

import re
import os
import sys
import pika
import time
import json
import codecs
import ConfigParser
from selenium import webdriver
from pyquery import PyQuery as pq
from elasticsearch import Elasticsearch
import xmltodict
from pyvirtualdisplay import Display

from captchaservice import captchaservice

import cv2
import cv2.cv as cv

import json
import demjson
import utility

from pysolr4 import *
import settings

reload(sys)
sys.setdefaultencoding('utf-8')

cf = ConfigParser.ConfigParser()
cf.readfp(codecs.open('settings.ini', 'r', 'utf-8-sig'))

MQ_SERVER = cf.get('rabbitmq', 'host')
#REDIS_SERVER = cf.get('redis', 'host')
DB_SERVER = cf.get('mysql', 'host')
DB_USERNAME = cf.get('mysql', 'user')
DB_PASSWORD = cf.get('mysql', 'password')
DB_DATABASE = cf.get('mysql', 'database')
ES_SERVER = cf.get('elasticsearch', 'host')
ES_INDEX = cf.get('elasticsearch', 'index')
ES_TYPE = cf.get('elasticsearch', 'type')

def filter_person(d, titles):
    results = []
    for title in titles:
        try:
            for item in d.find('li').filter(lambda i: title == pq(this).text().split()[1]):
                each = pq(item)
                results.append(each.text().split()[0])
        except:
            continue
    return results


def filter_lawyers(d):
    lawyers = []
    for item in d.find('li').filter(lambda i: u'律师' in pq(this).text() or u'委托代理人' in pq(this).text()):
        data = pq(item)
        splited_data = data.text().split()
        data_dict = {}
        if len(splited_data) == 3:
            data_dict['name'] = splited_data[0]
            data_dict['law_office'] = splited_data[-1]
            data_dict['law_office_search'] = data_dict['law_office']
            lawyers.append(data_dict)
    return lawyers


def trim_colon(text):
    if not text:
        text = ''
    elif u'：' in text:
        text = text.split(u'：')[1]
    return text.strip()

class Worker():
    def __init__(self):
        try:
            # RabbitMQ server
            self.mq_connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=MQ_SERVER))
            self.channel = self.mq_connection.channel()
            self.channel.queue_declare(queue='doc_queue', durable=True)  # 声明使用的队列，并设置为持久化
            print(' [*] Waiting for messages. To exit press CTRL+C')

            self.es = Elasticsearch(ES_SERVER)
        except Exception, e:
            print e
            sys.exit(0)

        #self.driver = webdriver.PhantomJS()   #selinium调用 浏览器后台运行
        display = Display(visible=0, size=(800, 600))
        display.start()
	self.driver = webdriver.Firefox()

        print 'driver done'

    #从队列读取数据
    def run(self):
        self.channel.basic_consume(self.callback, queue='doc_queue')
        self.channel.start_consuming()

    #获取文书详细信息
    def get_detail_page(self, link):  
	print 1
        doc_id = link.split('/')[-1]  #http://openlaw.cn/judgement/ea86414b0cac4075a3b88fcd9f8d4139
	print 'doc_id',doc_id
        #通过elasticsearch 判断该文书是否已经被处理过
        cache_flag = self.es.exists(ES_INDEX, ES_TYPE, doc_id)
        if cache_flag:   #已处理的文书
            print ' [!] {} in ES'.format(doc_id)
            return
        self.driver.get(link)
        page = self.driver.find_element_by_xpath('//*').get_attribute('outerHTML')  #源代码
        self.parse_detail_page(page, link)


    #处理文书
    def parse_detail_page(self, page, link):
	print 3
        doc_id = link.split('/')[-1]   #http://openlaw.cn/judgement/ea86414b0cac4075a3b88fcd9f8d4139
        d = pq(page)   #判断是否出现验证码
        try:
            if u'请输入验证码' in d.text():
            	print u'等待输入验证码 > '
	    	imgfoler = '/img/'
		basic = 'img'
		webimage = imgfoler + utility.get_unique_name()
		uniquename = basic + webimage
		self.driver.save_screenshot(uniquename + '_s.png')  #截屏 _s.png
		captcha_image = self.driver.find_element_by_xpath('//img[@id="kaptcha"]')
		loc = captcha_image.location
		loc['x']=int(loc['x'])
		loc['y']=int(loc['y'])
		image = cv.LoadImage(uniquename + '_s.png', True)
		out = cv.CreateImage((200,50), image.depth, 3)
		cv.SetImageROI(image,(loc['x'],loc['y'], 200, 50))
		cv.Resize(image,out)


		imgname =  uniquename + '.jpg'

		cv.SaveImage(imgname, out)

		# 使用外部服务解码
		result = captchaservice.getCaptcha(imgname)
		dictresult = json.loads(result)
		if dictresult.has_key('Error') :
		    resultno = 1
		    raise Exception('service does not work well !')
		#endif

		code = dictresult['Result']
		inputkey = self.driver.find_element_by_xpath('//input[@class="search-field"]')
		inputkey.clear()
		inputkey.send_keys(code)
		time.sleep(2)
		searchbtn = self.driver.find_element_by_xpath('//input[@type="submit"]') 
		searchbtn.click()
		time.sleep(10)
        except:
            pass
        data = {}
        title = d('h2.entry-title').text()
        if '404' in d('title').text():
            print ' [!] ERROR page, 404 not found, %s' % link
            return
        if not title:
            print ' [!] Empty page, resend %s' % link  #如果页面为空，则将链接再发送到队列
            self.channel.basic_publish(exchange='',
                          routing_key='doc_queue',
                          body=link,
                          properties=pika.BasicProperties(
                              delivery_mode=2,  # 使消息持久化
                          ))
            time.sleep(.5)
            return
        #print title
	print 4
        #提取结构化信息 侧边栏(sidebar)
        reason = trim_colon(d('aside#sidebar section').eq(0).find('li').filter(   
            lambda i: u'案由' in pq(this).text()).text())
        court = trim_colon(d('aside#sidebar section').eq(0).find('li').filter(
            lambda i: u'法院' in pq(this).text()).text())
        doc_type = trim_colon(d('aside#sidebar section').eq(0).find('li').filter(
            lambda i: u'类型' in pq(this).text()).text())
        status = trim_colon(d('aside#sidebar section').eq(0).find('li').filter(
            lambda i: u'程序' in pq(this).text()).text())
        date = trim_colon(d('li.ht-kb-em-date').text()).strip() #strip() 去前后空格
        regx = re.match(r'\d{4}-\d{2}-\d{2}', date)
        if not regx:
            date = '1970-01-01'
        case_id = trim_colon(d('li.ht-kb-em-category').text())
        content = d('div#entry-cont').text().strip(u' 允许所有人 查看 该批注 \
            允许所有人 编辑 该批注 取消 保存 Annotate')
        # 人物侧边栏
        persons = d('aside#sidebar section').eq(1)
        # 原告
        accuser = filter_person(persons, [u'原告', u'审请人', u'上诉人', u'再审申请人'])
        # 被告
        accused = filter_person(persons, [u'被告', u'被审请人', u'被上诉人'])
        # 审判长
        chief_judge = filter_person(persons, [u'审判长'])
        # 律师
        lawyers = filter_lawyers(persons)
        data['title'] = title
        data['title_search'] = title
        data['reason'] = reason
        data['court'] = court
        data['date'] = date
        data['doc_type'] = doc_type
        data['status'] = status
        data['content'] = content
        data['case_id'] = case_id
        data['lawyers'] = lawyers
        data['accuser'] = accuser
        data['accused'] = accused
        data['chief_judge'] = chief_judge
        data['url'] = link
        #导入elasticsearch
        #self.es.index(index=ES_INDEX, doc_type=ES_TYPE, id=doc_id, body=data)
	#print 'data',data
	#convertedDict = xmltodict.parse(data);
	realpath = link 

	extraction = {}
	extraction['realpath'] = realpath
	extraction['data'] = data

	data1 = {}
	data1['extraction'] = extraction
	convertedXml = xmltodict.unparse(data1);
    	# print "convertedXml=",convertedXml;
        try:
            folder = './result/'
            filename = folder +data['case_id'] +'.xml'
    	    f = open(filename,'w')
            f.write(convertedXml)
	    f.close()
        except:
            print 'error...'
	
    def callback(self, ch, method, properties, body):
        """"""
        print(" [x] Received %r" % body)  #body -队列里的链接-http://openlaw.cn/judgement/ea86414b0cac4075a3b88fcd9f8d4139
        if 'judgement' not in body:  #不合法的链接
            print ' [!] Link Error %s' % body
            ch.basic_ack(delivery_tag=method.delivery_tag)  #从队列删除
            return
        self.get_detail_page(body)
        print(" [x] Done")
        #consumer在收到message后会向RabbitMQ反馈已收到并处理了message告诉RabbitMQ可以删除该message 
        ch.basic_ack(delivery_tag=method.delivery_tag)


#def main():
#    try:
#        worker = Worker()
#        worker.run()
#    except Exception, e:
#        print e
#if __name__ == '__main__':
#    main()

