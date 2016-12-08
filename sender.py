# coding: utf-8
# 抓取法院页面上的文书链接，并发送到RabbitMQ消息队列中
# os 输入输出，pika 连接到rabbitmq服务器，time 时间间隔，json 转换Python对象为JSON序列，sys 系统
# random 随机序列，codecs 处理编码 Byte Order Mark标签，urlparse 分解、拼接url，ConfigParser 读写操作
# MySQLdb 数据库，selenium 自动化测试Javascript跳转，pyquery 解析 HTML 文档，elasticsearch 连接elasticsearch

import pika
import time
import sys
import codecs
import urlparse
import ConfigParser
import MySQLdb as mdb
from selenium import webdriver
from pyquery import PyQuery as pq
from elasticsearch import Elasticsearch
import change
from pyvirtualdisplay import Display
from captchaservice import captchaservice
import os
import signal

import cv2.cv as cv

import json
import utility
# 读setting.ini的配置
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

# Sender类，抓取法院页面上的文书链接，并发送到RabbitMQ消息队列中
class Sender():
    def __init__(self, incr_flag=False):
        try:
            # RabbitMQ server，pika连接到rabbitmq服务器
            self.mq_connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=MQ_SERVER))
            self.channel = self.mq_connection.channel()
            # 声明使用的队列，并设置为持久化
            self.channel.queue_declare(queue='doc_queue', durable=True)

            # ES server
            self.es = Elasticsearch(ES_SERVER)

            # MySQL server
            self.mysql_conn = mdb.connect(DB_SERVER, DB_USERNAME,
                                          DB_PASSWORD, DB_DATABASE)
            self.cur = self.mysql_conn.cursor()
        except Exception, e:
            print e
            sys.exit(0)

        #self.driver = webdriver.PhantomJS()
        display = Display(visible=0, size=(800, 600))
        display.start()
        self.driver = webdriver.Firefox()
        self.incr_flag = incr_flag

    def get_main_page(self, link):
        """获取法院下的文书链接和next_link"""
        if '?' in link:   #http://openlaw.cn/court/5832a3260f8341b88e73d19fa1929194?page=XX
            court_id = link.split('?')[0].split('/')[-1]
            page_num = int(link.split('?')[1].split('=')[-1])
        else:   #http://openlaw.cn/court/5832a3260f8341b88e73d19fa1929194
            court_id = link.split('/')[-1]
            page_num = 1

        # 更新断点页面数
        # 如果是增量抓取则不更新
        if not self.incr_flag:
            self.cur.execute('''UPDATE breakpoint SET page_num=%s
                             WHERE court_id=%s''', (page_num, court_id,))
            self.mysql_conn.commit()

        self.driver.get(link)
        #time.sleep(0.8)
        page = self.driver.find_element_by_xpath(   
            '//*').get_attribute('outerHTML')    #源代码
	
        exist_num = self.parse_main_page(page, link)   #获取文书链接，传到队列
        if self.incr_flag and (exist_num > 10):  #爬取的文书重复10个，则认为更新结束
            next_link = None
        else:
            next_link = self.get_next_link(page, link)   #获取next_link
        if next_link == 'err':   #验证码
            return link
        return next_link

    def get_next_link(self, page, link):
        """获取下一页链接"""
        d = pq(page)
        next_link = d('a.next.page-numbers').attr('href')
        if not next_link:
            # 如果不存在next_link，可能是最后一页
            # 也可能是网站问题，默认重试获取这个链接3次

            if u'请输入验证码' in d.text():
                print u'等待输入验证码 >'
                cpt_tip = self.driver.find_element_by_xpath('//img[@id="kaptcha"]')
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
                print '5'
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
                time.sleep(10)
                searchbtn = self.driver.find_element_by_xpath('//input[@type="submit"]')
                searchbtn.click()
                time.sleep(1)
                #return 'err'
            # 如果没有next_link，则判断3次，如果有则继续，没有就说明是最后一页
            for i in range(3):
                self.driver.get(link)
                # time.sleep(5)
                page = self.driver.find_element_by_xpath(
                    '//*').get_attribute('outerHTML')
                d = pq(page)
                next_link = d('a.next.page-numbers').attr('href')
                if next_link:
                    break
        # 再重新判断
        if next_link:
            next_link = urlparse.urljoin(link, next_link)
        return next_link

    def parse_main_page(self, page, link):
        """解析法院页面，找到结构化数据"""
        base_url = 'http://openlaw.cn'
        d = pq(page)
        articles = d('article')  #文书链接
        # if not articles:
        if not articles:   # 如果页面为空
#           with open('./errpage/' + hashlib.md5(link).hexdigest +
#                     '.html', 'wb') as F:
#              F.write(page)
            return self.get_main_page(link)
        exist_num = 0
        for each_article in articles:
            _link = pq(each_article)('h3 a').attr('href')   #/judgement/0e65d18135624599bb9365ad9881b115
            doc_link = urlparse.urljoin(base_url, _link)
            flag = self.publish_link(doc_link)  #去重 判断是否需要将文书链接发送到队列
            if flag:   #将文书链接发送到队列
                pass
                # print ' [x] {} sended!'.format(doc_link)
            else:
                exist_num += 1  #ES已处理的文书数加1
        return exist_num

     # 将文书链接加入消息队列
    def publish_link(self, link):
        #  # 判断文书链接是否在elasticsearch里
        doc_id = link.split('/')[-1]
        cache_flag = self.es.exists(ES_INDEX, ES_TYPE, doc_id)
        if cache_flag:   # 已处理的文书
            print ' [!] {} in cache'.format(doc_id)
            return  #不返回值，则flag=0
        if 'judgement' not in link:   #不合法的链接
            return
        #将文书链接发送到队列
        self.channel.basic_publish(exchange='',
                                   routing_key='doc_queue', body=link,
                                   properties=pika.BasicProperties(
                                       delivery_mode=2,  # 使消息持久化
                                   ))
        return True

    def run(self, link):
    #从数据库里的法院链接 获取court_id
        if '?' in link:  #HTTP://XXXX/ID?PAGE=120'
            court_id = link.split('?')[0].split('/')[-1]   #/court/court_id?page=XXXX
        else:
            court_id = link.split('/')[-1]    #/court/court_id

        try:
            next_link = self.get_main_page(link)   #获取当前页下的next_link，如果出现验证码则返回link,返回空则判断为最后一页
            i = 1
            while next_link:
                next_link = self.get_main_page(next_link)
                print next_link
                if i == 5:
                    time.sleep(5)
                    i = 1
                i += 1
            else:
                # 达到最后一页
                self.cur.execute('''UPDATE breakpoint SET status=1
                                 WHERE court_id=%s''', (court_id,))
                self.mysql_conn.commit()
                self.driver.quit()
        except Exception, e:
            print e
        finally:
            self.mysql_conn.close()
            self.mq_connection.close()
            self.driver.quit()


#def main():
#   sender = Sender()
 #   sender.run('http://openlaw.cn/court/dc33f00f87b8'
 #              '485abefc6504c500eb9a?page=1')

#if __name__ == '__main__':
#    sys.exit(main())
