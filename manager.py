# coding: utf-8

import sys
import codecs  #编码转换模块
import ConfigParser    #读取配置文件
import MySQLdb as mdb
import random
import multiprocessing
from sender import Sender
from worker import Worker
import shutil
import os
import time

cf = ConfigParser.ConfigParser()
cf.readfp(codecs.open('settings.ini', 'r', 'utf-8-sig'))  #字节序

DB_SERVER = cf.get('mysql', 'host')
DB_USERNAME = cf.get('mysql', 'user')
DB_PASSWORD = cf.get('mysql', 'password')
DB_DATABASE = cf.get('mysql', 'database')

MAX_SENDER = cf.getint('default', 'max_sender')
MAX_WORKER = cf.getint('default', 'max_worker')

INCR_FLAG = cf.getint('default', 'incr_flag')  # 0, 1
MAX_INCR_WORKER = cf.getint('default', 'max_incr_worker')

try:
    # MySQL server 连接数据库
    mysql_conn = mdb.connect(DB_SERVER, DB_USERNAME, DB_PASSWORD, DB_DATABASE)
    cur = mysql_conn.cursor()
except Exception, e:
    print e
    sys.exit(0)


def start_sender(url, incr_flag=False):
    """创建sender进程"""
    s = Sender(incr_flag)
    s.run(url)


def start_worker():
    """创建worker进程"""
    print 'New worker'
    w = Worker()
    w.run()

def add_process_url(num):
    """从数据库中随机选择一定量的数据"""
    mysql_conn.commit()
    cur.execute('''SELECT court_id, page_num FROM breakpoint
                WHERE status=0 and page_num=1 LIMIT 200''')
    data_set = cur.fetchall()  #返回所有数据[(ID, NUM), (ID, NUM).....]  200

    # (court_id, page_num)
    print '&&&&&&&&&&&&&&&&&&&&&&&&&&'
    print 'data_set = ',len(data_set)
    print '$$$$$$$$$$$$$$$$$$$$$$$$$$'
    # if len(data_set) == 0:
    #     list_re = ['','']
    #     return list_re
    random_data = random.sample(data_set, num)   # MAX_SENDER
    base_url = 'http://openlaw.cn/court/{}?page={}'
    return [base_url.format(i[0], i[1]) for i in random_data]  # ['HTTP://XXXX/ID?PAGE=120', 'HTTP://OPAXXXX/ID?PAGE=222']
    

def get_random_url(num):
    """从数据库中随机选择一定量的数据"""
    cur.execute('''SELECT court_id, page_num FROM breakpoint
                WHERE status=0 LIMIT 200''')
    data_set = cur.fetchall()  #返回所有数据[(ID, NUM), (ID, NUM).....]  200
    # 随机选10个数据
    # (court_id, page_num)
    random_data = random.sample(data_set, num)   # MAX_SENDER
    base_url = 'http://openlaw.cn/court/{}?page={}'
    return [base_url.format(i[0], i[1]) for i in random_data]  # ['HTTP://XXXX/ID?PAGE=120', 'HTTP://OPAXXXX/ID?PAGE=222']


def get_incr_url():
    """从数据库中获取需要增量抓取的链接
    即所有page_num不为1的链接
    """
    cur.execute('''SELECT court_id FROM breakpoint
                WHERE page_num!=1''')
    data_set = cur.fetchall()
    base_url = 'http://openlaw.cn/court/{}?page={}'
    return [base_url.format(i[0], 1) for i in data_set]


def main():
    # 增量抓取进程
    if os.path.exists('./img/'):
        shutil.rmtree('./img/')

    if not os.path.exists('./img/'):
        print 'not exist img dir '
        os.mkdir('./img/')

    if not os.path.exists('./img/img/'):
        os.mkdir('./img/img/')

    incr_jobs = []  #进程
    incr_urls = []   #链接
    if INCR_FLAG:
        incr_urls = get_incr_url()   #数据库中所有page_num不为1的链接
        p = multiprocessing.Process(target=start_sender,
                                    args=(incr_urls[0], True))
        incr_jobs.append(p)  #进程队列
        p.start()
        incr_urls.pop(0)  #爬完则删除

    # 随机取max_sender数量的链接, 并生成sender的进程
    urls = get_random_url(MAX_SENDER)   #列表中有max_sender个链接
    sender_jobs = []  #进程
    for url in urls:
        p = multiprocessing.Process(target=start_sender, args=(url,))
        sender_jobs.append(p)   #进程
        p.start()

    # 产生MAX_WORKER数量的worker进程
    worker_jobs = []   #进程
    for i in range(MAX_WORKER):
        p = multiprocessing.Process(target=start_worker)
        worker_jobs.append(p)
        p.start()

    # 持续监控worker_jobs和sender_jobs
    # 如果有进程退出，立马产生一个新进程
    while True:
        for i in sender_jobs:
            flag = i.is_alive()
            if not flag:   #该senger进程死掉了
                sender_jobs.remove(i)   #manager里删除该进程

            cf = ConfigParser.ConfigParser()
            cf.readfp(codecs.open('settings.ini', 'r', 'utf-8-sig'))
            max_sender_second = cf.getint('default', 'max_sender')

            #print 'len(sender_jobs)', len(sender_jobs)
            #print max_sender_second
            if len(sender_jobs) < max_sender_second:   #进程个数少于最大值
                # time.sleep(2)
                url = add_process_url(1)[0]
                print '[!]url ', url
                if url != ' ':
                    p = multiprocessing.Process(target=start_sender, args=(url,))   #产生一个新进程
                    sender_jobs.append(p)
                    p.start()
                else:
                    pass
                    tfile = './settings.ini'
                    lines=open(tfile,'r').readlines()
                    flen=len(lines)-1

                    for i in range(flen):

                        if 'max_sender' in lines[i]:
                            infos = lines[i].split("=")
                            bgq = infos[1]
                            bgh = int(bgq) - 1
 
                            lines[i]=lines[i].replace(str(bgq),str(bgh)+'\n')

                    open(tfile,'w').writelines(lines)
        for i in worker_jobs:
            flag = i.is_alive()
            if not flag:
                worker_jobs.remove(i)
            if len(worker_jobs) < MAX_WORKER:
                p = multiprocessing.Process(target=start_worker)
                worker_jobs.append(p)
                p.start()
        if INCR_FLAG:
            for i in incr_jobs:
                flag = i.is_alive()
                if not flag:
                    incr_jobs.remove(i)
                if len(incr_jobs) < MAX_INCR_WORKER:
                    # incr_urls中无数据，即全部抓取完毕
                    if not incr_urls:
                        continue
                    #增加进程
                    url = incr_urls[0]
                    p = multiprocessing.Process(target=start_sender,
                                                args=(url, True))
                    incr_jobs.append(p)  
                    p.start()
                    incr_urls.pop(0)

    mysql_conn.close()


if __name__ == '__main__':
    main()
