# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# # @Time    : 2018/9/3 14:36
# @File    : main.py
# @Software: PyCharm

import time
import threading
from handler.proxy import Proxy
from handler.symbol import Symbol
from spiders.bigtrade import BigTradeFromWeb
from datetime import datetime


class ProjectScheduler(object):

    def __init__(self):
        self.proxy = Proxy()
        self.symbol = Symbol()
        self.spider = None#BigTradeFromWeb()
        self.status = True
        self.close = True

    def proxy_run(self):
        while self.status:
            self.proxy.multi_execute(maxnum=10)

    def symbol_run(self):

        self.symbol.save_symbol()

    def spider_run(self):
        self.spider.insert_data(30)

    def scheduler(self):

        while True:
            now = datetime.today().now()
            open_am = datetime.strptime('{} 09:30:00'.format(datetime.today().date()), "%Y-%m-%d %H:%M:%S")
            close_am = datetime.strptime('{} 11:30:00'.format(datetime.today().date()), "%Y-%m-%d %H:%M:%S")
            open_pm = datetime.strptime('{} 13:00:00'.format(datetime.today().date()), "%Y-%m-%d %H:%M:%S")
            close_pm = datetime.strptime('{} 15:00:00'.format(datetime.today().date()), "%Y-%m-%d %H:%M:%S")
            # 交易时间 周一 至周五
            # 上午9:30 ~ 11:30
            weekday = datetime.today().weekday()
            # 0 - 4 星期一 ~ 星期五
            # 5 6 周末
            if weekday not in [5, 6]:
                if open_am <= now <= close_am or open_pm <= now <= close_pm:
                    self.status = True
                    th = threading.Thread(target=self.proxy_run)
                    th.daemon = True
                    th.start()
                    self.spider_run()
                    self.status = False
                    self.close = True
                else:
                    if self.close:
                        # 闭盘爬一次
                        self.status = True
                        th = threading.Thread(target=self.proxy_run)
                        th.daemon = True
                        th.start()
                        self.spider_run()
                        self.close = False
                        self.status = False
            else:
                # 休市
                print('周末休息...')
                p2 = threading.Thread(target=self.symbol_run)
                p2.daemon = True
                p2.start()
                slp = 2 * 24 * 60 * 60
                time.sleep(slp)


if __name__ == '__main__':

    contronlib = ProjectScheduler()
    contronlib.proxy_run()
    #contronlib.scheduler()

