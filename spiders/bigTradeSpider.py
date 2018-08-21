# !/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2018/8/15 19:21
# @Author  : Mat
# @File    : bigTradeSpider.py
# @Software: PyCharm


import logging
import requests
import time
import queue
import random
import threading
import re
from datetime import datetime, timedelta
from fake_useragent import UserAgent
from config.settings import *
from db.mongodb import MongodbBase


class BigTradeSpider(object):
    """
        爬取新浪网大单交易记录
    """

    def __init__(self):
        self.name = 'bigTrade'
        self.mongodb = MongodbBase()
        self.delay = [0.001, 0.003, 0.005,0.1]

    def fetch(self, url, proxies=None, method='get'):
        print(proxies, type(proxies))
        print('Downloading url: {}'.format(url))
        response = None
        headers = self.user_agent()
        if method == 'get':
            try:
                response = requests.get(url, headers=headers, proxies=proxies)
            except Exception:
                pass
        return response

    @staticmethod
    def user_agent():
        ua = UserAgent()
        if ua:
            headers = {
                'User-Agent': ua.random
            }
        else:
            headers = {
                'User-Agent': USER_AGENT_DEFAULT
            }
        return headers

    def html(self, url, proxy, retry=3):
        html = None
        random.choice(self.delay)
        response = self.fetch(url=url, proxies=proxy)
        if response is not None:
            http_code = response.status_code
            print(http_code)
            if http_code == 200:
                html = response.text
            elif http_code == 456:
                print('IP 限制')
            elif http_code == 407:
                print('proxy access')
            elif http_code == 502:
                print('bad getway')
        else:
            if retry > 0:
                return self.html(url, proxy, retry-1)
        return html

    def get_crawl_url(self):
        table = 'code'
        item = {
            'status': 'normal'
        }
        many = 'any'
        urls = self.mongodb.select(table, item=item, many=many)
        return [url['url'] for url in urls]

    def main(self, url, today=None, proxy=None):
        for u in url:
            url = u + today
            print(proxy)
            html = self.html(url, proxy)
            result = self.__data_fix_big_trade(url, html, today)

            if result == -1:
                symbol = re.search(r'symbol=(.*?)&', url).group(1)
                table = 'code'
                item = {
                    'code': symbol
                }
                mdate = {
                    'status': 'abnormal'
                }
                self.mongodb.update(table, item, mdate)
            elif result == 0:
                print('休市...')
            else:
                if isinstance(result, dict):
                    table = symbol = result['symbol']

                    item = {
                        'symbol': symbol,
                        'date': today
                    }
                    count = self.mongodb.select(table, item)
                    print('count::', count)
                    if count is None:
                        self.mongodb.insert(table, result)
                    else:
                        if len(count[today].keys()) != len(result[today]):
                            self.mongodb.update(table, item, result)
                        else:
                            print('数据未更新...')

    def proxy_ip(self, number=20):
        table = 'proxy'
        item = {
            'status': 'ok'
        }
        many = 'any'
        ips = self.mongodb.select(table, item, many)
        return [ip['ip'] for ip in ips[:number]]

    @staticmethod
    def __data_fix_big_trade(url, html, today):
        items = dict()
        symbol = re.search(r'symbol=(.*?)&', url).group(1)
        if 'null' in html:
            return -1
        elif 'ERRORNO:1146' in html:
            return 0
        elif 'symbol' in html:
            items['symbol'] = symbol
            items['name'] = re.search(r'name:"(.*?)"', html).group(1)
            items['date'] = today
            data = dict()
            for source in re.findall(r'{(.*?)}', html):
                item = dict()
                tick_time = re.search(r'ticktime:"(.*?)"', source).group(1)
                item['price'] = re.search(r'price:"(.*?)"', source).group(1)
                item['volume'] = re.search(r'volume:"(.*?)"', source).group(1)
                item['amount'] = str(float(item['volume']) * 100 * float(item['price']))
                item['prev_price'] = re.search(r'prev_price:"(.*?)"', source).group(1)
                item['kind'] = re.search(r'kind:"(.*?)"', source).group(1)
                data[tick_time] = item
            items[today] = data
            return items
        else:
            logging.info('未知的HTML数据: {}'.format(html))


if __name__ == '__main__':

    # tasks = queue.LifoQueue(50)
    start_time = time.time()
    date = datetime.today().date() + timedelta(days=-1)
    bt = BigTradeSpider()
    step = 200
    threads = []
    urls = bt.get_crawl_url()
    th = None
    proxys = bt.proxy_ip(30)
    threads = []
    for i in range(10):
        # th = threading.Thread(target=bt.main, args=(urls[i*step: i*step + step], str(date), 'http://' + proxys[i]))
        # th.setDaemon(True)
        # th.start()
        bt.main(url=urls[i*step: i*step + step], today=str(date), proxy={'http': 'http://' + proxys[i]})
    # # urls = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_Bill.GetBillList?symbol=sh600000&num=0&sort=ticktime&asc=1&volume=0&amount=0&type=1&day='
    # print(bt.main(urls, today=str(date)))
    # print(bt.proxy_ip(10))
    print('end time: ', time.time() - start_time)

    # 生成任务队列



