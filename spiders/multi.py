# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# # @Time    : 2018/8/20 17:28
# @File    : multi.py
# @Software: PyCharm

# 多线程网络爬虫


import requests
import threading
import time
import re
import random
from requests.exceptions import Timeout, ProxyError
from queue import Queue, Empty
from db.mongodb import MongodbBase
from fake_useragent import UserAgent


class MultiCrawler(object):
    name = 'volume'

    def __init__(self):
        self.q = Queue()
        self.ua = UserAgent()
        self.time = time.time()
        self.db = MongodbBase()
        self.lock_ip = set()
        self.start_time = time.time()
        self.count = 0
        self.lock = threading.RLock()

    def fetch(self, timeout=10):
        while self.q.qsize():
            if time.time() - self.time >= 300:
                break
            try:
                task = self.q.get(block=False)  # 如果队列空了，直接结束线程。根据具体场景不同可能不合理，可以修改
            except Empty:
                break
            else:
                url, headers, proxy = task
                if proxy is not None:
                    try:
                        resp = requests.get(url, headers=headers, proxies=proxy, timeout=timeout)
                    except Exception as exp:
                        with open('F:\\error.log', 'a+') as f:
                            f.write('{}--conn\n'.format(exp))
                        self.lock_ip.add(proxy['http'])
                        try:
                            resp = requests.get(url, headers=headers, timeout=timeout)
                        except Exception as sse:
                            with open('F:\\error.log', 'a+') as f:
                                f.write('{}\n'.format(sse))

                else:
                    resp = requests.get(url, headers=headers, timeout=timeout)
                http_code = resp.status_code
                if http_code == 200:
                    symbol = re.search(r'symbol=(.*?)&', url).group(1)
                    day = re.search(r'day=(\d{4}-\d{2}-\d{2})', url).group(1)
                    html = resp.text
                    if 'symbol:"{}"'.format(symbol) in html:
                        items = dict()
                        items['symbol'] = symbol
                        items['name'] = re.search(r'name:"(.*?)"', html).group(1)
                        items['date'] = day
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
                        items[day] = data
                        # 写入数据库
                        table = symbol
                        select = {
                            'symbol': symbol,
                            'date': day
                        }
                        count = self.db.select(table, select)
                        if count is None:
                            self.db.insert(table, items)
                        else:
                            if len(count[day].keys()) != len(items[day]):
                                self.db.update(table, select, items)
                            else:
                                print('数据未更新...')
                        print('proxy:', proxy)
                        self.count += 1
                        print('success total: ', self.count, 'total time: ', time.time() - self.time)
                        resp.close()
                    elif 'null' in html:
                        self.db.update(table='code', item={'symbol': symbol}, data={'status': 'abnormal'})
                    elif '__ERROR:"MYSQL"' in html:
                        print('未开市...')

                    else:
                        # 更换proxy
                        print('-' * 50, 'Error Code', '-' * 50, http_code)
                        if proxy is not None:
                            self.lock_ip.add(proxy['http'])
                        new_proxy = None
                        for _ in range(10):
                            new = 'http://' + random.choice(self.proxy)
                            if new not in self.lock_ip:
                                new_proxy = {'http': new}
                                break
                        new_req = [url, headers, new_proxy]
                        self.q.put(new_req)
                else:
                    # 更换proxy
                    print('-'*50, 'Error Code', '-'*50, http_code)
                    if proxy is not None:
                        self.lock_ip.add(proxy['http'])
                    new_proxy = None
                    for _ in range(10):
                        new = 'http://' + random.choice(self.proxy)
                        if new not in self.lock_ip:
                            new_proxy = {'http': new}
                            break
                    new_req = [url, headers, new_proxy]
                    self.q.put(new_req)
            finally:
                self.lock.acquire()
                try:
                    self.q.task_done()
                except ValueError:
                    pass
                self.lock.release()

    @property
    def urls(self):
        symbols = self.db.select(table='code', item={'status': 'normal'}, many='any')
        return [symbol['url'] for symbol in symbols]

    @property
    def proxy(self):
        ip_pool = self.db.select(table='proxy', item={'status': "OK", 'delay': {'$lt': 3000}}, many='any')
        pool = [ip['ip'] for ip in ip_pool]
        print(len(pool))
        return pool

    def task(self):
        for url in self.urls:
            headers = {'User-Agent': self.ua.random}
            proxy = {'http': 'http://' + random.choice(self.proxy)}
            link = url + '2018-08-10'
            self.q.put([link, headers, proxy])

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super(MultiCrawler, cls).__new__(cls, *args, *kwargs)
        return cls._instance

    def main(self, threads_num=36):
        self.task()
        for _ in range(threads_num):
            th = threading.Thread(target=self.fetch)
            th.daemon = True
            th.start()
        try:
            self.q.join()
        except KeyboardInterrupt as ke:
            print('----', ke)
        finally:
            print('End Time: ', time.time() - self.time, 'success total:', self.count)


class NotFoundThisData(Exception):
    pass


if __name__ == '__main__':
    ml = MultiCrawler()
    ml.main()









