# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# # @Time    : 2018/9/3 10:26
# @File    : bigtrade.py
# @Software: PyCharm

import requests
import random
import re
from db.mongodb import MongodbBase
from fake_useragent import UserAgent
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor


class BigTradeFromWeb(object):

    def __init__(self):
        self.db = MongodbBase()
        self.ua = UserAgent()
        self.lose_url = set()
        self.proxy_pool = self.proxies()
        self.urls = self.symbol()
        self.fail = set()
        self.success = 0

    def fetch(self, url, method='GET', timeout=10, retry=3):
        headers = {'User-Agent': self.ua.random}
        proxy = random.choice(self.proxy_pool)
        if proxy in self.fail:
            self.proxy_pool.remove(proxy)
            if len(self.proxy_pool) > 1:
                proxy = random.choice(self.proxy_pool)
            else:
                proxy = None
        proxies = {'http': proxy}
        try:
            delay = random.uniform(0, 1.5)
            time.sleep(delay)
            response = requests.get(url, headers=headers, proxies=proxies, timeout=timeout)
        except Exception:
            self.fail.add(proxy)
            if retry > 0:
                return self.fetch(url, retry=retry-1)
            else:
                self.lose_url.add(url)
            return 'Too Many Times'
        else:
            http_code = response.status_code
            if http_code == 200:
                html = response.text
                symbol = re.search(r'symbol=(.*?)&', url).group(1)
                day = re.search(r'day=(\d{4}-\d{2}-\d{2})', url).group(1)
                if 'symbol:"{}"'.format(symbol) in html:
                    print('HTML: ', html)
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
                        select = self.db.insert(table, items)
                        return select
                    else:
                        if len(count[day].keys()) != len(items[day]):
                            insert = self.db.update(table, select, items)
                            return insert
                    self.success += 1
                elif 'null' == html.strip():
                    self.lose_url.add(url)
                elif '__ERROR:"MYSQL"' in html:
                    error = 'NOT GET DATA'
                    return error
                else:
                    # 代理IP不可用
                    self.fail.add(proxy)
                    if retry > 0:
                        return self.fetch(url, retry=retry - 1)
                    else:
                        self.lose_url.add(url)
            else:
                self.fail.add(proxy)
                if retry > 0:
                    return self.fetch(url, retry=retry - 1)
                else:
                    self.lose_url.add(url)
                    return 'Too Many Times'

    def symbol(self):
        links = self.db.select(table='code', item={
            'status': 'normal'
        }, many='any')
        urls = []
        for url in links:
            link = url['url'] + '{}'.format(datetime.today().date())
            urls.append(link)
        return urls

    def proxies(self):
        pools = self.db.select(table='proxy', item={
            'status': 'OK',
            'delay': {'$lt': 3000}
        }, many='any')

        return [proxy['proxy'] for proxy in pools]

    def insert_data(self, thrds=25):
        begin = time.time()
        with ThreadPoolExecutor(thrds) as executor:
            for url, runner in zip(self.urls, executor.map(self.fetch, self.urls)):
                print('runner: ', runner)
                print(len(self.urls), len(self.proxy_pool), len(self.fail), len(self.lose_url))

        end = time.time()
        if end - begin < 300:
            with ThreadPoolExecutor(thrds) as executor:
                for url, runner in zip(self.lose_url, executor.map(self.fetch, self.lose_url)):
                    print('runner: ', runner)
                    print(len(self.urls), len(self.proxy_pool), len(self.fail), len(self.lose_url))


if __name__ == '__main__':
    import time
    start_time = time.time()
    bgt = BigTradeFromWeb()

    bgt.insert_data(30)

    print('Total Time: ', time.time() - start_time, 'SUCCESS COUNT: ', bgt.success)