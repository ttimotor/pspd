# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# # @Time    : 2018/8/16 9:31
# @File    : crawler.py
# @Software: PyCharm


import aiohttp
import asyncio
import socket
import re
import random
import time
import queue
import async_timeout
import threading
import multiprocessing
from datetime import datetime, timedelta
from fake_useragent import UserAgent
from config.settings import USER_AGENT_DEFAULT
from db.mongodb import MongodbBase


startTime = time.time()


class Crawler(object):

    @staticmethod
    def user_agents():
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

    def __init__(self):
        self.db = MongodbBase()
        self.proxies_lock = set()
        self.ip_pool = self.proxy
        self.url = self.symbol
        self.success = 0

    @property
    def proxy(self):
        ip_pool = self.db.select(table='proxy', item={'status': "OK"}, many='any')
        return [ip['ip'] for ip in ip_pool]

    @property
    def symbol(self):
        symbols = self.db.select(table='code', item={'status': 'normal'}, many='any')
        return [symbol['url'] for symbol in symbols]

    async def fetch(self, session, url, method='get', user_agent=None, proxies=None, delay=0.1, encoding='utf-8', retry=3):
        headers = user_agent
        print('proxies:', proxies)
        time.sleep(delay)
        if method == 'get':
            # 设置超时时间为3秒
            try:
                with async_timeout.timeout(3):
                    async with session.get(url, headers=headers, proxy=proxies) as response:
                        http_code = response.status
                        if http_code == 200:
                            self.success += 1
                            return await response.text(encoding=encoding)
                        else:
                            print('*'*20, 'Error Code: ', http_code, '*'*20)
                            if http_code == 502:
                                print('Server Not Response...')
                                new_delay = 2
                                if retry > 0:
                                    return await self.fetch(session, url, method=method, user_agent=user_agent,
                                                            proxies=proxies, delay=new_delay,encoding=encoding,
                                                            retry=retry - 1)
                            else:
                                print('#'*20, 'Other Error Code:', http_code)
                                print('IP 访问限制...')
                                self.proxies_lock.add(proxies)
                                ip = random.choice(self.ip_pool)
                                new_proxies = 'http://' + ip
                                if new_proxies in self.proxies_lock:
                                    new_proxies = None
                                if retry > 0:
                                    return await self.fetch(session, url, method=method, user_agent=user_agent,
                                                            proxies=new_proxies, delay=delay, encoding=encoding,
                                                            retry=retry - 1)
            except Exception as exp:
                print('EXP:', '-'*50, exp, proxies)
                self.proxies_lock.add(proxies)
                ip = random.choice(self.ip_pool)
                new_proxies = 'http://' + ip
                if new_proxies in self.proxies_lock:
                    new_proxies = None
                if retry > 0:
                    return await self.fetch(session, url, method=method, user_agent=user_agent,
                                            proxies=new_proxies, delay=delay, encoding=encoding,
                                            retry=retry - 1)

    async def html(self, url, proxies):
        for link in url:
            link = link + str(datetime.today().date() + timedelta(days=-4))
            symbol = re.search(r'symbol=(.*?)&', link).group(1)
            day = re.search(r'day=(\d{4}-\d{2}-\d{2})', link).group(1)
            agent = self.user_agents()
            async with aiohttp.ClientSession() as session:
                html = await self.fetch(session, link, user_agent=agent, proxies=proxies, encoding='gbk')
                if html is None:
                    pass
                else:
                    if 'symbol' in html:
                        await self.save_data(symbol, html, day=day)
                    elif 'null' in html:
                        await self.change_symbol(symbol)
                    elif 'ERRORNO:1146' in html:
                        print(link)
                        print('Stop Trade....')
                    else:
                        print('错误的数据: ', html)

    async def change_symbol(self, symbol):
        self.db.update(table='code', item={'symbol': symbol}, data={'status': 'abnormal'})

    async def save_data(self, symbol, html, day=str(datetime.today().date())):
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
            print('*'*50, items)
            self.db.insert(table, items)
        else:
            if len(count[day].keys()) != len(items[day]):
                self.db.update(table, select, items)
            else:
                print('数据未更新...')

    def run(self):
        loop = asyncio.get_event_loop()
        task = []
        ips = self.ip_pool
        urls = self.symbol
        step = len(urls) // len(ips) + 1
        today = str(datetime.today().date()) # + timedelta(days=-1))
        print(today)
        flg = False
        if len(ips) >= 10:
            for i in range(len(ips)):
                links = []
                for index in range(i * step, i * step + step, 1):
                    try:
                        links.append(urls[index] + today)
                    except IndexError:
                        flg = True
                        break
                task.append(self.html(links, proxies='http://' + ips[i]))
                if flg:
                    break
        loop.run_until_complete(asyncio.wait(task))
        loop.close()
        print('End Time: ', time.time() - startTime, 'success total:', self.success)

    def multi(self, url):
        print('in..')
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        # loop = asyncio.get_event_loop()
        task = []
        ip = random.choice(self.ip_pool)

        link = url
        task.append(self.html(link, proxies='http://' + ip))
        loop.run_until_complete(asyncio.wait(task))
        loop.close()
        print('End Time: ', time.time() - startTime, 'success total:', self.success)


if __name__ == '__main__':
    cwl = Crawler()
    MAX_NUM = 10
    threads = []
    all_urls = cwl.symbol
    step = len(all_urls) // MAX_NUM + 1
    for i in range(MAX_NUM):
        links = all_urls[i*step: (i+1)*step]
        with open('F:\\loglog.txt', 'a+') as f:
            f.write('%d \n' % len(links))
            for li in links:
                f.write('{}\n'.format(li))
        print(len(links), len(all_urls), i)
        th = threading.Thread(target=cwl.multi, args=(links, ))
        th.daemon = True
        threads.append(th)
        th.start()
    for t in threads:
        t.join()

