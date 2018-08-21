# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# # @Time    : 2018/8/16 8:56
# @File    : proxy.py
# @Software: PyCharm

"""
    需要维护一个IP池,保证IP池的速度
"""

import requests
import time
import re
import threading
import asyncio
import aiohttp
from db.mongodb import MongodbBase
from fake_useragent import UserAgent


class Proxy(object):

    def __init__(self):
        self.db = MongodbBase()
        self.table = 'proxy'
        self.ua = UserAgent()

    def from_xiguadaili(self, num=500):
        """
            西瓜代理
            网址:http://api3.xiguadaili.com
        """
        api = 'http://api3.xiguadaili.com/ip/?tid=555034421324158&num={}&delay=1&category=2&protocol=http'.format(num)
        try:
            resp = requests.get(api, timeout=3)
        except Exception as exp:
            print(exp)
        else:
            if resp.status_code == 200:
                html = resp.text
                for address in html.split('\r\n'):
                    ip, port = address.split(':')
                    item = {'ip': ip}
                    result = self.db.select(table=self.table, item=item)
                    if not result:
                        data = {
                            'ip': ip,
                            'port': int(port),
                            'delay': 3000,
                            'protocol': 'http',
                            'status': 'OK',
                        }
                        self.db.insert(table=self.table, data=data)

    def check_ip_pools(self, proxy, timeout=5):
        url = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_Bill.GetBillList'
        headers = {'User-Agent': self.ua.random}
        ip = re.search(r'(\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3})', proxy).group(1)
        item = {'ip': ip}
        if isinstance(proxy, str):
            proxies = {'http': proxy}
            start_time = time.time()
            try:
                resp = requests.get(url, headers=headers, proxies=proxies, timeout=timeout)
            except Exception as exp:
                print(exp)
                result = self.db.delete(table=self.table, item=item)
                print('delete exp:', result)
            else:
                if resp.status_code == 200:
                    delay = round(time.time() - start_time, 3)*1000
                    if delay <= 3000:
                        data = {'delay': delay}
                        print('response time:', delay//1000)
                        result = self.db.update(table=self.table, item=item, data=data)
                        print('update: ', result)
                    else:
                        data = {'status': 'fail'}
                        result = self.db.update(table=self.table, item=item, data=data)
                        print('delete wait too long: ', delay//1000, result)
                elif resp.status_code == 456:
                    data = {'status': 'lock'}
                    result = self.db.update(table=self.table, item=item, data=data)
                    print('update lock: ', result)
                else:
                    result = self.db.delete(table=self.table, item=item)

                    print('delete: ', result)
        else:
            raise ValueError('proxy 的类型必须是字符串, proxy得到的参数:{} 类型是 {}'.format(proxy, type(proxy)))

    def proxy(self):
        item = {'delay': {'$gt': 1000}}
        ippool = self.db.select(table=self.table, item=item, many='any')
        proxies = []
        for ip in ippool:
            pro = 'http://' + ip['ip'] + ':' + str(ip['port'])
            proxies.append(pro)
        return proxies

    def action(self):
        while True:
            pool = self.proxy()
            if len(pool) <= 20:
                self.from_xiguadaili(num=50-len(pool))
            for p in pool:
                self.check_ip_pools(p)
                print(p)
            time.sleep(1)

    def main(self, num=20):
        threads = []
        for _ in range(num):
            th = threading.Thread(target=self.action)
            th.daemon = True
            threads.append(th)

        for t in threads:
            t.start()
            t.join()


if __name__ == '__main__':

    pr = Proxy()

    pr.main()












