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
import logging
from concurrent.futures import ThreadPoolExecutor
from db.mongodb import MongodbBase
from fake_useragent import UserAgent


logger = logging.getLogger('Proxy')
logger.setLevel(level=logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class Proxy(object):

    def __init__(self):
        self.db = MongodbBase()
        self.table = 'proxy'
        self.ua = UserAgent()

    def from_xiguadaili_get_proxies(self, num=1000, timeout=3):
        proxies = []
        """
            西瓜代理
            网址:http://api3.xiguadaili.com
        """
        api = 'http://api3.xiguadaili.com/ip/?tid=555034421324158&num={}&delay=1&category=2&protocol=http'.format(num)
        try:
            resp = requests.get(api, timeout=timeout)
        except Exception as exp:
            logger.debug("xiguadali: ---> {}".format(exp))
        else:
            if resp.status_code == 200:
                html = resp.text
                for address in html.split('\r\n'):
                    ip, port = address.split(':')
                    pro = "http://{ip}:{port}".format(ip=ip, port=port)
                    logger.info('组合成新代理地址: {}'.format(pro))
                    proxies.append(pro)
        return proxies

    def check_ping_proxy(self, proxy, timeout=5):
        print('+*+', proxy)
        url = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_Bill.GetBillList'
        headers = {'User-Agent': self.ua.random}
        proxies = {'http': proxy}
        start_time = time.time()
        try:
            response = requests.get(url, headers=headers, proxies=proxies, timeout=timeout)
        except Exception as exp:
            logger.debug("sina api: ---> {}".format(exp))
        else:
            if response.status_code == 200:
                delay = round(time.time() - start_time, 3) * 1000
                logger.info('This IP {} max delay is {} second'.format(proxy, delay))
                count = self.db.select(table=self.table, item={'proxy': proxy})
                if not count:
                    value = {
                        'proxy': proxy,
                        'delay': delay,
                        'protocol': 'http',
                        'status': 'OK',
                    }
                    insert = self.db.insert(table=self.table, data=value)
                    logger.info('插入数据: {}'.format(insert))
                    return insert

    def multi_execute(self, maxnum=20):
        pools = self.from_xiguadaili_get_proxies()
        with ThreadPoolExecutor(maxnum) as executor:
            for url, runner in zip(pools, executor.map(self.check_ping_proxy, pools)):
                logger.info('url: {} --> runner: {}'.format(url, runner))


if __name__ == '__main__':
    while True:
        tt = time.time()
        proxx = Proxy()

        proxx.multi_execute()












