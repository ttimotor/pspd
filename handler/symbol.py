# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# # @Time    : 2018/8/16 12:10
# @File    : symbol.py
# @Software: PyCharm


import requests
import re
import os

path = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
print(path)

from db.mongodb import MongodbBase


class Symbol(object):

    URL = 'http://quote.eastmoney.com/stocklist.html'

    def __init__(self):
        self.mongo = MongodbBase()

    def save_symbol(self):

        resp = requests.get(self.URL)

        if resp.status_code == 200:
            html = resp.text
            patten = '<li><a target="_blank" href="http://quote.eastmoney.com/(.*?)\.html">(.*?)\((\d{6})\)</a></li>'
            href = re.findall(patten, html)
            for code, name, _ in href:
                if code[2:5] == '600' or code[2:5] == '601' or code[2:5] == '603' \
                        or code[2:5] == '000' or code[2:5] == '002' or code[2:5] == '300':
                    url = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_Bill.GetBillList?' \
                          'symbol={}&num=0&sort=ticktime&asc=1&volume=0&amount=0&type=1&day='.format(code)
                    status = 'normal'
                    data = {
                        'symbol': code,
                        'url': url,
                        'status': status
                    }
                    if self.mongo.select(table='code', item={'symbol': code}, many='one'):
                        print('已经存在...')
                    else:
                        self.mongo.insert(table='code', data=data)


if __name__ == '__main__':

    sy = Symbol()

    sy.save_symbol()