# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# # @Time    : 2018/8/16 15:21
# @File    : demo.py
# @Software: PyCharm

import re


sss = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_Bill.GetBillList?symbol=sh600000&num=0&sort=ticktime&asc=1&volume=0&amount=0&type=1&day=2018-08-15'

print(len(re.search(r'day=(\d{4}-\d{2}-\d{2})', sss).group(1)))

ip = 'http://117.44.247.37:8908'

ip = ip.split('//')[-1].split(':')

print(ip)