#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/3/14 15:02
# @Author  : xshxu@abcft.com
# @Site    : 
# @File    : option_code_timestamp.py
# @Software: PyCharm

from pymongo import MongoClient
from pymongo import (DESCENDING, ASCENDING)
import pandas as pd

client = MongoClient('127.0.0.1', 27017)
my_db = client.option_data_us_tick
my_col = my_db.HeadTimestamps

option_timestamp = pd.DataFrame(list(my_col.find({})))
option_timestamp.to_csv('option_code_timestamp.csv')


#searching for the start time of existed data
option_code = list(my_col.find({},{_id:0,option_code:1}))
option_code = list(set(option_code))
option_start = []
for item in option_code:
    my_col.find({'option_code':item})

print('done')
