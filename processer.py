#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/14 12:12
# @Author  : xshxu@abcft.com
# @Site    : 
# @File    : processer.py
# @Software: PyCharm

import time
from threading import Thread
from ContractSamples import ContractSamples
from date_list import *
from datetime import timedelta
from stock_HeadTimestamp import *
from stock_code_define import *


class Processer(Thread):
    def __init__(self, client):
        super().__init__()
        self.client = client
        self.stock_code_map = stock_code_map

    def run(self):
        if self.client.started:
            return

        self.client.started = True

        if self.client.globalCancelOnly:
            print("Executing GlobalCancel only")
            self.client.reqGlobalCancel()
        else:
            print("Executing requests")
            # self.historicalDataRequests_req_HeadTimestamp()
            # self.historicalDataRequests_req_Seconds()
            # self.historicalDataRequests_req_Days()
            # self.optionsOperations_req()
            self.option_tikc_req()
            # self.mktData_req_opt()
            # self.historicalDataRequests_req_opt_Seconds()

            print("Executing requests ... finished")

    def historicalDataRequests_req_HeadTimestamp(self):
        # Requesting historical data
        # ! [reqHeadTimeStamp]
        for index in self.stock_code_map.keys():
            if self.client.process_done:
                break
            else:
                stock_code = self.stock_code_map[index]
                print('Start to get', stock_code, str(index))
                self.client.reqHeadTimeStamp(index, ContractSamples.OptionWithLocalSymbol(stock_code), "TRADES", 0, 1)
                time.sleep(10)
                print('Finish query', stock_code)

    def historicalDataRequests_req_Seconds(self):
        flag = 0
        for queryTime in date_list:
            if flag == 1:
                queryTime += timedelta(hours=16)
            else:
                queryTime = datetime.datetime(2017,12,8,10,30)
                flag = 1

            if self.client.process_done:
                break

            while queryTime.hour > 9:
                for index in self.stock_code_map.keys():
                    if self.client.process_done:
                        break
                    else:
                        stock_code = self.stock_code_map[index]
                        print('Start to get', stock_code, str(index), str(queryTime))
                        self.client.reqHistoricalData(index, ContractSamples.USStockAtSmart(stock_code),
                                               queryTime.strftime("%Y%m%d %H:%M:%S"),
                                               "1800 S", "1 secs", "TRADES", 1, 1, False, [])
                        time.sleep(10)
                        print('Finish query', stock_code + str(queryTime))
#                print(xx)
                queryTime -= timedelta(seconds=1800)

        print('request done')

    def historicalDataRequests_req_Days(self):
        queryTime = datetime.datetime(2016, 12, 25)
        while queryTime > datetime.datetime(1980, 1, 1):
            for index in self.stock_code_map.keys():
                if self.client.process_done:
                    break
                stock_code = self.stock_code_map[index]
                headTimestamp = datetime.datetime.strptime(stock_HeadTimestamp[stock_code], '%Y/%m/%d')
                if queryTime > headTimestamp:
                    print('Start to get', stock_code, str(index), str(queryTime))
                    self.client.reqHistoricalData(index, ContractSamples.USStockAtSmart(stock_code),
                                                  queryTime.strftime("%Y%m%d %H:%M:%S"),
                                                  "1 Y", "1 day", "TRADES", 1, 1, False, [])
                    time.sleep(10)
                    print('Finish query', stock_code + str(queryTime))
                    #                print(xx)
            queryTime -= timedelta(days=360)

        print('request done')

    def optionsOperations_req(self):
        # ! [reqsecdefoptparams]
        # self.client.reqSecDefOptParams(1, "IBM", "", "STK", 8314)
        # self.client.reqContractDetails(210, ContractSamples.OptionForQuery())
        # self.client.reqMktData(1002, ContractSamples.USStockAtSmart('AAPL'), "", False, False, []) #OptionForQuery()
        # self.client.reqHistoricalData(1, ContractSamples.OptionForQuery(),
        #                               "20180224 16:00:00", "900 S", "1 secs", "TRADES", 1, 1, False, [])

        # self.client.reqHistoricalTicks(10000001, ContractSamples.OptionWithLocalSymbol("AAPL  180316C00180000"),
        #                         "20180309 10:58:17", "", 1000, "TRADES", 1, True, [])
        self.opt_tick_req_single_code(10000001, "AAPL  180316C00180000", datetime.datetime(2018,3,12,9,30))
        # self.client.reqHeadTimeStamp(10000, ContractSamples.OptionWithLocalSymbol("AAPL  180420C00180000"), "TRADES", 0, 1)

        # self.client.reqMktData(1000, ContractSamples.OptionWithLocalSymbol("AAPL  180420C00180000"), "100,101,104,106,233,236,258", False, False, [])
        # ! [reqsecdefoptparams]

        print('request done!!')

    def historicalDataRequests_req_opt_Seconds(self):
        for index in self.stock_code_map.keys():
            queryTime = datetime.datetime(2018, 1, 16, 16, 00)
            endday = queryTime - timedelta(days=180)

            while queryTime > endday:
                if self.client.process_done:
                    break
                else:
                    stock_code = self.stock_code_map[index]
                    print('Start to get', stock_code, str(index), str(queryTime))
                    self.client.reqHistoricalData(index, ContractSamples.OptionWithLocalSymbol(stock_code),
                                           queryTime.strftime("%Y%m%d %H:%M:%S"),
                                           "1800 S", "1 secs", "TRADES", 1, 1, False, [])
                    time.sleep(11)
                    print('Finish query', stock_code + str(queryTime))

                if queryTime.hour == 10 and queryTime.minute == 0:
                    if queryTime.isoweekday() == 1:
                        queryTime -= timedelta(days=2,hours=18)
                    else:
                        queryTime -= timedelta(hours=18)
                else:
                    queryTime -= timedelta(seconds=1800)

        print('request done')

    def mktData_req_opt(self):
        for index in self.stock_code_map.keys():
            stock_code = self.stock_code_map[index]
            print('Start to get', stock_code, str(index))
            self.client.reqMktData(index, ContractSamples.OptionWithLocalSymbol(stock_code), "100,101,104,106,233,236,258", False, False, [])
            print('Finish query', stock_code)

        print('request done')

    def option_tikc_req(self):
        for index in self.stock_code_map.keys():
            if self.client.process_done:
                break
            else:
                stock_code = self.stock_code_map[index]
                self.client.reqContractDetails(index, ContractSamples.OptionForQuery(stock_code))
                time.sleep(11)

        while not self.client.req_opt_contract_end and not self.client.process_done:
            print('waiting for req_opt_contract_end Done')
            time.sleep(0.5)

        option_code_map = self.client.option_code_map
        for index in option_code_map:
            if self.client.process_done:
                break
            else:
                option_code = option_code_map[index]
                self.client.opt_req_next_code = False
                queryTime = datetime.datetime(2018, 3, 9, 9, 30)
                self.opt_tick_req_single_code(index, option_code, queryTime)

        print('request option tick data done!')

    def opt_tick_req_single_code(self,index,option_code,query_Time):
        queryTime = query_Time
        while not self.client.opt_req_next_code and not self.client.process_done:
            self.client.reqHistoricalTicks(index, ContractSamples.OptionWithLocalSymbol(option_code),
                                           queryTime.strftime("%Y%m%d %H:%M:%S"), "", 1000, "TRADES", 1, True, [])
            time.sleep(15)
            while not self.client.process_done:
                if self.client.opt_req_next_time:
                    if queryTime.isoweekday() == 1:
                        queryTime -= timedelta(days=3)
                    else:
                        queryTime -= timedelta(days=1)
                    queryTime = datetime.datetime.combine(queryTime.date(),query_Time.time())
                    print(queryTime)
                    self.client.opt_req_next_time = False
                    print('next time...............')
                    break
                elif self.client.opt_req_continue:
                    queryTime = self.client.lasttime
                    self.client.opt_req_continue = False
                    print('continue...............')
                    break
                else:
                    time.sleep(0.5)
                    print('sleeping.................')