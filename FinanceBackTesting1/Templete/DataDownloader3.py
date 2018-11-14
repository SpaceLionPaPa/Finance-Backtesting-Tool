# encoding=utf-8
# Program: this is a program to downloader from WIND
# Writer: Qijie Li
# Date: Oct.25.2017
from WindPy import w
import numpy as np
import datetime
import csv
import copy
import pandas as pd
import os
import shutil


def toPeriodList(periodType, dailyList):
    periodSeries = copy.deepcopy(pd.DatetimeIndex(dailyList)).to_period(
        freq=periodType).to_series().astype(str)
    periodSeries.index = periodSeries.values
    uniquePeriodSeries = np.unique(periodSeries.tolist())
    return uniquePeriodSeries, periodSeries

__mataclass__=type # Use new type classes
class DataDownloader:

    def __init__(self, stockCodelist = [], marketGroupCode = [], startDate = u'2017-09-08', endDate = [], *args):
        '''
        初始化参数
        '''
        self.startDate = startDate
        if startDate ==[]:
            self.startDate = str(datetime.date.today() - datetime.timedelta(days=1))
        self.endDate = endDate
        if self.endDate == []:
            self.endDate = str(datetime.date.today())
            # self.endDate = datetime.date.today() - datetime.timedelta(days=1)
        self.marketGroupCode = marketGroupCode
        if self.marketGroupCode == []:
            self.marketGroupCode = 'a001010100000000'
        # 交易日数据获取
        w.start()
        self.tradedayList = w.tdays(self.startDate, self.endDate, u"").Data[0]
        self.strDayList = [str(i.date()) for i in self.tradedayList]
        tradedayDf = pd.DataFrame(self.tradedayList, index=self.tradedayList)
        tradedayDf.to_csv('tradedayList.csv')
        # 周期序列字典
        self.periodTypeList = ['D', 'W', 'M', 'A']
        self.uniPeriodListDcit = {i: toPeriodList(periodType=i, dailyList=self.tradedayList)[0] for i in self.periodTypeList}
        self.periodListDcit = {i: toPeriodList(periodType=i, dailyList=self.tradedayList)[1] for i in self.periodTypeList}
        self.lastTradeday = self.tradedayList[-1].date()
        self.processedFilenameDict = {}
        # stockCodelist 获取
        self.stockCodelist = stockCodelist
        if self.stockCodelist == []:
            # 取所有时间段股票代码
            periodType = 'A'# 按年循环
            uniPeriodList = self.uniPeriodListDcit[periodType]
            periodList = self.periodListDcit[periodType]
            for p in range(len(uniPeriodList)):   # 周期迭代
                period = uniPeriodList[p]
                periodLocSer = pd.Series(periodList.loc[period])
                tempSlice = periodList.index.get_loc(period)
                if len(periodLocSer) >= 2:
                    stLoc = tempSlice.start
                    enLoc = tempSlice.stop - 1
                else:
                    stLoc = tempSlice
                    enLoc = tempSlice
                dateRange = [self.strDayList[stLoc], self.strDayList[enLoc]]
                yearStockCodelist = w.wset(u"sectorconstituent", u"date=" + dateRange[-1] +
                                           u";sectorid=" + self.marketGroupCode + u";field=wind_code").Data[0]  # 年末
                if not p:   # 第一期
                    fianlStockCodelist = list(yearStockCodelist)
                else:
                    fianlStockCodelist = list(set(fianlStockCodelist).union(set(yearStockCodelist)))
                fianlStockCodelist.sort()
            self.stockCodelist = fianlStockCodelist
            # csvFile1 = open(u'StockCodes' + self.strDayList[-1] + '.csv', u'w', newline=u'')
            # csvWriter = csv.writer(csvFile1)
            # csvWriter.writerow(self.stockCodelist)  # 第一行股票代码
            # csvFile1.close()
        w.stop()

    def __getDatas(self):
        '''
        生成分析中使用频率高的类属性
        '''

    def download_marketData(self, indexNameList = []):
        '''
        从WIND获取目标数据
        :param: startDate: 数据下载时间起点
        :return:数据csv文件
        '''
        # 内置参数
        cnMarketDict = {u'open': u'开盘价', 'high': u'最高价', 'low': u'最低价', 'close': u'收盘价',
                        'volume': u'成交量', 'amt': u'成交额', 'vwap': u'均价', 'turn': u'换手率',
                        'vol_ratio': u'量比', 'pct_chg': '涨跌幅', 'mkt_cap_ard': u'总市值',
                        'mkt_cap_float': '流通市值', u'maxupordown': u'涨跌停', u'PB_LF': u'PB_LF',
                        'PE_TTM': u'PE_TTM', 'PS_TTM': u'PS_TTM', u'pb': u'pb'}
        mktFileNameDict = {u'open': u'open', 'high': u'high', 'low': u'low', 'close': u'close',
                           'volume': u'volume', 'amt': u'amt', 'vwap': u'vwap', 'turn': u'turn',
                           'vol_ratio': 'vol_ratio', u'pct_chg': 'pct_chg', 'mkt_cap_ard': u'SZ_Total',
                           'mkt_cap_float': u'SZ_LT', u'maxupordown': u'maxupordown', 'PB_LF': u'PB_LF',
                           'PE_TTM': u'PE_TTM', 'PS_TTM': u'PS_TTM', u'pb': u'pb'}
        startDate = copy.deepcopy(self.startDate)
        lastTradeday = copy.deepcopy(self.lastTradeday)

        # 设置全市场数据参数
        w.start()
        defaultIndexNameList = [u'open', u'high', u'low', u'close', u'volume', u'turn']
        if indexNameList == []:
            indexNameList = defaultIndexNameList
        stockCodelist = copy.deepcopy(self.stockCodelist)
        joinStockCode  = u','.join(stockCodelist)
        colName = copy.deepcopy(stockCodelist)
        colName.insert(0, u'Date')
        # 下载数据
        for i in range(len(indexNameList)):
            if indexNameList[i] in ['vol_ratio']:
                periodType = 'D'
            else:
                periodType = 'A'
            dataSet = {}
            dateRangeDict = {}
            uniPeriodList = self.uniPeriodListDcit[periodType]
            periodList = self.periodListDcit[periodType]
            for p in range(len(uniPeriodList)):   # 周期迭代
                period = uniPeriodList[p]
                periodLocSer = pd.Series(periodList.loc[period])
                tempSlice = periodList.index.get_loc(period)
                if len(periodLocSer) >= 2:
                    stLoc = tempSlice.start
                    enLoc = tempSlice.stop - 1
                else:
                    stLoc = tempSlice
                    enLoc = tempSlice
                dateRange = [self.strDayList[stLoc], self.strDayList[enLoc]]
                # 下载数据
                downLoadFormula = 'w.wsd(joinStockCode, indexNameList[i], dateRange[0], dateRange[-1],'
                if indexNameList[i] == u"mkt_cap_float":
                    result = eval(downLoadFormula + '"unit=1;currencyType=;PriceAdj=F")')
                elif indexNameList[i] == u"mkt_cap_ard":
                    result = eval(downLoadFormula + '"unit=1;PriceAdj=F")')
                elif indexNameList[i] in [u"pb"]:
                    result = eval(downLoadFormula + '"ruleType=3;PriceAdj=F")')
                elif indexNameList[i] in ['vol_ratio']:   # 逐天下载
                    result = eval(downLoadFormula + '"VolumeRatio_N=5;PriceAdj=F")')
                else:
                    result = eval(downLoadFormula + '"PriceAdj=F")')   # 普通行情数据，例如开盘价，换手率
                dataSet[period] = result
                dateRangeDict[period] = [stLoc, enLoc]
            print(cnMarketDict[indexNameList[i]] + u'全时间段下载完毕')  # 保存数据至csv文件
            strFilename = os.getcwd() + u'/DownloadedData/OriginalData/' + cnMarketDict[indexNameList[i]] + \
                          startDate + u'-' + str(lastTradeday) + u'.csv'
            self.processedFilenameDict[mktFileNameDict[indexNameList[i]]] = strFilename
            csvFile = open(strFilename, u'w', newline=u'')
            csvWriter = csv.writer(csvFile)
            csvWriter.writerow(colName)  # 第一行股票代码
            for key in uniPeriodList:  # 周期
                st = dateRangeDict[key][0]
                end = dateRangeDict[key][-1]
                csvTradedayList = self.strDayList[st:end+1]
                for j in range(len(csvTradedayList)):  # 不同交易日
                    row = [i[j] for i in dataSet[key].Data]  # j 时间段所有股票代码数据
                    row.insert(0, csvTradedayList[j])
                    csvWriter.writerow(row)
            csvFile.close()
        w.stop()
        print(u'所有全市场数据下载完毕')

    def download_indexData(self, indexCodeList = []):
        # 内置参数
        startDate = copy.deepcopy(self.startDate)
        endDate = copy.deepcopy(self.endDate)
        lastTradeday = copy.deepcopy(self.lastTradeday)
        cnIndexDict = {u"000016.SH": u'上证50指数', u"000905.SH": u'中证500指数', u"000001.SH": u'上证指数',
                       u"000300.SH": u'沪深300指数', u"399005.SZ": u'中小板指数', u'000852.SH': u'中证1000',
                       u"399001.SZ": u'深圳成指', u"399006.SZ": u'创业板', "399902.SZ": '中证流通',
                      "SPBCNCOT.SPI": '标普中国债券指数', "NH0100.NHF": '南华商品指数',
                       "049.CS": '中债银行间债券总财富(总值)指数', "884100.WI": '新三板指数',
                       "885006.WI": u'混合债券型一级基金指数', "885007.WI": '长期纯债型基金指数',
                       "885008.WI": '混合债券型二级基金指数'}
        indexFileNameDict = {u"000016.SH": u'sz50', u"000905.SH": u'zz500', u"000001.SH": u'szzs',
                             u"000300.SH": u'hs300', u"399005.SZ": u'zxb', u'000852.SH': u'zz1000',
                             u"399001.SZ": u'szcz', u"399006.SZ": u'cyb'}
        # 设置指数数据
        w.start()
        if indexCodeList == []:
            indexCodeList = [u"000016.SH", u"000905.SH", u"000001.SH", u"000300.SH",
                             u"399005.SZ", u'000852.SH', u"399001.SZ", u"399006.SZ"]
        # 指数数据下载
        fieldName = [u"open", u"high", u"low", u"close", u"volume"]
        # fieldName = [u"open", u"close"]
        joinFieldName = ','.join(fieldName)
        fieldCNName = [u'开盘', u'最高', u'最低', u'收盘', u'成交量']
        colName2 = copy.deepcopy(fieldCNName)
        colName2.insert(0, u'Date')
        for i in range(len(indexCodeList)):
            dataSet = {}
            # 按年下载数据
            yearNum = int(startDate.split(u'-')[0])
            yearTradedaySet = {}
            tempStartDate = startDate
            latestYearNum, lastEndDate = int(endDate.split(u'-')[0]), int(endDate.split(u'-')[2])
            while yearNum <= latestYearNum:
                if yearNum == int(latestYearNum):
                    tempEndDate = endDate
                else:
                    tempEndDate = str(yearNum) + u'-12-31'
                yearTradedayList = w.tdays(tempStartDate, tempEndDate, u"").Data[0]
                yearFirstTradeday = yearTradedayList[0].date()
                yearLastTradeday = yearTradedayList[-1].date()
                # 下载数据
                indexResult = w.wsd(indexCodeList[i], joinFieldName, str(yearFirstTradeday),
                                    str(yearLastTradeday), u"PriceAdj=F")
                dataSet[str(yearNum)] = indexResult
                yearTradedaySet[str(yearNum)] = yearTradedayList
                yearNum += int(1)
                tempStartDate = str(yearNum) + u'-01-01'
            print(cnIndexDict[indexCodeList[i]] + u'全时间段下载完毕')
            # 创建文件
            tempDf = pd.DataFrame()
            strFilename = os.getcwd() + u'/DownloadedData/OriginalData/' + cnIndexDict[indexCodeList[i]] + \
                          startDate + r'-' + str(lastTradeday) + u'.csv'
            self.processedFilenameDict[indexFileNameDict[indexCodeList[i]]] = strFilename
            csvFile = open(strFilename, u'w', newline=u'')
            csvWriter = csv.writer(csvFile)
            csvWriter.writerow(colName2) # 第一行股票代码
            for key in yearTradedaySet: # 不同年
                csvTradedayList = yearTradedaySet[key]
                for j in range(len(csvTradedayList)):
                    row = [i[j] for i in dataSet[key].Data]  # j 时间段所有股票代码数据
                    row.insert(0, csvTradedayList[j].date())
                    csvWriter.writerow(row)
                    tempDf = tempDf.append([row])
            csvFile.close()
        w.stop()
        print(u'所有指数数据下载完毕')

    def download_fundData(self, fundCodeList = []):
        # 内置参数
        startDate = copy.deepcopy(self.startDate)
        endDate = copy.deepcopy(self.endDate)
        lastTradeday = copy.deepcopy(self.lastTradeday)
        cnIndexDict = {}
        for i in range(len(fundCodeList)):
            if fundCodeList[i][-3] == '.':
                cnIndexDict[fundCodeList[i]] = fundCodeList[i][:-3]
        # 设置指数数据
        w.start()
        # 指数数据下载
        fieldName = ["NAV_acc", "NAV_adj", "nav", u'fund_investobject']
        joinFieldName = ','.join(fieldName)
        fieldCNName = ['单位净值', '累计单位净值', '复权单位净值', u'投资目标']
        colName2 = copy.deepcopy(fieldCNName)
        colName2.insert(0, u'Date')
        for i in range(len(fundCodeList)):
            dataSet = {}
            # 按年下载数据
            yearNum = int(startDate.split(u'-')[0])
            yearTradedaySet = {}
            tempStartDate = startDate
            latestYearNum, lastEndDate = int(endDate.split(u'-')[0]), int(endDate.split(u'-')[2])
            while yearNum <= latestYearNum:
                if yearNum == int(latestYearNum):
                    tempEndDate = endDate
                else:
                    tempEndDate = str(yearNum) + u'-12-31'
                yearTradedayList = w.tdays(tempStartDate, tempEndDate, u"").Data[0]
                yearFirstTradeday = yearTradedayList[0].date()
                yearLastTradeday = yearTradedayList[-1].date()
                # 下载数据
                indexResult = w.wsd(fundCodeList[i], joinFieldName, str(yearFirstTradeday),
                                    str(yearLastTradeday), u"PriceAdj=F")
                dataSet[str(yearNum)] = indexResult
                yearTradedaySet[str(yearNum)] = yearTradedayList
                yearNum += int(1)
                tempStartDate = str(yearNum) + u'-01-01'
            print(cnIndexDict[fundCodeList[i]] + u'全时间段下载完毕')
            # 创建文件
            tempDf = pd.DataFrame()
            strFilename = os.getcwd() + u'/DownloadedData/OriginalData/' + cnIndexDict[fundCodeList[i]] + \
                          startDate + r'-' + str(lastTradeday) + u'.csv'
            # self.processedFilenameDict[indexFileNameDict[indexCodeList[i]]] = strFilename
            csvFile = open(strFilename, u'w', newline=u'')
            csvWriter = csv.writer(csvFile)
            csvWriter.writerow(colName2)   # 第一行股票代码
            for key in yearTradedaySet:   # 不同年
                csvTradedayList = yearTradedaySet[key]
                for j in range(len(csvTradedayList)):
                    row = [i[j] for i in dataSet[key].Data]  # j 时间段所有股票代码数据
                    row.insert(0, csvTradedayList[j].date())
                    csvWriter.writerow(row)
                    tempDf = tempDf.append([row])
            csvFile.close()
        w.stop()
        print(u'所有指数数据下载完毕')

    def downloadIfSetor(self, sectorid=[]):
        # 内置参数
        startDate = copy.deepcopy(self.startDate)
        lastTradeday = copy.deepcopy(self.lastTradeday)
        stockCodelist = copy.deepcopy(self.stockCodelist)
        sectorCNDict = {u'1000006526000000': u'ST股判断条件', u'1000000090000000': u'沪深300判断条件',
                        u'1000008491000000': u'中证500判断条件', u'1000000087000000': u'上证50判断条件',
                        u'1000012163000000': u'中证1000判断条件'}
        sectorFileDict = {u'1000006526000000': u'is_be_st', u'1000000090000000': u'isHS300',
                          u'1000008491000000': u'isZZ500',  u'1000000087000000': u'isSZ50',
                          u'1000012163000000': u'iszz1000'}
        # 设置1，0 判断条件
        w.start()
        if sectorid == []:
            sectorid = [u'1000006526000000', u'1000000090000000', u'1000008491000000', u'1000000087000000']
        colName = copy.deepcopy(stockCodelist)
        colName.insert(0, u'Date')
        # xx板块股票代码下载
        for q in range(len(sectorid)):
            periodType = 'M'
            dataSet = {}
            uniPeriodList = self.uniPeriodListDcit[periodType]
            periodList = self.periodListDcit[periodType]
            tempDateRecord = []
            for p in range(len(uniPeriodList)):  # 周期迭代
                period = uniPeriodList[p]
                periodLocSer = pd.Series(periodList.loc[period])
                tempSlice = periodList.index.get_loc(period)
                if len(periodLocSer) >= 2:
                    stLoc = tempSlice.start
                    enLoc = tempSlice.stop - 1
                else:
                    stLoc = tempSlice
                    enLoc = tempSlice
                dateRange = [self.strDayList[stLoc], self.strDayList[enLoc]]
                # 下载数据
                tempSTDataSet = w.wset(u"sectorconstituent", u"date=" + dateRange[-1]
                       + u";sectorid=" + sectorid[q] + u";field=date,wind_code")
                stCode = tempSTDataSet.Data[1]
                tempStockCode = copy.deepcopy(stockCodelist)
                for j in range(len(tempStockCode)):
                    if stCode.count(tempStockCode[j]) == int(1):
                        tempStockCode[j] = 1
                    else:
                        tempStockCode[j] = 0
                dataSet[period] = tempStockCode
                tempDateRecord.append(dateRange[-1])
            print(sectorCNDict[sectorid[q]] + u'全时间段下载完毕')  # 保存数据至csv文件
            strFilename = os.getcwd() + u'/DownloadedData/OriginalData/' + sectorCNDict[sectorid[q]] + \
                          startDate + u'-' + str(lastTradeday) + u'.csv'
            self.processedFilenameDict[sectorFileDict[sectorid[q]]] = strFilename
            # 创建文件
            csvFile = open(strFilename, u'w', newline=u'')
            csvWriter = csv.writer(csvFile)
            csvWriter.writerow(colName)  # 第一行股票代码
            for j in range(len(uniPeriodList)):  # 不同周期
                row = dataSet[uniPeriodList[j]]  # j 时间段所有股票代码数据
                row.insert(0, tempDateRecord[j])
                csvWriter.writerow(row)
            csvFile.close()
        w.stop()
        print(u'所有板块数据下载完毕')

    def downloadSimpleData(self, simpleDataList):
        '''
        下载简单数据，比如ipo_date
        :param simpleDataList:
        :return:
        '''
        cnSimDict = {'ipo_date': u'ipo_date'}
        simFileNameDict = {'ipo_date': u'ipo_date'}
        # 设置参数
        startDate = copy.deepcopy(self.startDate)
        lastTradeday = copy.deepcopy(self.lastTradeday)
        stockCodelist = copy.deepcopy(self.stockCodelist)
        joinStockCode  = u','.join(stockCodelist)
        colName = stockCodelist
        colName.insert(0, 'Date')
        # 下载数据
        w.start()
        for i in range(len(simpleDataList)):
            tempDf = pd.DataFrame()
            strFilename = os.getcwd() + u'/DownloadedData/OriginalData/' + cnSimDict[simpleDataList[i]] + \
                          startDate + u'-' + str(lastTradeday) + u'.csv'
            self.processedFilenameDict[simFileNameDict[simpleDataList[i]]] = strFilename
            csvFile = open(strFilename, 'w', newline='')
            csvWriter = csv.writer(csvFile)
            csvWriter.writerow(colName)
            if simpleDataList[i] in ['ipo_date']: # ipo只需要最新时间的数据
                result = w.wsd(joinStockCode, simpleDataList[i], str(lastTradeday), str(lastTradeday), "")
                row = [str(i.date()) for i in result.Data[0]]
                row.insert(0, str(lastTradeday))
            csvWriter.writerow(row)
            tempDf = tempDf.append([row])
            print(cnSimDict[simpleDataList[i]] + u'全时段数据下载完成')
        w.stop()
        print(u'简单数据下载完成')

    def downloadReportData(self, rptDateProcess = []):
        '''
        财报数据下载和披露日处理
        :param rptDateProcess:
        :return:
        '''
        # 内置参数
        startDate = copy.deepcopy(self.startDate)
        tradedayList = copy.deepcopy(self.tradedayList)
        lastTradeday = copy.deepcopy(self.lastTradeday)
        stockCodelist = copy.deepcopy(self.stockCodelist)
        # 设置参数
        if rptDateProcess == []: # 披露日数据处理
            pass

    def sectorClassify(self, outPutPath):
        ''' 行业分类'''

        # 内置参数
        cnMarketDict = {u'industryType=1': u'申万行业一级', u'industryType=2': u'申万行业二级',
                        u'industryType=3': u'申万行业三级'}
        mktFileNameDict = {u'industryType=1': u'industryType=1', u'industryType=2': u'industryType=2',
                           u'industryType=3': u'industryType=3'}
        periodType = 'M'
        uniPeriodList = self.uniPeriodListDcit[periodType]
        periodList = self.periodListDcit[periodType]
        stockCodelist = copy.deepcopy(self.stockCodelist)
        colName = copy.deepcopy(stockCodelist)
        joinStockCode = u','.join(stockCodelist)
        indexTypeList = [i for i in cnMarketDict]
        w.start()       #下载数据
        for i in range(len(indexTypeList)):
            dataSet = {}
            tempDateRecord = []
            for p in range(len(uniPeriodList)):  # 周期迭代
                period = uniPeriodList[p]
                periodLocSer = pd.Series(periodList.loc[period])
                tempSlice = periodList.index.get_loc(period)
                if len(periodLocSer) >= 2:
                    stLoc = tempSlice.start
                    enLoc = tempSlice.stop - 1
                else:
                    stLoc = tempSlice
                    enLoc = tempSlice
                dateRange = [self.strDayList[stLoc], self.strDayList[enLoc]]
                # 下载数据
                downLoadFormula = 'w.wsd(joinStockCode, "industry_swcode", dateRange[-1], dateRange[-1],'
                result = eval(downLoadFormula + 'indexTypeList[i])')
                dataSet[period] = result
                tempDateRecord.append(dateRange[-1])
                print(period)
            print(u'%s全时间段下载完毕' % indexTypeList[i])  # 保存数据至csv文件
            strFilename = outPutPath + u'/' + cnMarketDict[indexTypeList[i]] + \
                          self.startDate + u'-' + str(self.lastTradeday) + u'.csv'
            self.processedFilenameDict[mktFileNameDict[indexTypeList[i]]] = strFilename
            csvFile = open(strFilename, u'w', newline=u'')
            csvWriter = csv.writer(csvFile)
            csvWriter.writerow(colName)  # 第一行股票代码
            for j in range(len(uniPeriodList)):  # 不同周期
                row = dataSet[uniPeriodList[j]].Data[0]  # j 时间段所有股票代码数据
                row.insert(0, tempDateRecord[j])
                csvWriter.writerow(row)
            csvFile.close()
        w.stop()    # 函数结束


    def CNtoENFilename(self):
        '''
        将中文文件名转化为英文文件名
        :return:
        '''
        cwd = os.getcwd()
        for key in self.processedFilenameDict:
            filename = self.processedFilenameDict[key]
            trainFile = filename
            tmpTargetFile = cwd + u'/DownloadedData/ENData/' + key + ".csv"
            shutil.copyfile(trainFile, tmpTargetFile)
        print(u'英文文件拷贝完成')
        return self.processedFilenameDict



'''Test code'''
if __name__== u"__main__":

    def functionForTesting1(timeRange, indexNameList, indexCodeList, fundCodeList, sectorid, simpleDataList, stockCodelist):
        '''
        funciton for test
        :return:
        '''
        testClass = DataDownloader(stockCodelist=stockCodelist, startDate=timeRange[0], endDate=timeRange[-1])
        # testClass.download_marketData(indexNameList=indexNameList)
        # testClass.download_indexData(indexCodeList=indexCodeList)
        # testClass.download_fundData(fundCodeList=fundCodeList)
        testClass.downloadIfSetor(sectorid=sectorid)
        # testClass.downloadSimpleData(simpleDataList=simpleDataList)
        # testClass.sectorClassify()
        testClass.CNtoENFilename()

    def functionForTesting2(timeRange, indexNameList, marketGroupCode, stockCodelist):
        '''
        funciton for test
        :return:
        '''
        testClass = DataDownloader(marketGroupCode=marketGroupCode,
                                   stockCodelist=stockCodelist, startDate=timeRange[0], endDate=timeRange[-1])
        # testClass.download_marketData(indexNameList = indexNameList)
        # testClass.download_indexData(indexCodeList=indexCodeList)
        # testClass.downloadIfSetor(sectorid=sectorid)
        # testClass.downloadSimpleData(simpleDataList=simpleDataList)
        testClass.CNtoENFilename()

    # # 基金
    # a = pd.read_excel('fundList.xlsx')
    # badName = a['good'].tolist()
    # tempName1 = [i.upper() for i in badName]
    # finalName1 = copy.deepcopy(tempName1)
    # for i in range(len(tempName1)):
    #     a = tempName1[i][0]
    #     if tempName1[i][0] == 'J':
    #         finalName1[i] = tempName1[i] + '.OF'
    #     else:
    #         finalName1[i] = tempName1[i] + '.XT'
    # fundCodeList = finalName1

    # 日期格式 u'2017-10-23'
    # 行情数据
    indexNameList1 = ['maxupordown']
    stockCodelist = list(pd.read_csv('stockCode2018-01-03.csv').columns)
    indexCodeList1 = [u"000001.SH", u"399001.SZ", u"399006.SZ"]
    sector = [u'1000000090000000']
    a1 = functionForTesting1(timeRange = [u'2013-01-23', u'2018-01-23'], indexNameList= indexNameList1,
                            indexCodeList = indexCodeList1, fundCodeList=[], sectorid=sector,
                             simpleDataList=[u'ipo_date'], stockCodelist=stockCodelist)
    #
    # # 其他数据
    # indexNameList1 = [u"PB_LF", u'PS_TTM', 'pct_chg', 'maxupordown']
    # stockCodelist = []
    # a2 = functionForTesting2(timeRange=[u'2015-01-09', u'2017-12-22'], indexNameList=indexNameList1, stockCodelist=stockCodelist, marketGroupCode='1000018860000000')