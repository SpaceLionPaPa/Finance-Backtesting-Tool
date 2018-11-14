# encoding=utf-8
# Program: 这是一个生成boolean值来筛选合格股票的程序
# Author: Qijie Li
# Date: Nov.18.2017

import datetime
import numpy as np
import pandas as pd
import copy
# from API.DataAPI import APIClass

# 同一股票数量的类
__mataclass__ = type # Use new type classes
class DataProcessing(object):
    CSV_PATH = u'../Data/Main/'
    TIME_SERIES_FILE = CSV_PATH + u'close_stocks.csv'
    INDEX_COLUMN_NO = 0

    def __init__(self, dataPath):
        DataProcessing.CSV_PATH = dataPath
        # self.APIeg = APIClass('../API/indexconfig.txt')

    def _read_csv(self, fileName, startTime=None, endTime=None):
        ''' 加载CSV文件，支持中文文件名称
        '''
        strFullFileName = DataProcessing.CSV_PATH + fileName + u'.csv'
        df = pd.read_csv(strFullFileName, parse_dates=True,
                         index_col=DataProcessing.INDEX_COLUMN_NO, encoding=u'gbk')
        # 截取目标时间段
        if startTime:
            temp1 = df.index.get_loc(startTime)
        else:
            temp1 = 0
        if endTime:
            temp2 = df.index.get_loc(endTime) + 1
        else:
            temp2 = None
        df = df.iloc[temp1:temp2]
        return df

    def ifTradingDay(self, startTime=None, endTime=None):
        '''
        对原始日线数据处理，生成是否是交易日的boolean值
        :return: 1 #是交易日， 0 #不是交易日
        '''
        volume = self._read_csv(u'volume_stocks', startTime, endTime)
        volume = volume.fillna(0)
        volume[np.abs(volume) > 0.] = 1
        volume[volume != 1] = 0
        volume.to_csv(DataProcessing.CSV_PATH + u'is_market_day_stocks.csv')

    def dfTimeMakeUp(self, targetDFName, fillMethod='bfill', startTime=None, endTime=None):
        '''
        对已有Pandas.Dataframe数据进行时间上的补全
        补全方法-fillna
        '''
        volume = self._read_csv(u'volume_stocks', startTime, endTime)
        targetDF = self._read_csv(targetDFName)
        if volume.columns.all() == targetDF.columns.all():
            tempDF = pd.DataFrame(index=volume.index, columns=volume.columns)
            targetTimeIndex = targetDF.index.astype(str)
            lastLoc = 0
            for i in range(len(targetTimeIndex)):
                tempDate = targetTimeIndex[i]
                nowLoc = volume.index.get_loc(tempDate)
                tempDF.loc[tempDate] = targetDF.loc[tempDate]
                if not i:
                    st = lastLoc
                    pass
                else:
                    st = lastLoc + 1
                tempDF[st:nowLoc+1] = tempDF[st:nowLoc+1].fillna(method=fillMethod)
                lastLoc = copy.deepcopy(nowLoc)
            tempDF.to_csv(DataProcessing.CSV_PATH + u'%s_stocks.csv' % targetDFName)
            print(u'%s 时间补全完毕' % targetDFName)
        else:
            print(u'Columns长度不一致')
            return

    def ifNewStock(self, startTime=None, endTime=None, newStockDays=40, tradeDaySource=u'Api'):
        '''
        对原始日线数据处理，生成是否是新股的boolean值
        用到交易所交易日--通过接口获取
        :return: 1 #新股(上市日期小于40或未上市)， 0 #非新股
        '''
        volume = self._read_csv(u'volume_stocks', startTime, endTime)
        timeIndex = volume.index.astype(str)
        length1 = len(timeIndex)
        firstDate = timeIndex[0]
        primaryDate = pd.date_range(end=firstDate, periods=1000, freq=u'D').astype(str)[0]
        if tradeDaySource == u'Wind':
            from WindPy import w
            w.start()
            prevList = pd.Index(w.tdays(primaryDate, firstDate, u"").Data[0][:-1]).astype(str)
            w.stop()
        elif tradeDaySource == u'Api':
            prevList = pd.Index(self.APIeg.getTradeDate(start=primaryDate, end=firstDate))
        else:  # 从本地读取交易所交易日
            prevList = self._read_csv(u'tradeDay_stocks').index.astype(str)
        tradedayList = prevList.union(timeIndex)  # 整个交易日Index
        newStockDF = copy.deepcopy(volume)
        IPODf = self._read_csv(u'ipo_date_stocks')
        IPOSeries = IPODf.iloc[0]
        stockName = IPOSeries.index

        firstDateLoc = tradedayList.get_loc(firstDate)
        newTimeList = tradedayList[(firstDateLoc - newStockDays):]
        length2 = len(newTimeList)
        prevFirstDate = newTimeList[0]
        for i in range(len(stockName)):
            stock = stockName[i]
            ipoDate = IPOSeries[i]
            tempSeries = copy.deepcopy(pd.Series(np.zeros(length1), index=timeIndex))
            if ipoDate > prevFirstDate:
                newTimeListLoc = newTimeList.get_loc(ipoDate)
                unNewDateLoc = min(len(newTimeList) - 1, newTimeListLoc + newStockDays)
                if unNewDateLoc <= length2 - 1 and unNewDateLoc >= newStockDays:
                    tempSeries[:newTimeListLoc] = 1
            newStockDF[stock] = pd.Series(tempSeries)
        newStockDF.to_csv(DataProcessing.CSV_PATH + u'is_new_stock_%s_stocks.csv' % newStockDays)

    def ifNormalMktValue(self, startTime=None, endTime=None):
        mktValues = self._read_csv(u'sz_total', startTime, endTime)
        pctValues = mktValues.rank(na_option='keep', numeric_only=True, pct=True, axis=1)
        pctList = [0.05, 0.1, 0.15]
        for i in range(len(pctList)):
            pctValues = pctValues[pctValues >= pctList[i]]
            pctValues = pctValues[pctValues <= 1 - pctList[i]]
            tempDf = pctValues > 0
            finalDf = tempDf.replace({True: 1, False: 0})
            finalDf.to_csv(
                DataProcessing.CSV_PATH + u'isNormalMktValues_stocks'
                + str(int(pctList[i]*100)) + '.csv')

    def ifPositivePE(self, startTime=None, endTime=None):
        mktValues = self._read_csv(u'PE_TTM', startTime, endTime)
        tempDf = mktValues >= 0
        finalDf = tempDf.replace({True:1, False:0})
        finalDf.to_csv(DataProcessing.CSV_PATH + u'isPositivePE_stocks.csv')

    # < ============================================= 涨停/跌停板函数 =========================================== >
    # def tradingBoards(self, limitType=1, pctLimit=0.0995, ST_pctLimit=0.0495):
    #     '''
    #     普通涨停函数
    #     :param limitType: 1 涨停； 2 跌停
    #     :param retParam1: 普通股票涨跌幅度限制
    #     :param retParam2: ST股票涨跌幅
    #     :return: 涨停或者跌停的股票
    #     '''
    #     st = self.st()
    #     pctCondition = self.pct()
    #     pctCondition = pctCondition.fillna(0)
    #     pctCondition[pctCondition == float(u'inf')] = 0
    #     if limitType == 1:
    #         limitStock = self.close()[pctCondition >= pctLimit].index  # 涨幅超过9.95%
    #         ST_limitStock = st & self.close()[pctCondition >= ST_pctLimit].index  # st股票涨幅超过4.95%
    #         allLimitStock = limitStock | ST_pctLimit
    #     if limitType == -1:
    #         limitStock = self.close()[pctCondition <= -pctLimit].index  # 跌幅超过9.5%
    #     return limitStock
    # <==================================================== 涨停/跌停函数 ===============================================>
    # def tradingBoards(self, pctLimit=0.0995, ST_pctLimit=0.0495, startTime=None, endTime=None):
    #     '''
    #     普通涨停/跌停板 函数
    #     :param limitType: 1 涨停； 2 跌停
    #     :param retParam1: 普通股票涨跌幅度限制
    #     :param retParam2: ST股票涨跌幅
    #     :return: 1：涨停； -1：跌停； 0：其他
    #     '''
    #     name = [u'high', u'low', u'is_be_st', u'pct_chg']
    #     data = {}
    #     for i in name:
    #         data[i] = self._read_csv(i, startTime, endTime)
    #     st = data[u'is_be_st']
    #     pct = data[u'pct_chg']
    #     pct = pct.fillna(0)
    #     pct[pct == float(u'inf')] = 0
    #     stPct = pct[st == 1]
    #     limitStock = copy.deepcopy(pct)
    #     ST_limitStock = copy.deepcopy(stPct)
    #     allLimitStock = copy.deepcopy(pct)
    #     limitStock[pct >= pctLimit * 100] = 1  # 涨幅超过9.95%
    #     limitStock[pct <= -pctLimit * 100] = -1  # 跌幅超过9.95%
    #     limitStock[abs(limitStock) != 1] = 0
    #     ST_limitStock[stPct >= ST_pctLimit * 100] = 1    # st股票涨幅超过4.95%
    #     ST_limitStock[stPct <= -ST_pctLimit * 100] = -1  # st股票跌幅超过4.95%
    #     ST_limitStock[abs(ST_limitStock) != int(1)] = 0
    #     allLimitStock[(limitStock == 1) | (ST_limitStock == 1)] = 1
    #     allLimitStock[(limitStock == -1) | (ST_limitStock == -1)] = -1
    #     allLimitStock[abs(allLimitStock) != 1] = 0
    #     allLimitStock.to_csv(DataProcessing.CSV_PATH + u'tradingBoards.csv')

    def ifYiziban(self, startTime=None, endTime=None):
        '''
        对原始日线数据处理，生成是否是一字板的boolean值
        :return: 1：一字板涨停； -1：一字板跌停； 0：其他
        '''
        name = [u'zt_stocks', u'high_stocks', u'low_stocks']
        data = {}
        for i in name:
            data[i] = self._read_csv(i, startTime, endTime)
        zt = data[u'zt_stocks']
        high = data[u'high_stocks']
        low = data[u'low_stocks']
        # 最高价等于最低价
        yiziban = copy.deepcopy(zt)        # YZB涨停/跌停的股票
        yiziban[high != low] = int(0)
        yiziban.to_csv(DataProcessing.CSV_PATH + u'yiziban_stocks.csv')

    def ifZRZT(self, startTime=None, endTime=None):
        '''
        自然涨停板
        :return: 1：一字板涨停； -1：一字板跌停； 0：其他
        '''
        name = [u'zt_stocks', u'yiziban_stocks', u'pct_stocks']
        data = {}
        for i in name:
            data[i] = self._read_csv(i, startTime, endTime)
        zt = data[u'zt_stocks']
        pct_chg = data[u'pct_stocks']
        yzb = data[u'yiziban_stocks']
        zrzt = copy.deepcopy(zt)           # 自然涨停
        zrzt[abs(yzb) == 1] = 0
        zrzt[pct_chg >= 11] = 0            # 剔除新股IPO第一天情况
        zrzt.to_csv(DataProcessing.CSV_PATH + u'zrzt_stocks.csv')

    def onceZTF(self, startTime=None, endTime=None):
        name = [u'high_stocks', u'close_stocks']
        data = {}
        for i in name:
            data[i] = self._read_csv(i, startTime, endTime)
        high = data[u'high_stocks']
        close = data[u'close_stocks']
        ZTPrice = close.shift(1).fillna(method='ffill') * 1.0995
        onceZT = high >= ZTPrice
        onceZT = onceZT.replace({True: int(1), False: int(0)})
        onceZT.to_csv(DataProcessing.CSV_PATH + u'once_zt_stocks.csv')
    # < ============================================= 涨停/跌停板函数 =========================================== >
    # def reciprocal_value(self):
    #     '''
    #     对因子取倒数
    #     :return:
    #     '''
    #     cwd = './Raw_Data_daily/取倒数'
    #     temp = list(os.walk(cwd))
    #     csvFile = [i for i in temp[0][2] if os.path.splitext(i)[1] == '.csv']
    #     fileName = [os.path.splitext(each)[0] for each in csvFile]
    #     cur_dir = os.getcwd()
    #     for j in range(len(fileName)):  # 读取csv文件
    #         dataName = fileName[j]
    #         totalPath = cwd + "/" + dataName + '.csv'
    #         tmpTargetFile = "temp.csv"  # 生成临时文件代替中文文件名
    #         strFullFileName = totalPath
    #         shutil.copyfile(strFullFileName, tmpTargetFile)
    #         df = self._read_csv(tmpTargetFile)
    #         newDf = 1/df
    #         # 输出到指定文件夹
    #         outputPath = cur_dir + cwd + '/倒数值'
    #         newDf.to_csv(outputPath + r'/' + dataName + u'_reciprocal' + '.csv')
    #         os.remove('temp.csv')
    #     pass # 程序结束

# Test code
if __name__ == "__main__":
    dataPath = u'../Data/Main/'
    test = DataProcessing(dataPath)
    b = test.ifNewStock(newStockDays=int(252), tradeDaySource=u'Wind')
    # d = test.ifTradingDay()
    # e = test.ifNormalMktValue()
    # f = test.ifPositivePE()
    # g = test.reciprocal_value()
    # h = test.tradingBoards()
    # i = test.ifYiziban()
    # j = test.ifZRZT()
    # k = test.onceZTF()
    #l = test.dfTimeMakeUp(targetDFName='sw1', fillMethod='bfill')

