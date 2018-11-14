# -*- coding:utf-8 -*-

# T1 strategy
import copy
import pandas as pd
import numpy as np
from Templete.Constant import *
from Templete.TempleteTools import dfComparing
from Templete.StrategyTemplete1 import StrategyTemplete1
from Templete.Utilities import *


class MyStrategy(StrategyTemplete1):

    def __init__(
            self, tester, strategyNamelist, testContent,
            buyFee, sellFee, initFund, startTime, dataSourceType, timeFreq):

        super(MyStrategy, self).__init__(
            tester, strategyNamelist, testContent,
            buyFee, sellFee, initFund, startTime, dataSourceType, timeFreq)
        self.AtrLength = int(10)
        self.SKlength = int(30)
        self.SKRange = int(10)
        self.range = int(10)
        self.TEntryLength = int(3)
        self.BarInter = None
        self.EntryIvLength = int(30)
        self.ADDratio = 0.2
        self.ExitTime = int(5)

    def TrimData(self):
        '''
        清洗和预处理数据
        :return: 处理好的 pandas.Dataframe
        '''
        # TODO: 1. 数据清洗和预处理
        cleanedDf = self.cleanedDf()

        # 计算其他指标值
        self.atr = pd.DataFrame(index=cleanedDf.index, columns=cleanedDf.columns)
        self.EntrySht = pd.DataFrame(index=cleanedDf.index, columns=cleanedDf.columns)
        self.EntryLngLv = pd.DataFrame(index=cleanedDf.index, columns=cleanedDf.columns)
        self.ZDFlag = pd.DataFrame(index=cleanedDf.index, columns=cleanedDf.columns)
        for i in self.mktCodeIndex(objectType=u'stocks'):
            cleaned_series = cleanedDf[i].dropna()                                           # 剔除停牌的时间点---volume等于0
            h, l, o, c = [self.index_DF(j, objectType=u'stocks')[i][cleaned_series.index] for j in [u'high', u'low', u'open', u'close']]
            # atr
            tempDf1 = h - l
            tempDf2 = np.abs(c.shift(1) - h)
            tempDf3 = np.abs(c.shift(1) - l)
            mtr = dfComparing(tempDf1, tempDf2, tempDf3)
            self.atr[i] = pd.Series(mtr.rolling(int(self.AtrLength)).mean(), index=cleanedDf.index)
            # EntrySht
            self.EntrySht[i] = pd.Series(h.shift(1).rolling(self.TEntryLength).max() * (1 - self.range / float(100)), index=cleanedDf.index)
            # EntryLngLv
            self.EntryLngLv[i] = pd.Series(l.shift(1).rolling(self.EntryIvLength).min(), index=cleanedDf.index)

            tempSeries = c                                                                  # 挑出符合条件的时间序列
            hs300 = self.index_DF(u'hs300', u'stocks')[u'close'][cleaned_series.index]      ##  沪深300每天收盘价
            tempCha = tempSeries/hs300 * 3000                                               ##  个股收盘价/HS300*3000
            prehiband = tempCha.shift(1).rolling(self.SKlength).max()
            preLoband = tempCha.shift(1).rolling(self.SKlength).min()
            R1 = prehiband.rolling(self.SKlength).max() - preLoband.rolling(self.SKlength).min()
            R2 = h.shift(1) - l.shift(1)
            SumR1 = R1 * self.SKlength
            SumR2 = R2.rolling(self.SKlength).sum() / hs300 * 3000
            RealR = SumR2 / SumR1 * 100
            longCond1 = RealR.shift(1) > self.SKRange
            longCond2 = RealR < self.SKRange
            comb1Cond = longCond1 & longCond2
            result = comb1Cond.replace({True: 1, False: 0})
            self.ZDFlag[i] = pd.Series(result, index=cleanedDf.index)
        # self.ZDFlag = self.ZDFlag.fillna(method='ffill')
        return super(MyStrategy, self).TrimData()

    def onBar(self):
        '''
        切片操作函数
        '''
        # TODO: 2. 选股处理
        # 2.1 设置参数
        priceMode, volMode, singleAmt = 0, 2, 100000.
        ZDFlag = self.ZDFlag.loc[self.TradeTime]
        EntrySht = self.EntrySht.loc[self.TradeTime]
        h_1 = self.index(u'high', objectType=u'stocks', ref=1)
        l_1 = self.index(u'low', objectType=u'stocks', ref=1)
        l = self.index(u'low', objectType=u'stocks')
        print(self.TradeTime)
        # 2.2 策略选股
        if self.TradeTime >= self.testTimeList()[1]:                            # 第二天以后
            holding = self.getPositionIndex(direction=1, objectType=u'stocks')  # 最新持仓
            # 今开仓
            tempIndex1 = ZDFlag[ZDFlag == 1].index
            tempIndex2 = h_1[h_1 != l_1].index
            tempIndex3 = l[l < EntrySht].index
            sigBuy = tempIndex1 & tempIndex2 & tempIndex3                       # 信号给出开仓标的
            openIndex = sigBuy.difference(holding)                              # 今开仓
            addIndex = self.addPostion(sigBuy)                                  # 今加仓
            EntryLngLv = self.EntryLngLv.loc[self.TradeTime]
            high = self.index(u'high', objectType=u'stocks')
            closeIndex = holding & high[high > EntryLngLv].index                # 今平仓
            t1 = closeIndex
            t2 = holding

            # 2.3 建仓
            if not len(holding) >= int(20):
                # 2.3.1 第一次开仓
                if not openIndex.empty:
                    # for filterType in [1, 3]:                                     # filter剔除的是头一天
                    #     openIndex = self.filter(openIndex, filterType, objectType=u'stocks')
                    sz = self.ref(self.index_DF(u'mkt_cap_ard', objectType=u'stocks'), targetIndex=openIndex, offset=1) # 最后排名，选出前几只股票
                    openIndex = self.indexRank(openIndex, sz, rankType=1, rankNum=15)
                    openRecord = self.orderRecorder(action=1, codeIndex=openIndex, objectType=u'stocks', direction=1)
                    self.smartSendOrder(openRecord, priceMode, volMode, singleAmt)
                # 2.3.2 加仓
                if not addIndex.empty:
                    # for filterType in [1, 3]:                                     # filter剔除的是头一天
                    #     addIndex = self.filter(addIndex, filterType, objectType=u'stocks')
                    sz = self.ref(self.index_DF(u'mkt_cap_ard', objectType=u'stocks'), targetIndex=addIndex, offset=1)  # 最后排名，选出前几只股票
                    addIndex = self.indexRank(addIndex, sz, rankType=1, rankNum=15)
                    # 2.4.2
                    openRecord2 = self.orderRecorder(action=1, codeIndex=addIndex, objectType=u'stocks', direction=1)
                    self.smartSendOrder(openRecord2, priceMode, volMode, singleAmt)

            # 2.4 平仓
            if not closeIndex.empty:
                for filterType in [1]:  # 剔除停牌
                    closeIndex = self.filter(closeIndex, filterType, objectType=u'stocks')
                closeRecord = self.orderRecorder(action=-1, codeIndex=closeIndex, objectType=u'stocks', direction=1)
                self.smartSendOrder(closeRecord, priceMode, volMode=1)
        pass  # 切片完成

    def addPostion(self, targetIndex):
        ''' 加仓函数 '''
        posiontIndex = self.getPositionIndex(direction=1, objectType=u'stocks')
        tempIndex = targetIndex & posiontIndex
        if not tempIndex.empty:
            LastEntryPrice = pd.Series(
                [self.lastEntryPrice(direction=1, objectType=u'stocks', objectCode=i, priceType=0) for i in tempIndex],
                index=tempIndex)
            l = self.index(u'low', objectType=u'stocks')[tempIndex]
            cond1 = LastEntryPrice - self.ADDratio * self.atr.loc[self.TradeTime][tempIndex]
            return tempIndex[cond1 > l]
        else:
            return pd.Index([])

    def priceF(self, orderDict):
        ''' 自定义价格函数 '''
        objectCode = orderDict[u'objectCode']
        action = orderDict[u'action']
        # 开仓
        open = self.index(u'open', objectType=u'stocks')[objectCode]
        if action == 1:
            EntrySht = self.EntrySht.loc[self.TradeTime][objectCode]
            price = min(EntrySht, open)

        # 平仓
        elif action == -1:
            EntryLngLv = self.EntryLngLv.loc[self.TradeTime][objectCode]
            price = max(EntryLngLv, open)
        self.dataTarget_Price = price
        return price


# 测试用例 ---------------------------------------------------------------------------
def aTestCase(primaryDate, startTime, endTime):
    '''
    funciton for test
    :return:
    '''
    time1 = time.clock()
    benchmarkName = u'zz500_stocks'  # zz500, hs300, szzs, zxb, sz50
    fund = 2000000.
    feeRatio = [0.0005, 0.001]
    impactCostRatio = [0., 0.]
    timeFreq = u'D'  # 回测时间类型 {u'min':分钟, u'D':日线}
    indexNameList = [u'close_stocks', u'high_stocks', u'mkt_cap_ard_stocks',
                     u'low_stocks', u'open_stocks', u'volume_stocks',
                     u'turn_stocks', u'st_stocks', u'new_stock40_stocks',
                     u'yzb_stocks', u'zt_stocks', u'hs300_stocks']   #WIND indexNameList
    stockCodelist = []
    strategyNamelist = [u'搅拌机']
    tester = u'殷欣'
    testContent = u'搅拌机'
    dataSourceType = DATA_SOURCE_CSV

    t1 = MyStrategy(tester, strategyNamelist, testContent, openFeeRatio, closeFeeRatio, fund, startTime, dataSourceType, timeFreq)
    t1.loadData(primaryDate, endTime, stockCodelist, indexNameList, benchmarkName)
    t1.TrimData()
    t1.Trading()
    t1.outPut()
    print('回测完成')
    timeCost = (time.clock() - time1)/60.
    print("回测共耗时 %.1f 分钟" % timeCost)

#
#
#
if __name__== u"__main__":
    dataStartTime = u'2017-08-04'
    testStartTime = u'2017-11-06'
    testEndTime = u'2018-02-28'

    aTestCase(dataStartTime, testStartTime, testEndTime)  # 回测时间一定要大于数据起始时间1天