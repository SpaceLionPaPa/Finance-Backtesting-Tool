#!/usr/bin/python
# -*- coding: utf-8 -*-

# fund scenario strategy
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from datetime import date
import sys
if sys.getdefaultencoding() != 'utf-8':# 重新设置python 编码为utf-8
    reload(sys)
    sys.setdefaultencoding('utf-8')
import copy
from Templete.Constant import *
from Templete.StrategyTemplete1 import StrategyTemplete1
from Templete.Utilities import *

class MyStrategy(StrategyTemplete1):
    '''
    每季度
    低于历史均值买入，高于历史均值卖出
    '''

    def __init__(
            self, tester, strategyNamelist, testContent,
            feeRatio, timeSeriesFile, initFund, startTime,
            dataSourceType, timeFreq, impactCostRatio):

        super(MyStrategy, self).__init__(
            tester, strategyNamelist, testContent,
            feeRatio, timeSeriesFile, initFund, startTime,
            dataSourceType, timeFreq, impactCostRatio)

        self.periodTypeList = [u'D', u'M', u'Q']
        self.todayDate = date.today().isoformat()

    def TrimData(self):
        '''
        清洗和预处理数据
        :return: 处理好的 pandas.Dataframe
        '''
        # 数据导入
        self.roa = copy.deepcopy(self.index_DF(u'roa', objectType=u'stocks'))
        # self.earningYield = copy.deepcopy(1/self.index_DF(u'peg', objectType=u'stocks'))
        self.peg = copy.deepcopy(self.index_DF(u'peg', objectType=u'stocks'))
        cleanedDf = self.cleanedDf()
        close = self.index_DF('close', u'stocks')

        mvWindow = 252
        self.mvStd = pd.DataFrame(index=cleanedDf.index, columns=cleanedDf.columns)
        self.mvMean = pd.DataFrame(index=cleanedDf.index, columns=cleanedDf.columns)
        for i in self.mktCodeIndex('stocks'):
            print(u'清洗数据中，列为%s' % i)
            cleaned_series = cleanedDf[i].dropna()                                           # 剔除停牌的时间点---volume等于0
            c = close[i][cleaned_series.index]
            self.mvStd[i] = pd.Series(c.rolling(int(mvWindow)).std(), index=cleanedDf.index)
            self.mvMean[i] = pd.Series(c.rolling(int(mvWindow)).mean(), index=cleanedDf.index)

        # self.targetDf = self.roa.rank(axis=1, ascending=False) + self.earningYield.rank(axis=1, ascending=False)
        # self.stm_issuingdate = read_csvEx('stm_issuingdate_stocks.csv', indexCol=0)
        self.mvStd.to_csv('../Data/Main/mvStd.csv')
        self.mvMean.to_csv('../Data/Main/mvMean.csv')
        return super(MyStrategy, self).TrimData()

    def onBar(self):
        '''
        切片操作函数
        '''
        curDate = self.TradeTime
        if curDate == '2012-08-29':
            pass
        print(curDate)
        if curDate == self.testTimeList()[0]:
            self.addBuffer('amtSeries', pd.Series([]))

        # month = self.getPeriod(freq=u'M').split('-')[-1]
        direction = 1
        rankTarget1 = self.roa.loc[self.getTime(ref=1)].sort_values(ascending=False)[:50]
        peg = self.peg.loc[self.getTime(ref=1)].loc[rankTarget1.index]
        rankTarget = peg[peg > 0]
        # rankTarget = self.earningYield.loc[self.getTime(ref=1)].loc[rankTarget.index]
        yzb = self.index(u'yiziban', u'stocks')
        yzb = yzb[yzb == 1].index
        tradeIndex = self.cleanedDf().loc[curDate].dropna().index
        targetIndex = rankTarget[rankTarget.index.difference(yzb) & tradeIndex].sort_values(ascending=True)[:20].index  # 小值排前面

        ref_C = self.index_DF(u'close', u'stocks').loc[self.getTime(ref=1)][targetIndex]
        mvStd = self.index_DF(u'mvStd', u'stocks').loc[self.getTime(ref=1)][targetIndex]
        mvMean = self.index_DF(u'mvMean', u'stocks').loc[self.getTime(ref=1)][targetIndex]
        buyBias, sellBias = 1.0, 0.5

        sell = ref_C[(ref_C - mvMean) > sellBias * mvStd].index & self.getPositionIndex(direction, u'stocks')
        print(u'卖出%s ' % sell)

        # 卖出
        if not sell.empty:
            holdingVol = self.getPositionData(direction, u'stocks', 1)
            mvCost = self.getPositionData(direction, u'stocks', 2)
            amtSeries = mvCost * holdingVol[sell]
            self.addBuffer('amtSeries', amtSeries)
            self.closePosition(direction, u'stocks', targetIndex=sell, priceMode=1, volMode=1)    # 开盘卖出u

        # 买入
        holding = self.getPositionIndex(direction, u'stocks')  # 最新持仓
        buy = ref_C[(ref_C - mvMean) < -buyBias * mvStd].index.difference(holding)
        if not buy.empty:
            pass
        print(u'买入%s' % buy)
        if holding.empty:   # 空仓直接买入
            if not buy.empty:
                openOrder = self.orderRecorder(1, buy, u'stocks', direction)
                priceMode, volMode, sliceFund = 1, 1, self.initFund
                self.smartSendOrder(openOrder, priceMode, volMode, sliceFund)
        else:
            # addSeries = self.refBuffer('amtSeries', ref=0)
            # addLen = len(amtSeries.index)
            # if not addSeries.index.empty:
            if not buy.empty:    # 财报披露后批量换仓
                # closingAmt = addSeries.sum()
                closingAmt = self.getAvailableCapital()
                if closingAmt > 0:
                    addIndex = buy  # 补位标的
                    addNum = len(addIndex)
                    eachAmt = float(closingAmt) / addNum
                    priceMode, volMode = 1, 0
                    if not addIndex.empty:
                        for i in addIndex:  # 买入报单
                            self.tempVol = eachAmt / self.index(u'close', u'stocks')[i]
                            openOrder = self.orderRecorder(1, pd.Index([i]), u'stocks', direction)  # 记录交易单
                            self.smartSendOrder(openOrder, priceMode, volMode)

        # 输出最新一期标的
        if self.TradeTime == self.time_List()[-1]:
            pass
        pass  # 切片完成

    def mySendOrderF(self, theoreticalSell, targetIndex, holdingBuy,):
        # 买入标的报单
        direction = 1
        priceMode, volMode, sliceFund = 1, 0, self.initFund     # 开盘换仓
        price = (1 + self.openFeeRatio) * self.index(u'open', u'stocks')[targetIndex]
        openVolume = np.floor(sliceFund / float(len(price)) / price / 1.) * int(1)
        holdingVol = self.getPositionData(direction, u'stocks', dataType=1)

        if not theoreticalSell.empty:  # 卖出报单
            self.closePosition(direction, u'stocks', targetIndex=theoreticalSell, priceMode=priceMode, volMode=1)

        for i in targetIndex:  # 买入报单
            openVol = openVolume.loc[i]
            if i in holdingBuy:
                toBuyVol = openVolume.loc[i]
                holdingBuyVol = holdingVol.loc[i]
                openVol = toBuyVol - holdingBuyVol
                if openVol < 0:  # 减仓
                    reduceVolume = abs(openVol)
                    self.tempVol = reduceVolume
                    self.closePosition(direction, u'stocks', pd.Index([i]), priceMode, volMode)

            # 开仓买入
            if openVol >= 0.:
                self.tempVol = openVol
                openOrder = self.orderRecorder(1, pd.Index([i]), u'stocks', direction)  # 记录交易单
                self.smartSendOrder(openOrder, priceMode, volMode)

    def volF(self):
        ''' 自定义成交量函数 '''
        volume = copy.deepcopy(self.tempVol)
        self.tempVol = None
        return volume

# 测试用例 ---------------------------------------------------------------------------
def aTestCase(primaryDate, startTime, endTime):
    '''
    funciton for test
    :return:
    '''
    time1 = time.clock()
    benchmarkName = u'wind_a_stocks'
    timeSeriesFile = u'volume_cleaned_stocks.csv'
    fund = int(10000000)
    feeRatio = [0.002, 0.002]
    impactCostRatio = [0., 0.]
    timeFreq = u'D'                                 # 回测时间类型 {u'min':分钟, u'D':日线}
    indexNameList = [u'volume_stocks', u'open_stocks', u'high_stocks',
                     u'low_stocks', u'close_stocks', u'roa_stocks',
                     u'peg_stocks', u'yiziban_stocks']
    stockCodelist = []
    strategyNamelist = [u'magicalFormula2']
    tester = u'李琦杰'
    testContent = u'magicalFormula'
    dataSourceType = DATA_SOURCE_CSV

    t1 = MyStrategy(tester, strategyNamelist, testContent, feeRatio, timeSeriesFile,
                    fund, startTime, dataSourceType, timeFreq, impactCostRatio)
    t1.loadData(primaryDate, endTime, stockCodelist, indexNameList, benchmarkName)
    t1.TrimData()
    t1.Trading()
    t1.outPut()
    print('回测完成')
    timeCost = (time.clock() - time1)/60.
    print("回测共耗时 %.1f 分钟" % timeCost)

#
#
if __name__== u"__main__":
    # aTestCase(u'2010-06-01', u'2012-04-27', u'2012-05-07')  # 日期格式u'2017-09-04'
    aTestCase(u'2010-06-01', u'2012-04-27', u'2017-07-31')  # 日期格式u'2017-09-04'