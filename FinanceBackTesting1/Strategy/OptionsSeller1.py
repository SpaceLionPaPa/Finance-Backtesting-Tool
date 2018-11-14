# -*- coding:utf-8 -*-

# Towl strategy
import numpy as np
import copy
import pandas as pd
import time
from datetime import date
from Templete.TempleteTools import hisVolitility, biasF, strToDatetime
from Templete.Constant import *
from Templete.OptionsTools import timeValue
from Templete.StrategyTemplete1 import StrategyTemplete1
# import matplotlib.pyplot as plt




def unionRecord(*args):
    ''' 合并订单字典 '''
    recordDict = {}
    for record in args:
        for i in record:
            recordDict[i] = record[i]
    return recordDict

class MyStrategy(StrategyTemplete1):

    def __init__(
            self, tester, strategyNamelist, testContent,
            feeRatio, timeSeriesFile, initFund, startTime,
            dataSourceType, timeFreq, impactCostRatio):
        ''' {'Vlength': '成交量周期',
            'SlopeLength': '计算斜率周期',
            'range': '斜率阀值',
            'len': '通道周期',
            'AtrRange': 'ATR阀值',
            'R': '进场斜率阀值',
            'ADDratio': '加仓幅度',
            'ExitTime': '离场时间'} '''
        super(MyStrategy, self).__init__(
            tester, strategyNamelist, testContent,
            feeRatio, timeSeriesFile, initFund, startTime,
            dataSourceType, timeFreq, impactCostRatio)

        self.periodTypeList = [u'D', u'M', u'A']

        self.dataTarget = u'50etf'
        self.uaTarget = u'50etf'

        # 期权参数
        self.priceG = 0.5           # 期权挡位
        self.closeingDays = 3       # 移仓天数
        self.marginRatio = 0.8      #
        self.M = 0.04
        self.todayDate = date.today().isoformat()

    def TrimData(self):
        '''
        清洗和预处理数据
        :return: 处理好的 pandas.Dataframe
        '''
        # 清洗和生成数据
        days = int(40)
        self.c = self.index_DF(self.dataTarget, objectType=u'stocks')[u'close']   # 标的收盘价
        self.ma = self.c.rolling(days).mean()
        self.volitility = hisVolitility(self.c, days)
        self.signal = pd.Series(index=self.c.index)
        cond1 = self.c.shift(1) > self.ma.shift(1) * (1 + self.M)
        cond2 = self.c.shift(1) < self.ma.shift(1) * (1 - self.M)
        self.signal[cond1] = 1                                                    # 看涨信号
        self.signal[cond2] = -1                                                   # 看跌信号
        self.signal = self.signal.fillna(0)                                       # 震荡信号
        self.priceGears()
        return super(MyStrategy, self).TrimData()

    def priceGears(self):
        ''' 期权挡位选择 '''
        closeDf = self.index_DF(u'close', objectType=u'options')
        exp_priceDf = pd.DataFrame(index=closeDf.index, columns=closeDf.columns)
        exp_priceDf.iloc[0] = self.index_DF(u'exe_price', objectType=u'options').iloc[0]
        exp_priceDf = exp_priceDf.fillna(method='ffill')
        cDf = pd.DataFrame(index=closeDf.index, columns=closeDf.columns)
        cDf.iloc[:, 0] = self.c
        cDf = cDf.fillna(axis=1, method='ffill')
        callOutPrcie = (exp_priceDf - cDf) / 0.05
        self.outpriceRatio = callOutPrcie
        self.outpriceRatio[self.callPutIndex(-1)] = -callOutPrcie[self.callPutIndex(-1)]
        self.omt1 = copy.deepcopy(self.volitility)
        self.omt2 = copy.deepcopy(self.volitility)
        cond2 = self.volitility <= 0.1
        cond3 = pd.eval('self.volitility > 0.1 & self.volitility <= 0.2')
        cond4 = pd.eval('self.volitility > 0.2 & self.volitility <= 0.4')
        cond5 = pd.eval('self.volitility > 0.4')
        self.omt1[pd.eval("cond2")] = 1.4; self.omt2[pd.eval("cond2")] = 2.6
        self.omt1[pd.eval("cond3")] = 1.8; self.omt2[pd.eval("cond3")] = 3.
        self.omt1[pd.eval("cond4")] = 2.6; self.omt2[pd.eval("cond4")] = 3.8
        self.omt1[pd.eval("cond5")] = 3.; self.omt2[pd.eval("cond5")] = 4.2

    def onBar(self):
        ''' 切片操作函数'''
        # TODO: 2. 选股处理
        print(self.TradeTime)
        targetDict = {}
        curMonthDict = {}
        inMonthDict = {}

        for i in [1, -1]:   # {1:call, -1:put}
            secIndex = self.index(u'sec_name', u'options')
            AIndex = secIndex[secIndex.str.contains(u'A')]
            tempOption = self.callPutIndex(i)
            # 1 近月合约/次近月合约
            recentMonth = (self.recentMonth(objectType=u'options') & tempOption).difference(AIndex.index)
            inMonth = self.inMonth(recentMonth)
            curMonthDict[i] = inMonth
            if inMonth.empty:           # 本月合约都已到期，换到次近月合约
                recentMonth_1 = (self.recentMonth(objectType=u'options', futureMonths=int(1)) & tempOption).difference(AIndex.index)  # 次近月合约
                inMonth = self.inMonth(recentMonth_1)
                inMonthDict[i] = inMonth

                # 2 期权虚值档位选择
            targetDict[i] = self.gearChooseing(inMonth)
        print('符合挡位字典%s' % targetDict)

        # 3 选择标的
        targetList = []
        # 看涨
        if self.signal.loc[self.TradeTime] == 1:                                            # signal用的T-1数据产生的信号
            if self.signal.loc[self.TradeTime] != self.signal.loc[self.getTime(ref=1)]:
                targetList.append(targetDict[-1])
                self.myOrders(targetList)
            else:
                newTargetDict = {-1:targetDict[-1]}
                self.shiftPosition(curMonthDict, inMonthDict, newTargetDict)                # 移仓

        # 看跌
        elif self.signal.loc[self.TradeTime] == -1:
            if self.signal.loc[self.TradeTime] != self.signal.loc[self.getTime(ref=1)]:
                targetList.append(targetDict[1])
                self.myOrders(targetList)
            else:
                newTargetDict = {1:targetDict[1]}
                self.shiftPosition(curMonthDict, inMonthDict, newTargetDict)                # 移仓

        # 震荡
        elif self.signal.loc[self.TradeTime] == 0:
            if self.signal.loc[self.TradeTime] != self.signal.loc[self.getTime(ref=1)]:
                for i in targetDict:
                    if targetDict[i]:
                        targetList.append(targetDict[i])
                self.myOrders(targetList)
            else:
                self.shiftPosition(curMonthDict, inMonthDict, targetDict)                     # 移仓

    def gearChooseing(self, target):
        ''' 挡位选择 '''
        lastLoc = self.getTime(ref=1)       # T-1 数据
        tempSeries = copy.deepcopy(self.outpriceRatio[target]).loc[lastLoc]
        omt1, omt2 = self.omt1.loc[lastLoc], self.omt2[lastLoc]
        tempSeries2 = tempSeries[pd.eval('tempSeries > omt1 & tempSeries < omt2')]
        result = None
        if not tempSeries2.empty:
            result = tempSeries2.sort_values(ascending=False).index[0]    # 选择档位在OMT1和OMT2之间的靠近OMT2的合约
            return result

    def inMonth(self, targetIndex):
        ''' 选出符合移仓条件的标的 '''
        lastTradeday = self.index_DF(u'lasttradingdate', u'options').iloc[0]
        tempSeries = lastTradeday[targetIndex]
        tempSeriesValue = pd.DatetimeIndex(tempSeries)
        curDate = strToDatetime(self.TradeTime).date()
        leftDays = pd.Series([(i.date() - curDate).days for i in tempSeriesValue], index=tempSeries.index)
        inMonth = leftDays[leftDays >= self.closeingDays].index
        return inMonth

    def myOrders(self, targetList):
        ''' 触发信号的选股函数和报单函数 '''
        holding = self.getPositionIndex(-1, u'options')
        # 平仓标的
        closeIndex = (holding).difference(pd.Index(targetList))
        self.closePosition(-1, u'options', closeIndex, priceMode=1, volMode=1)
        # 开仓标的
        for i in targetList:
            if i:
                openCode = i
                openVolume = self.initFund * self.marginRatio / float(self.maint_margin()[openCode])
                # 持有量判断
                if openCode in holding:
                    holdingVolume = self.getPosition(-1, u'options', openCode, dataType=1)
                    openVolume = openVolume - holdingVolume
                    if openVolume < 0:  # 减仓
                        reduceVolume = abs(openVolume)
                        self.tempVol = reduceVolume
                        self.closePosition(-1, u'options', pd.Index([openCode]), priceMode=1, volMode=0)
                # 买入标的
                if openVolume >= 0.:
                    self.tempVol = openVolume
                    openOrder = self.orderRecorder(1, openCode, u'options', -1)  # 记录交易单
                    self.smartSendOrder(openOrder, priceMode=1, volMode=0)

    def shiftPosition(self, curMonthDict, inMonthDict, targetDict):
        ''' 不触发信号的移仓函数 '''
        holding = self.getPositionIndex(-1, u'options')
        exe_Mode = self.index_DF(u'exe_mode', objectType=u'options').iloc[0]
        tempDict = {u'认购':1, u'认沽':-1}
        for i in holding:
            if i:
                callPutType = tempDict[exe_Mode[i]]
                if curMonthDict[callPutType].empty and i not in inMonthDict[callPutType]:                                     # 换月移仓--本月合约都已交割
                    self.closePosition(-1, u'options', pd.Index([i]), priceMode=1, volMode=1) # 平掉不符合月份的合约

        # 开仓符合月份的合约
        targetIndex = targetDict.values()
        if self.getPositionIndex(-1, u'options').empty:
            for i in targetIndex:
                if i:
                    callPutType = tempDict[exe_Mode[i]]
                    if curMonthDict[callPutType].empty:                                     # 换月移仓--本月合约都已交割
                        openVolume = self.initFund * self.marginRatio / float(self.maint_margin()[i])
                        self.tempVol = openVolume
                        openOrder = self.orderRecorder(1, i, u'options', -1)                # 记录交易单
                        self.smartSendOrder(openOrder, priceMode=1, volMode=0)

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
    benchmarkName = u'50etf_stocks'                                # zz500, hs300, szzs, zxb, sz50
    timeSeriesFile = u'close_options.csv'
    fund = int(5000000)
    feeRatio = [0.0005, 0.001]
    impactCostRatio = [0.001, 0.001]
    timeFreq = u'D'  # 回测时间类型 {u'min':分钟, u'D':日线}
    indexNameList = [u'open_options', u'high_options', u'low_options', u'close_options',
                     u'exe_price_options', u'exe_mode_options', u'50etf_stocks',
                     u'maint_margin_options', u'contractmultiplier_options',
                     u'lasttradingdate_options', u'sec_name_options']    # WIND indexNameList
    stockCodelist = []
    strategyNamelist = [u'OptionsSeller1']
    tester = u'李琦杰'
    testContent = u'期权卖方策略'
    dataSourceType = DATA_SOURCE_CSV

    t1 = MyStrategy(tester, strategyNamelist, testContent, feeRatio, timeSeriesFile, fund, startTime, dataSourceType, timeFreq, impactCostRatio)
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
#
if __name__== u"__main__":
    dataStartTime = u'2016-01-04'
    testStartTime = u'2016-04-11'
    testEndTime = u'2017-10-09'

    aTestCase(dataStartTime, testStartTime, testEndTime)  # 回测时间一定要大于数据起始时间1天