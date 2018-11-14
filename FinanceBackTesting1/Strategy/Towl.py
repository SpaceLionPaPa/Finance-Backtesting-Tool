# -*- coding:utf-8 -*-

# Towl strategy
import numpy as np
import copy
import pandas as pd
from datetime import date
import matplotlib.pyplot as plt
from Templete.StrategyTemplete1 import StrategyTemplete1
from Templete.OptionsTools import timeValue
from Templete.TempleteTools import *
from Templete.Constant import *



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
        self.Vlength = int(20)
        self.SlopeLength = int(5)
        self.Range = 2.
        self.Len = int(5)
        self.AtrRange = float(0)
        self.AtrLength = int(20)
        self.R = 50
        self.ADDratio = 1.
        self.ExitTime = int(5)
        self.dataTarget = u'sz50'
        self.uaTarget = u'50etf'
        self.todayDate = date.today().isoformat()

    def mvVol_F(self, targetName, Vlength=int(10)):
        ''' 成交量均线指标'''
        Vlength = int(Vlength)
        targetData = self.index_DF(targetName, objectType=u'stocks')
        volume, close = (targetData[i] for i in [u'volume', u'close'])
        preBaramount = close.shift(1) * volume.shift(1)
        preBarvolL = volume.shift(1).rolling(Vlength).sum()
        preBaramountL = preBaramount.rolling(Vlength).sum()
        self.MaTodayL = preBaramountL/preBarvolL
        return True

    def slope_F(self, targetName, SlopeLength, Range):
        ''' 斜率指标 '''
        SlopeLength = int(SlopeLength)
        Range = float(Range)
        targetData = self.index_DF(targetName, objectType=u'stocks')
        low, high, open, close = \
            (targetData[i] for i in [u'low', u'high', u'open', u'close'])
        self.slope = \
            (self.MaTodayL - self.MaTodayL.shift(SlopeLength)) / SlopeLength * 100
        SDvalue = low.shift(1) - self.MaTodayL
        LDvalue = self.MaTodayL - high.shift(1)

        realopenD_Shifted = \
            minToDaily_DF(open, type=u'open', dailyOffset=1)     # (日线开盘价)
        realCLoseD_Shifted = \
            minToDaily_DF(close, type=u'close', dailyOffset=1)   # (日线收盘价)

        self.SFlag = pd.Series(index=self.MaTodayL.index)
        longCond1 = SDvalue.rolling(SlopeLength).min() > 0.
        longCond2 = realopenD_Shifted / realCLoseD_Shifted > 0.96
        longCond3 = self.slope > Range
        comb1Cond = longCond1 & longCond2 & longCond3
        shortCond1 = LDvalue.rolling(SlopeLength).min() > 0.
        shortCond2 = realopenD_Shifted / realCLoseD_Shifted < 1.04
        shortCond3 = self.slope < (0 - Range)
        comb2Cond = shortCond1 & shortCond2 & shortCond3
        self.SFlag[pd.eval('comb1Cond')] = int(1)
        self.SFlag[pd.eval('comb2Cond')] = int(-1)
        self.SFlag = self.SFlag.fillna(method='ffill')
        return True

    def passage_F(self, targetName, Len):
        ''' 通道计算 '''
        Len = int(Len)
        targetData = self.index_DF(targetName, objectType=u'stocks')
        low, high = \
            (targetData[i] for i in [u'low', u'high'])
        HH1 = high.shift(1).rolling(Len).max()
        LL1 = low.shift(1).rolling(Len).min()
        self.RealH = pd.Series(index=high.index)
        self.RealL = pd.Series(index=low.index)
        highCond1 = high.shift(Len + 1) > HH1
        highCond2 = high.shift(Len + 1) > HH1.shift(Len + 1)
        comb1Cond = highCond1 & highCond2
        lowCond1 = low.shift(Len + 1) < LL1
        lowCond2 = low.shift(Len + 1) < LL1.shift(Len + 1)
        comb2Cond = lowCond1 & lowCond2
        # self.RealH = high.shift(Len + 1)[pd.eval('comb1Cond')]
        # self.RealL = low.shift(Len + 1)[pd.eval('comb2Cond')]
        self.RealH[pd.eval('comb1Cond')] = high.shift(Len + 1)[pd.eval('comb1Cond')]
        self.RealL[pd.eval('comb2Cond')] = low.shift(Len + 1)[pd.eval('comb2Cond')]
        self.RealH = self.RealH.fillna(method='ffill')
        self.RealL = self.RealH.fillna(method='ffill')
        # a = pd.DataFrame()
        # a['L[len+1]'] = low.shift(Len + 1)
        # a['LL1'] = LL1
        # a['LL1[len+1]'] = LL1.shift(Len + 1)
        # a['RealL'] = self.RealH
        # a.plot()
        # plt.show()
        return True

    def ATR_F(self, targetName, N):
        ''' 计算ATR '''
        low = self.index_DF(targetName, objectType=u'stocks')[u'low']
        high = self.index_DF(targetName, objectType=u'stocks')[u'high']
        close = self.index_DF(targetName, objectType=u'stocks')[u'close']
        tempDf1 = high - low
        tempDf2 = np.abs(close.shift(1) - high)
        tempDf3 = np.abs(close.shift(1) - low)
        mtr = dfComparing(tempDf1, tempDf2, tempDf3)
        self.atr = mtr.rolling(int(N)).mean()
        return True

    def TrimData(self):
        '''
        清洗和预处理数据
        :return: 处理好的 pandas.Dataframe
        '''
        # 清洗和生成数据(跳过空值)
        self.mvVol_F(self.dataTarget, self.Vlength)
        self.slope_F(self.dataTarget, self.SlopeLength, self.Range)
        self.passage_F(self.dataTarget, self.Len)
        # self.ATR_F(self.dataTarget, self.AtrLength)
        # c_2 = self.index_DF('sz50', objectType=u'stocks')[u'close'].shift(2)
        # l_1 = self.index_DF('sz50', objectType=u'stocks')[u'low'].shift(1)
        # strIndex = pd.Index(range(len(c_2.index)))
        # cond1 = pd.DataFrame()
        # cond1['c[2]'] = c_2
        # cond1['MaTodayL[1]'] = self.MaTodayL.shift(1)
        # cond1.index = strIndex
        # cond2 = pd.DataFrame()
        # cond2['slope[1]'] = self.slope.shift(1)
        # cond2['R'] = pd.Series([self.R] * len(self.slope), index=self.slope.index)
        # cond2.index = strIndex
        # cond3 = pd.DataFrame()
        # cond3['ATR[2]/C[2]*100'] = self.atr.shift(2) / c_2 * 100
        # cond3['AtrRange'] = pd.Series([self.AtrRange] * len(self.slope), index=self.slope.index)
        # cond3.index = strIndex
        # cond4 = pd.DataFrame()
        # cond4['Sflag[1]'] = self.SFlag.shift(1)
        # cond4['1'] = pd.Series([1] * len(self.slope), index=self.slope.index)
        # cond4.index = strIndex
        # cond5 = pd.DataFrame()
        # cond5['L[1'] = self.index_DF('sz50', objectType=u'stocks')[u'low'].shift(1)
        # cond5['Real[1'] = self.RealL.shift(1)
        # cond5.index = strIndex
        # longCond1 = c_2 <= self.MaTodayL.shift(1)
        # longCond2 = self.slope.shift(1) > self.R
        # longCond3 = self.atr.shift(2) / c_2 * 100 > self.AtrRange
        # longCond4 = self.SFlag.shift(1) == 1
        # longCond5 = l_1 < self.RealL.shift(1)
        # combCond = pd.eval('longCond1 & longCond2 & longCond3 & longCond4 & longCond5')
        # fig = plt.figure()
        # for i in range(1, 6):
        #     ax = fig.add_subplot(5, 1, i)
        #     eval('ax.plot(cond%s)' % i)
        # plt.show()
        return super(MyStrategy, self).TrimData()

    def myOpenOrder_F(self, targetName, uaTarget):
        ''' 开仓函数--切片级别 '''
        # 定义参数
        direction = -1
        holding = self.getPositionIndex(direction, u'options')
        c_2 = self.index(targetName, objectType=u'stocks', ref=int(2))[u'close']
        MaTodayL_1 = self.df_Ref(self.MaTodayL, ref=1)
        slope_1 = self.df_Ref(self.slope, ref=1)
        # ATR_2 = self.df_Ref(self.atr, ref=2)
        Sflag_1 = self.df_Ref(self.SFlag, ref=1)
        L_1 = self.index(targetName, objectType=u'stocks', ref=int(1))[u'low']
        H_1 = self.index(targetName, objectType=u'stocks', ref=int(1))[u'high']
        RealL_1 = self.df_Ref(self.RealL, ref=1)
        RealH_1 = self.df_Ref(self.RealH, ref=1)
        # cond3 = ATR_2 / c_2 * 100 > self.AtrRange
        ua_Price = self.index(uaTarget, objectType=u'stocks')[u'open']                 # 期权标的价格
        exe_Price = self.index_DF(u'exe_price', objectType=u'options').iloc[0]         # 行权价格
        price = self.index(u'open', objectType=u'options').drop(holding)
        recentMonthIndex = self.recentMonth(objectType=u'options')
        long, short = [None] * 2

        # 看多的合约（direction !=-1）
        longCond1 = c_2 <= MaTodayL_1
        longCond2 = slope_1 > self.R
        longCond4 = Sflag_1 == 1
        longCond5 = L_1 < RealL_1
        comb1Cond = longCond1 & longCond2 & longCond4 & longCond5
        if comb1Cond:                                                  # 选择时间价值最高的认沽期权以Data1.open价卖出
            longPrice = copy.deepcopy(price[self.callPutIndex(-1) & recentMonthIndex])   # 认沽期权的价格
            for i in range(len(longPrice.index)):
                name = longPrice.index[i]
                longPrice[i] = timeValue(longPrice[name], ua_Price, exe_Price[name], callput=-1)
            long = longPrice.sort_values(ascending=False).index[0]    # 选择时间价值最高的认沽期权以Data1.open价卖出

        # 看空的合约
        shortCond1 = c_2 >= MaTodayL_1
        shortCond2 = slope_1 < 0. - self.R
        shortCond4 = Sflag_1 == -1
        shortCond5 = H_1 > RealH_1
        comb2Cond = shortCond1 & shortCond2 & shortCond4 & shortCond5
        if comb2Cond:  # 选择时间价值最高的认购期权以Data1.open价卖出
            shortPrice = copy.deepcopy(price[self.callPutIndex(1) & recentMonthIndex])   # 认购期权的价格
            for i in range(len(shortPrice.index)):
                name = shortPrice.index[i]
                shortPrice[i] = timeValue(shortPrice[name], ua_Price, exe_Price[name], callput=1)
            short = shortPrice.sort_values(ascending=False).index[0]  # 选择时间价值最高的认购期权以Data1.open价卖出

        # 回调一期
        if self.getLoc() != 0:
            # 报单
            priceMode, volMode, singleAmt = 1, 2, 100000.
            for orderIndex in [long, short]:
                if orderIndex:
                    openOrder = self.orderRecorder(1, orderIndex, u'options', direction)  # 记录交易单
                    self.smartSendOrder(openOrder, priceMode, volMode, singleAmt)
            return pd.Index([long]), pd.Index([short])

    def myAddPositon_F(self, undoLong, undoShort, targetName, uaTarget):
        ''' 加仓函数--切片级别 '''
        direction = -1
        price = self.index(u'open', objectType=u'options')              # 期权价格
        ua_Price = self.index(uaTarget, objectType=u'stocks')[u'open']  # 期权标的价格
        exe_Price = self.index_DF(u'exe_price', objectType=u'options').iloc[0]  # 行权价格
        c_1 = self.index(targetName, objectType=u'stocks', ref=int(1))[u'close']
        l_1 = self.index(targetName, objectType=u'stocks', ref=int(1))[u'low']
        h_1 = self.index(targetName, objectType=u'stocks', ref=int(1))[u'high']
        holding = self.getPositionIndex(direction, u'options')
        long, short = [None] * 2

        if not len(holding) >= int(20):
            # 多加仓
            longPrice = copy.deepcopy(price[self.callPutIndex(-1)])  # 认沽期权的价格
            tempTargetIndex1 = (longPrice.index & holding).difference(undoLong)
            if not tempTargetIndex1.empty:
                LastEntryPrice1 = pd.Series(
                    [self.lastEntryPrice(direction, u'options', objectCode=i, priceType=1) for i in tempTargetIndex1],
                    index=tempTargetIndex1)
                longSeries = LastEntryPrice1[LastEntryPrice1 > (l_1 + self.ADDratio /100. * float(c_1))]
                if not longSeries.empty:
                    for i in range(len(longSeries)):
                        name = longSeries.index[i]
                        longSeries[i] = timeValue(longPrice[name], ua_Price, exe_Price[name], callput=-1)
                    long = longSeries.sort_values(ascending=False).index[0]
                    print('LastEntryPrice is %s' % LastEntryPrice1[long])
                    print('l_1 + self.ADDratio /100. * float(c_1) is %s' % (
                            l_1 + self.ADDratio / 100. * float(c_1)))

            # 空加仓
            shortPrice = copy.deepcopy(price[self.callPutIndex(1)])  # 认购期权的价格
            tempTargetIndex2 = (shortPrice.index & holding).difference(undoShort)
            if not tempTargetIndex2.empty:
                LastEntryPrice2 = pd.Series(
                    [self.lastEntryPrice(direction, u'options', objectCode=i, priceType=1) for i in tempTargetIndex2],
                    index=tempTargetIndex2)
                shortSeries = LastEntryPrice2[LastEntryPrice2 > (h_1 - self.ADDratio /100. * float(c_1))]
                if not shortSeries.empty:
                    for i in range(len(shortSeries)):
                        name = shortSeries.index[i]
                        shortSeries[i] = timeValue(shortPrice[name], ua_Price, exe_Price[name], callput=1)
                    short = shortSeries.sort_values(ascending=False).index[0]
                    print('LastEntryPrice is %s' % LastEntryPrice2[short])
                    print('h_1 - self.ADDratio /100. * float(c_1) is %s' %
                          (h_1 - self.ADDratio / 100. * float(c_1)))

        if self.getLoc() != 0:
            priceMode, volMode, singleAmt = 1, 2, 100000.
            # 报单
            for orderIndex in [long, short]:
                if orderIndex:
                    addOrder = self.orderRecorder(1, orderIndex, u'options', direction)  # 记录交易单
                    self.smartSendOrder(addOrder, priceMode, volMode, singleAmt)

    def myExit_F(self):
        ''' 离场函数 '''
        h_1 = self.index(self.dataTarget, objectType=u'stocks', ref=int(1))[u'high']
        l_1 = self.index(self.dataTarget, objectType=u'stocks', ref=int(1))[u'low']
        reahH_1 = self.RealH.loc[self.getTime(ref=1)]
        reahL_1 = self.RealL.loc[self.getTime(ref=1)]
        longCond = h_1 > reahH_1
        shortCond = l_1 < reahL_1
        closeIndex1, closeIndex2 = [pd.Index([])]*2
        if longCond:
            closeIndex1 = self.callPutIndex(-1)
        if shortCond:
            closeIndex2 = self.callPutIndex(1)
        closeIndex = closeIndex1.append(closeIndex2)
        if not closeIndex.empty:
            self.closePosition(direction=-1, objectType=u'options', targetIndex=closeIndex, priceMode=1, volMode=1)  # 卖出对应方向和种类的全部合约
        else:
            return False

    def onBar(self):
        ''' 切片操作函数'''
        # TODO: 2. 选股处理
        # 开仓
        undoLong, undoShort = self.myOpenOrder_F(self.dataTarget, self.uaTarget)

        # 加仓
        pass
        self.myAddPositon_F(undoLong, undoShort, self.dataTarget, self.uaTarget)

        # 平仓
        self.myExit_F()
        print(self.TradeTime)

# 测试用例 ---------------------------------------------------------------------------
def aTestCase(primaryDate, startTime, endTime):
    '''
    funciton for test
    :return:
    '''
    time1 = time.clock()
    benchmarkName = ''                                # zz500, hs300, szzs, zxb, sz50
    timeSeriesFile = u'volume_options.csv'
    fund = int(5000000)
    feeRatio = [0.0005, 0.001]
    impactCostRatio = [0.001, 0.001]
    timeFreq = u'1T'  # 回测时间类型 {u'min':分钟, u'D':日线}
    indexNameList = [u'mkt_options', u'exe_price_options', u'exe_mode_options',
                     u'oi_options', u'sz50_stocks', u'50etf_stocks',
                     u'maint_margin_options', u'contractmultiplier_options',
                     u'lasttradingdate_options']    # WIND indexNameList
    stockCodelist = []
    strategyNamelist = [u'Towl']
    tester = u'李琦杰'
    testContent = u'期权策略'
    dataSourceType = DATA_SOURCE_HTTPAPI

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
    dataStartTime = u'2018-01-22 9:25:00'
    testStartTime = u'2018-01-23 9:25:00'
    testEndTime = u'2018-01-23 15:00:00'

    aTestCase(dataStartTime, testStartTime, testEndTime)  # 回测时间一定要大于数据起始时间1天