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
    ROA逆序打分 + 1/PE 逆序打分，挑选得分小的前20
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
        # self.roa = copy.deepcopy(self.index_DF('roa', objectType='stocks'))
        self.roa = copy.deepcopy(self.index_DF('roa', objectType='stocks'))
        # self.earningYield = copy.deepcopy(1/self.index_DF('pe_ttm', objectType='stocks'))
        # self.peg = copy.deepcopy(self.index_DF('peg', objectType='stocks'))
        self.peg = copy.deepcopy(self.index_DF('peg', objectType='stocks'))
        # self.targetDf = self.roa.rank(axis=1, ascending=False) + self.earningYield.rank(axis=1, ascending=False)    # 大数值得分小
        # self.stm_issuingdate = read_csvEx('stm_issuingdate_stocks.csv', indexCol=0)
        return super(MyStrategy, self).TrimData()

    def onBar(self):
        '''
        切片操作函数
        '''
        curDate = self.TradeTime
        # if curDate == '2013-05-03':
        #     pass
        print(curDate)
        month = self.getPeriod(freq=u'M').split('-')[-1]
        direction = 1
        holding = self.getPositionIndex(direction, u'stocks')                                   # 最新持仓
        peg = self.peg.loc[self.getTime(ref=1)]
        peg = peg[peg > 0].rank(ascending=True)             # 小值给小分
        # rankTarget = self.targetDf.loc[self.getTime(ref=1)]
        roa = self.roa.loc[self.getTime(ref=1)]
        roa = roa[roa > 0].rank(ascending=False)                     # 大值给小分
        unionIndex = roa.index & peg.index
        rankTarget = roa[unionIndex] + peg[unionIndex]
        yzb = self.index(u'yiziban', u'stocks')
        yzb = yzb[yzb == 1].index
        targetIndex = rankTarget[rankTarget.index.difference(yzb)].sort_values(ascending=True)[:5].index    # 小值排前
        if self.getLoc() == self.getPeriodrange(freq=u'M')[-1] and month in ['04', '08', '10']:   # 4, 8, 10 月底操作

            if not targetIndex.empty:
                # 复杂报单 - 换仓（根据实际标的的量来开平仓）
                holdingBuy = targetIndex & holding  # 已持有的买入标的
                theoreticalSell = (holding).difference(targetIndex)  # 卖出不在持仓的标的基金
                self.mySendOrderF(theoreticalSell, targetIndex, holdingBuy)

        # 卖点
        # # 止盈
        # newHolding = self.getPositionIndex(direction, u'stocks')
        # mvCost = self.getPositionData(direction, u'stocks', 2)
        # holdingVol = self.getPositionData(direction, u'stocks', 1)
        # holdingPL = self.index(u'open', u'stocks')[newHolding] - mvCost
        # stopProfit = holdingPL[holdingPL > 0.30].index
        # if not stopProfit.empty:  # 卖出持仓
        #     Log(str(curDate))
        #     print(stopProfit)
        #     Log(u'进行止盈')
        #     closingAmt = copy.deepcopy(mvCost * holdingVol)  # todo 加入止盈后补位标的
        #     self.closePosition(direction, u'stocks', targetIndex=stopProfit, priceMode=1, volMode=1)    # 收盘止盈
        #     # additionVol =

        # 输出最新一期标的
        if self.TradeTime == self.time_List()[-1]:
            pass
        pass  # 切片完成

    def mySendOrderF(self, theoreticalSell, targetIndex, holdingBuy,):
        # 买入标的报单
        direction = 1
        priceMode, volMode, sliceFund = 1, 0, self.initFund # 开盘换仓
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
    strategyNamelist = [u'magicalFormula0']
    tester = u'李琦杰'
    testContent = u'magicalFormula0'
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
    # aTestCase(u'2010-06-01', u'2012-04-27', u'2017-03-31')  # 日期格式u'2017-09-04'
    aTestCase(u'2010-06-01', u'2012-04-27', u'2016-12-30')  # 日期格式u'2017-09-04'