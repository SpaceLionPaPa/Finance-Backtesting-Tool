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
    ROA先挑选， PE再挑选
    止盈+补位
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
        self.roa = copy.deepcopy(self.index_DF('roa', objectType='stocks'))
        self.peg = copy.deepcopy(self.index_DF('estpeg_FTM', objectType='stocks'))
        self.roe = copy.deepcopy(self.index_DF('roe_ttm2', objectType='stocks'))
        self.grossprofitmargin = copy.deepcopy(self.index_DF('grossprofitmargin', objectType='stocks'))
        self.pcf_ocf_ttm = copy.deepcopy(self.index_DF("pcf_ocf_ttm", objectType='stocks'))
        # self.earningYield = copy.deepcopy(1/self.index_DF('pb_lf', objectType='stocks'))
        # self.targetDf = self.roa.rank(axis=1, ascending=False) + self.earningYield.rank(axis=1, ascending=False)
        # self.stm_issuingdate = read_csvEx('stm_issuingdate_stocks.csv', indexCol=0)
        return super(MyStrategy, self).TrimData()

    def onBar(self):
        '''
        切片操作函数
        '''
        curDate = self.TradeTime
        # if curDate == '2017-03-31':
        #     pass
        print(curDate)
        month = self.getPeriod(freq=u'M').split('-')[-1]
        direction = 1
        periodEn1 = self.getPeriodrange(freq=u'Q', ref=1)[-1]
        periodEn2 = self.getPeriodrange(freq=u'Q', ref=2)[-1]
        periodEn3 = self.getPeriodrange(freq=u'Q', ref=3)[-1]
        lastPeriodRoa1 = self.roa.iloc[periodEn1]
        lastPeriodRoa2 = self.roa.iloc[periodEn2]
        lastPeriodRoa3 = self.roa.iloc[periodEn3]
        filterIndex = lastPeriodRoa1[lastPeriodRoa1 > 0].index & lastPeriodRoa2[lastPeriodRoa2 > 0].index & lastPeriodRoa3[lastPeriodRoa3 > 0].index
        roa = self.roa.loc[self.getTime(ref=1)].drop(filterIndex).dropna()       # 剔除上期为负的股票
        roa = roa[roa < 80].sort_values(ascending=False)[:200]
        # roa = roa.sort_values(ascending=False)[:200]
        # roa = roa[roa < 80].sort_values(ascending=False)[:400]
        # roe = self.roe.loc[self.getTime(ref=1)]
        # roe = roe[roe < 80].dropna().sort_values(ascending=False)[:200]
        grossprofitmargin = self.grossprofitmargin.loc[self.getTime(ref=1)].dropna()[roa.index]
        rankTarget0 = grossprofitmargin.sort_values(ascending=False)[:100]  # 大值排前
        pcf_ocf_ttm = self.pcf_ocf_ttm.loc[self.getTime(ref=1)].dropna()[rankTarget0.index]
        rankTarget1 = pcf_ocf_ttm[pcf_ocf_ttm > 0].sort_values(ascending=True)[:100]
        pass
        peg = self.peg.loc[self.getTime(ref=1)][rankTarget1.index]
        rankTarget1 = peg[peg > 0]
        print("rankTarget1 %s " % rankTarget1)
        # rankTarget = self.earningYield.loc[self.getTime(ref=1)].loc[rankTarget1.index]
        yzb = self.index(u'yiziban', u'stocks')
        yzb = yzb[yzb == 1].index
        newStock = self.index(u'is_new_stock_504', u'stocks')
        newStock = newStock[newStock == 1].index
        tradeIndex = self.cleanedDf().loc[curDate].dropna().index
        targetIndex = rankTarget1[rankTarget1.index.difference(yzb | newStock) & tradeIndex].sort_values(ascending=True)[:10].index
        # targetIndex = rankTarget[rankTarget.index.difference(yzb) & tradeIndex].sort_values(ascending=True)[:5]

        cond1 = self.getLoc() == self.getPeriodrange(freq=u'M')[-1] and month in ['04', '08', '10']     # 4, 8, 10 月底操作
        # cond1 = self.getLoc() == self.getPeriodrange(freq=u'M')[-1] and month in ['03',  '04', '07', '08', '10']     # 4, 8, 10 月底操作
        theoreticalSell = pd.Index([])
        if cond1:    # 财报披露后批量换仓
            # 复杂报单 - 换仓（根据实际标的的量来开平仓）
            # # 均值筛选
            # buyBias = 1.
            # ref_C = self.index_DF(u'close', u'stocks').loc[self.getTime(ref=1)][targetIndex]
            # mvStd = self.index_DF(u'mvStd', u'stocks').loc[self.getTime(ref=1)][targetIndex]
            # mvMean = self.index_DF(u'mvMean', u'stocks').loc[self.getTime(ref=1)][targetIndex]
            # targetIndex = ref_C[(ref_C - mvMean) < -buyBias * mvStd].index
            if not targetIndex.empty:
                holding = self.getPositionIndex(direction, u'stocks')  # 最新持仓
                holdingBuy = targetIndex & holding  # 已持有的买入标的
                theoreticalSell = (holding).difference(targetIndex)  # 卖出不在持仓的标的基金
                self.mySendOrderF(theoreticalSell, targetIndex, holdingBuy)

        # 止盈
        mvCost = self.getPositionData(direction, u'stocks', 2)
        holdingVol = self.getPositionData(direction, u'stocks', 1)
        holdingPL = self.index(u'open', u'stocks')[self.getPositionIndex(direction, u'stocks')] - mvCost
        stopProfit = copy.deepcopy(holdingPL[holdingPL > 0.2].index)
        cond2 = not stopProfit.empty

        if cond2:   # 卖点一
            print(stopProfit)
            Log(u'进行止盈')
            closingAmt = copy.deepcopy(mvCost * holdingVol[stopProfit]).sum()  # todo 加入止盈后补位标的
            self.closePosition(direction, u'stocks', targetIndex=stopProfit, priceMode=2, volMode=1)    # 收盘止盈

            # 补位标的
            addNum = len(stopProfit)
            eachAmt = float(closingAmt)/addNum
            undoIndex = self.getPositionIndex(direction, u'stocks') | stopProfit | theoreticalSell
            targetIndex = targetIndex.difference(undoIndex)[:addNum]
            priceMode = 1
            volMode = 0
            if not targetIndex.empty:
                for i in targetIndex:  # 买入报单
                    self.tempVol = eachAmt/self.index(u'close', u'stocks')[i]
                    openOrder = self.orderRecorder(1, pd.Index([i]), u'stocks', direction)  # 记录交易单
                    self.smartSendOrder(openOrder, priceMode, volMode)
        pass

        ## 输出最新一期标的
        # if self.TradeTime == self.time_List()[-1]:
        #     pass
        # pass  # 切片完成

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
    benchmarkName = u'hs300_stocks'
    timeSeriesFile = u'volume_cleaned_stocks.csv'
    fund = int(10000000)
    feeRatio = [0.002, 0.002]
    impactCostRatio = [0., 0.]
    timeFreq = u'D'                                 # 回测时间类型 {u'min':分钟, u'D':日线}
    indexNameList = [u'volume_stocks', u'open_stocks', u'high_stocks',
                     u'low_stocks', u'close_stocks', u'roa_stocks', u'estpeg_FTM_stocks',
                     u'estpeg_FY1_stocks', u'estpeg_FY2_stocks', u'roe_ttm2_stocks', u'grossprofitmargin_stocks',
                     u'yiziban_stocks',  u'mvStd_stocks', u'mvMean_stocks', u'is_new_stock_504_stocks',
                     u"pcf_ocf_ttm_stocks", u"pcf_ncf_ttm_stocks", u"pcf_ocflyr_stocks", u"pcf_nflyr_stocks"]
    stockCodelist = []
    strategyNamelist = [u'magicalFormula1']
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
    aTestCase(u'2010-06-01', u'2014-04-30', u'2017-07-31')  # 日期格式u'2017-09-04'