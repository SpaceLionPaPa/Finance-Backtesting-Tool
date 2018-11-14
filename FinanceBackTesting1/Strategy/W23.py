# encoding: UTF-8

# W23 strategy
import copy
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from API.DataAPI import APIClass
from Templete.Constant import *
from Templete.StrategyTemplete1 import StrategyTemplete1
from Templete.Utilities import *

class MyStrategy(StrategyTemplete1):

    def __init__(
            self, tester, strategyNamelist, testContent,
            feeRatio, timeSeriesFile, initFund, startTime,
            dataSourceType, timeFreq, impactCostRatio):

        super(MyStrategy, self).__init__(
            tester, strategyNamelist, testContent,
            feeRatio, timeSeriesFile, initFund, startTime,
            dataSourceType, timeFreq, impactCostRatio)

        self.periodTypeList = [u'W']
        self.APIeg = APIClass('../API/indexconfig.txt')
        self.todayDate = date.today().isoformat()

    def TrimData(self):
        '''
        清洗和预处理数据
        :return: 处理好的 pandas.Dataframe
        '''
        # TODO: 1. 数据清洗和预处理
        return super(MyStrategy, self).TrimData()

    def onBar(self):
        '''
        切片操作函数
        '''
        # TODO: 2. 选股处理
        periodType = u'W'
        direction = 1
        periodName = self.getPeriod(freq=periodType)
        periodSeris = self.periodSeriesDict[periodType]
        theoreticalBuy = pd.Index([])
        holding = self.getPositionIndex(direction, u'stocks')                                   # 最新持仓
        if periodName != periodSeris[0]:                                                        # 第二周买入
            loc1 = self.getPeriodrange(freq=periodType, ref=1)[-1]                              # 上周最后一个交易日
            if datetime.strptime(self.TradeTime, "%Y-%m-%d").weekday() in [1, 3, 4]:            # 周2, 周3, 周5
                mktName = self.cleanedDf().iloc[loc1].index
                st = self.index_DF(u'st', u'stocks').iloc[loc1]
                st = st[st != 1].index
                tempName1 = mktName & st                                                        # ST筛选
                tempName2 = mktName[self.index_DF(u'new_stock200', u'stocks').iloc[loc1] != 1]  # 新股筛选
                ref1_wPct = self.period(u'pct', u'stocks', freq=u'W', ref=1)                     # 上周涨幅   # pct--百分数* 100
                sigBuy = tempName1 & tempName2
                # 买点1-- 星期二买入
                if datetime.strptime(self.TradeTime, "%Y-%m-%d").weekday() == 1:                # 星期二
                    if not sigBuy.empty:
                        prevZT = self.index_DF(u'zt', u'stocks').iloc[loc1]
                        index1 = prevZT[prevZT != 1].index
                        loc2 = self.getPeriodrange(freq=periodType)[0]                          # 周一
                        date_loc2 = self.time_List()[loc2]
                        if datetime.strptime(date_loc2, "%Y-%m-%d").weekday() == 0:             # 周一
                            prevZT2 = self.index_DF(u'zt', u'stocks').iloc[loc2]
                            index2 = prevZT2[abs(prevZT2) != 1].index                           # 周一不涨停也不跌停
                            tempBuy = sigBuy & index1 & index2
                            tempBuy = ref1_wPct[tempBuy].sort_values(ascending=False)[:5].index # 上周涨跌幅取最大的前5
                            theoreticalBuy = tempBuy.difference(holding)

                pass
                # 买点一结束
                # 买点2-- 星期四或周五买入
                if datetime.strptime(self.TradeTime, "%Y-%m-%d").weekday() in [3, 4]:           # 周四或周五
                    curLoc = self.time_List().index(self.TradeTime)
                    for name in [u'szzz', u'szcz', u'cyb']:
                        seri = copy.deepcopy(self.index_DF(name, u'stocks')[u'close'])
                        seriPct = seri/seri.shift(1).fillna(method='ffill')
                        pctData = seriPct.iloc[max(0,curLoc - 1)]
                        if name == u'szzz':
                            cond = pctData < -0.015
                        if name in [u'szcz', u'cyb']:
                            cond = pctData < -0.02
                        if cond:                                                                # 周四或周五开仓
                            if datetime.strptime(self.TradeTime, "%Y-%m-%d").weekday() in [3]:  # 周四
                                tempBuy = sigBuy
                            if datetime.strptime(self.TradeTime, "%Y-%m-%d").weekday() in [4]:  # 周五
                                w4buy = holding & sigBuy
                                if w4buy.empty:
                                    tempBuy = sigBuy
                            for filterType in [1, 2]:                                           # 剔除本期涨停一字板
                                tempBuy = self.filter(tempBuy, filterType)
                                theoreticalBuy = tempBuy.difference(holding)
                # 买点二结束
                #  市场状态剔除
                for filterType in [1, 2]:  # 剔除本期涨停一字板
                    theoreticalBuy = self.filter(theoreticalBuy, filterType, u'stocks', ref=0)
                openOrder = self.orderRecorder(1, theoreticalBuy, u'stocks', direction)        # 记录交易单
                priceMode, volMode, sliceFund = 1, 1, 10000000.
                self.smartSendOrder(openOrder, priceMode, volMode, sliceFund)

        # 每天卖出持仓
        if not holding.empty:
            theoreticalSell = self.filter(holding, filterType=1)                         # 卖出剔除停牌
            if not theoreticalSell.empty:
                self.closePosition(1, u'stocks', targetIndex=theoreticalSell, priceMode=2, volMode=1)

        # 输出最新一期标的
        if self.TradeTime == self.time_List()[-1]:
            locDate = copy.deepcopy(self.TradeTime)
            periodType = u'W'
            lastDateRange = [locDate, str(pd.date_range(
                start=locDate, periods=1, freq=periodType)[0].date())]
            lastTradeday = self.APIeg.getTradeDate(start=lastDateRange[0], end=lastDateRange[-1])[-1]      # 判断本周最后一个交易日

            if locDate == lastTradeday:
                dataLoc = self.getPeriodrange(freq=periodType)[-1]                   # 本周最后一个交易日
                ref1_wPct = self.period(u'pct', u'stocks', freq=periodType)          # 本周涨幅   pct--百分数* 100
            else:
                dataLoc = self.getPeriodrange(freq=periodType, ref=1)[-1]            # 上周最后一个交易日
                ref1_wPct = self.period(u'pct', u'stocks', freq=periodType, ref=1)   # 上周涨幅   pct--百分数* 100

            prevZT = self.index_DF(u'zt', u'stocks').iloc[dataLoc]
            index1 = prevZT[prevZT != 1].index
            mktName = self.cleanedDf().columns
            st = self.index_DF(u'st', u'stocks').iloc[dataLoc]
            st = st[st != 1].index
            tempName1 = mktName & st                                                 # ST筛选
            tempName2 = \
                mktName[self.index_DF(u'new_stock200', u'stocks').iloc[dataLoc] != 1]# 新股筛选
            tempBuy = tempName1 & tempName2 & index1
            for filterType in [1]:                                                   # 剔除停牌
                tempBuy = self.filter(tempBuy, filterType, u'stocks', ref=0)
            # tempBuy = tempBuy.drop(tempBuy[np.isnan(self.vol()[tempBuy])])         # 剔除停牌
            sigBuy = ref1_wPct[tempBuy].sort_values(ascending=False)[:5].index       # 上周涨跌幅取最大的前5
            target = pd.DataFrame([sigBuy], index=[locDate])
            target.to_csv(u'../Output/' + self.strategyNamelist[0] + u'-LatestTarget' + self.todayDate + u'.csv')
        pass  # 切片完成


# 测试用例 ---------------------------------------------------------------------------
def aTestCase(primaryDate, startTime, endTime):
    '''
    funciton for test
    :return:
    '''
    time1 = time.clock()
    benchmarkName = u'zz500_stocks' # zz500, hs300, szzs, zxb, sz50
    timeSeriesFile = u'volume_cleaned_stocks.csv'
    fund = int(20000000)
    feeRatio = [0.0005, 0.001]
    impactCostRatio = [0., 0.]
    timeFreq = u'D'  # 回测时间类型 {u'min':分钟, u'D':日线}
    indexNameList = [u'mkt_stocks', u'st_stocks', u'pct_stocks', u'new_stock200_stocks',
                     u'yzb_stocks', u'zt_stocks', u'szzz_stocks', u'szcz_stocks',
                     u'cyb_stocks']  # WIND indexNameList
    stockCodelist = []
    strategyNamelist = [u'W23']
    tester = u'李琦杰'
    testContent = u'W23策略'
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
#
#
if __name__== u"__main__":
    aTestCase(u'2017-05-08', u'2017-08-04', u'2018-01-23') # 日期格式u'2017-09-04'