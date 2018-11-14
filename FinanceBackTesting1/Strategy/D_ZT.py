# encoding: UTF-8

# 二连版策略 strategy
import pandas as pd
from datetime import date
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
        direction = 1
        holding = self.getPositionIndex(direction, u'stocks')                                   # 最新持仓
        if datetime.strptime(self.TradeTime, "%Y-%m-%d").weekday() in [1, 2]:                   # 周二买，周三买
            curLoc = self.getLoc(self.TradeTime)
            prevSt = self.index_DF(u'st', u'stocks').iloc[curLoc - 1]
            tempIndex = prevSt[prevSt != 1].index & self.cleanedDf().loc[self.TradeTime].dropna().index    # 剔除ST股票，没用量比剔除条件
            tempIndex = tempIndex & self.index_DF(u'volume', u'stocks').iloc[curLoc-1].dropna().index      # 剔除选股日停牌的股票
            ref1_date, ref2_date = self.time_List()[curLoc - 1], self.time_List()[curLoc - 2]
            unIPOstock1, unIPOstock2 = self.unIPO(tempIndex, ref1_date), self.unIPO(tempIndex, ref2_date)
            if self.TradeTime == '2017-08-08':
                pass
            tempIndex = tempIndex.difference(unIPOstock1).difference(unIPOstock2)               # 剔除未上市的股票
            zrzt_Seri = self.ref(self.index_DF(u'zrzt', u'stocks'), tempIndex, ref=1)           # 选股日自然涨停
            zrzt_Seri = zrzt_Seri[zrzt_Seri == 1]
            zt_Seri = self.ref(self.index_DF(u'zt', u'stocks'), zrzt_Seri.index, ref=2)         # 选股前1日涨停
            tempIndex1 = zt_Seri[zt_Seri == 1].index
            continuousZTNum = pd.Series(index=tempIndex1)
            # 连板判断
            for i in range(len(tempIndex1)):
                stock = tempIndex1[i]
                continuousZTNum[i] = self.continuousZT_Num(stockCode=stock, loc=curLoc-1)
            rankedStock = continuousZTNum.sort_values(ascending=False)[:3].index
            theoreticalBuy = pd.Index(rankedStock).difference(holding)

            # 买入报单
            openOrder = self.orderRecorder(1, theoreticalBuy, u'stocks', direction)  # 记录交易单
            priceMode, volMode, sliceFund = 1, 1, 10000000.
            self.smartSendOrder(openOrder, priceMode, volMode, sliceFund)

        # 每天卖出持仓
        if not holding.empty:
            theoreticalSell = self.filter(holding, filterType=1)                         # 卖出剔除停牌
            if not theoreticalSell.empty:
                self.closePosition(1, u'stocks', targetIndex=theoreticalSell, priceMode=2, volMode=1)

        # 输出最新一期标的
        if self.TradeTime == self.time_List()[-1]:
            # 输出最新一期标的
            curLoc = self.getLoc(self.TradeTime)
            prevSt = self.index_DF(u'st', u'stocks').iloc[curLoc]
            tempIndex = prevSt[prevSt != 1].index & \
                        self.cleanedDf().iloc[curLoc].dropna().index                    # 剔除ST股票，没用量比剔除条件
            ref1_date, ref2_date = self.getTime(), self.getTime(ref=1)
            unIPOstock1, unIPOstock2 = self.unIPO(tempIndex, ref1_date), self.unIPO(tempIndex, ref2_date)
            tempIndex = tempIndex.difference(unIPOstock1).difference(unIPOstock2)           # 剔除未上市的股票
            zrzt_Seri = self.index_DF(u'zrzt', u'stocks').iloc[curLoc][tempIndex]                      # 前1交易日自然涨停
            zrzt_Seri = zrzt_Seri[zrzt_Seri == 1]
            zt_Seri = self.ref(self.index_DF(u'zt', u'stocks'), zrzt_Seri.index, ref=1) # 前2交易日涨停
            tempIndex1 = zt_Seri[zt_Seri == 1].index
            continuousZTNum = pd.Series(index=tempIndex1)

            # 连板判断
            for i in range(len(tempIndex1)):
                stock = tempIndex1[i]
                continuousZTNum[i] = self.continuousZT_Num(stockCode=stock, loc=curLoc-1)
            sigBuy = continuousZTNum.sort_values(ascending=False)[:3].index
            target = pd.DataFrame([sigBuy], index=[self.TradeTime])
            target.to_csv(u'../Output/' + self.strategyNamelist[0] + u'-LatestTarget' + self.todayDate + u'.csv')
        pass  # 切片完成

    def continuousZT_Num(self, stockCode, loc):
        ''' 返回连续涨停扳的涨停个数
            要求loc为连扳最后一个涨停的loc
        '''
        tempZTSeri = self.index_DF(u'zt', u'stocks')[stockCode].iloc[:loc].dropna()
        unZTseries = tempZTSeri[tempZTSeri != 1]
        if not unZTseries.index.empty:
            unZTDate = str(tempZTSeri[tempZTSeri != 1].index[-1].date())                 # 第一个非涨停日
        else:                                                                            # 新股上市全是涨停
            return len(tempZTSeri)
        continuousZTLoc = self.getLoc(unZTDate) + 1
        ztSeri = self.index_DF(u'zt', u'stocks')[stockCode].iloc[continuousZTLoc:loc]
        return len(ztSeri)

# 测试用例 ---------------------------------------------------------------------------
def aTestCase(primaryDate, startTime, endTime):
    '''
    funciton for test
    :return:
    '''
    time1 = time.clock()
    benchmarkName = u'zz500_stocks'
    timeSeriesFile = u'volume_cleaned_stocks.csv'
    fund = int(20000000)
    feeRatio = [0.0005, 0.001]
    impactCostRatio = [0., 0.]
    timeFreq = u'D'                                 # 回测时间类型 {u'min':分钟, u'D':日线}
    indexNameList = [u'mkt_stocks', u'st_stocks', u'zt_stocks', u'zrzt_stocks', u'ipo_date_stocks']
    stockCodelist = []
    strategyNamelist = [u'Double ZT']
    tester = u'李琦杰'
    testContent = u'Double ZT'
    dataSourceType = DATA_SOURCE_HTTPAPI

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
if __name__== u"__main__":
    aTestCase(u'2018-01-08', u'2018-03-01', u'2018-03-20') # 日期格式u'2017-09-04'