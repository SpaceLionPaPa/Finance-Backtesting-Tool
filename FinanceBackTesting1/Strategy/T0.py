# encoding: UTF-8

# demo strategy
import copy
import pandas as pd
import numpy as np
from datetime import date
from Templete.Constant import *
from Templete.StrategyTemplete import StrategyTemplete
from Templete.Utilities import *


class MyStrategy(StrategyTemplete):

    def __init__(
            self, tester, strategyNamelist, testContent,
            buyFee, sellFee, initFund, startTime, dataSourceType):

        super(MyStrategy, self).__init__(
            tester, strategyNamelist, testContent,
            buyFee, sellFee, initFund, startTime, dataSourceType)
        self.todayDate = date.today().isoformat()
        self.prevAction = None
        self.periodTypeList = ['W']
        self.basicNumber = 100.00
        self.biasNumber = 100.00

    def TrimData(self):
        '''
        清洗和预处理数据
        :return: 处理好的 pandas.Dataframe
        '''
        # TODO: 1. 数据清洗和预处理
        print(u'正在清洗和预处理数据')
        print(u'清洗和预处理结束')
        return super(MyStrategy, self).TrimData()


    def onBar(self):
        '''
        切片操作函数
        '''
        # TODO: 2. 选股处理
        self.addBuffer('basic', self.basicNumber)
        self.addBuffer('bias', self.biasNumber)
        theoreticalBuytemp2 = pd.Index([])
        curLoc = self.getLoc(self.TradeTime)
        # 2.1 指标选股
        if self.TradeTime >= self.testTimeList()[1]:                                        # 第一天以后
            ref1_Sig = self.index_DF(u'zrzt').iloc[curLoc-1]
            index1 = ref1_Sig[ref1_Sig == 1].index                                          # T-1 涨停
            sigBuy = index1
            if self.TradeTime == self.testTimeList()[1]:                                    # 第二天--第一次出现信号
                theoreticalBuy = copy.deepcopy(sigBuy)                                      # 今理论开仓
                theoreticalSell = copy.deepcopy(pd.Index([]))                               # 今理论平仓
            else:                                                                           # 第一天以后
                holding = self.getPositionIndex()                                           # 最新持仓
                theoreticalBuy = sigBuy.difference(holding)                                 # 买入不在集合的股票
                theoreticalSell = holding                                                   # 今卖出（已在组合股票不再买入）

            # 2.2 选择筛选条件                                                                #  该筛选条件对当天交易有效
            for filterType in [1, 2]:                                                       # 1 停牌; 2 一字板; 3 ST; 4 新股
                theoreticalBuy = self.filter(theoreticalBuy, filterType)
            for filterType in [1]:
                theoreticalSell = self.filter(theoreticalSell, filterType)
            prevTurn = self.index_DF(u'turn').loc[self.TradeTime]                           # 最后排名，选出前几只股票
            theoreticalBuytemp1 = self.indexRank(
                theoreticalBuy, prevTurn, rankType=1, rankNum=100)                          # 昨日涨停，去1字板
            theoreticalBuytemp = self.indexRank(
                theoreticalBuytemp1, prevTurn, rankType=1, rankNum=10)                      # 昨日涨停，去1字板，前十
            tempmean = self.pct()[theoreticalBuytemp1].mean()
            self.basicNumber = self.refBuffer('basic', 1) * (1 + tempmean/100)
            self.addBuffer('basic', self.basicNumber)
            self.addBuffer('bias', self.biasNumber)
            if self.TradeTime >= self.testTimeList()[9]:
                result = 0
                for k in range(0, 9):
                    result = result+self.refBuffer('basic', k+1)
                self.biasNumber = (self.basicNumber*10/(self.basicNumber+result)-1)*100
                if self.refBuffer('bias', 1) < 3:
                    # 当日剔除

                    theoreticalBuytemp2 = theoreticalBuytemp
                #else:
                #    theoreticalBuytemp2 = pd.Index([])

            tradeSeries = self.tradeSeriesF(theoreticalBuytemp2, theoreticalSell)
            # 2.3 生成报单的tradeSeries

            # TODO: 3. 模拟交易
            self.smartSendOrder(tradeSeries, buyPriceMode=1, sellPriceMode=2, buyVolMode=1,
                                sellVolMode=1, sliceFund=10000000, sliceVol=100)
        pass    # 回测结束

        # 输出最新一期标的
        if self.TradeTime == self.time_List()[-1]:
            theoreticalBuytemp2 = pd.Index([])
            ref1_Sig = self.index_DF(u'zrzt').loc[self.TradeTime]
            index1 = ref1_Sig[ref1_Sig == 1].index  # T-1 涨停
            sigBuy = index1
            # 2.2 选择筛选条件                                                                 # 该筛选条件对当天交易有效
            sigBuy = \
                sigBuy.drop(sigBuy[self.index__originalDF(u'st').loc[self.TradeTime][sigBuy] == 1])      # 剔除ST
            prevTurn = self.index_DF(u'turn').loc[self.TradeTime]                            # 最后排名，选出前几只股票
            theoreticalBuytemp1 = self.indexRank(sigBuy, prevTurn, rankType=1, rankNum=100)  # 昨日涨停，去1字板
            theoreticalBuytemp = self.indexRank(theoreticalBuytemp1, prevTurn, rankType=1, rankNum=10)  # 昨日涨停，去1字板，前十
            if self.TradeTime >= self.testTimeList()[9]:
                if self.refBuffer('bias', 0) < 3:
                    theoreticalBuytemp2 = theoreticalBuytemp
            target = pd.DataFrame([theoreticalBuytemp2], index=[self.TradeTime])
            target.to_csv(u'../Output/' + self.strategyNamelist[0] + u'-LatestTarget' + self.todayDate + u'.csv')
        pass    # 最新选股输出结束


# 测试用例 ---------------------------------------------------------------------------
def aTestCase(primaryDate, startTime, endTime):
    '''
    funciton for test
    :return:
    '''

    time1 = time.clock()
    benchmarkName = u'zz500' # zz500, hs300, szzs, zxb, sz50
    fund = int(20000000)
    buyFee = 0.0005
    sellFee = 0.001
    indexNameList = [u'close', u'open', u'volume', u'turn', u'st', u'new_stock', u'yzb', u'pct', u'zrzt'] #WIND indexNameList
    stockCodelist = []
    strategyNamelist = [u'T0']
    tester = u'殷欣'
    testContent = u'T0策略'
    dataSourceType = DATA_SOURCE_CSV



    t1 = MyStrategy(tester, strategyNamelist, testContent, buyFee, sellFee, fund, startTime, dataSourceType)
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
    aTestCase(u'2017-06-05', u'2017-08-04', u'2018-01-08') # 日期格式u'2017-09-04'