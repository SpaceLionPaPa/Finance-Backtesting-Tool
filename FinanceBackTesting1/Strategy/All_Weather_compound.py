# -*- coding: utf-8 -*-
# all-weather strategy
# auther: Qijie Li

import sys
if sys.getdefaultencoding() != 'utf-8':  # 重新设置python 编码为utf-8
    reload(sys)
    sys.setdefaultencoding('utf-8')
import numpy as np
import pandas as pd
import copy
from Templete.tempFile import mktCN_Dict
from Templete.Constant import *
from Templete.StrategyTemplete2 import StrategyTemplete1
from Templete.Utilities import *


class MyStrategy(StrategyTemplete1):
    '''
    全天候策略
    '''

    def __init__(
            self, tester, strategyNamelist, testContent,
            feeRatio, timeSeriesFile, initFund, startTime,
            dataSourceType, timeFreq, impactCostRatio):

        super(MyStrategy, self).__init__(
            tester, strategyNamelist, testContent,
            feeRatio, timeSeriesFile, initFund, startTime,
            dataSourceType, timeFreq, impactCostRatio)
        self.targetStd = 0.05   # 百分比
        self.periodTypeList = [u'D', u'W', u'M']
        self.firstTriger = True

    def TrimData(self):
        # 指标
        st, en = self.time_List()[0], self.endTime
        indicesData = pd.read_csv('indicesData.csv', index_col=0, parse_dates=True).loc[st: en]
        indicesData = indicesData.rename(columns={i: mktCN_Dict[i] for i in indicesData.columns})
        self.targetClose = copy.deepcopy(indicesData)
        self.indiceRet = indicesData[[u'沪深300', u'南华商品指数', u'中证企业债', u'中债-国债总财富(总值)指数', u'中债-金融债券总财富(总值)指数']].pct_change()
        marcoIndices = pd.read_csv('marcoIndices.csv', index_col=0, parse_dates=True).loc[st: en]
        self.marcoIndices = marcoIndices.rename(columns={i: mktCN_Dict[i] for i in marcoIndices.columns}).fillna(method='ffill').dropna()
        return

    def onBar(self):
        curLoc = self.getLoc()
        curTime = self.TradeTime
        locRange = self.getPeriodrange(freq='M')
        if datetime.strptime(self.TradeTime, "%Y-%m-%d").month in [1, 4, 7, 10]:  # 本季度第一个交易日
            if datetime.strptime(self.TradeTime, "%Y-%m-%d").weekday() in [0]:  # 周1
                midLoc = locRange[0] + int(round((locRange[-1] - locRange[0]) / 2))
                self.addBuffer(u'tempLoc', curLoc)
                if self.refBuffer(u'tempLoc', ref=1) <= midLoc and curLoc > midLoc:
                    print(curTime)
                    lastDate = self.getTime(1)
                    refDate = self.getTime(253)
                    marco = self.marcoIndices.loc[lastDate]
                    indicesRet = self.indiceRet.loc[refDate: lastDate]
                    indicesWeight = 1/indicesRet.std()
                    gdpCond = (marco[u'GDP: 当季同比'] - marco[u'预测GDP: 当季同比']) >= 0
                    cpiCond = (marco[u'CPI: 当月同比'] - marco[u'预测CPI: 当月同比']) >= 0
                    if gdpCond:
                        list1 = [u'沪深300', u'南华商品指数']
                    else:
                        list1 = [u'中债-金融债券总财富(总值)指数', u'中债-国债总财富(总值)指数']

                    if cpiCond:
                        list2 = [u'中债-国债总财富(总值)指数', u'南华商品指数']
                    else:
                        list2 = [u'中证企业债', u'中债-金融债券总财富(总值)指数']
                    if gdpCond and cpiCond:
                        self.note2 = u'经济超预期&通胀超预期'
                    if gdpCond and not cpiCond:
                        self.note2 = u'经济超预期&通胀低于预期'
                    if not gdpCond and cpiCond:
                        self.note2 = u'经济低于预期&通胀超预期'
                    if not gdpCond and not cpiCond:
                        self.note2 = u'经济低于预期&通胀低于预期'
                    assetWeight1 = indicesWeight[list1] / indicesWeight[list1].sum()
                    assetWeight2 = indicesWeight[list2] / indicesWeight[list2].sum()
                    asset1 = (indicesRet[list1] * assetWeight1).sum(axis=1)
                    asset2 = (indicesRet[list2] * assetWeight2).sum(axis=1)
                    stdWeight1 = self.targetStd / asset1.std()
                    stdWeight2 = self.targetStd / asset2.std()
                    actualWeight1 = stdWeight1 * assetWeight1 * 0.25
                    actualWeight2 = stdWeight2 * assetWeight2 * 0.25
                    targetSeries = (actualWeight1.append(actualWeight2)).groupby(level=0).sum()

                    if self.firstTriger:
                        direction = 1
                        holding = self.getPositionIndex(direction, u'stocks')  # 最新持仓
                        holdingBuy = targetSeries.index & holding  # 已持有的买入标的
                        theoreticalSell = (holding).difference(targetSeries.index)  # 卖出不在持仓的标的基金
                        self.mySendOrderF(theoreticalSell, targetSeries, holdingBuy)
                        self.firstTriger = False
                    else:
                        if str(gdpCond) + str(cpiCond) != self.refBuffer(u'市道', ref=0):  # 市道改变-调仓
                            direction = 1
                            holding = self.getPositionIndex(direction, u'stocks')  # 最新持仓
                            holdingBuy = targetSeries.index & holding  # 已持有的买入标的
                            theoreticalSell = (holding).difference(targetSeries.index)  # 卖出不在持仓的标的基金
                            self.mySendOrderF(theoreticalSell, targetSeries, holdingBuy)
                    self.addBuffer(u'市道', str(gdpCond)+ str(cpiCond))
        self.interestRate = self.targetClose[u'银行间质押式1日回购利率'][self.TradeTime]/100./365.
        return

    def mySendOrderF(self, theoreticalSell, targetSeries, holdingBuy):
        ''' 复利报单 '''
        # 买入标的报单
        direction = 1
        priceMode, volMode = 0, 0
        applicationRatio = 0.003    # 申购费

        if not theoreticalSell.empty:  # 先平仓不继续持有地标的
            self.closePosition(direction, u'stocks', targetIndex=theoreticalSell, priceMode=0, volMode=1)

        # 浮动净值
        positionIndex = self.getPositionIndex(direction, u'stocks')
        holdingVol = self.getPositionData(direction, u'stocks', dataType=1)
        navSeries = self.targetClose[positionIndex].loc[self.TradeTime]
        priceGap = navSeries - self.getPositionData(direction, u'stocks', dataType=2)
        multiplier = pd.Series([1.] * len(positionIndex), index=positionIndex)
        holdingPL = priceGap * multiplier * holdingVol * int(direction) * (1 - self.closeFeeRatio)
        PL = holdingPL.sum()        # 浮动盈亏
        floatingValue = self.getCaiptal() + PL      # 复利账户金额
        sliceFund = floatingValue/(1 + applicationRatio)

        # 杠杆成本
        price_close = self.targetClose[targetSeries.index].loc[self.TradeTime]
        openVolume = np.floor(sliceFund / float(len(price_close)) / price_close / 1.) * int(1)
        totalVol = openVolume * (targetSeries)

        for i in targetSeries.index:  # 买入报单
            openVol = totalVol.loc[i]
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
                fundIndex = pd.Index([i])
                leverageVol = totalVol[i] - openVolume[i]
                if leverageVol > 0:
                    leveragDf = pd.DataFrame(
                        [[leverageVol, openVolume[i], price_close[i], self.TradeTime]],
                        columns=[u'leverageVol', u'selfVol', u'costPrice', u'time'], index=fundIndex)
                else:
                    leveragDf = None
                openOrder = self.orderRecorder(1, fundIndex, u'stocks', direction, leveragDf)  # 记录交易单
                self.smartSendOrder(openOrder, priceMode, volMode)
        return

    def priceF(self, objectCode):
        ''' 自定义成交量函数 '''
        price = copy.deepcopy(self.targetClose[objectCode].loc[self.TradeTime])
        return price

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
    timeSeriesFile = u'hs300_stocks.csv'
    fund = int(10000000)
    feeRatio = [0.000, 0.000]
    impactCostRatio = [0., 0.]
    timeFreq = u'D'     # 回测时间类型 {u'min':分钟, u'D':日线}
    indexNameList = []
    stockCodelist = []
    strategyNamelist = [u'全天候策略_复利计算']
    tester = u'李琦杰'
    testContent = u'不同宏观经济情景等风险配置资产'
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
if __name__== u"__main__":
    st, en = u'2015-01-05', u'2018-10-26'
    # import pymysql
    # from sqlalchemy import create_engine
    # dbName = 'tempStocks'
    # dbConn = pymysql.connect(host="localhost", user="root", password="324265", db=dbName, port=3306)
    # engine = create_engine("mysql+pymysql://root:" + '324265' + "@localhost/%s" % dbName)
    # codeList = ['000300.SH', 'H11008.CSI', 'CBA01201.CS', 'CBA00601.CS', 'NH0100.NHF', 'DR001.IB']
    # # codeList = ['M0039354', 'M0061673', 'M0000612', 'M0061676']
    # codeStr = str(codeList).strip('[').strip(']').replace("'", "`")
    # indicesSql = ''' SELECT `TIME`, %s FROM indices_close WHERE `TIME` BETWEEN "%s" AND "%s"''' % (codeStr, st, en)
    # indicesData = pd.read_sql(indicesSql, engine).set_index(u'TIME')
    # indicesData.index = pd.DatetimeIndex(indicesData.index)
    # indicesData = indicesData.sort_index()
    # indicesData.to_csv('indicesData.csv')

    aTestCase(u'2010-01-08', st, en)  # 日期格式u'2017-09-04'
