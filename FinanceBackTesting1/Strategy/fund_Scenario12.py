# -*- coding: utf-8 -*-

# fund scenario strategy
import numpy as np
import pandas as pd
from datetime import date
import sys
if sys.getdefaultencoding() != 'utf-8':# 重新设置python 编码为utf-8
    reload(sys)
    sys.setdefaultencoding('utf-8')
import copy
import math
from Templete.Constant import *
from Templete.TempleteTools import read_csvEx, seriesTop
from Templete.StrategyTemplete2 import StrategyTemplete1
from Templete.Utilities import *

class MyStrategy(StrategyTemplete1):
    '''
    时间频率--日度
    市场趋势情景--- 主观划分指标， 根据市道标签调仓--{显著上升:换仓, 震荡：换仓, 显著下降:平仓}
    市道：rps择时
    指标：夏普
    '''

    def __init__(
            self, tester, strategyNamelist, testContent,
            feeRatio, timeSeriesFile, initFund, startTime,
            dataSourceType, timeFreq, impactCostRatio,
            dateFile, marketPoint):

        super(MyStrategy, self).__init__(
            tester, strategyNamelist, testContent,
            feeRatio, timeSeriesFile, initFund, startTime,
            dataSourceType, timeFreq, impactCostRatio)

        self.dataFile = dateFile
        self.marketPoint = marketPoint
        self.periodTypeList = [u'D', u'W']
        self.todayDate = date.today().isoformat()

    def TrimData(self):
        '''
        清洗和预处理数据
        :return: 处理好的 pandas.Dataframe
        '''
        # 数据导入
        fundNav = read_csvEx(self.dataFile, 0)
        benchName = self.benchmarkName.strip(u'_stocks')
        bench = self.index_DF(benchName, u'stocks')[u'close'].to_frame(benchName)
        fundNav = fundNav.replace({0: np.nan})
        pct_fund = fundNav.pct_change()
        pct_fund.iloc[0, :] = 0.

        # 直接给出市道
        scenarioCond = self.marketPoint
        scenarioCond = scenarioCond.replace({u'下行': u'显著下降', u'上行': u'显著上升'}).loc[bench.index]
        self.stockLatest = pd.read_csv(u'../Data/Main/股票型基金日期.csv', index_col=0)
        self.hybridLatest = pd.read_csv(u'../Data/Main/混合型基金日期.csv', index_col=0)
        self.fixedincomeLatest = pd.read_csv(u'../Data/Main/债券型基金日期.csv', index_col=0)
        self.actualNetasset = read_csvEx(u'../Data/Main/基金规模.csv', 0)

        self.fundNavFirst = fundNav
        self.pct_fund = pct_fund
        self.scenarioCond = scenarioCond
        self.firstTriger = True
        self.typeDict = {u'显著上升': u'股票型基金', u'震荡': u'混合型基金', u'显著下降': u'债券型基金'}

        # 超额收益

        self.tempBenchPct = self.benchmark.values()[0].pct_change().loc[pct_fund.index]
        self.tempBenchPctDf = pd.DataFrame({i: self.tempBenchPct.iloc[:, 0] for i in self.pct_fund})
        # self.exPct = pct_fund - tempDf
        return super(MyStrategy, self).TrimData()

    def scenarioHistF3(self, st, en, scenarioName, num, dataType):
        ''' 情景分布生成 '''
        scenarioCond = copy.deepcopy(self.scenarioCond)
        scenarioIndex = scenarioCond[scenarioCond == scenarioName].index
        scenarioSeries = pd.Series(scenarioIndex, index=scenarioIndex)
        targePeriodIndex = scenarioSeries.loc[st:en].index

        # pct_Data = self.pct_fund.loc[targePeriodIndex]
        pct_Data = self.pct_fund.loc[targePeriodIndex]
        colLen = pct_Data.count(axis=0)
        lenFilter = colLen[colLen >= num].index
        pct_Data = pct_Data[lenFilter]

        if dataType == u'IR':
            # 超额收益，带相关性
            tempDf = pd.concat([pct_Data, self.tempBenchPct.loc[targePeriodIndex]], axis=1)
            corrSeries = tempDf.corr().loc[u'close'].loc[lenFilter].dropna()
            corrFilter = corrSeries.index
            tempBenchPct = self.tempBenchPctDf[lenFilter & corrFilter].loc[targePeriodIndex] * corrSeries
            exPct = pct_Data[corrFilter] - tempBenchPct

        # 时间衰减IR
        # tempLen = pct_Data.index.size
        # tempGap = math.e
        # a1 = 1./sum([pow(tempGap, i) for i in range(tempLen)])
        # dataGap = [a1] + [a1*pow(tempGap, i)for i in range(1, tempLen)]

        fundType = [u'股票型基金', u'债券型基金', u'混合型基金']
        typeDict = {}
        for typeName in fundType:
            scenarioSummary = {u'mean': {}, u'std': {}, u'IR': {}, u'sortino': {}, u'sharpe': {}}
            rf = 0.1 / 252.
            if typeName == u'股票型基金':
                timeFilter = self.stockLatest.index[self.stockLatest['最新净值日期'] > en]
            elif typeName == u'债券型基金':
                rf = 0.05 / 252.
                timeFilter = self.fixedincomeLatest.index[self.fixedincomeLatest['最新净值日期'] > en]
            elif typeName == u'混合型基金':
                timeFilter = self.hybridLatest.index[self.hybridLatest['最新净值日期'] > en]

            if dataType == u'IR':
                filterData = exPct[exPct.columns & timeFilter]
            elif dataType == u'sharpe':
                filterData = pct_Data[pct_Data.columns & timeFilter]
            scenarioSummary[u'mean'][scenarioName] = filterData.mean(axis=0)  # 默认skipna=True
            scenarioSummary[u'std'][scenarioName] = filterData.std(axis=0)
            tempMean = copy.deepcopy(scenarioSummary[u'mean'][scenarioName])
            tempStd = copy.deepcopy(scenarioSummary[u'std'][scenarioName])

            # 时间衰减IR -- 测试效果不好
            # timeDecayWeight = pd.DataFrame(np.transpose([dataGap] * len(filterData.columns)), index=targePeriodIndex, columns=filterData.columns)
            # timeDecayData = pd.eval('filterData * timeDecayWeight')
            # weightedMean = timeDecayData.mean(axis=0)
            # weightedStd = np.sqrt((np.power(filterData - weightedMean, 2) * np.power(timeDecayWeight/tempLen, 2)).sum(axis=0))
            # scenarioSummary[u'sharpe'][scenarioName] = (weightedMean - rf)/weightedStd

            # # 下行标准差
            # filterDf = pct_Data[lenFilter & rptFilter]
            # downsideStd = filterDf[pd.eval('filterDf < tempMean')]
            # downsideStd = np.sqrt(np.power(downsideStd, 2).sum() / float(num))
            # tempSortino = copy.deepcopy(tempMean)

            # tempIR = copy.deepcopy(tempMean)
            # tempIR[pd.eval('tempMean > rf')] = ((tempMean - rf)/tempStd)[pd.eval('tempMean > rf')].values
            # tempIR[pd.eval('tempMean < -rf')] = ((tempMean + rf)/tempStd)[pd.eval('tempMean < -rf')].values
            # tempIR[pd.eval('tempMean >= -rf and tempMean <= rf')] = 0.
            # scenarioSummary[u'IR'][scenarioName] = tempIR                                 # 不包含变动小的基金
            scenarioSummary[dataType][scenarioName] = (tempMean - rf)/tempStd               # 含变动小的基金

            # tempSortino[pd.eval('tempMean > rf')] = ((tempMean - rf) / downsideStd)[pd.eval('tempMean > rf')].values
            # tempSortino[pd.eval('tempMean < -rf')] = ((tempMean + rf) / downsideStd)[pd.eval('tempMean < -rf')].values
            # tempSortino[pd.eval('tempMean >= -rf and tempMean <= rf')] = 0.
            # scenarioSummary[u'sortino'][scenarioName] = tempSortino                     # 不包含变动小的基金

            sortResult = seriesTop(scenarioSummary[dataType][scenarioName], 5, ascending=False)[0]
            # tempResult = seriesPercentage(scenarioSummary[dataType][scenarioName], 0.01, u'right')[0]     # 历史分布--百分比
            typeDict[typeName] = {dataType: {scenarioName: sortResult}}
        return typeDict

    def onBar(self):
        '''
        切片操作函数
        '''
        print(self.TradeTime)
        direction = 1
        self.closeFeeRatio = 0.01
        holding = copy.deepcopy(self.getPositionIndex(direction, u'stocks'))            # 最新持仓
        dataType = u'sharpe'
        periodNum = 60  # 60天

        # 按市道标签调仓
        refTime = self.getTime(ref=1)                                                   # 上一天市道
        if refTime in self.scenarioCond.index:
            scenarioName = self.scenarioCond.loc[refTime]                               # 市道名称
            self.scenarioName = copy.deepcopy(scenarioName)

            if self.typeDict[scenarioName] == u'债券型基金':
                self.closeFeeRatio = 0.001

            scenarioSt, scenarioEn = self.getScenarioRange(scenarioName, periodNum - 1)
            if pd.to_datetime(scenarioSt) >= pd.to_datetime(self.time_List()[0]):

                if self.firstTriger:
                    scenarioResult = self.scenarioHistF3(scenarioSt, scenarioEn, scenarioName, periodNum, dataType)     # 过去1~53周分布
                    targetIndex = scenarioResult[self.typeDict[scenarioName]][dataType][scenarioName].index         # 上周不同市道结果

                    # 复杂报单 -卖出剔除已持有的标的
                    holdingBuy = targetIndex & holding  # 已持有的买入标的
                    theoreticalSell = (holding).difference(targetIndex)  # 卖出不在持仓的标的基金
                    self.mySendOrderF(theoreticalSell, targetIndex, holdingBuy)
                    self.firstTriger = False
                else:
                    if scenarioName != self.refBuffer(u'市道', ref=0):  # 市道改变-调仓
                        scenarioResult = self.scenarioHistF3(scenarioSt, scenarioEn, scenarioName, periodNum, dataType)    # 过去1~53周分布
                        targetIndex = scenarioResult[self.typeDict[scenarioName]][dataType][scenarioName].index     # 上周不同市道结果

                        # 复杂报单 -卖出剔除已持有的标的
                        holdingBuy = targetIndex & holding  # 已持有的买入标的
                        theoreticalSell = (holding).difference(targetIndex)  # 卖出不在持仓的标的基金

                        self.mySendOrderF(theoreticalSell, targetIndex, holdingBuy)
            self.addBuffer(u'市道', scenarioName)

        # # 输出最新一期标的
        # if self.TradeTime == self.time_List()[-1]:
        #     pass  # 切片完成

    def getScenarioRange(self, scenarioName, ref):
        ''' 获取情景分析最近一期至ref期后的时间起点和终点 '''
        en = self.getTime(ref=1)
        scenarioIndex = self.scenarioCond[self.scenarioCond == scenarioName].index
        scenarioEn = scenarioIndex[-1]
        for i in range(1, len(scenarioIndex)):
            if pd.to_datetime(scenarioIndex[i]) > pd.to_datetime(en):
                scenarioEn = scenarioIndex[i-1]
                break
        enLoc = scenarioIndex.get_loc(scenarioEn)
        scenarioSt = scenarioIndex[enLoc - ref]
        scenarioSt, scenarioEn = str(scenarioSt.date()), str(scenarioEn.date())
        return scenarioSt, scenarioEn

    def mySendOrderF(self, theoreticalSell, targetIndex, holdingBuy):
        # 买入标的报单
        direction = 1
        applicationRatio = 0.015
        priceMode, volMode, sliceFund = 0, 0, self.initFund / (1 + applicationRatio)
        fundNav = self.fundNavFirst[targetIndex].loc[self.TradeTime]
        openVolume = np.floor(sliceFund / float(len(fundNav)) / fundNav / 1.) * int(1)
        holdingVol = self.getPositionData(direction, u'stocks', dataType=1)

        if not theoreticalSell.empty:  # 卖出报单
            self.closePosition(direction, u'stocks', targetIndex=theoreticalSell, priceMode=0, volMode=1)

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

    def priceF(self, objectCode):
        ''' 自定义成交量函数 '''
        price = copy.deepcopy(self.fundNavFirst[objectCode].loc[self.TradeTime])
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
    benchmarkName = u'中证基金指数_stocks'
    timeSeriesFile = u'基金净值.csv'
    fund = int(10000000)
    feeRatio = [0.015, 0.01]
    impactCostRatio = [0., 0.0]
    timeFreq = u'D'                                 # 回测时间类型 {u'min':分钟, u'D':日线}
    indexNameList = []
    stockCodelist = []
    strategyNamelist = [u'Smart Indicator FOF策略']
    tester = u'李琦杰'
    testContent = u'不同市道配置评价指标地基金, 选前5'
    dataSourceType = DATA_SOURCE_CSV
    dataFile = u'../Data/Main/基金净值.csv'
    marketPoint = pd.read_excel(u'../Data/Main/市道划分.xlsx', index_col=0, parse_dates=True, encoding='gbk').iloc[:, 3]

    t1 = MyStrategy(tester, strategyNamelist, testContent, feeRatio, timeSeriesFile,
                    fund, startTime, dataSourceType, timeFreq, impactCostRatio,
                    dataFile, marketPoint)
    t1.loadData(primaryDate, endTime, stockCodelist, indexNameList, benchmarkName)
    t1.TrimData()
    t1.Trading()
    t1.outPut()
    print('回测完成')
    timeCost = (time.clock() - time1)/60.
    print("回测共耗时 %.1f 分钟" % timeCost)
#
if __name__== u"__main__":
    aTestCase(u'2010-01-08', u'2015-10-08', u'2018-08-31')  # 日期格式u'2017-09-04'
    # aTestCase(u'2010-01-08', u'2016-06-30', u'2018-08-31')  # 日期格式u'2017-09-04'