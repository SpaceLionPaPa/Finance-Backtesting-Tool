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
from Templete.Constant import *
from Templete.TempleteTools import read_csvEx, seriesTop
from Templete.StrategyTemplete2 import StrategyTemplete1
from Templete.Utilities import *

class MyStrategy(StrategyTemplete1):
    '''
    时间频率--日度
    市场趋势情景--- 主观划分指标， 根据市道标签调仓--{显著上升:换仓, 震荡：换仓, 显著下降:平仓}
    市道：{时间：一季度中出现次数最多的市道标签, 指标：IR}
    '''

    def __init__(
            self, tester, strategyNamelist, testContent,
            feeRatio, timeSeriesFile, initFund, startTime,
            dataSourceType, timeFreq, impactCostRatio,
            dateFile, marketPoint, fundLatestRptDate):

        super(MyStrategy, self).__init__(
            tester, strategyNamelist, testContent,
            feeRatio, timeSeriesFile, initFund, startTime,
            dataSourceType, timeFreq, impactCostRatio)

        self.dataFile = dateFile
        self.marketPoint = marketPoint
        self.fundLatestRptDate = fundLatestRptDate
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
        fundLatestRptDate = self.fundLatestRptDate[pct_fund.columns].dropna()

        self.fundNavFirst = fundNav
        self.pct_fund = pct_fund
        self.scenarioCond = scenarioCond
        self.fundLatestRptDate = fundLatestRptDate
        self.firstTriger = True
        return super(MyStrategy, self).TrimData()

    def scenarioHistF(self, st, en, scenarioName, num):
        ''' 情景分布生成 '''
        # if self.TradeTime == '2017-06-08':
        #     pass
        scenarioSummary = {u'mean': {}, u'std': {}, u'IR': {}, u'sortino': {}, u'sharpe': {}}
        distDict = {u'mean': {}, u'std': {}, u'IR': {}, u'sortino': {}, u'sharpe': {}}
        scenarioCond = copy.deepcopy(self.scenarioCond)
        scenarioIndex = scenarioCond[scenarioCond == scenarioName].index
        # scenarioPeriod = pd.Series(index=freqTransfer(scenarioIndex, freq='W').to_series().astype(str).values)
        scenarioSeries = pd.Series(scenarioIndex, index=scenarioIndex)
        targePeriodIndex = scenarioSeries.loc[st:en].index

        pct_Data = self.pct_fund.loc[targePeriodIndex]
        colLen = pct_Data.count(axis=0)
        lenFilter = colLen[colLen >= num].index
        rptFilter = self.fundLatestRptDate[self.fundLatestRptDate >= self.TradeTime].index
        filterData = pct_Data[lenFilter & rptFilter]

        scenarioSummary[u'mean'][scenarioName] = filterData.mean(axis=0)  # 默认skipna=True
        scenarioSummary[u'std'][scenarioName] = filterData.std(axis=0)
        tempMean = copy.deepcopy(scenarioSummary[u'mean'][scenarioName])
        tempStd = copy.deepcopy(scenarioSummary[u'std'][scenarioName])
        rf = 0.1/252.

        # timeDecayWeight = pd.DataFrame(np.transpose([(dateGap/dateGap.sum()).values] * len(filterData.columns)), index=targePeriodIndex, columns=filterData.columns)
        # timeDecayData = pd.eval('filterData * timeDecayWeight')
        # weightedMean = timeDecayData.mean(axis=0)
        # weightedStd = np.sqrt((np.power(filterData - weightedMean, 2) * np.power(timeDecayWeight/len(targePeriodIndex), 2)).sum(axis=0))
        # scenarioSummary[u'sharpe'][scenarioName] = (weightedMean - rf)/weightedStd

        tempIR = copy.deepcopy(tempMean)
        # # 下行标准差
        # filterDf = pct_Data[lenFilter & rptFilter]
        # downsideStd = filterDf[pd.eval('filterDf < tempMean')]
        # downsideStd = np.sqrt(np.power(downsideStd, 2).sum() / float(num))
        # tempSortino = copy.deepcopy(tempMean)

        # tempIR[pd.eval('tempMean > rf')] = ((tempMean - rf)/tempStd)[pd.eval('tempMean > rf')].values
        # tempIR[pd.eval('tempMean < -rf')] = ((tempMean + rf)/tempStd)[pd.eval('tempMean < -rf')].values
        # tempIR[pd.eval('tempMean >= -rf and tempMean <= rf')] = 0.
        # scenarioSummary[u'IR'][scenarioName] = tempIR                                   # 不包含变动小的基金
        # scenarioSummary[u'IR'][scenarioName] = tempMean/tempStd                         # 含变动小的基金

        scenarioSummary[u'sharpe'][scenarioName] = (tempMean - rf)/tempStd

        # tempSortino[pd.eval('tempMean > rf')] = ((tempMean - rf) / downsideStd)[pd.eval('tempMean > rf')].values
        # tempSortino[pd.eval('tempMean < -rf')] = ((tempMean + rf) / downsideStd)[pd.eval('tempMean < -rf')].values
        # tempSortino[pd.eval('tempMean >= -rf and tempMean <= rf')] = 0.
        # scenarioSummary[u'sortino'][scenarioName] = tempSortino                         # 不包含变动小的基金

        distDict[u'sharpe'][scenarioName] = seriesTop(scenarioSummary[u'sharpe'][scenarioName], 10, ascending=False)[0]
        # distDict[u'IR'][scenarioName] = seriesTop(scenarioSummary[u'IR'][scenarioName], 5, ascending=False)[0]
        # rank = distDict[u'IR'][scenarioName].to_csv('rank.csv')
        # distDict[u'IR'][scenarioName] = seriesPercentage(scenarioSummary[u'IR'][scenarioName], 0.01, u'right')[0]     # 历史分布--百分比
        return distDict

    def onBar(self):
        '''
        切片操作函数
        '''
        print(self.TradeTime)
        self.closeFeeRatio = 0.01
        direction = 1
        holding = copy.deepcopy(self.getPositionIndex(direction, u'stocks'))            # 最新持仓

        # 按市道标签调仓
        refTime = self.getTime(ref=1)                                                   # 上一天市道
        if refTime in self.scenarioCond.index:
            scenarioName = self.scenarioCond.loc[refTime]                               # 市道名称
            self.scenarioName = copy.deepcopy(scenarioName)

            if scenarioName in [u'显著上升', u'震荡']:
                periodNum = 60  # 60天
                scenarioSt, scenarioEn = self.getScenarioRange(scenarioName, periodNum - 1)

                if pd.to_datetime(scenarioSt) >= pd.to_datetime(self.time_List()[0]):

                    dataType = u'sharpe'
                    if self.firstTriger:
                        scenarioResult = self.scenarioHistF(scenarioSt, scenarioEn, scenarioName, periodNum)  # 过去1~53周分布
                        targetIndex = scenarioResult[dataType][scenarioName].index  # 上周不同市道结果

                        # 复杂报单 -卖出剔除已持有的标的
                        holdingBuy = targetIndex & holding  # 已持有的买入标的
                        theoreticalSell = (holding).difference(targetIndex)  # 卖出不在持仓的标的基金
                        self.mySendOrderF(theoreticalSell, targetIndex, holdingBuy)
                        self.firstTriger = False
                    else:
                        if scenarioName != self.refBuffer(u'市道', ref=0):               # 市道改变-调仓
                            scenarioResult = self.scenarioHistF(scenarioSt, scenarioEn, scenarioName, periodNum)  # 过去1~53周分布
                            targetIndex = scenarioResult[dataType][scenarioName].index  # 上周不同市道结果

                            # 复杂报单 -卖出剔除已持有的标的
                            holdingBuy = targetIndex & holding  # 已持有的买入标的
                            theoreticalSell = (holding).difference(targetIndex)  # 卖出不在持仓的标的基金

                            self.mySendOrderF(theoreticalSell, targetIndex, holdingBuy)

            elif scenarioName == u'显著下降':     # 平仓
                if not holding.empty:  # 卖出持仓
                    self.closePosition(direction, u'stocks', targetIndex=holding, priceMode=0, volMode=1)
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

    def mySendOrderF(self, theoreticalSell, targetIndex, holdingBuy,):
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
    impactCostRatio = [0., 0.]
    timeFreq = u'D'                                 # 回测时间类型 {u'min':分钟, u'D':日线}
    indexNameList = []
    stockCodelist = []
    strategyNamelist = [u'市道夏普率FOF策略，单利']
    tester = u'李琦杰'
    testContent = u'市道夏普率排序，只在混合型基金里跳前5'
    dataSourceType = DATA_SOURCE_CSV
    dataFile = u'../Data/Main/基金净值.csv'
    marketPoint = pd.read_excel(u'../Data/Main/市道划分.xlsx', index_col=0, parse_dates=True, encoding='gbk').iloc[:, 3]
    # fundLatestRptDate = pd.read_excel(u'../Data/Main/stkcodefinl2.xlsx', parse_dates=True, encoding='gbk').iloc[:,[0, 3]]
    # fundLatestRptDate = pd.Series(fundLatestRptDate.iloc[:, 1].values, index=fundLatestRptDate.iloc[:, 0])
    fundLatestRptDate = pd.read_csv(u'../Data/Main/混合型基金日期.csv', index_col=0)[u'最新净值日期']

    t1 = MyStrategy(tester, strategyNamelist, testContent, feeRatio, timeSeriesFile,
                    fund, startTime, dataSourceType, timeFreq, impactCostRatio,
                    dataFile, marketPoint, fundLatestRptDate)
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