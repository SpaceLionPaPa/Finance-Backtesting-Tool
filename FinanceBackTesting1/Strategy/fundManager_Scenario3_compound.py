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
            dateFile, marketPoint):

        super(MyStrategy, self).__init__(
            tester, strategyNamelist, testContent,
            feeRatio, timeSeriesFile, initFund, startTime,
            dataSourceType, timeFreq, impactCostRatio)

        self.dataFile = dateFile
        self.marketPoint = marketPoint
        self.periodTypeList = [u'Q', u'M', u'A']

    def TrimData(self):
        '''
        清洗和预处理数据
        :return: 处理好的 pandas.Dataframe
        '''
        # 数据导入
        fundNav = read_csvEx(self.dataFile, 0)
        fundNav = fundNav.replace({0: np.nan})
        pct_fund = fundNav.pct_change()
        pct_fund.iloc[0, :] = 0.

        # 基金经理代码
        self.managerIndex = []

        # 直接给出市道
        self.fund_predfund_manager = pd.read_csv(u'../Data/Main/fund_predfund_manager.csv', encoding='gbk')
        managerIndex = self.fund_predfund_manager.index

        # 基金经理管理基金时间处理
        stCol = []
        enCol = []
        lastDate = self.time_List()[-1]
        for i in range(self.fund_predfund_manager.index.size):
            tempStr = self.fund_predfund_manager[u'fund_manage_time'].iloc[i]
            try:
                if isinstance(tempStr, unicode):
                    if u'至今' in tempStr:
                        tempSt, tempEn = [tempStr.strip(u'至今'), lastDate]
                    else:
                        tempSt, tempEn = tempStr.split(u'-')
                else:
                    tempSt, tempEn = [None, None]
            except:
                tempSt, tempEn = [None, None]
            stCol.append(tempSt)
            enCol.append(tempEn)

        self.fund_predfund_manager[u'fund_manage_time_st'] = pd.Series(pd.DatetimeIndex(stCol, index=managerIndex))
        self.fund_predfund_manager[u'fund_manage_time_en'] = pd.Series(pd.DatetimeIndex(enCol, index=managerIndex))
        self.actualNetasset = read_csvEx(u'../Data/Main/基金规模.csv', 0)
        self.fundNavFirst = read_csvEx(u'../Data/Main/基金净值.csv', 0)
        self.fund_list = pd.read_csv(u'../Data/Main/fund_list.csv', index_col=0, encoding='utf-8')
        self.firstTriger = True
        self.pct_fund = pct_fund
        return super(MyStrategy, self).TrimData()

    def scenarioHistF2(self, longRef, shortRef, scenarioName, num):
        ''' 情景分布函数：长短维度'''

        scenarioSummary = {u'mean': {}, u'std': {}, u'IR': {}, u'sortino': {}, u'sharpe': {}, u'excess_sharpe': {}}
        distDict = {u'mean': {}, u'std': {}, u'IR': {}, u'sortino': {}, u'sharpe': {}, u'excess_sharpe': {}}
        en = self.getTime(ref=1)  # 上一天
        longSt = self.getTime(ref=longRef)
        shortSt = self.getTime(ref=shortRef)
        benchmark = self.benchmark.values()[0].pct_change(fill_method=None)
        benchmark = benchmark.rename(columns={u'close': u'benchmark'})
        if scenarioName:
            scenarioCond = copy.deepcopy(self.scenarioCond)
            scenarioIndex = scenarioCond[scenarioCond == scenarioName].index
            scenarioSeries = pd.Series(scenarioIndex, index=scenarioIndex)
            targePeriodIndex = scenarioSeries.loc[longSt:en].index
            pct_Data = self.pct_fund.loc[targePeriodIndex]
        else:
            pct_Data = self.pct_fund.loc[longSt:en]
            pct_Data_short = self.pct_fund.loc[shortSt:en]
        colLen = pct_Data.count(axis=0)
        lenFilter = colLen[colLen >= num].index

        filterData = pct_Data[lenFilter]
        filterData_short = pct_Data_short[lenFilter]
        corrData = filterData.corrwith(benchmark.loc[filterData.index].benchmark)
        tempSeries = benchmark[u'benchmark']
        corrBench = pd.DataFrame({i: tempSeries for i in filterData.columns}) * corrData
        excessPct = filterData - corrBench

        scenarioSummary[u'mean'][scenarioName] = filterData.mean(axis=0)  # 默认skipna=True
        scenarioSummary[u'std'][scenarioName] = filterData.std(axis=0)
        tempMean = scenarioSummary[u'mean'][scenarioName]
        tempStd = scenarioSummary[u'std'][scenarioName]
        shortMean = filterData_short.mean(axis=0)
        shortStd = filterData_short.std(axis=0)

        # 时间衰减IR -- 测试效果不好
        # timeDecayWeight = pd.DataFrame(np.transpose([dataGap] * len(filterData.columns)), index=targePeriodIndex, columns=filterData.columns)
        # timeDecayData = pd.eval('filterData * timeDecayWeight')
        # weightedMean = timeDecayData.mean(axis=0)
        # weightedStd = np.sqrt((np.power(filterData - weightedMean, 2) * np.power(timeDecayWeight/tempLen, 2)).sum(axis=0))
        # scenarioSummary[u'sharpe'][scenarioName] = (weightedMean - rf)/weightedStd

        # 下行标准差
        # downsideStd = filterData[pd.eval('filterData < tempMean')]
        # downsideStd = np.sqrt(np.power(downsideStd, 2).sum() / float(num))
        # tempSortino = copy.deepcopy(tempMean)
        # tempSortino[pd.eval('tempMean > rf')] = ((tempMean - rf) / downsideStd)[pd.eval('tempMean > rf')].values
        # tempSortino[pd.eval('tempMean < -rf')] = ((tempMean + rf) / downsideStd)[pd.eval('tempMean < -rf')].values
        # tempSortino[pd.eval('tempMean >= -rf and tempMean <= rf')] = 0.
        # scenarioSummary[u'sortino'][scenarioName] = tempSortino                       # 不包含变动小的基金
        # downsideStd_short = filterData_short[pd.eval('filterData_short < shortMean')]
        # downsideStd_short = np.sqrt(np.power(downsideStd_short, 2).sum() / float(num))
        # tempSortino_short = (shortMean - rf)/downsideStd_short
        # tempSortino_short[pd.eval('shortMean > rf')] = ((shortMean - rf) / downsideStd_short)[pd.eval('shortMean > rf')].values
        # tempSortino_short[pd.eval('shortMean < -rf')] = ((shortMean + rf) / downsideStd_short)[pd.eval('shortMean < -rf')].values
        # tempSortino_short[pd.eval('shortMean >= -rf and shortMean <= rf')] = 0.

        # tempIR = copy.deepcopy(tempMean)
        # tempIR[pd.eval('tempMean > rf')] = ((tempMean - rf)/tempStd)[pd.eval('tempMean > rf')].values
        # tempIR[pd.eval('tempMean < -rf')] = ((tempMean + rf)/tempStd)[pd.eval('tempMean < -rf')].values
        # tempIR[pd.eval('tempMean >= -rf and tempMean <= rf')] = 0.
        # scenarioSummary[u'IR'][scenarioName] = tempIR                                 # 不包含变动小的基金
        # scenarioSummary[u'IR'][scenarioName] = tempMean/tempStd                         # 含变动小的基金

        # # 夏普率
        # scenarioSummary[u'sharpe'][scenarioName] = (tempMean - rf)/tempStd
        # tempSharpe_short = (shortMean - 0.06/252)/shortStd

        # 超额夏普
        excessSharpe = (excessPct.mean(axis=0))/excessPct.std(axis=0)

        # tempIR.to_csv('IR.csv')
        # distDict[u'IR'][scenarioName] = seriesTop(scenarioSummary[u'IR'][scenarioName], 10, ascending=False)[0]
        # rank = distDict[u'IR'][scenarioName].to_csv('rank.csv')
        # distDict[u'IR'][scenarioName] = seriesPercentage(scenarioSummary[u'IR'][scenarioName], 0.01, u'right')[0]     # 历史分布--百分比
        longSort = seriesTop(excessSharpe, 5, ascending=False)[0]  # 长期
        # shortSort = seriesTop(shortMean.loc[longSort.index], 5, ascending=False)[0]          # 短期
        # longSort = seriesTop(tempMean, 5, ascending=False)[0]  # 长期
        distDict[u'excess_sharpe'][scenarioName] = longSort
        return distDict

    def onBar(self):
        '''
        切片操作函数
        '''
        direction = 1
        priceMode, volMode = 0, 0
        dataType = u'excess_sharpe'
        periodNum = 252
        refTime = self.getTime(ref=1)
        refTime2 = self.getTime(ref=252)
        delist = [u'混合债券型一级基金', u'混合债券型二级基金']
        curMonth = self.getPeriod(freq=u'M').split('-')[-1]

        if self.getLoc() == self.getPeriodrange(freq=u'M')[0] and curMonth in ['01', '07']:           # 按半年调仓
            print(self.TradeTime)
            # if pd.to_datetime(st) >= pd.to_datetime(self.time_List()[0]):
            scenarioResult = self.scenarioHistF2(periodNum, 10, None, periodNum)           # 过去periodNum天的分布
            managerIndex = scenarioResult[dataType][None].index
            tempIndex = pd.Index([self.TradeTime]).append(managerIndex)
            self.managerIndex.append(tempIndex)
            targetIndexList = []
            fullIndex = pd.Index([])

            # 基金份额确定
            for tempName in managerIndex:
                index1 = self.fund_predfund_manager[u'flag'][self.fund_predfund_manager[u'flag'] == tempName].index
                timeFilter1 = self.fund_predfund_manager[u'fund_manage_time_st'].loc[index1]
                timeFilter2 = self.fund_predfund_manager[u'fund_manage_time_en'].loc[index1]
                index2 = timeFilter1[timeFilter1 <= refTime].index & timeFilter2[timeFilter2 > refTime].index
                targetIndex = pd.Index(self.fund_predfund_manager[u'fund_code'].loc[index2])

                # 基金筛选
                # 1. 成立日期筛选
                setupDate = self.fund_list[u'fund_setupdate'].loc[targetIndex]
                targetIndex = setupDate[setupDate < refTime2].index     # 剔除不满一年
                # 2. 二级分类筛选
                investType = self.fund_list[u'fund_investtype'].loc[targetIndex]
                targetIndex = investType[investType.isin(delist) == False].index   # 剔除分级债券基金

                if not targetIndex.empty:
                    targetIndexList.append(targetIndex)
                    fullIndex = fullIndex.append(targetIndex)
            fullIndex = pd.Index(fullIndex.unique())

            # 复利--最新组合价值
            # 复杂报单 -卖出剔除已持有的标的
            holding = copy.deepcopy(self.getPositionIndex(direction, u'stocks'))  # 最新持仓)
            holdingBuy = fullIndex & holding  # 已持有的买入标的
            theoreticalSell = holding.difference(fullIndex)  # 卖出不在持仓的标的基金

            # 卖出不继续持有地标的
            if not theoreticalSell.empty:
                self.closePosition(direction, u'stocks', targetIndex=theoreticalSell, priceMode=0, volMode=1)

            # 浮动净值
            positionIndex = self.getPositionIndex(direction, u'stocks')
            holdingVol = self.getPositionData(direction, u'stocks', dataType=1)
            navSeries = self.fundNavFirst[positionIndex].loc[self.TradeTime]
            priceGap = navSeries - self.getPositionData(direction, u'stocks', dataType=2)
            multiplier = pd.Series([1.] * len(positionIndex), index=positionIndex)
            holdingPL = priceGap * multiplier * holdingVol * int(direction) * (1 - self.closeFeeRatio)
            PL = holdingPL.sum()
            floatingValue = self.getCaiptal() + PL

            seedFund = pd.Series()
            for targetIndex in targetIndexList:
                dividedSeedfund = float(floatingValue / len(targetIndexList))
                num1 = targetIndex.size
                # netasset = self.actualNetasset[targetIndex].loc[refTime]                      # 基金规模
                # seedFund = seedFund.append(dividedSeedfund * (netasset / netasset.sum()))     # 标的金额-规模加权
                seedFund = seedFund.append(pd.Series([dividedSeedfund/num1]*num1, index=targetIndex))   # 标的金额-等全
            seedFund = seedFund.groupby(level=0).sum()                                          # 每个标的金额

            # 买入标的报单
            fundNav = self.fundNavFirst[fullIndex].loc[self.TradeTime]
            openVolume = np.floor(seedFund / fundNav)
            holdingVol = self.getPositionData(direction, u'stocks', dataType=1)
            for i in fullIndex:  # 买入报单
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
            pass  # on bar 报单完毕

        # 输出最新一期标的
        if self.TradeTime == self.time_List()[-1]:
            pd.DataFrame(self.managerIndex).to_csv(u'../Output/managerIndex_%s.csv' %(self.strategyNamelist[0]), encoding='utf_8_sig', header=None)
            pass  # 切片完成

    def mySendOrderF2(self, theoreticalSell, targetIndex, holdingBuy,):
        ''' 复利报单 '''
        # 买入标的报单
        direction = 1
        applicationRatio = 0.015
        priceMode, volMode = 0, 0

        if not theoreticalSell.empty:  # 先平仓不继续持有地标的
            self.closePosition(direction, u'stocks', targetIndex=theoreticalSell, priceMode=0, volMode=1)

        # 浮动净值
        positionIndex = self.getPositionIndex(direction, u'stocks')
        holdingVol = self.getPositionData(direction, u'stocks', dataType=1)
        navSeries = self.fundNavFirst[positionIndex].loc[self.TradeTime]
        priceGap = navSeries - \
                   self.getPositionData(direction, u'stocks', dataType=2)
        # multiplier = self.c_multiplier(objectType)[positionIndex]
        multiplier = pd.Series([1.] * len(positionIndex), index=positionIndex)
        holdingPL = priceGap * multiplier * holdingVol * int(direction) * (1 - self.closeFeeRatio)
        PL = holdingPL.sum()
        floatingValue = self.getCaiptal() + PL
        sliceFund = floatingValue / (1 + applicationRatio)
        fundNav = self.fundNavFirst[targetIndex].loc[self.TradeTime]
        openVolume = np.floor(sliceFund / float(len(fundNav)) / fundNav / 1.) * int(1)

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
    benchmarkName = u'hs300_stocks'
    timeSeriesFile = u'haha2.csv'
    fund = 10000000.
    feeRatio = [0.015, 0.01]
    impactCostRatio = [0., 0.]
    timeFreq = u'D'                                 # 回测时间类型 {u'min':分钟, u'D':日线}
    indexNameList = []
    stockCodelist = []
    strategyNamelist = [u"Manager's excess_sharpe FOF策略"]
    tester = u'李琦杰'
    testContent = u'hs300的超额收益的sharpe, 标的等权, 复利'
    dataSourceType = DATA_SOURCE_CSV
    dataFile = u'../Data/Main/haha2.csv'
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
#
#
if __name__== u"__main__":
    # aTestCase(u'2010-01-08', u'2015-10-08', u'2018-02-23')  # 季度
    # aTestCase(u'2010-01-08', u'2015-07-01', u'2018-08-31')  # 半年
    aTestCase(u'2010-01-08', u'2015-01-05', u'2018-08-31')  # 年