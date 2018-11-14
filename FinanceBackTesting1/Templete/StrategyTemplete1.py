# -*- coding:utf-8 -*-

# Program: This is a templete script to run backtest for hybird contracts strategy.
# Writer: Qijie Li
# Date: January.31.2018

import copy
import pandas as pd
import numpy as np
import shutil
import os.path as osPath
from Templete.DataProcessingCode import DataProcessing
from Templete.OutputCode import OutputClass
from Templete import TempleteTools
from TempleteTools import *
from Templete.OptionsTools import callPutIndexEx
from Templete.Constant import *
from Templete.Utilities import *

def LogSomething(path, file, strParm, otherStr):
    Log(u'数据搜索目录: ' + path)
    Log(u'时间序列文件: %s, 时间列索引号: %d.' %
        (file, strParm))
    Log(otherStr)

class StrategyTemplete1(object):
    '''
    期权回测的类
    功能：历史回测
    公共方法：
    '''
    CSV_PATH          = u'../Data/Main/'
    INDEX_COLUMN_NO   = 0

    def __init__(self, tester, strategyNamelist, testContent,
                 feeRatio, timeSeriesFile, initFund, startTime,
                 dataSourceType = DATA_SOURCE_CSV,
                 timeFreq=u'D', impactCostRatio= [0.]*2):
        '''
        初始化程序, 参数说明
        :param feeRatio:            feeRatio[0]开仓手续费率, feeRatio[-1] 平仓手续费率
        :param timeSeriesFile:      时间数据的导入文件名
        :param initFund:            初始资金
        :param startTime:           回测起始日期
        :param dataSourceType:      加载数据类型, 取值范围: DATA_SOURCE_CSV, DATA_SOURCE_WIND, DATA_SOURCE_MONGODB
        :param timeFreq:            时间频率 {u'1T':1分钟, u'D':日线}
        :param impactCostRatio:     冲击成本比率
        '''

        Log(u'策略初始化......')

        if   dataSourceType == DATA_SOURCE_CSV:
            LogSomething(
                StrategyTemplete1.CSV_PATH, timeSeriesFile,
                StrategyTemplete1.INDEX_COLUMN_NO, u'导入CSV包')

        if dataSourceType   == DATA_SOURCE_HTTPAPI:
            StrategyTemplete1.CSV_PATH = u'../Data/ApiData/'
            # 清理空路径数据
            cur_dir = os.getcwd()
            templete_Dir = os.path.dirname(cur_dir)
            dataPath = templete_Dir + u'/Data/ApiData/'         # 返回上级目录并新建路径
            if os.path.isdir(dataPath):                         # 如果路径名存在
                shutil.rmtree(dataPath)                         # 删除原来文件夹
            os.mkdir(dataPath)                                  # 创建文件夹
            try:
                from API.DataAPI import APIClass
                self.APIeg = APIClass(u'../API/indexconfig.txt')
                self.indicesNameList = self.APIeg.indicesList
                LogSomething(
                    StrategyTemplete1.CSV_PATH, timeSeriesFile,
                    StrategyTemplete1.INDEX_COLUMN_NO, u'导入API包')

            except BaseException, e:
                Log(u'未导入API包!')

        self.__Buffer           = {}

        self.endTime            = u''
        self.primaryTime        = u''                            # 数据下载起点
        self.benchmark          = {}
        self.position           = {int(1): {},
                                   int(-1): {}
                                   }                             # 1 多头； -1 空头

        # 参数设置:
        self.tester             = tester
        self.testContent        = testContent
        self.strategyNamelist   = strategyNamelist
        self.dataSourceType     = dataSourceType                 # 数据下载起点
        self.openFeeRatio       = feeRatio[0]
        self.closeFeeRatio      = feeRatio[-1]
        self.oImpactCostR       = impactCostRatio[0]
        self.cImpactCostR       = impactCostRatio[-1]
        self.timeSeriesFile     = timeSeriesFile
        self.initFund           = copy.deepcopy(initFund)        # 初始资金
        self.capital            = copy.deepcopy(initFund)        # 资金
        self.availableCapital   = copy.deepcopy(initFund)        # 可用资金
        self.startTime          = strToDatetime(startTime)
        self.timeFreq           = timeFreq
        self.benchmarkName      = u''

        self.stockCodelist      = []
        self.indexNameList      = []
        self.orderNumber        = int(0)

        self.histRecord                     = {}
        self.histRecord[u'timeSeries']      = []
        self.histRecord[u'IndexData']       = {}
        self.histRecord[u'orderRecord']     = {}
        self.histRecord[u'positionRecord']  = {}
        self.histRecord[u'fundRecord']      = {}
        self.histRecord[u'PLRecord']        = {}
        self.periodTypeList                 = [u'D', u'W', u'5T']

        # 加载配置
        self.indexFileMap   = {}
        self.indexFileMap   = loadSetting(u"../Setting/indexFileMap.json")
        self.TradeTime      = u''

        # 加载类实例
        self.dataProcessing = DataProcessing(StrategyTemplete1.CSV_PATH)
        pass

    #if '私有 加载时间序列':
    def _loadTimeSeriesFromCSV(self, startTime, endTime):
        """ 从volume_xxx.csv文件中加载时间序列, 时间序列通过INDEX_COLUMN_NO指定
            参数:
                startTime: 数据开始时间
                endTime:   数据结束时间
        """
        # 检查时间序列来源--volume是否存在
        Log(u'加载CSV数据文件......')

        try:
            timeData = read_csvEx(
                StrategyTemplete1.CSV_PATH + self.timeSeriesFile,
                StrategyTemplete1.INDEX_COLUMN_NO).index

            startIdx = -1
            endIdx   = -1

            lenght = len(timeData)
            for i in range(lenght):
                if timeData[i] >= startTime:
                        startIdx = i
                        self.primaryTime = timeData[i]
                        break

            for i in range(lenght)[::-1]:
                if timeData[i] <= endTime:
                        endIdx = i
                        self.endTime = timeData[i]
                        break

            if startIdx < 0 or endIdx < 0:
                Log(u'加载CSV数据文件出错: 指定时间范围不在数据中!')
                return None
            result = timeData[startIdx: (endIdx + 1)]

            return result

        except IOError, e:
            Log(u'加载CSV数据文件出错: ')
            Log(str(e))
            return None

    def _loadTimeSeriesFromHTTPAPI(self, startTime, endTime):
        """
        下载API数据
        """
        # 设置参数
        tempStr = copy.deepcopy(self.timeSeriesFile.split(u'_'))
        objectType = tempStr[-1].split(u'.csv')[0]
        timeIndex = tempStr[0]
        targetPath = StrategyTemplete1.CSV_PATH

        Log(u'采集时间序列数据')

        if objectType == u'stocks':
            if self.timeFreq == u'D':
                self.APIeg.getAPI_IndexDailyDF(
                    timeIndex, str(startTime.date()), str(endTime.date()),
                    toCSV=True, toCleaning=True, toPath=targetPath)
            elif self.timeFreq == u'1T':
                self.APIeg.getMinData(
                    timeIndex, str(startTime.date()), str(endTime.date()),
                    toCSV=True, toCleaning=True, toPath=targetPath)

        elif objectType == u'options':
            if self.timeFreq == u'D':
                self.APIeg.getOptionsDailyData(
                    timeIndex, str(startTime.date()), str(endTime.date()),
                    toCSV=True, toPath=targetPath)
            elif self.timeFreq == u'1T':
                self.APIeg.getOptionsMinData(
                    timeIndex, str(startTime.date()), str(endTime.date()),
                    toCSV=True, toPath=targetPath)

        return self._loadTimeSeriesFromCSV(startTime, endTime)

    def _loadTimeSeries(self, startTime, endTime):
        """ 加载时间序列
            副作用: 将时间序列填充到 histRecord['timeSeries'] 中
            返回值: bool
            参  数:
                    startTime:      开始时间
                    endTime:        结束时间
        """
        timeSeries = None

        if   self.dataSourceType == DATA_SOURCE_CSV:
            timeSeries = self._loadTimeSeriesFromCSV(startTime, endTime)

        elif self.dataSourceType == DATA_SOURCE_HTTPAPI:
            timeSeries = self._loadTimeSeriesFromHTTPAPI(startTime, endTime)

        if not timeSeries.empty:
            Log(u'时间序列长度: %d' % len(timeSeries))
            self.histRecord[u'timeSeries'] = pd.DatetimeIndex(timeSeries)
            return True

        return False

    #if '私有 加载指标数据':
    def _read_csv(self, filePath, startTime=None, endTime=None):
        ''' 加载CSV文件，支持中文文件名称
        '''
        indexCol = copy.deepcopy(StrategyTemplete1.INDEX_COLUMN_NO)
        df = TempleteTools.read_csvEx(filePath, indexCol, startTime, endTime)
        return df

    def __stocks_SpecialIndexChecking(self, indexName, st, end, path):
        ''' 返回股票特殊指标的数据 '''
        if indexName == u'yzb':
            print(u'%s is generating' % indexName)
            for j in [u'zt', u'high', u'low']:
                if not osPath.exists(path + j + u'.csv'):
                    self.APIeg.getAPI_IndexDailyDF(
                        j, st, end, toCSV=True, toPath=path)
            self.dataProcessing.ifYiziban()
            return

        elif indexName == u'zrzt':
            print(u'%s is generating' % indexName)
            if not osPath.exists(u'yiziban' + u'.csv'):
                self.__stocks_SpecialIndexChecking(u'yzb', st, end, path)
            for j in [u'pct']:
                if not osPath.exists(path + j + u'.csv'):
                    self.APIeg.getAPI_IndexDailyDF(j, st, end, toCSV=True, toPath=path)
            self.dataProcessing.ifZRZT()
            return

        elif indexName == u'once_zt':
            print(u'%s is generating' % indexName)
            for j in [u'high', u'close']:
                if not osPath.exists(path + j + u'.csv'):
                    print('%s is generating' % j)
                    self.APIeg.getAPI_IndexDailyDF(
                        j, st, end, toCSV=True, toPath=path)
            self.dataProcessing.onceZTF()
            return

        elif indexName[:9] == u'new_stock':
            print(u'%s is generating' % indexName)
            self.APIeg.getAPI_IndexDailyDF(
                'ipo_date', st, end, toCSV=True, toPath=path)  # 获取 ipo_date
            newStockDays = indexName[9:]
            if len(newStockDays) == 0:
                self.dataProcessing.ifNewStock(newStockDays=int(40), tradeDaySource=u'Api')
            else:
                self.dataProcessing.ifNewStock(newStockDays=int(newStockDays), tradeDaySource=u'Api')
            return

    def _loadIndexFromFile(self, targetPath, startTime, endTime, loadIndexList):
        '''
        从CSV文件加载指标数据
        :param targetPath:
        :param startTime: 数据起始时间
        :param endTime: 数据截至时间
        :param loadIndexList:加载指标列表
        :return:
        '''
        # 指标名称处理
        targetFilename = copy.deepcopy(loadIndexList)
        nameList = [strMarkFilter(i, -1) for i in loadIndexList]
        objectTypeList = [i.split(u'_')[-1] for i in loadIndexList]
        startTime = str(pd.to_datetime(startTime).date())
        endTime = str(pd.to_datetime(endTime).date())
        for i in range(len(loadIndexList)):
            indexName = loadIndexList[i]
            if indexName in self.indexFileMap.keys():  # 存在指定的指标名称
                mapName = self.indexFileMap[indexName]
                if fileChecking(StrategyTemplete1.CSV_PATH, mapName + u'.csv'):  # 文件包含指标名称
                    targetFilename[i] = mapName

        # 导入数据
        objectTypeList = pd.Index(objectTypeList)
        dataSet1 = {j: {} for j in objectTypeList.unique()}
        try:
            for i in range(len(targetFilename)):
                name = nameList[i]
                objectType = objectTypeList[i]
                if name in [u'ipo_date', u'exe_price', u'exe_mode',
                            u'maint_margin', u'contractmultiplier',
                            u'lasttradingdate']:   # 加载特殊数据
                    dataSet1[objectType][name] = self._read_csv(
                        targetPath + targetFilename[i] + u'.csv')
                    Log(u'已加载 [%s] 指标文件. 长度: %d' %
                        (targetFilename[i], len(dataSet1[objectType][name])))
                    continue
                else:
                    dataSet1[objectType][name] = self._read_csv(
                        targetPath + targetFilename[i] + u'.csv',
                        startTime, endTime)
                    Log(u'已加载 [%s] 指标文件. 长度: %d' %
                        (targetFilename[i], len(dataSet1[objectType][name])))
            return dataSet1
        except Exception, e:
            Log(u'从CSV文件加载指标数据, 产生异常: ')
            Log(str(e))
        return None

    def _loadIndexFromAPI(self, targetPath, startTime, endTime, loadIndexList):
        ''' 从接口导入数据 '''
        objectTypeList = [i.split(u'_')[-1] for i in loadIndexList]
        nameList = [strMarkFilter(i, -1) for i in loadIndexList]

        APIindexDict = self.APIeg.indexDict
        stocks_MKTindex = [u'volume', u'open', u'high', u'low', u'close', u'amt',
                    u'pct', u'turn', u'zt', u'st', u'ipo_date'] + self.indicesNameList
        dailyIndex = [u'zt', u'st', u'ipo_date'] + \
                     [u'margin', u'maint_margin', u'contractmultiplier', u'lasttradingdate', u'exe_mode', u'exe_price']

        for i in range(len(loadIndexList)):
            indexName = nameList[i]
            objectType = objectTypeList[i]
            #  1. 下载数据
            if objectType == u'stocks':

                if indexName in APIindexDict[u'index_market'] or indexName in stocks_MKTindex:

                    if self.timeFreq == u'D' or indexName in dailyIndex:
                        self.APIeg.getAPI_IndexDailyDF(indexName, str(startTime.date()), str(endTime.date()), toCSV=True, toPath=targetPath)
                    elif self.timeFreq == u'1T':
                        self.APIeg.getMinData(indexName, str(startTime.date()), str(endTime.date()), toCSV=True, toPath=targetPath)

            elif objectType in [u'options', u'futures']:

                if self.timeFreq == u'D' or indexName in dailyIndex:
                    self.APIeg.getOptionsDailyData(indexName, str(startTime.date()), str(endTime.date()), toCSV=True, toPath=targetPath)
                elif self.timeFreq == u'1T':
                    self.APIeg.getOptionsMinData(indexName, str(startTime.date()), str(endTime.date()), toCSV=True, toPath=targetPath)

            #  2. 生成非下载数据
            if objectType == u'stocks':
                tempList = [u'yzb', u'zrzt', u'once_zt']
                if indexName in tempList or indexName[:9] == u'new_stock':
                    self.__stocks_SpecialIndexChecking(indexName, str(startTime.date()), str(endTime.date()), path=targetPath)

        result = self._loadIndexFromFile(targetPath, startTime, endTime, loadIndexList)
        return result

    def _loadIndexData(self, startTime, endTime, stockCodelist, indexNameList, benchmarkName):
        """ 加载指标数据
            副作用: 将指标数据填充到 histRecord['IndexData']
                    将基准数据填充到self.benchmark[benchmarkName]
            返回值: bool
            参  数:
                    startTime:      开始时间
                    endTime:        结束时间
                    stockCodelist:  股票代码列表
                    indexNameList:  指标名称列表
                    benchmarkName:  基准名称
        """
        # 将基准名称合并到指标, 一同读取
        if benchmarkName == '':
            loadIndexList = indexNameList
        else:
            loadIndexList = indexNameList + [benchmarkName]

        # 导入指标处理
        # 关键行情数据
        for strType in [u'_stocks', u'_options', u'_futures']:
            if u'mkt' + strType in loadIndexList:
                loadIndexList = \
                    loadIndexList + [(i + strType) for i in [u'open', u'close', u'high', u'low', u'volume']]
                loadIndexList.remove(u'mkt' + strType)
                break
        for i in range(len(loadIndexList)):
            if u'volume_stocks' == loadIndexList[i]:
                loadIndexList.insert(i + 1, u'volume_cleaned_stocks')
                break

        Log(u'采集指标数据' )

        if   self.dataSourceType == DATA_SOURCE_CSV:
            self.histRecord[u'IndexData'] = \
                self._loadIndexFromFile(StrategyTemplete1.CSV_PATH, startTime, endTime, loadIndexList)
        elif self.dataSourceType == DATA_SOURCE_HTTPAPI:
            self.histRecord[u'IndexData'] = \
                self._loadIndexFromAPI(StrategyTemplete1.CSV_PATH, startTime, endTime, loadIndexList)
        elif self.dataSourceType == DATA_SOURCE_WIND:
            self.histRecord[u'IndexData'] = \
                self._loadIndexFromWind(startTime, endTime, stockCodelist, loadIndexList)

        # 另存基准数据
        if benchmarkName != u'':
            newBenchmarkName = u'_'.join(benchmarkName.split(u'_')[:-1])
            if newBenchmarkName in self.histRecord[u'IndexData'][u'stocks']:
                self.benchmark[benchmarkName] = self.histRecord[u'IndexData'][u'stocks'][newBenchmarkName]
                return True
        else:
            return True
        Log(u'加载指标数据失败!')
        return False

    #if '公共方法, 数据':
    def loadData(self, primaryTime, endTime, stockCodelist, indexNameList, benchmarkName):
        """ 加载数据到策略
            功  能: 将时间索引数据填充到策略属性: histRecord['timeSeries']
                    指标数据填充到策略属性:      histRecord['IndexData']
                    将基准数据填充到策略属性:    benchmark[benchmarkName]
                    依据加载的数据生成新的策略属性：
            返回值: bool
            参  数:
                    primaryTime:    数据开始时间
                    endTime:        数据结束时间
                    stockCodelist:  股票代码列表
                    indexNameList:  指标名称列表
                    benchmarkName:  基准名称
        """
        stockCodelist = [code.lower() for code in stockCodelist]
        indexNameList = [code.lower() for code in indexNameList]
        self.indexNameList = copy.deepcopy(indexNameList)
        benchmarkName = benchmarkName.lower()
        primaryDatetime, endDatetime = \
            (strToDatetime(i) for i in (primaryTime, endTime))

        result = self._loadTimeSeries(primaryDatetime, endDatetime)
        if result:
            result = self._loadIndexData(
                primaryDatetime, endDatetime, stockCodelist, indexNameList, benchmarkName)

            # 时间序列处理
            if self.timeFreq not in self.periodTypeList:
                self.periodTypeList.append(self.timeFreq)
            self.uniquePeriodDcit = \
                {i: freqTransfer(self.time_DF(), i).to_series().astype(str).tolist()
                 for i in self.periodTypeList}
            self.periodSeriesDict = \
                {i: self.periodSeriesF(freq=i) for i in self.periodTypeList}  # 周期索引字典

            if self.timeFreq in [u'D']:  # 返回周期最后一个位置
                self.dateLoc = \
                    range(len(self.uniquePeriodDcit[u'D']))
            elif self.timeFreq in [u'1T']:
                self.dateLoc = \
                    [(self.time_DF().get_loc(i).stop - 1) for i in self.uniquePeriodDcit[u'D']]
            self.histRecord[u'fundRecord'][primaryTime] = copy.deepcopy(self.initFund)
        return result

    def dateEnd(self):
        ''' 判断当前时刻是否为收盘时刻 '''
        timeLoc = self.getLoc()
        return timeLoc in self.dateLoc

    def index_DF(self, name, objectType):
        """ 获取清洗后的指标 DataFrame版本
            返回值:  成功, 指标 DataFrame;  失败, None
            参  数: 指标名称
        """
        name = name.lower()
        if name in self.histRecord[u'IndexData'][objectType]:
            return self.histRecord[u'IndexData'][objectType][name]
        return None

    def time_List(self, type=0):
        '''
        :param type: { 0:list[str], 1:index(datetime):}
        :return: 时间序列
        '''
        if type == 1:
            return self.histRecord[u'timeSeries']
        return self.histRecord[u'timeSeries'].astype(str).tolist()

    def time_DF(self):
        """ 获取时间(DataFrame版本) """
        return self.time_List(type=1)

    def testTimeList(self):
        """ 获取回测时间列表"""
        dateTimeList = self.time_List(type=1)
        for i in range(len(dateTimeList)):
            if dateTimeList[i] >= self.startTime:
                return dateTimeList[i:].astype(str).tolist()

    def periodSeriesF(self, freq):
        """ 返回周期化的时间Series """
        timeIndex = self.time_DF()
        if freq in [u'W', u'M', u'Q', u'A']:
            newSeries = timeIndex.to_period(freq=freq).to_series().astype(str)
            finalSeries = pd.Series(timeIndex, index=newSeries)
        else:
            tempSeries = timeIndex.to_series().asfreq(freq=freq).dropna()
            newSeries = copy.deepcopy(tempSeries).reindex(timeIndex)
            newSeries = newSeries.fillna(method='ffill')
            newSeries.index = newSeries.values
            newSeries = newSeries.to_period(freq=freq)
            finalSeries = pd.Series(timeIndex, index=newSeries.index)
        return finalSeries

    def index(self, name, objectType, time=None, ref=int(0)):
        """ 获取指标 DataFrame
            返回值: 成功, 指标 DataFrame;  失败, None
            参  数: 指标名称
        """
        name = name.lower()
        if name in self.histRecord[u'IndexData'][objectType]:
            if time:
                curLoc = self.getLoc(time) - ref
            else:
                curLoc = self.getLoc(self.TradeTime) - ref
            return self.index_DF(name, objectType).iloc[curLoc]
        return None

    def df_Ref(self, df, ref):
        ''' 返回df 回调后的切片结果 '''
        return df.loc[self.getTime(ref=int(ref))]

    def ref(self, data, targetIndex, ref):
        '''
        股票回调函数--跳过停牌的交易日
        :param data: 是导入的已经清洗的index的名字或者是自定义生成的已经清洗好了的Dataframe
        :param ref: 回调期个数
        :param targetIndex: 集合的index
        :return:返回某集合（默认全市场）的指标的第ref个有效数据
        '''
        if not targetIndex.empty:
            tingpaiDf = self.cleanedDf()[targetIndex]
            if len(targetIndex) >= 2:
                indexDf = data[targetIndex]
            else:
                indexDf = pd.DataFrame(data[targetIndex], columns=targetIndex)
            date = copy.deepcopy(self.TradeTime)
            curLoc = self.getLoc(date)
            pos = max(0, curLoc - ref)                                                   # 交易所交易日回调
            tempDf = np.isnan(tingpaiDf.iloc[pos:curLoc])
            condition = tempDf.any(axis=0)
            tingpaiIndex = condition[condition].index                                       # 停牌的股票
            series = indexDf.iloc[pos]
            for i in range(len(tingpaiIndex)):                                              # 停牌股票进行特殊回调
                stockCode = tingpaiIndex[i]
                originalCol = copy.deepcopy(tingpaiDf[stockCode][:curLoc])
                col = originalCol.dropna()
                cleanTime = col.index
                tradePos = max(1, ref)
                if len(col) < tradePos:
                    value = indexDf[stockCode].iloc[0]
                    print(u'%s, %s进行回调%s 时超出数据范围' % (self.TradeTime, stockCode, ref))
                else:
                    value = indexDf[stockCode].loc[cleanTime[-tradePos]]
                series[stockCode] = value
            return series

    def cleanDf(self, df):
        '''
        返回清洗后的Dataframe
        :return:
        '''
        cleanedDf = TempleteTools.cleaningDfEx(df, self.index_DF(u'volume', objectType=u'stocks'))
        return cleanedDf

    def cleanedDf(self):
        ''' 返回已经清洗的数据 '''
        return self.index_DF(u'volume_cleaned', objectType=u'stocks')

    def cleanData(self, objectCode):
        '''
        :return: objectCode清洗后的序列
        '''
        return TempleteTools.cleanDataEx(self.cleanedDf(), objectCode)

    def getPeriodrange(self, freq, ref=0):
        ''' 返回period 中起始和结束日期的位置 '''
        periodName = self.getPeriod(freq, ref=ref)
        ser = pd.Series(self.periodSeriesDict[freq].loc[periodName])
        locRange = self.periodSeriesDict[freq].index.get_loc(periodName)
        if len(ser) >= 2:                                          # 本周期有多个交易日
            st = locRange.start
            en = locRange.stop - 1
        else:
            st = locRange
            en = locRange
        return st, en

    def __profitAndLoss(self):
        ''' 计算当期（日）浮动盈亏 -- 切片级别 '''
        PL = 0.
        # 不同方向
        for direction in [1, -1]:                                                       # 1:多头, -1空头
            objectList = self.position[direction].keys()
            # 不同合约种类
            for objectType in objectList:
                positionIndex = self.getPositionIndex(direction, objectType)
                holdingVol = self.getPositionData(direction, objectType, dataType=1)
                priceGap = self.index(u'close', objectType)[positionIndex] - \
                           self.getPositionData(direction, objectType, dataType=2)
                multiplier = self.c_multiplier(objectType)[positionIndex]
                holdingPL = priceGap * multiplier * holdingVol * int(direction)
                PL += holdingPL.sum()                                                   # 浮动盈亏
        floatingValue = self.getCaiptal() + PL                                          # 浮动净值
        tempDateLsit = copy.deepcopy(self.histRecord[u'fundRecord'].keys())
        tempDateLsit.sort()
        self.histRecord[u'PLRecord'][self.TradeTime] \
            = floatingValue - self.histRecord[u'fundRecord'][tempDateLsit[-1]]
        self.histRecord[u'fundRecord'][self.TradeTime] = floatingValue
        return True

    def Trading(self):
        print(u'开始回测')
        for tm in self.testTimeList():
            self.TradeTime = tm
            self.fireBar(tm)
            if self.dateEnd():  # 当天收盘后计算盈亏和头寸
                self.__profitAndLoss()
        print(u'回测完成')

    def fireBar(self, tm):
        self.histRecord[u'orderRecord'][tm] = {}
        self.__Tplus1()                          # T+1 每日开始时更新可用仓位
        self.onBar()
        self.histRecord[u'positionRecord'][tm] = copy.deepcopy(self.position)

    # <==================================================== 工具函数 ===============================================>
    def mktCodeIndex(self, objectType):
        ''' 返回objectType的全市场代码index'''
        return self.index_DF(u'volume', objectType).columns

    def indexRank(self, targetSeries, rankFactor, rankType, rankNum):
        result = TempleteTools.indexRankEx(targetSeries, rankFactor, rankType, rankNum)
        return result

    def getLoc(self, time=None):
        ''' 返回time时点在时间序列上的位置'''
        if time:
            return self.time_List().index(time)
        return self.time_List().index(self.TradeTime)

    def getTime(self, ref=0):
        ''' 返回回调ref期的时点 '''
        loc = self.getLoc(self.TradeTime)
        ref = max(0, int(ref))
        return self.time_List()[loc-ref]

    def getDate(self, ifLoc=False, ref=0):
        '''
        :param ifLoc: 返回时间序列上的位置
        :param ref: 回调天数--正数
        :return: 返回（默认当前）时点的日期或日期在时间序列上的位置
        '''
        timeLoc = self.getLoc()
        for i in range(len(self.dateLoc)):
            dateLoc = self.dateLoc[i]
            if timeLoc <= dateLoc:
                ref_i = max(0, i -ref)
                if ifLoc:
                    return self.dateLoc[ref_i]
                return self.uniquePeriodDcit[u'D'][ref_i]

    def c_multiplier(self, objectType):
        ''' 返回全市场合约乘数'''
        if objectType == u'stocks':
            temp = self.index(u'volume', objectType)
            return pd.Series([int(1)] * len(temp.index), index=temp.index)
        else:
            tempDate = self.getDate()
            if objectType == u'options':
                return self.index_DF(u'contractmultiplier', objectType=u'options').loc[tempDate]
            elif objectType == u'futures':
                return self.index_DF(u'contractmultiplier', objectType=u'futures').iloc[tempDate]

    def filter(self, targetIndex, filterType=None, objectType=u'', ref=1):
        '''
        :param index:
        :param filterType: {1:停牌筛选, 2:一字板筛选, 3:ST筛选, 4:新股筛选}
        :param ref:
        :return: 剔除ref时期后不符合条件的index
        '''

        dropIndex = pd.Index([])
        if filterType == None:
            return False
        elif filterType == 1:   #停牌
            targetIndex = self.df_Ref(self.cleanedDf(), ref=ref)[targetIndex].dropna().index
            return targetIndex
        else:
            if filterType == 2:   # 一字板
                dropIndex = targetIndex[self.index(u'yzb', objectType, ref=ref)[targetIndex] == 1]
            elif filterType == 3:   # st股
                dropIndex = targetIndex[self.index(u'st', u'stocks', ref=ref)[targetIndex] == 1]
            elif filterType == 4:   # 新股
                dropIndex = targetIndex[self.index(u'new_stock', u'stocks', ref=ref)[targetIndex] == 1]
            targetIndex = targetIndex.drop(dropIndex)
            return targetIndex

    def addBuffer(self, key, data):
        ''' 添加回调数据 '''
        if not key in self.__Buffer:
            self.__Buffer[key] = []
        self.__Buffer[key].append(data)

    def refBuffer(self, key, ref):
        ''' 回调数据'''
        en = max(0, len(self.__Buffer[key]) - 1)
        idx = max(0, en - ref)
        return self.__Buffer[key][idx]

    def refRangeBuffer(self, key, index, offset, length):
        pass

    def closePosition(self, direction, objectType, targetIndex=pd.Index([]), priceMode=1, volMode=1):
        '''
        :param direction:   {1：多, -1：空}
        :param objectType:  {'stock':股票, 'options':期权, 'futures':期货}
        :param priceMode:   {1:开盘价, 2:收盘价, 3:最高价, 4:最低价}
        :param volMode:     {1:全部平仓}
        :return:            便捷平仓函数
        '''
        positionIndex = self.getPositionIndex(direction, objectType)  # 现在持有的标的
        targetIndex = targetIndex & positionIndex
        if not targetIndex.empty:
            closeRecord = self.orderRecorder(-1, targetIndex, objectType, direction)
            self.smartSendOrder(closeRecord, priceMode, volMode)
            return True
        else:
            return False

    def getOrderRecord(self, ref=0):
        '''
        :param ref: 回调期限
        :return: 返回交易信息
        '''
        refTime = self.getTime(ref=int(ref))
        return self.histRecord[u'orderRecord'][refTime]

    def lastEntryPrice(self, direction, objectType, objectCode, priceType=0):
        '''
        :param priceType {0:自身进场价格, 1:目标标的进场价格}:
        :return:上一个进场的价格
        '''
        # 检验是否有持仓
        test = self.getPosition(direction=direction, objectType=objectType, objectCode=objectCode)
        if test:
            # 回调查找
            for i in range(1, len(self.histRecord[u'orderRecord'])):
                tempAllRecord = self.getOrderRecord(ref=i)
                if tempAllRecord:   # 如果存在单号
                    orderNum = tempAllRecord.keys()
                    orderNum.sort(reverse=True)
                    for i in orderNum:
                        resultCode = tempAllRecord[i][u'objectCode']
                        if resultCode == objectCode:
                            if priceType == 0:
                                return tempAllRecord[i][u'price']
                            elif priceType == 1:
                                return tempAllRecord[i][u'dataTarget_Price']
                else:   # 该期不存在单号
                    continue
        return False

    def recentMonth(self, objectType, futureMonths=0):
        '''
        :param shiftMonths: 近月月数 {0：近月, 1次近月/下个月, 2 下下个月}
        :return: 近月合约代码index
        '''
        toMonth = strToDatetime(self.getPeriod(freq=u'M', ref=-int(futureMonths)), timeFreq=u'M')
        toYear = strToDatetime(self.getPeriod(freq=u'A'), timeFreq=u'A')
        series = self.index_DF(u'lasttradingdate', objectType).iloc[0]
        tempSeries = TempleteTools.lastTradedayEx(series, u'A')     # 挑选同年
        series1 = series[tempSeries == toYear]

        tempSeries2 = TempleteTools.lastTradedayEx(series1, u'M')
        series2 = series1[tempSeries2 == toMonth]
        return series2.index

    def period(self, indexName, objectType, freq=None, ref=int(0), method=u''):
        '''
        返回周期数据
        :param indexName:
        :param freq: 周期类型
        :param ref: 回调期限
        :param method: 'sum' 求和; 'mean'均值; 'max'最大值; 'min':最小值
        :return:
        '''
        if not freq:
            print (u"请按格式输入周期化格式， 如'W':表示周度")
            return

        if freq:
            refStart, refEnd = self.getPeriodrange(freq, ref)                       # 挑出周期位置
            tempParm = self.index_DF(indexName, objectType)

            if indexName in [u'open']:                                              # 返回指标值
                return tempParm.iloc[refStart]
            elif indexName in [u'close']:
                return tempParm.iloc[refEnd]
            elif indexName in [u'high']:
                return tempParm.max(axis=0)
            elif indexName in [u'low']:
                return tempParm.min(axis=0)
            elif indexName in [u'pct', u'amount', u'volume']:
                return tempParm.iloc[refStart:(refEnd + 1)].sum(axis=0)             # axis = 0 纵向计算

            if method:                                                              # 采用自定义方法
                result = tempParm.iloc[refStart:refEnd + 1]
                result = eval('result.' + eval('method') + '()')
                return result

    def getPeriod(self, freq, ref=int(0)):
        '''
        获取时段
        :param freq:
        :param ref: ref>0 向过去回调取， ref<0 向未来回调
        :return:
        '''
        periodIndex = copy.deepcopy(self.periodSeriesDict[freq].index)
        loc = self.getLoc()
        period = periodIndex[loc]                            # 当前周期
        uniqueLoc = self.uniquePeriodDcit[freq].index(str(period)) - int(ref)
        refPeriod = self.uniquePeriodDcit[freq][max(0, uniqueLoc)]
        return refPeriod

    def unTrade(self, date=None, objectType=u'stock'):
        '''
        :param date:        日期str
        :param objectType:  [u’stocks’, u’futures’, u'options']
        :return:            {u’stocks’:股票返回全市场停牌的股票包含未上市的股票index, else:返回没有交易的标的代码index}
        '''
        if date:
            vol = self.index(u'volume', objectType, time=date)
        else:
            vol = self.index(u'volume', objectType, time=self.getDate())
        return vol[np.isnan(vol)].index

    # 股票工具函数
    def unIPO(self, codeIndex, date):
        ''' 返回在date日期index里未上市的股票代码 '''
        IPODf = self.histRecord[u'IndexData'][u'stocks'][u'ipo_date']
        IPOSeries = IPODf[codeIndex].iloc[0]
        stockName = IPOSeries.index
        unIPOStock = []
        for i in range(len(stockName)):
            stock = stockName[i]
            ipoDate = IPOSeries[i]
            if ipoDate > date:
                unIPOStock.append(stock)
        unIPOStock = pd.Index(unIPOStock)
        return unIPOStock

    # 期货工具函数
    def margin(self):
        ''' 返回期货保证金比例'''
        tempDate = self.getDate()
        return self.index_DF(u'margin', objectType=u'futures').loc[tempDate]

    # 期权工具函数
    def maint_margin(self):
        ''' 返回期权维持保证金 '''
        tempDate = self.getDate()
        return self.index_DF(u'maint_margin', objectType=u'options').loc[tempDate]

    def callPutIndex(self, callput):
        '''
        :param callput: {1: call, -1: put}
        :return: 全市场call或put的代码index
        '''
        exe_Mode = self.index_DF(u'exe_mode', objectType=u'options').iloc[0]
        return callPutIndexEx(callput, exe_Mode)

    # <==================================================== 工具函数 ===============================================>
    def getPosition(self, direction, objectType, objectCode=None, dataType=0, ref=0):
        '''
        返回持仓数据
        :param dataType: {1: u'数量', 2: u'摊薄成本', 3: u'可用数量'}
        '''
        ref = max(0, int(ref))
        if ref == 0:
            if objectCode:
                if dataType == 1:
                    return self.position[direction][objectType][objectCode][u'volume']
                elif dataType == 2:
                    return self.position[direction][objectType][objectCode][u'costPrice']
                elif dataType == 3:
                    return self.position[direction][objectType][objectCode][u'availableVol']
                else:
                    return self.position[direction][objectType][objectCode]
            else:
                if direction in self.position.keys():
                    if objectType in self.position[direction].keys():
                        return self.position[direction][objectType]
        else:
            keyTime = self.getTime(ref=ref)
            if keyTime in self.histRecord[u'positionRecord'].keys():
                refedResult = self.histRecord[u'positionRecord'][keyTime]
                if objectCode:
                    if dataType == 1:
                        return refedResult[direction][objectType][objectCode][u'volume']
                    elif dataType == 2:
                        return refedResult[direction][objectType][objectCode][u'costPrice']
                    elif dataType == 3:
                        return refedResult[direction][objectType][objectCode][u'availableVol']
                    else:
                        return refedResult[direction][objectType][objectCode]
                else:
                    if direction in refedResult.keys():
                        if objectType in refedResult[direction].keys():
                            return refedResult[direction][objectType]

    def getPositionIndex(self, direction, objectType):
        result = self.getPosition(direction, objectType)
        if result:
            return pd.Index(self.getPosition(direction, objectType).keys())
        else:
            return pd.Index([])

    def getPositionData(self, direction, objectType, dataType):
        '''
        :param dataType: {1: u'数量', 2: u'摊薄成本', 3: u'可用数量'}
        '''
        tempIndex = self.getPositionIndex(direction, objectType)
        tempList = [self.getPosition(direction, objectType, i, dataType) for i in tempIndex]
        return pd.Series(tempList, index=tempIndex)

    def getCaiptal(self):
        ''' 获取当前资金 '''
        return self.capital

    def amountAfCostF(self, amount, cost):
        amountAfCost = amount + abs(cost)
        return amountAfCost

    def tradingCostF(self, objectType, direction, action, *agrs):
        ''' 计算交易成本 '''
        if objectType == u'options':                                                # 期权
            volume = agrs[0]
            if direction == -1 and action == 1:                                     # 期权卖开不收手续费
                costFee = 0.
            else:
                costFee = -1.8 * volume

        elif objectType in [u'stocks', u'futures']:                                 # 股票
            amount = agrs[0]
            if action == 1:
                costFee = -amount * self.openFeeRatio
            elif action == -1:
                costFee = -amount * self.closeFeeRatio
        return costFee

    def mvCostPrice(self, sendOrderDict):
        '''
        只有加仓浮动成本价才会变化，平仓浮动成本不变
        :param direction: 头寸方向
        :param code: 合约代码
        :param vol: 成交量
        :param price: 报单价格
        :param objectType: 头寸合约品种
        :return: 返回持仓合约的移动平均成本
        '''
        objectCode = sendOrderDict[u'objectCode']
        objectType = sendOrderDict[u'objectType']
        direction = sendOrderDict[u'direction']
        price = sendOrderDict[u'price']
        volume = sendOrderDict[u'volume']
        prevVol = \
            self.getPosition(direction, objectType, objectCode, dataType=1)
        prevWeightPrice = \
            self.getPosition(direction, objectType, objectCode, dataType=2)
        if price == prevWeightPrice:
            return price
        weight = prevVol / float(volume + prevVol)
        weightedPrice = prevWeightPrice * weight + price * (1 - weight)             # 移动平均成本价
        return weightedPrice                                                        # 新成本价

    def impactCostF(self, directio):
        ''' 返回冲击成本价格'''

    def __Tplus1(self):
        '''
        T+1机制, 在每天开始时更新持仓标的的可用数量
        '''
        direction = 1
        todayFirstLoc = self.getDate(ifLoc=True, ref=1) + 1
        if self.getLoc() == todayFirstLoc:                  # 每天开始时
            for i in self.getPositionIndex(direction, u'stocks'):
                # 更新可用数量
                self.position[direction][u'stocks'][i][u'availableVol'] = \
                    self.position[direction][u'stocks'][i][u'volume']

    def __postionUpdater(self, sendOrderDict):
        '''
        对报单信息进行头寸更新
        objectType: [options, futures stock]
        direction: {1:多头, -1:空头}
        '''

        objectCode = sendOrderDict[u'objectCode']
        objectType = sendOrderDict[u'objectType']
        action = sendOrderDict[u'action']
        direction = sendOrderDict[u'direction']
        price = sendOrderDict[u'price']
        volume = sendOrderDict[u'volume']

        # 1 按交易动作分类
        if action == 1:                                                                      # 开仓
            # 1.1 合约种类不存在
            if objectType not in self.position[direction]:
                if objectType == u'stocks':
                    availableVol = 0.
                else:
                    availableVol = volume
                # 新建合约字典
                self.position[direction][objectType] = \
                    {objectCode:
                         {u'volume': volume,
                          u'costPrice': price,
                          u'availableVol': availableVol}}

            # 1.2 合约种类存在
            else:
                # 1.2.1 合约存在
                if objectCode in self.position[direction][objectType]:
                    # 对持有的头寸进行更新
                    self.position[direction][objectType][objectCode][u'volume'] = \
                        self.getPosition(direction, objectType, objectCode, dataType=1) + volume # 更新持仓量
                    self.position[direction][objectType][objectCode][u'costPrice'] = \
                        self.mvCostPrice(sendOrderDict)                                         # 更新成本
                # 1.2.2 合约不存在
                else:
                    # 新建合约字典
                    if objectType == u'stocks':
                        availableVol = 0.
                    else:
                        availableVol = volume
                    self.position[direction][objectType][objectCode] = \
                        {u'volume': volume,
                         u'costPrice': price,
                         u'availableVol': availableVol}

        if action == -1:                                                                        # 平仓
            self.position[direction][objectType][objectCode][u'volume'] = \
                self.getPosition(direction, objectType, objectCode, dataType=1) - volume        # 更新量
            if self.getPosition(direction, objectType, objectCode, dataType=1) == int(0):       # 移除标的
                self.position[direction][objectType].pop(objectCode)
            else:                                                                               # 更新可以用数量
                self.position[direction][objectType][objectCode][u'availableVol'] = \
                    self.getPosition(direction, objectType, objectCode, dataType=3) - volume

    def __closingPL(self, sendOrderDict):
        ''' 计算平仓盈亏 '''
        objectCode = sendOrderDict[u'objectCode']
        objectType = sendOrderDict[u'objectType']
        direction = sendOrderDict[u'direction']
        price = sendOrderDict[u'price']
        volume = sendOrderDict[u'volume']
        multiplier = sendOrderDict[u'multiplier']
        costPrice = self.getPosition(direction, objectType, objectCode, dataType=2)
        prcieGap = price - costPrice
        pl = prcieGap * multiplier * volume * direction
        return pl

    def __cpitaclUpdate(self, amtChg):
        ''' 资金更新函数 '''
        self.capital = self.getCaiptal() + amtChg

    def getOrderNumber(self):
        ''' 返回交易单号 '''
        return self.orderNumber

    def __update_OrderNumber(self):
        ''' 更新交易单号 '''
        self.orderNumber = self.orderNumber + int(1)

    def __commonPriceMode(self, orderDict, priceMode):
        '''
        常用定价模式
        :param priceMode: {1:开盘价, 2:收盘价, 3:最高价, 4:最低价}
        '''
        # todo 设置dataTarget
        modeDict = {1: u'open', 2: u'close', 3: u'high', 4: u'low'}
        objectCode = orderDict[u'objectCode']
        objectType = orderDict[u'objectType']
        price = self.index(modeDict[priceMode], objectType=objectType)[objectCode]
        if objectType in [u'stocks']:
            self.dataTarget_Price = price
        elif objectType in [u'options', u'futures']:
            self.dataTarget_Price = self.index(self.dataTarget, objectType=u'stocks')[modeDict[priceMode]]
        return price

    def __commonVolMode(self, orderDict, volMode, *args):
        '''
        :param volMode: {开仓：{1:等金额, 2:固定金额, 3:等量}, 平仓：{1:全部平仓}}
        :param sliceFund: 等金额模式的slcie金额
        :param sliceVol: 等量模式的数量
        :return:
        '''
        objectType  = orderDict[u'objectType']
        objectCode  = orderDict[u'objectCode']
        direction   = orderDict[u'direction']
        action      = orderDict[u'action']
        volume      = None

        if action == 1:
            if volMode in [1, 2]:
                if volMode == 1:                # 等金额
                    sliceFund, orderLen, price = [i for i in args]
                    singleAmt = float(sliceFund) / float(orderLen)
                elif volMode == 2:              # 固定金额
                    singleAmt, price = [i for i in args]
                if objectType == u'stocks':
                    volume = TempleteTools.tradingVolume(orderDict, singleAmt, price)
                if objectType == u'options':
                    maint_margin = orderDict[u'maint_margin']
                    volume = TempleteTools.tradingVolume(orderDict, singleAmt, price, maint_margin)
                elif objectType == u'futures':
                    margin = orderDict[u'margin']
                    multiplier = orderDict[u'multiplier']
                    volume = TempleteTools.tradingVolume(orderDict, singleAmt, price, multiplier, margin)
            elif volMode == 3:                  # 等量
                sliceVol = args
                volume = float(sliceVol)
                if sliceVol < 100.:
                    print(u'买入笔数不能小于100')
                    sys.exit()
        elif action == -1:
            # T+1机制
            if objectType == u'stocks':
                availableVol = self.getPosition(direction, objectType, objectCode, dataType=3)
            else:
                availableVol = self.getPosition(direction, objectType, objectCode, dataType=1)
            if volMode == 1:  # 全部平仓
                volume = availableVol
        return volume

    def orderRecorder(self, action, codeIndex, objectType, direction):
        '''
        :param action:          交易动作
        :param codeIndex:       代码index 或者单个字符串
        :return:                根据方向生成交易单号字典
        '''
        data = {}
        if isinstance(codeIndex, unicode) or isinstance(codeIndex, str):
            codeIndex = pd.Index([codeIndex])
        if not codeIndex.empty:
            for i in codeIndex:
                orderNumber = self.getOrderNumber()                 # 获取单号
                data[orderNumber] = {u'objectCode': i, u'objectType': objectType, u'direction': direction}
                self.__update_OrderNumber()
        return {action: data}

    def getAvailableCapital(self):
        ''' 获取可用资金 '''
        return self.availableCapital

    def __updateAvailableCapital(self, objectType, action, amt, cost):
        ''' 可用资金 暂时只支持股票'''
        if objectType == u'stocks':
            if action == 1:
                self.availableCapital = self.availableCapital - amt - cost
            elif action == -1:
                self.availableCapital = self.availableCapital + amt
            return self.availableCapital

    def sendOrder(self, sendOrderDict):
        ''' 报单函数 '''
        orderNum    = sendOrderDict[u'orderNumber']
        objectCode  = sendOrderDict[u'objectCode']
        objectType  = sendOrderDict[u'objectType']
        action      = sendOrderDict[u'action']
        direction   = sendOrderDict[u'direction']
        price       = sendOrderDict[u'price']
        volume      = sendOrderDict[u'volume']
        multiplier  = self.c_multiplier(objectType)[objectCode]
        amt         = price * multiplier * volume                       # 原始金额

        if objectType == u'options':
            costFee = self.tradingCostF(objectType, direction, action, volume)
        elif objectType in [u'stocks', u'futures']:
            costFee = self.tradingCostF(objectType, direction, action, amt)

        amtAfCost = self.amountAfCostF(amt, costFee)
        if action == -1:                                        # 平仓：手续费+平仓盈亏
            amtChg = costFee + self.__closingPL(sendOrderDict)  # 开仓：只计算手续费
        else:
            amtChg = costFee

        self.__cpitaclUpdate(amtChg)                            # 更新资金
        self.__postionUpdater(sendOrderDict)                    # 更新仓位-- 更新仓位属性
        self.__updateAvailableCapital(objectType, action, amt, costFee)

        if action == 1:                                         # 记录交易结果
            feeRatio = self.openFeeRatio
        if action == -1:
            feeRatio = self.closeFeeRatio
        self.histRecord[u'orderRecord'][self.TradeTime][orderNum] = {}
        for i in sendOrderDict.keys():
            self.histRecord[u'orderRecord'][self.TradeTime][orderNum][i] = sendOrderDict[i]
        self.histRecord[u'orderRecord'][self.TradeTime][orderNum][u'amount before cost'] = amt
        self.histRecord[u'orderRecord'][self.TradeTime][orderNum][u'feeCost'] = abs(costFee)
        self.histRecord[u'orderRecord'][self.TradeTime][orderNum][u'amount after cost'] = amtAfCost
        self.histRecord[u'orderRecord'][self.TradeTime][orderNum][u'feeRatio'] = feeRatio
        self.histRecord[u'orderRecord'][self.TradeTime][orderNum][u'leverage'] = 1
        if objectType == u'options':
            marginRatio = str(sendOrderDict[u'maint_margin']) + '元'
        elif objectType == u'futures':
            marginRatio = str(sendOrderDict[u'margin']) + '%'
        elif objectType == u'stocks':
            marginRatio = str(100) + '%'
        self.histRecord[u'orderRecord'][self.TradeTime][orderNum][u'marginRatio'] = marginRatio
        return True

    def smartSendOrder(self, orderRecord, priceMode, volMode, *arg):
        '''
        自定义模拟交易函数
        :param tradeIndex:      交易代码Index
        :param action:          {1：开仓, -1：平仓}
        :param direction:       {1：多, -1：空}
        :param objectType:      {'stock':股票, 'options':期权, 'futures':期货}
        :param priceMode:       {0:自定义价格函数, 1:开盘价, 2:收盘价, 3:最高价, 4:最低价}
        :param volMode:         {0: 自定义数量函数, {开仓：{1:等金额, 2:固定金额, 3:等量}, 平仓：{1:全部平仓}}}
        '''
        if orderRecord:
            action = orderRecord.keys()[0]
            orderNumList = orderRecord[action].keys()
            orderNumList.sort()
            orderLen = len(orderNumList)
            for i in range(orderLen):                                   # 按单号正序进行模拟交易
                eachNum    = orderNumList[i]
                orderDict  = orderRecord[action][eachNum]
                objectCode = orderDict[u'objectCode']
                objectType = orderDict[u'objectType']
                direction  = int(orderDict[u'direction'])
                orderDict[u'orderNumber'] = eachNum
                orderDict[u'action'] = action
                orderDict[u'multiplier'] = self.c_multiplier(objectType)[objectCode]
                if objectType == u'options':
                    orderDict[u'note2'] = self.index_DF(u'exe_mode', u'options').iloc[0][objectCode]
                # 保证金比例或维持保证金
                if objectType == u'options':
                    orderDict[u'margin'] = int(100)
                if objectType == u'options':
                    orderDict[u'maint_margin'] = self.maint_margin()[objectCode]
                elif objectType == u'futures':
                    orderDict[u'margin'] = self.margin().iloc[0][objectCode]

                # if objectCode == '002016.SZ' and self.TradeTime == '2013-05-03':
                #     pass
                # 1 产生价格
                if priceMode == 0:
                    price = self.priceF(objectCode)
                else:
                    price = self.__commonPriceMode(orderDict, priceMode)
                if np.isnan(price):                                     # 价格不存在
                    continue

                # 1.1 报单价格检验
                if price < self.index(u'low', objectType=objectType)[objectCode]:
                    print(u'%s 开仓价格低于%s的最低价' % (objectCode, self.TradeTime))
                    continue

                # 1.2 冲击成本
                impactCostRatio = 0.
                if action == 1:
                    if self.oImpactCostR:
                        impactCostRatio = self.oImpactCostR
                elif action == -1:
                    if self.cImpactCostR:
                        impactCostRatio = self.cImpactCostR
                price = price * (1 + int(direction) * impactCostRatio)

                # 2 产生交易量--理论成交量--未报单
                if volMode == 0:
                    orderVol = self.volF()
                else:
                    if action == 1:
                        if volMode == 1:                                # 等金额
                            sliceFund = float(arg[0])
                            orderVol = \
                                self.__commonVolMode(orderDict, volMode, sliceFund, orderLen, price)
                        elif volMode == 2:                              # 每支固定金额
                            singleAmt = float(arg[0])
                            orderVol = \
                                self.__commonVolMode(orderDict, volMode, singleAmt, price)
                        elif volMode == 3:                              # 等数量
                            sliceVol = float(arg[0])
                            orderVol = \
                                self.__commonVolMode(orderDict, volMode, sliceVol)
                    elif action == -1:
                        orderVol = self.__commonVolMode(orderDict, volMode)

                # 3 报单
                if action == 1:
                    # 3.1 开仓
                    # 3.1.2
                    # TODO 资金检验暂时不开启
                    # capital = self.getCaiptal()
                    # amt = price * orderVol
                    # amtAfCost = self.amountAfCostF(action, amt, self.costF(action, amt))  # 已计算手续费
                    # if fund >= amtAfCost:
                    volume = orderVol

                    if objectCode in self.getPositionIndex(direction, objectType):
                        orderDict[u'note1'] = u'加仓'

                elif action == -1:
                    # 3.2 平仓
                    # 3.2.1 报单价格检验
                    if price > self.index(u'high', objectType=objectType)[objectCode]:
                        print(u'%s 平仓价格高于%s的最高价' % (objectCode, self.TradeTime))
                        continue
                    # 3.2.2 # 持仓检验
                    if objectCode in self.getPositionIndex(direction, objectType):
                        pos = \
                            self.getPosition(direction, objectType, objectCode, dataType=1) # 实际持仓量
                        if pos >= orderVol:                                                 # 仓位检验
                            volume = orderVol
                        else:
                            volume = pos
                    else:
                        print(u'%s - 错误的平仓下单：合约类型是%s, 方向是%s, 合约代码是%s的合约不在持仓中'
                              % (self.TradeTime, objectType, direction, objectCode))
                    if volume == self.getPosition(direction, objectType, objectCode, dataType=1):
                        orderDict[u'note1'] = u'清仓'
                    elif volume < self.getPosition(direction, objectType, objectCode, dataType=1):
                        orderDict[u'note1'] = u'减仓'
                orderDict[u'price'] = price
                orderDict[u'dataTarget_Price'] = self.dataTarget_Price
                orderDict[u'volume'] = volume
                self.sendOrder(orderDict)
        return True

    #if '公共方法, 用户重写':
    def TrimData(self):
        return True

    def onBar(self):
        Log(u'用户必须重写stockSelectingLogic函数, 原型如下:')
        Log(u'')
        Log(u' def onBar(self)')
        Log(u'    return [交易属性]')
        Log(u'')
        return ['交易属性']

    def priceF(self, *args):
        ''' 报单定价函数 '''
        pass

    def volF(self, *args):
        ''' 报单定量函数 '''
        # # todo 应该放在外部修改
        # sliceFund, orderLen, price = [i for i in args]
        # if orderLen <= 5:
        #     orderLen = 5
        # return self.__commonVolMode(orderDict, volMode, sliceFund, orderLen, price)

    # <==================================================== 输出函数 ===============================================>
    def outPut(self):
        '''输出数据'''
        benchmarkName = self.benchmark.keys()
        testIndex = pd.DatetimeIndex(self.testTimeList())
        dateFreq = copy.deepcopy(self.histRecord[u'PLRecord'].keys())
        dateFreq.sort()
        inputData = {
            u'timeFreq': self.testTimeList(),
            u'dateFreq': dateFreq,
            u'fee': {self.openFeeRatio, self.closeFeeRatio},
            u'strategyNameList': self.strategyNamelist,
            u'benchmark': {i: pd.DataFrame(self.benchmark[i], index=testIndex)
                           for i in benchmarkName},
            u'outPutData':
                {self.strategyNamelist[0]:
                     {i: self.histRecord[i] for i in
                      [u'fundRecord', u'orderRecord', u'PLRecord', u'positionRecord']}
                 }}
        inputData[u'outPutData'][self.strategyNamelist[0]][u'intiFund'] = self.initFund
        egClass = OutputClass(inputData)
        egClass.plotNetVal()                                             # 净值曲线
        # egClass.periodRet()                                              # 周期化数据
        egClass.recordOutput()                                           # 成交记录
        egClass.Report_multipleFactors(self.tester, self.testContent)    # 写入报告
    # <==================================================== 输出函数 ===============================================>



