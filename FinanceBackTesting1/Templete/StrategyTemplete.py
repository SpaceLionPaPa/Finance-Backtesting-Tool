# encoding: UTF-8

# Program: This is a templete code to run backtest for momentum strategy.
# Writer: Qijie Li
# Date: Oct.25.2017

import copy
import pandas as pd
import numpy as np
import os.path as osPath
import shutil
from Templete import TempleteTools
from Templete.AllDataCleaning import allCleaning
from Templete.DataProcessingCode import DataProcessing
from Templete.OutputCode import OutputClass
from Templete.Constant import *
from Templete.Utilities import *
from datetime import datetime, timedelta

class StrategyTemplete(object):
    '''
    股票回测的类
    功能：对动量策略进行历史回测
    公共方法：
    '''
    CSV_PATH          = u'../Data/Main/'
    TIME_SERIES_FILE  = CSV_PATH + u'volume.csv'
    INDEX_COLUMN_NO   = 0

    def __init__(self, tester, strategyNamelist, testContent,
                 buyFee, sellFee, initFund, startTime, dataSourceType = DATA_SOURCE_CSV):
        '''
        初始化程序, 参数说明:
            initFund:       初始资金
            fee:            手续费率
            movePriceRatio: 滑价率
            benchmarkName:  基准名称
            testTimeRange:  回测时间范围
            primaryDate:    数据起始日期
            indexNameList:  指标名称列表
            startTime:      回测时间--大于primaryDate
            dataSourceType: 加载数据类型, 取值范围: DATA_SOURCE_CSV, DATA_SOURCE_WIND, DATA_SOURCE_MONGODB
        '''
        Log(u'策略初始化......')

        if   dataSourceType == DATA_SOURCE_CSV:
             Log(u'数据搜索目录: ' + StrategyTemplete.CSV_PATH)
             Log(u'时间序列文件: %s, 时间列索引号: %d.' %
                 (StrategyTemplete.TIME_SERIES_FILE, StrategyTemplete.INDEX_COLUMN_NO))
             Log(u'导入csv包')
        elif dataSourceType == DATA_SOURCE_WIND:
             Log(u'导入Wind插件包')
             try:
                 from WindPy import w
             except BaseException, e:
                 Log(u'未安装Wind插件包!')
        elif dataSourceType == DATA_SOURCE_MONGODB:
             Log(u'导入pymongo包')
             try:
                 import pymongo
             except BaseException, e:
                 Log(u'未安装pymongo包!')
        elif dataSourceType == DATA_SOURCE_HTTPAPI:
            StrategyTemplete.CSV_PATH = u'../Data/ApiData/'
            StrategyTemplete.TIME_SERIES_FILE = StrategyTemplete.CSV_PATH + u'volume.csv'
            # 清理空路径数据
            cur_dir = os.getcwd()
            templete_Dir = os.path.dirname(cur_dir)
            self.dataPath = templete_Dir + '/Data/ApiData/'                 # 返回上级目录并新建路径
            if os.path.isdir(self.dataPath):                                # 如果路径名存在
                shutil.rmtree(self.dataPath)                                # 删除原来文件夹
            os.mkdir(self.dataPath)                                         # 创建plot文件夹
            try:
                from API.DataAPI import APIClass
                self.APIeg = APIClass('../API/indexconfig.txt')
                Log(u'数据搜索目录: ' + StrategyTemplete.CSV_PATH)
                Log(u'时间序列文件: %s, 时间列索引号: %d.' %
                    (StrategyTemplete.TIME_SERIES_FILE, StrategyTemplete.INDEX_COLUMN_NO))
                Log(u'导入API包')
            except BaseException, e:
                Log(u'未导入API包!')

        self.__Buffer       = {}
        self.endTime        = u''
        self.primaryDate    = u'' # 数据下载起点
        self.benchmark      = {}
        self.position       = {}
        self.histRecord     = {}
        self.histRecord[u'timeSeries'] = []
        self.histRecord[u'IndexData'] = {}
        self.histRecord[u'orderRecord'] = {}
        self.histRecord[u'positionRecord'] = {}
        self.histRecord[u'fundRecord'] = {}
        self.periodTypeList = []


        # 参数设置:
        self.tester = tester
        self.testContent = testContent
        self.strategyNamelist = strategyNamelist
        self.dataSourceType = dataSourceType                 # 数据下载起点
        self.buyFee         = buyFee
        self.sellFee        = sellFee
        self.initFund       = copy.deepcopy(initFund)        # 初始资金
        self.capital        = copy.deepcopy(initFund)        # 资金
        self.fundValue      = copy.deepcopy(initFund)        # 账户价值
        self.startTime      = startTime

        self.benchmarkName  = u''

        self.stockCodelist  = []
        self.indexNameList  = []

        # 加载配置
        self.indexFileMap   = {}

        self.indexFileMap   = loadSetting(u"../Setting/indexFileMap.json")

        self.TradeTime      = u''

        # 加载类实例
        self.dataProcessing = DataProcessing(StrategyTemplete.CSV_PATH)
        pass

    #if '私有 加载时间序列':
    def _loadTimeSeriesFromCSV(self, startTime, endTime):
        """ 从CSV文件中加载时间序列
            加载文件通过TIME_SERIES_FILE常量指定, 加载列通过INDEX_COLUMN_NO指定
            参数:
                startTime: 开始时间
                endTime:   结束时间
        """

        Log(u'加载CSV数据文件......')

        try:
            timeData = pd.read_csv(
                StrategyTemplete.TIME_SERIES_FILE, parse_dates=True,
                index_col=StrategyTemplete.INDEX_COLUMN_NO, encoding=u'gbk').index
            timeData = timeData.astype(str).tolist()

            startIdx = -1
            endIdx   = -1

            lenght = len(timeData)
            for i in range(lenght):
                if timeData[i] >= startTime:
                        startIdx = i
                        self.primaryDate = timeData[i]
                        break

            for i in range(lenght)[::-1]:
                if timeData[i] <= endTime:
                        endIdx = i
                        self.endTime = timeData[i]
                        break

            if startIdx < 0 or endIdx < 0:
                Log(u'加载CSV数据文件出错: 指定时间范围不在数据中!')
                return None
        
            result = timeData[startIdx : (endIdx + 1)]
            return result

        except IOError, e:
            Log(u'加载CSV数据文件出错: ')
            Log(str(e))
            return None

    def _loadTimeSeriesFromWind(self, startTime, endTime):
        """ 加载万得数据
            参数:
                startTime: 开始时间
                endTime:   结束时间
        """

        Log(u'加载万得数据......')

        try:
            w.start()

            self.windTradedayList = w.tdays(startTime, endTime, u'').Data[0]

            self.startTime = str(self.windTradedayList[0].date())
            self.endTime   = str(self.windTradedayList[-1].date())

            result = [str(i.date()) for i in self.windTradedayList]
            return result

        except IOError, e:
            Log(u'加载万得数据出错: ')
            Log(str(e))
            return None

        finally:
            w.stop()

    def _loadTimeSeriesFromMongoDB(self, startTime, endTime):
        """ 加载MongoDB数据
            未实现
        """
        Log(u'加载MongoDB时间序列, 未实现')
        return None

    def _loadTimeSeriesFromHTTPAPI(self, startTime, endTime):
        """ 下载API数据
        """
        Log(u'下载HTTPAPI时间序列')
        timeIndex = 'volume'
        print('%s is generating' % timeIndex)
        self.APIeg.getAPI_IndexDailyDF(
            timeIndex, start=startTime, end=endTime,
            toCSV=True, toCleaning=True, toPath=StrategyTemplete.CSV_PATH)
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
        elif self.dataSourceType == DATA_SOURCE_WIND:
                timeSeries = self._loadTimeSeriesFromWind(startTime, endTime)
        elif self.dataSourceType == DATA_SOURCE_MONGODB:
                timeSeries = self._loadTimeSeriesFromMongoDB(startTime, endTime)
        elif self.dataSourceType == DATA_SOURCE_HTTPAPI:
                timeSeries = self._loadTimeSeriesFromHTTPAPI(startTime, endTime)
        if timeSeries:
            Log(u'时间序列长度: %d' % len(timeSeries))
            self.histRecord['timeSeries'] = timeSeries
            return True

        return False

    #if '私有 加载指标数据':
    def _read_csv(self, filePath, startTime=None, endTime=None):
        ''' 加载CSV文件，支持中文文件名称
        '''
        indexCol = copy.deepcopy(StrategyTemplete.INDEX_COLUMN_NO)
        df = TempleteTools.read_csvEx(filePath, indexCol, startTime, endTime)
        return df

    def _loadIndexFromFile(self, targetPath, startTime, endTime, loadIndexList):
        ''' 从CSV文件加载指标数据'''
        test = self.indexFileMap.keys()
        targetFilename = [self.indexFileMap[i] for i in loadIndexList]
        dataSet1 = {}
        dataSet2 = {}
        try:
            for i in range(len(targetFilename)):
                if loadIndexList[i] in [u'ipo_date']:                                               # 加载ipo_date
                    dataSet1[loadIndexList[i]] = self._read_csv(
                        targetPath + targetFilename[i] + u'.csv')
                    Log(u'已加载 [%s] 指标文件. 长度: %d' %
                        (targetFilename[i], len(dataSet1[loadIndexList[i]])))
                    continue
                dataSet1[loadIndexList[i]] = self._read_csv(
                    targetPath + targetFilename[i] + u'.csv',
                    startTime, endTime)                                                             # 加载数据
                Log(u'已加载 [%s] 指标文件. 长度: %d' %
                    (targetFilename[i], len(dataSet1[loadIndexList[i]])))
                if loadIndexList[i] in [u'volume']:                                                 # 加载清洗数据
                    dataSet2[loadIndexList[i]] = \
                        self._read_csv(targetPath + targetFilename[i] + u'Cleaned' + u'.csv',
                        startTime, endTime)                                                         # _read_csv已经截取时间
                    Log(u'已加载 [%s] 指标文件. 长度: %d' %
                        (targetFilename[i] + u'Cleaned', len(dataSet2[loadIndexList[i]])))
            return dataSet1, dataSet2
        except Exception, e:
            Log(u'从CSV文件加载指标数据, 产生异常: ')
            Log(str(e))
        return None

    def _loadIndexFromAPI(self, targetPath, startTime, endTime, loadIndexList):
        ''' 从接口导入数据 '''
        APIindexDict = self.APIeg.indexDict
        MKTindex = [u'volume', u'open', u'high', u'low', u'close', u'amount',
                    u'pct', u'turn', u'zt', u'st', u'ipo_date']
        indices = [u'cyb', u'hs300', u'sz50', u'szcz', u'szzs', u'zxb',
                   u'zz1000', u'zz500']

        for i in range(len(loadIndexList)):
            indexName = loadIndexList[i]
            
            if not indexName == 'volume':
                #  1. 下载数据
                print('%s is generating' % indexName)
                # if indexName in MKTindex:
                #     self.APIeg.gerLatest5yearDailyDF(indexName, toCSV=True, toPath=targetPath)
                if indexName in APIindexDict[u'index_market'] or indexName in MKTindex:
                    self.APIeg.getAPI_IndexDailyDF(
                        indexName, start=startTime, end=endTime, toCSV=True, toPath=targetPath)
                elif indexName in indices:
                    self.APIeg.getAPI_indicesDF(
                        indexName, start=startTime, end=endTime, toCSV=True, toPath=targetPath)

                #  2. 生成非下载数据
                elif indexName == u'yzb':
                    for j in [u'zt', u'high', u'low']:
                        if not osPath.exists(targetPath + j + u'.csv'):
                            print('%s is generating' % j)
                            self.APIeg.getAPI_IndexDailyDF(
                                j, start=startTime, end=endTime, toCSV=True, toPath=targetPath)
                    self.dataProcessing.ifYiziban()
                elif indexName == u'zrzt':
                    if not osPath.exists('yiziban' + u'.csv'):
                        print('%s is generating' % 'yzb')
                        for j in [u'zt', u'high', u'low']:
                            if not osPath.exists(targetPath + j + u'.csv'):
                                print('%s is generating' % j)
                                self.APIeg.getAPI_IndexDailyDF(
                                    j, start=startTime, end=endTime, toCSV=True, toPath=targetPath)
                        self.dataProcessing.ifYiziban()
                    for j in [u'pct']:
                        if not osPath.exists(targetPath + j + u'.csv'):
                            print('%s is generating' % j)
                            self.APIeg.getAPI_IndexDailyDF(
                                j, start=startTime, end=endTime, toCSV=True, toPath=targetPath)
                    self.dataProcessing.ifZRZT()
                elif indexName == u'once_zt':
                    for j in [u'high', u'close']:
                        if not osPath.exists(targetPath + j + u'.csv'):
                            print('%s is generating' % j)
                            self.APIeg.getAPI_IndexDailyDF(
                                j, start=startTime, end=endTime, toCSV=True, toPath=targetPath)
                    self.dataProcessing.onceZTF()
                elif indexName[:9] == u'new_stock':
                    self.APIeg.getAPI_IndexDailyDF(
                        'ipo_date', start=startTime, end=endTime, toCSV=True, toPath=targetPath)  # 获取 ipo_date
                    newStockDays = indexName[9:]
                    if len(newStockDays) == 0:
                        self.dataProcessing.ifNewStock(newStockDays=int(40), tradeDaySource=u'Api')
                    else:
                        self.dataProcessing.ifNewStock(newStockDays=int(newStockDays), tradeDaySource=u'Api')
        result = self._loadIndexFromFile(targetPath, startTime, endTime, loadIndexList)
        return result

    def _loadIndexFromWind(self, startTime, endTime, stockCodelist, loadIndexList):
        ''' 从Wind接口加载指标数据
        '''
        try:
            downLoader = DataDownloader(startDate = startTime, endDate = endTime, stockCodelist = stockCodelist)
            downLoader.download_marketData(indexNameList = loadIndexList)
            downLoader.CNtoENFilename()
            return downLoader.allDataDict
        except Exception, e:
            Log(u'从Wind接口加载指标数据, 产生异常: ')
            Log(str(e))

        return None

    def _loadIndexFromMongoDB(self, startTime, endTime, stockCodelist, loadIndexList):
        ''' 从MongoDb加载指标数据
            未实现
        '''
        Log(u'从MongoDb加载指标数据, 未实现!')
        return None

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
        loadIndexList = indexNameList + [benchmarkName]
        if   self.dataSourceType == DATA_SOURCE_CSV:
            self.histRecord[u'IndexData'], self.histRecord[u'CleanedIndex'] = \
                self._loadIndexFromFile(StrategyTemplete.CSV_PATH, startTime, endTime, loadIndexList)
        elif self.dataSourceType == DATA_SOURCE_MONGODB:
            self.histRecord[u'IndexData'] = \
                self._loadIndexFromMongoDB(startTime, endTime, stockCodelist, loadIndexList)
        elif self.dataSourceType == DATA_SOURCE_WIND:
            self.histRecord[u'IndexData'] = \
                self._loadIndexFromWind(startTime, endTime, stockCodelist, loadIndexList)
        elif self.dataSourceType == DATA_SOURCE_HTTPAPI:
            self.histRecord[u'IndexData'], self.histRecord[u'CleanedIndex'] = \
                self._loadIndexFromAPI(StrategyTemplete.CSV_PATH, startTime, endTime, loadIndexList)

        # 另存基准数据
        if self.histRecord[u'IndexData'] and benchmarkName in self.histRecord[u'IndexData']:
            self.benchmark[benchmarkName] = self.histRecord[u'IndexData'][benchmarkName]
            return True

        Log(u'加载指标数据失败!')
        return False

    #if '公共方法, 数据':
    def loadData(self, primaryDate, endTime, stockCodelist, indexNameList, benchmarkName):
        """ 加载数据到策略
            功  能: 将时间索引数据填充到策略属性: histRecord['timeSeries']  
                    指标数据填充到策略属性:      histRecord['IndexData'] 
                    将基准数据填充到策略属性:    benchmark[benchmarkName]
                    依据加载的数据生成新的策略属性：
            返回值: bool
            参  数:
                    primaryDate:    数据开始时间
                    endTime:        数据结束时间
                    stockCodelist:  股票代码列表
                    indexNameList:  指标名称列表
                    benchmarkName:  基准名称
        """
        stockCodelist = [code.lower() for code in stockCodelist]
        indexNameList = [code.lower() for code in indexNameList]
        self.indexNameList = copy.deepcopy(indexNameList)
        benchmarkName = benchmarkName.lower()

        result = self._loadTimeSeries(primaryDate, endTime)
        if result:
            result = \
                self._loadIndexData(primaryDate, endTime, stockCodelist, indexNameList, benchmarkName)
            self.periodSeriesDict = \
                {i: self.periodSeriesF(freq=i) for i in self.periodTypeList} # 周期索引字典
            self.uniquePeriodDcit = \
                {i: np.unique(self.periodSeriesF(freq=i)).tolist() for i in self.periodTypeList}
        return result

    #  常用指标
    def close_DF(self):
        """ 获取收盘价 DataFrame版本 """
        return self.index_DF(u'close')

    def open_DF(self):
        """ 获取开盘价 DataFrame版本 """
        return self.index_DF(u'open')

    def low_DF(self):
        """ 获取最低价 DataFrame版本 """
        return self.index_DF(u'low')

    def high_DF(self):
        """ 获取最高价 DataFrame版本 """
        return self.index_DF(u'high')

    def vol_DF(self):
        """ 获取成交量 DataFrame版本 """
        self.index_DF(u'volume')

    def turn_DF(self):
        """ 获取换手率 DataFrame版本 """
        return self.index_DF(u'turn')

    def amount_DF(self):
        """ 获取成交额 DataFrame版本 """
        return self.index_DF(u'amt')

    def pct_DF(self):
        """ 获取涨跌幅 DataFrame版本 """
        return self.index_DF(u'pct')

    def indexCleanedDF(self):
        """ 获取清洗后的指标 DataFrame版本 """
        if u'volume' in self.histRecord[u'CleanedIndex']:
            return copy.deepcopy(self.histRecord[u'CleanedIndex'][u'volume'])
        return None

    def index_DF(self, name):
        """ 获取清洗后的指标 DataFrame版本
            返回值:  成功, 指标 DataFrame;  失败, None
            参  数: 指标名称
        """
        name = name.lower()
        if name in self.histRecord[u'IndexData']:
            return copy.deepcopy(self.histRecord[u'IndexData'][name])
        return None

    def time_List(self):
        """ 获取时间(List版本) """
        return self.histRecord['timeSeries']

    def getLoc(self, date):
        ''' 返回当前dataframe的时间序列位置'''
        return self.time_List().index(date)

    def getDate(self, offset=None):
        ''' 返回前几期的date'''
        loc = self.getLoc(self.TradeTime)
        if not offset:
            return self.time_List()[loc-offset]
        else:
            return self.time_List()[loc]

    def time_DF(self):
        """ 获取时间(DataFrame版本) """
        return pd.Index(self.time_List())

    def testTimeList(self):
        """ 获取回测时间列表"""
        dateTimeList = copy.deepcopy(self.time_List())
        for i in range(len(dateTimeList)):
            if dateTimeList[i] >= self.startTime:
                return dateTimeList[i:]

    def periodSeriesF(self, freq):
        """ 返回周期化的时间Series"""
        periodSeries = pd.DatetimeIndex(self.time_DF()).to_period(freq=freq).to_series().astype(str)
        periodSeries.index = periodSeries.values
        return periodSeries

    def stockCode(self):
        """ 获取股票代码列表"""
        return copy.deepcopy(self.histRecord[u'IndexData'][u'volume'].columns)

    def index(self, name, date=None):
        """ 获取指标 DataFrame
            返回值: 成功, 指标 DataFrame;  失败, None
            参  数: 指标名称
        """
        name = name.lower()
        if name in self.histRecord[u'IndexData']:
            if date:
                return self.histRecord[u'IndexData'][name].loc[date]
            else:
                return self.histRecord[u'IndexData'][name].loc[self.TradeTime]
        return None

    def cleanedSeries(self, date=None):
        """ 获取指标 DataFrame
            返回值: 成功, 指标 DataFrame;  失败, None
            参  数: 指标名称
        """
        if date:
            return self.indexCleanedDF().loc[date]
        else:
            return self.indexCleanedDF().loc[self.TradeTime]

    def close(self, date=None):
        """ 获取收盘价(时间切片版本) DataFrame """
        if date:
            return self.index(u'close', date)
        else:
            return self.index(u'close')

    def open(self, date=None):
        """ 获取开盘价(时间切片版本) DataFrame """
        if date:
            return self.index(u'open', date)
        else:
            return self.index(u'open')

    def low(self, date=None):
        """ 获取最低价(时间切片版本) DataFrame """
        if date:
            return self.index(u'low', date)
        else:
            return self.index(u'low')

    def high(self, date=None):
        """ 获取最高价(时间切片版本) DataFrame """
        if date:
            return self.index(u'high', date)
        else:
            return self.index(u'high')

    def vol(self, date=None):
        """ 获取成交量(时间切片版本) DataFrame """
        if date:
            return self.index(u'volume', date)
        else:
            return self.index(u'volume')

    def turn(self, date=None):
        """ 获取换手率(时间切片版本) DataFrame """
        if date:
            return self.index(u'turn', date)
        else:
            return self.index(u'turn')

    def amount(self, date=None):
        """ 获取成交额(时间切片版本) DataFrame """
        if date:
            return self.index(u'amount', date)
        else:
            return self.index(u'amount')

    def st(self, date=None):
        ''' st股票列表 '''
        if date:
            return self.index(u'st', date)
        else:
            return self.index(u'st')

    def newStock(self, date=None):
        ''' 新股的股票列表 '''
        if date:
            return self.index(u'new_stock', date)
        else:
            return self.index(u'new_stock')

    def pct(self, date=None):
        """ 涨跌幅"""
        if date:
            return self.index(u'pct', date)
        else:
            return self.index(u'pct')

    def unIPO(self, index, date):
        ''' 返回在date日期index里未上市的股票代码 '''
        IPODf = self.histRecord[u'IndexData'][u'ipo_date']
        IPOSeries = IPODf[index].iloc[0]
        stockName = IPOSeries.index
        unIPOStock = []
        for i in range(len(stockName)):
            stock = stockName[i]
            ipoDate = IPOSeries[i]
            if ipoDate > date:
                unIPOStock.append(stock)
        unIPOStock = pd.Index(unIPOStock)
        return unIPOStock

    def unTrade(self, date=None):
        '''
        返回全市场停牌的股票包含未上市的股票
        '''
        if date:
            vol = self.vol(date)
            return vol[np.isnan(vol)].index
        else:
            return self.vol()[np.isnan(self.vol())].index

    def ref(self, data, targetIndex, offset):
        '''
        回调函数--跳过停牌的交易日
        :param data: 是导入的已经清洗的index的名字或者是自定义生成的已经清洗好了的Dataframe
        :param offset: 回调期个数
        :param targetIndex: 集合的index
        :return:返回某集合（默认全市场）的指标的第offset个有效数据
        '''
        if not targetIndex.empty:
            tingpaiDf = self.indexCleanedDF()[targetIndex]
            if len(targetIndex) >= 2:
                indexDf = data[targetIndex]
            else:
                indexDf = pd.DataFrame(data[targetIndex], columns=targetIndex)
            date = copy.deepcopy(self.TradeTime)
            curLoc = self.getLoc(date)
            pos = max(0, curLoc - offset)                                                   # 交易所交易日回调
            tempDf = np.isnan(tingpaiDf.iloc[pos:curLoc])
            condition = tempDf.any(axis=0)
            tingpaiIndex = condition[condition].index                                       # 停牌的股票
            series = indexDf.iloc[pos]
            for i in range(len(tingpaiIndex)):                                              # 停牌股票进行特殊回调
                stockCode = tingpaiIndex[i]
                originalCol = copy.deepcopy(tingpaiDf[stockCode][:curLoc])
                col = originalCol.dropna()
                cleanTime = col.index
                tradePos = max(1, offset)
                if len(col) < tradePos:
                    value = indexDf[stockCode].iloc[0]
                    print(u'%s, %s进行回调%s 时超出数据范围' % (self.TradeTime, stockCode, offset))
                else:
                    value = indexDf[stockCode].loc[cleanTime[-tradePos]]
                series[stockCode] = value
            return series

    def cleanDf(self, df):
        '''
        返回清洗后的Dataframe
        :return:
        '''
        cleanedDf = TempleteTools.cleaningDfEx(df, self.index_DF(u'volume'))
        return cleanedDf

    def cleanData(self, df, tempIndex):
        '''
        清除序列上非0的点
        :param Data:
        :param temp:
        :return:
        '''
        return TempleteTools.cleanDataEx(df, tempIndex)

    def getPeriod(self, freq, ref=int(0)):
        ''' 获取当前时段索引 '''
        periodSeries = copy.deepcopy(self.periodSeriesDict[freq])
        loc = self.time_List().index(self.TradeTime)
        period = periodSeries.iloc[loc]                             # 当前周期
        uniqueLoc = self.uniquePeriodDcit[freq].index(period) - ref
        refPeriod = self.uniquePeriodDcit[freq][max(0, uniqueLoc)]
        return refPeriod

    def getPeriodrange(self, freq, ref=0):
        ''' 返回period 中所有日期 '''
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

    def period(self, indexName, freq=None, ref=int(0), method=None):
        '''
        返回周期数据
        :param indexName:
        :param freq: 周期类型
        :param ref: 回调期限
        :param method: None 默认的实际意义; 'sum' 求和; 'avg'均值; 'max'最大值; 'low':最小值
        :return:
        '''

        if not freq:
            print ("请按格式输入周期化格式， 如'W':表示周度")
            return

        if freq:
            periodSeries = copy.deepcopy(self.periodSeriesDict[freq])               # 挑出周期位置
            refPeriod = self.getPeriod(freq, ref)
            periodDf = pd.Series(periodSeries.loc[refPeriod])
            if len(periodDf) >= 2:                                                  # refPeriod 有多个交易日
                refStart = periodSeries.index.get_loc(refPeriod).start
                if ref >= 1:
                    refEnd = periodSeries.index.get_loc(refPeriod).stop - 1
                elif ref == 0:
                    refEnd = self.time_List().index(self.TradeTime)
            else:                                                                   # refPeriod 只有一个交易日
                refStart = periodSeries.index.get_loc(refPeriod)
                refEnd = copy.deepcopy(refStart)
            tempParm = copy.deepcopy(self.index_DF(indexName))

            if not method:                                                          # 无调用方法
                if indexName in ['open']:                                           # 返回指标值
                    if not method:
                        return tempParm.iloc[refStart]
                elif indexName in ['close']:
                    if not method:
                        return tempParm.iloc[refEnd]
                elif indexName in ['amount', 'turn', 'volume']:
                    if not method:
                        return tempParm.iloc[refStart:(refEnd + 1)].sum(axis=0)
                elif indexName in ['pct']:
                    return tempParm.iloc[refStart:(refEnd + 1)].sum(axis=0)         # axis = 0 纵向计算
            else:                                                                   # 采用调用方法
                result = tempParm.iloc[refStart:refEnd + 1]
                if method == 'sum':
                    result = result.sum(axis=0)                                     # 计算自带剔除空值功能
                if method == 'avg':
                    result = result.mean(axis=0)
                if method == 'max':
                    result = result.max(axis=0)
                if method == 'low':
                    result = result.low(axis=0)
                return result

    def _profitAndLoss(self):
        ''' 当期盈亏 -- 切片级别 '''
        trading = self.histRecord[u'orderRecord'][self.TradeTime]
        tradingIndex = pd.Index(trading.keys())
        tradingAction = pd.Series([trading[i][u'action'] for i in tradingIndex], index=tradingIndex)
        tradingPrice = pd.Series([trading[i][u'price'] for i in tradingIndex], index=tradingIndex)
        tradingVol = pd.Series([trading[i][u'volume'] for i in tradingIndex], index=tradingIndex)
        tradingFeeCost = pd.Series([trading[i][u'feeCost'] for i in tradingIndex], index=tradingIndex)
        tradingPL = pd.Series(np.zeros(len(tradingIndex)), index=tradingIndex)
        buy = tradingIndex[tradingAction == 1]
        sell = tradingIndex[tradingAction == -1]
        tradingPL[buy] = (self.close()[buy] - tradingPrice[buy]) * tradingVol[buy] - tradingFeeCost[buy]  # 买入
        tradingPL[sell] = (tradingPrice[sell] - self.ref(self.index_DF(u'close'), targetIndex=sell, offset=1)) * \
                          tradingVol[sell] - tradingFeeCost[sell]  # 卖出
        tradingPLSum = tradingPL.sum()
        # TODO 每笔交易盈亏以后有时间输出到excel结果里去
        holdingPositon = self.getPositionIndex().drop(pd.Index(buy))                    # 持仓不包括今天买入的
        unTradeHoldingPostion = self.unTrade() & holdingPositon                         # 持仓中停牌的股票
        tradeHoldingPosition = holdingPositon.drop(unTradeHoldingPostion)
        holdingVol = self.getPositionSeries()[tradeHoldingPosition]
        ser1 = self.close()[tradeHoldingPosition]                                       # 收盘价
        ser2 = self.ref(
            self.index_DF(u'close'), targetIndex=tradeHoldingPosition, offset=1)        # 前一个交易日收盘价
        holdingPL = (ser1-ser2) * holdingVol
        holdingPLSum = holdingPL.sum()
        self.histRecord[u'fundRecord'][self.TradeTime] = copy.deepcopy(holdingPLSum + tradingPLSum)

    def Trading(self):
        print(u'开始回测')
        for tm in self.testTimeList():
            self.TradeTime = tm
            self.fireBar(tm)
        print(u'回测完成')

    def fireBar(self, tm):
        self.histRecord[u'orderRecord'][tm] = {}
        self.onBar()
        self._profitAndLoss()
        self.histRecord[u'positionRecord'][tm] = copy.deepcopy(self.getPosition())


    def tradeSeriesF(self, buyIndex, sellIndex):
        """ 依据买卖股票，生成报单series """
        buySeries = pd.Series([int(1)] * len(buyIndex), index=buyIndex)
        sellSeries = pd.Series([int(-1)] * len(sellIndex), index=sellIndex)
        return pd.concat([buySeries, sellSeries])

    # <==================================================== 工具函数 ===============================================>
    def indexRank(self, targetSeries, rankFactor, rankType, rankNum):
        result = TempleteTools.indexRankEx(targetSeries, rankFactor, rankType, rankNum)
        return result

    def filter(self, index, filterType=None):
        """
        剔除前一天不符合条件的index
        """
        dropIndex = pd.Index([])
        loc = self.time_List().index(self.TradeTime)
        if filterType==None:
            return

        elif filterType == 1:                                                       # 停牌筛选
            dropIndex = \
                index[np.isnan(self.indexCleanedDF().iloc[max(0, loc - 1)][index])]

        elif filterType == 2:                                                       # 一字板筛选
            dropIndex = \
                index[self.index_DF(u'yzb').iloc[max(0, loc - 1)][index] == 1]      # 策略的一字涨停的股票

        elif filterType == 3:                                                       # ST筛选
            dropIndex = \
                index[self.index_DF(u'st').iloc[max(0, loc - 1)][index] == 1]

        elif filterType == 4:                                                       # 新股筛选
            dropIndex = \
                index[self.index_DF(u'new_stock').iloc[max(0, loc - 1)][index] == 1]
        index = index.drop(dropIndex)
        return index

    def newFilter(self, index, dateLoc, filterType=None):
        """
        剔除前一天不符合条件的index
        """
        dropIndex = pd.Index([])
        if filterType == None:
            return

        elif filterType == 1:  # 停牌筛选
            dropIndex = \
                index[np.isnan(self.indexCleanedDF().iloc[dateLoc][index])]

        elif filterType == 2:  # 一字板筛选
            dropIndex = \
                index[self.index_DF(u'yzb').iloc[dateLoc][index] == 1]  # 策略的一字涨停的股票

        elif filterType == 3:  # ST筛选
            dropIndex = \
                index[self.index_DF(u'st').iloc[dateLoc][index] == 1]

        elif filterType == 4:  # 新股筛选
            dropIndex = \
                index[self.index_DF(u'new_stock').iloc[dateLoc][index] == 1]
        index = index.drop(dropIndex)
        return index

    def addBuffer(self, key, data):
        if not key in self.__Buffer:
            self.__Buffer[key] = []
        self.__Buffer[key].append(data)

    def refBuffer(self, key, offset):
        en = max(0, len(self.__Buffer[key]) - 1)
        idx = max(0, en - offset)
        return self.__Buffer[key][idx]

    def refRangeBuffer(self, key, index, offset, length):
        pass
    # <==================================================== 工具函数 ===============================================>

    # if '报单函数':
    def getPosition(self, code=None):
        if not code:
            return self.position
        return self.position[code]

    def getPositionIndex(self):
        return pd.Index(self.getPosition().keys())

    def getPositionSeries(self):
        tempIndex = self.getPositionIndex()
        tempList = [self.getPosition(i) for i in tempIndex]
        return pd.Series(tempList, index=tempIndex)

    def getCaiptal(self):
        return self.capital

    def amountAfCostF(self, action, amount, cost):
        if action == 1:
            amountAfCost = amount + cost
        if action == -1:
            amountAfCost = amount - cost
        return amountAfCost

    def costF(self, action, amount):
        if action == 1:
            costFee = amount * self.buyFee
        if action == -1:
            costFee = amount * self.sellFee
        return costFee

    def __postionUpdate(self, code, action, vol):
        if action == 1:
            if code in self.position:
                self.position[code] = self.getPosition(code) + vol
            else:                                                                   # 没有股票，添加新的Series
                self.position[code] = vol
        if action == -1:
            self.position[code] = self.getPosition(code) - vol
            if self.position[code] == int(0):                                       # 移除股票
                self.position.pop(code)

    def __fundUpdate(self, action, amount):
        capital = self.getCaiptal()
        if action == 1:
            self.capital = capital - amount
        if action == -1:
            self.capital = capital + amount

    def sendOrder(self, stock, action, price, volume):
        ''' 报单函数 '''
        amt = price * volume
        cost = self.costF(action, amt)
        amtAfCost = self.amountAfCostF(action, amt, cost)
        self.__fundUpdate(action, amtAfCost)                                        # 更新资金
        self.__postionUpdate(stock, action, volume)                                 # 更新资金

        if action == 1:                                                              # 记录交易结果
            feeRatio = self.buyFee
        if action == -1:
            feeRatio = self.sellFee
        self.histRecord[u'orderRecord'][self.TradeTime][stock] = \
            {u'stockCode': stock, u'action': action,
             u'price': price, u'volume': volume,
             u'amount before cost': amt, u'feeCost': cost,
             u'amount after cost': amtAfCost, u'feeRatio': feeRatio,
             u'leverage': 1, u'marginRatio': 100}

    def smartSendOrder(self, tradeSeries, buyPriceMode, sellPriceMode,
                       buyVolMode, sellVolMode, sliceFund, sliceVol=100):
        '''
        自定义模拟交易函数
        :param self:
        :param tradeSeries:     策略交易股票Series
        :param buyPriceMode:    1:开盘买入 2：收盘买入 3: 最高价买入
        :param sellPriceMode:   1:开盘卖出 2：收盘卖出
        :param buyVolMode:      1：sliceFund等金额买入 2：等笔数买入
        :param sellVolMode:     1：报单的股票全部卖出
        :param sliceFund:        每期可用资金
        :param sliceVol:         每期买入笔数
        :return:
        '''
        buyStock = tradeSeries[tradeSeries == 1].index
        sellStock = tradeSeries[tradeSeries == -1].index
        priceSeries = copy.deepcopy(tradeSeries)

        # TODO 1. 依据规则产生价格和成交量
        if not buyStock.empty:
            # 1.1.1 产生买入价格
            if buyPriceMode == 1:
                priceSeries[buyStock] = self.open()[buyStock]
            elif buyPriceMode == 2:
                priceSeries[buyStock] = self.close()[buyStock]
            elif buyPriceMode == 3:
                priceSeries[buyStock] = self.high()[buyStock]
            # 1.1.2 产生买入交易量
            if buyVolMode == 1:
                singleAmt = float(sliceFund) / float(len(buyStock))
            elif buyVolMode == 2:
                sliceVol = float(sliceVol)
                if sliceVol < 100.:
                    print(u'买入笔数不能小于100')
                    sys.exit()

        if not sellStock.empty:
            # 1.2.1 产生卖出价格
            if sellPriceMode == 1:
                priceSeries[sellStock] = self.open()[sellStock]
            elif sellPriceMode == 2:
                priceSeries[sellStock] = self.close()[sellStock]
            # 1.2.2 卖出的成交量默认为买入时的成交量

        # TODO 2. 报单
        for i in range(len(tradeSeries)):
            action = tradeSeries[i]
            code = tradeSeries.index[i]
            price = priceSeries[code]
            if action == 1:                                                             # 买入
                fund = self.getCaiptal()                                                # 资金
                orderVol = 0
                if buyVolMode == 1:
                    orderVol = np.round(singleAmt / float(price), decimals=-2)          # 取100整数
                elif buyVolMode == 2:
                    orderVol = sliceVol
                amt = price * orderVol
                amtAfCost = self.amountAfCostF(action, amt, self.costF(action, amt))    # 已计算手续费
                # 2.1 资金检验
                if fund >= amtAfCost:
                    self.sendOrder(code, action, price, orderVol)
                else:
                    lots = np.round(fund / price, decimals=-2)
                    if lots:
                        self.sendOrder(code,  action, price, lots)
            elif action == -1:                                                          # 卖出
                if code in self.getPosition():                                          # 只对已持有的股票卖出
                    pos = self.getPosition(code)
                    orderVol = -0
                    if sellVolMode == 1:
                        orderVol = self.getPosition()[code]                             # 卖出该股票所有的头寸
                    # 2.2 仓位检验
                    if pos >= orderVol:
                        self.sendOrder(code, action, price, orderVol)
                    elif pos > 0:
                        self.sendOrder(code, action, price, pos)
        return buyPriceMode, sellPriceMode


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

    # <==================================================== 输出函数 ===============================================>
    def outPut(self):
        '''输出数据'''
        benchmarkName = self.benchmark.keys()
        testIndex = pd.DatetimeIndex(self.testTimeList())
        inputData = {
            u'timeFreq': self.testTimeList(),
            u'fee': {self.buyFee, self.sellFee},
            u'strategyNameList': self.strategyNamelist,
            u'benchmark': {i: pd.DataFrame(self.benchmark[i], index=testIndex)
                           for i in benchmarkName},
            u'outPutData':
                {self.strategyNamelist[0]:
                     {u'fundRecord': self.histRecord[u'fundRecord'],
                      u'intiFund': self.initFund,
                      u'orderRecord': self.histRecord[u'orderRecord'],
                      u'positionRecord': self.histRecord[u'positionRecord']}
                 }
        }
        egClass = OutputClass(inputData)
        egClass.plotNetVal(excessRet=True)                                             # 净值曲线
        egClass.periodRet()                                              # 周期化数据
        egClass.recordOutput()                                           # 成交记录
        egClass.Report_multipleFactors(self.tester, self.testContent)    # 写入报告
    # <==================================================== 输出函数 ===============================================>



