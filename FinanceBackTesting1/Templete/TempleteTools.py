# -*- coding: utf-8 -*-
# This is a script for often used funcitons
# Writer: Qijie Li

from sys import version_info
if version_info.major == 2 and version_info.minor == 7:
    import sys
    if sys.getdefaultencoding() != 'utf-8':# 重新设置python 编码为utf-8
        reload(sys)
        sys.setdefaultencoding('utf-8')
import pandas as pd
import copy
import numpy as np
from math import sqrt
import os
import json
import codecs
import csv
# from WindPy import w
import cPickle as pickle
# from tkinter import *
import seaborn
import scipy.stats as sta
import statsmodels.api as sm
import matplotlib.pyplot as plt

def cleanDataEx(df, objectCode):
    '''
    清洗df中数据
    :param objectCode:
    :return: df中objectCode这列上非0的点
    '''
    series = df[objectCode]
    series[series == 0] = np.nan
    new_series = series.dropna()
    return new_series

def read_csvEx(fileName, indexCol=0, startTime=None, endTime=None):
    '''
    加载CSV文件
    index为DatetimeIndex 格式
    '''
    f = open(fileName, 'rb')
    df = pd.read_csv(f, index_col=indexCol, parse_dates=True, encoding='gbk')

    # 截取目标时间段
    if startTime:
        temp1 = df.index.get_loc(startTime)
    else:
        temp1 = 0
    if endTime:
        temp2 = df.index.get_loc(endTime) + 1
    else:
        temp2 = None
    df = df.iloc[temp1:temp2]
    return df

def read_csv2(fileName, indexCol=0, startTime=None, endTime=None):

    '''
    加载CSV文件
    index为DatetimeIndex 格式
    '''
    f = open(unicode(fileName, 'utf-8'), 'r')
    df = pd.read_csv(f, index_col=indexCol, parse_dates=True, encoding='gbk')

    # 截取目标时间段
    if startTime:
        temp1 = df.index.get_loc(startTime)
    else:
        temp1 = 0
    if endTime:
        temp2 = df.index.get_loc(endTime) + 1
    else:
        temp2 = None
    df = df.iloc[temp1:temp2]
    return df

def indexRankEx(targetSeries, rankFactor, rankType=1, rankNum = None):
    '''
    获取按指标排序后股票代码
    rankType 1 越大越好； 0 越小越好
    '''
    if rankNum:
        if rankType == 1:
            rankSeries = rankFactor[targetSeries].sort_values(ascending=False).dropna()
        elif rankType == 0:
            rankSeries = rankFactor[targetSeries].sort_values(ascending=True).dropna()
        if len(targetSeries) <= rankNum:  # 排序后买入股票
            print(u'排序目标的长度小于排序数')
            result = rankSeries.index
        else:
            result = rankSeries[:rankNum].index  # 包含已在持仓的股票
        return result
    return None

def cleaningDfEx(targetDf, DfForCleaning):
    ''' 清洗加载的数据 '''
    colName = targetDf.columns
    prevIndex = copy.deepcopy(targetDf.index)
    cleaningCol = DfForCleaning.columns
    if len(cleaningCol) == len(colName):
        for i in colName:
            series_stand = cleanDataEx(DfForCleaning, i)  # 剔除停牌的时间点---volume等于0
            cleanedSeries = targetDf[i][series_stand.index]  # 挑出符合条件的时间序列
            targetDf[i] = pd.Series(cleanedSeries, index=prevIndex)
    else:
        print (u'股票长度不一致')
        return None
    return targetDf

def dataChecking(toCheckDataPath, asCheckingDataPath, start, end):
    ''' 数据检查功能 '''
    df1 = read_csvEx(toCheckDataPath, indexCol=0, startTime=start, endTime=end)
    df2 = read_csvEx(asCheckingDataPath, indexCol=0, startTime=start, endTime=end)
    col1 = df1.columns
    col2 = df2.columns
    dif_DF = df1 - df2[col1]
    return dif_DF

def fileChecking(targetPath, fileName):
    ''' 检查targetPath是否存在 '''
    return os.path.exists(targetPath + fileName)

def minToDaily_DF(df, type, dailyOffset=None):
    ''' 分钟DF转化为日线DF '''
    if type == 'close':
        minStr = ' 15:00:00'
    elif type == 'open':
        minStr = ' 09:25:00'
    index = df.index
    tempSer = pd.Series(index.to_period(freq='D').unique()).astype(str) + minStr
    tempLoc = pd.DatetimeIndex(tempSer.values)
    if isinstance(df, pd.DataFrame):
        finalDf = pd.DataFrame(index=df.index, columns=df.columns)
    elif isinstance(df, pd.Series):                 # 数据类型为pd.Series
        finalDf = pd.Series(index=df.index)
    if dailyOffset:
        finalDf.loc[[i for i in tempLoc]] = df.loc[[i for i in tempLoc]].shift(dailyOffset)
        lastLoc = df.index.get_loc(tempLoc[dailyOffset - 1])
    else:
        finalDf.loc[[i for i in tempLoc]] = df.loc[[i for i in tempLoc]]
    if type == 'close':
        if dailyOffset:
            finalDf[lastLoc + 1:] = finalDf[lastLoc + 1:].fillna(method='bfill')
        else:
            finalDf = finalDf.fillna(method='bfill')
    elif type == 'open':
        finalDf = finalDf.fillna(method='ffill')
    return finalDf

def dfComparing(*args):
    '''
    数据值比较函数
    支持多个相同的数据结构比较
    返回比较后数据结构
    '''
    comparedDf = args[0]
    for i in range(1, len(args)):
        targetData = args[i]
        comparedDf = np.maximum(comparedDf, targetData)
    return comparedDf

def strMarkFilter(targetStr, pos):
    ''' 剔除某符号后的字符串'''
    result = '_'.join(targetStr.split('_')[:pos])
    return result

def tradingVolume(orderDict, amount, price, *args):
    '''
    合约定量函数
    multiplier: 合约乘数
    margin: 交易保证金比例
    maint_margin: 期权维持保证金
    :return 合约交易量
    '''
    objectType = orderDict[u'objectType']
    direction = orderDict[u'direction']
    volume = None
    if objectType == u'stocks':
        volume = np.floor(amount / price / 100.) * int(100)
    elif objectType == u'options':
        if direction == 1:
            volume = np.floor(amount/price)
        elif direction == -1:
            mint_margin = float(args[0])
            volume = np.floor(amount/mint_margin)
    elif objectType == u'futures':
        multiplier, margin = [float(i) for i in args]
        denominator = price * multiplier * margin
        volume = np.floor(amount/denominator)
    return volume

def strToDatetime(strData, timeFreq=None):
    '''
    :param strData:
    :param timeFreq: {u'D':day, u'W':week, u'M':months, u'A':'year'}
    :return:将字符串格式转换为datetime格式
    '''
    myDatetime = pd.DatetimeIndex([strData])
    if timeFreq:
        timeFreqDict = {u'D': 'days', u'W': 'week', u'M': 'month', u'Q': 'quarter', u'A': 'year'}
        method = timeFreqDict[timeFreq]
        return eval('myDatetime.' + eval('method'))[0]
    else:
        return myDatetime[0]
    # a = datetime.strptime(strData, '%Y-%m-%d %H:%M:%S')
    # pass

def lastTradedayEx(originalSeries, timeFreq=u'D'):
    '''
    :param originalSeries: 时间格式的pd.Series
    :param timeFreq: {u'D':day, u'W':week, u'M':months, u'A':'year'}
    :return: 返回最后交易日的日期结果
    '''
    timeFreqDict = {u'D': 'day', u'W': 'week', u'M': 'month', u'A': 'year'}
    codeIndex = originalSeries.index
    method = timeFreqDict[timeFreq]
    series = eval('pd.DatetimeIndex(originalSeries).' + eval("method"))
    newSeries = pd.Series(series, index=codeIndex)
    return newSeries

def freqTransfer(indexParm, freq, how='end'):
    '''
    时间序列转换函数
    :param series: values和index 都是datetimeIndex
    :param freq:  {u'D': day,
                 u'W': week,
                 u'M': months,
                 u'A': year,
                 u'SA': 半年，
                 u'H': hour,
                 u'T':min}
    :param how: 截取方式
    :return:
    '''
    if freq in [u'W', u'M', u'Q', u'A']:
        newIndex = indexParm.to_period(freq=freq).unique()
        return newIndex
    else:
        tempIndex = indexParm.to_series()
        tempSeries = tempIndex.asfreq(freq=freq, how=how, method=None).dropna()
        newSeries = tempSeries.to_period(freq=freq)
        return newSeries.index

def toPeriodList(periodType, dailyList):
    '''
    :param periodType: {'D': 天, 'M': 月, 'Q':季度, 'SA': 半年, 'A':年}
    :param dailyList: 日频数据
    :return: 返回日线频率转化后的结果
    '''
    if periodType == 'SA':
        pass
    elif periodType == 'D':
        periodSeries = pd.Series(dailyList, index=dailyList)
        uniquePeriodSeries = dailyList
    else:
        periodSeries = copy.deepcopy(pd.DatetimeIndex(dailyList)).to_period(
            freq=periodType).to_series().astype(str)
        periodSeries.index = periodSeries.values
        uniquePeriodSeries = np.unique(periodSeries.tolist())
    return uniquePeriodSeries, periodSeries

def getPeriodDate(uniPeriodList, periodList, dailyList, type=2):
    '''
    :param uniPeriodList:
    :param periodList:
    :param type: {0:全部取到, 1: 取周期第一个, 2取周期最后一个}
    :return: 返回周期列表
    '''
    if len(periodList) != len(dailyList):
        print(u'无法比较，因为日线长度和周期长度不一致')
    else:
        dateList = []
        for p in range(len(uniPeriodList)):  # 周期迭代
            period = uniPeriodList[p]
            periodLocSer = pd.Series(periodList.loc[period])
            tempSlice = periodList.index.get_loc(period)
            if len(periodLocSer) >= 2:
                stLoc = tempSlice.start
                enLoc = tempSlice.stop - 1
            else:
                stLoc = tempSlice
                enLoc = tempSlice
            if type == 0:
                tempSlice = dailyList[stLoc], dailyList[enLoc]
                dateList.append(tempSlice)
            else:
                if type == 1:
                    enLoc = stLoc
                elif type == 2:
                    stLoc = enLoc
                tempSlice = dailyList[stLoc:enLoc + 1]
                [dateList.append(i) for i in tempSlice]
        return dateList

def biasF(closeDf, days):
    ''' 偏离度/乖离率'''
    mvDf = closeDf.rolling(days).mean()
    biasDf = closeDf/mvDf - 1
    return biasDf

def hisVolitility(closeDf, days):
    ''' 历史波动率 '''
    n = int(days)
    ln = np.log(closeDf / closeDf.shift(1))
    volitility = ln.rolling(n).std() * sqrt(int(250))
    return volitility

def toPeriodDF(dailyDf, freq, method=None, isCompound=True):
    '''
    :param dailyDf: dataframe
    :param freq: {u'D':day, u'W':week, u'M':months, u'Q'quater, u'A':'year'}
    :param method:  {'pct_chg':涨跌幅, 'sum':求和， 'mean':求均值， ’std':标准差, 'lastLoc':周期最后一个}
    :param isCompound: {涨跌幅默认使用复利计算, isCompound=False:涨跌幅使用单利计算}
    :return: 时间频率切换后的dataframe
    '''

    finalDf = copy.deepcopy(dailyDf)
    timeIndex = finalDf.index
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
    finalDf.index = finalSeries.index

    if method:
        uniqueIndex = pd.Index(np.unique(finalDf.index))
        uniqueDf = []
        if method == u'pct_chg':
            finalDf = finalDf/finalDf.shift(1) - 1
            finalDf.iloc[0] = 0.
        for i in uniqueIndex:
            tempSlice = finalDf.loc[i]
            if len(tempSlice.shape) <= 1:   # 只有一行数据时
                if len(tempSlice.index) >= 2:
                    tempSlice = pd.DataFrame(tempSlice).transpose()
                else:
                    tempSlice = pd.DataFrame(tempSlice.values, index=pd.Index([i]))
            else:   # 有多行时--dataFrame
                tempSlice = pd.DataFrame(tempSlice.values, index=tempSlice.index)
            if method in [u'sum', u'mean', u'std']:
                tempRow = eval('tempSlice.' + eval('method') + '(axis=0)')
            elif method in [u'lastLoc']:
                tempRow = tempSlice.iloc[-1]
            else:
                if isCompound:  # 默认复利计算
                    tempRow = (1 + tempSlice).prod(axis=0) - 1
                else:   # 单利计算
                    tempRow = tempSlice.sum(axis=0)
            uniqueDf.append(pd.Series(tempRow).values)
        if not finalDf.columns.empty:
            uniqueDf = pd.DataFrame(uniqueDf, index=uniqueIndex, columns=finalDf.columns)
        else:
            uniqueDf = pd.DataFrame(uniqueDf, index=uniqueIndex)
        return uniqueDf
    else:
        return finalDf

def annualvolatility(weekRet):
    return np.std(weekRet) * np.sqrt(52)

def annualreturn(weekRet):
    return np.mean(weekRet) * 52

def sharpe_ratio(weekRet, rf):
    tempResult = (annualreturn(weekRet) - rf)/float(annualvolatility(weekRet))
    if tempResult < 0.:
        tempResult = 0.
    return tempResult

def downsideSTD(weekRet):
    '''下行波动率'''
    downsidenetvalue_array = []
    for i in range(len(weekRet)):
        if weekRet[i] < np.mean(weekRet):
            downsidenetvalue_array.append(weekRet[i])
    tempSquare = np.power(downsidenetvalue_array, 2)
    std_downSide = np.sqrt(np.sum(tempSquare) / len(weekRet) * 52.)
    return std_downSide

def sortino_ratio(weekRet, rf):
    if downsideSTD(weekRet):
        return (annualreturn(weekRet) - rf) / float(downsideSTD(weekRet))
    else:
        return None

def blockSummary(dataDict):
    ''' 数据描述函数 '''
    weekNetRet = dataDict[u'netValue']
    rf = dataDict[u'riskFreeRatio']
    scenarioCond = dataDict[u'scenario']
    c_level = dataDict[u'confidence level']
    resultDict = {}
    totalLen = weekNetRet.size
    for i in scenarioCond:
        resultDict[i] = {u'周期数': None,
                         u'周期收益率区间范围': None,
                         u'场景收益占比': None,
                         u'波动率': None,
                         u'夏普率': None,
                         u'Sortino比率': None}

        if not scenarioCond[i] is None:
            cond = scenarioCond[i]
            scenarioNetRet = weekNetRet[cond]
            num = scenarioNetRet.size
            if not scenarioNetRet.empty:
                if num > int(1):
                    numRatio = num / float(totalLen)
                    volatility = annualvolatility(scenarioNetRet)
                    sp = sharpe_ratio(scenarioNetRet, rf)
                    sortino_r = sortino_ratio(scenarioNetRet, rf)
                    retRange = u'~'.join(["%.2f%%" % (j * 100) for j in distRange(scenarioNetRet, c_level)])
                    resultDict[i] = {u'周期数': num,
                                     u'周期收益率区间范围': retRange,
                                     u'场景收益占比': numRatio,
                                     u'波动率': volatility,
                                     u'夏普率': sp,
                                     u'Sortino比率': sortino_r}
    return resultDict

def tableGraph(title, df):
    myCols = df.columns
    myIndex = df.index

    root = Tk()
    root.title(title)
    cvWidth, cvHeight = 1000, 400
    cv = Canvas(root, width=cvWidth, height=cvHeight, bg='snow')
    Label(root, text=title, font=('sans-serif', 14)).pack(side=TOP)

    edgeGap = 10
    rowLen = myIndex.size
    rowGap = (cvHeight - int(20) - int(edgeGap) * 2) / float(rowLen + 1)
    colLen = myCols.size
    colGap = (cvWidth - int(edgeGap) * 2) / float(colLen + 1)
    blueline = cv.create_line(edgeGap, 10, cvWidth - edgeGap, 10, fill=seaborn.xkcd_rgb['flat blue'])      # cv.create_line(10<-距离左边, 10<-距离上边, 830-距离右边, 10<-到达的点)

    # 按列
    for i in list(range(colLen + 1)):
        y = 10. + rowGap/2.
        for j in list(range(rowLen + 1)):
            if not j:
                if not i:
                    x = 10. + colGap/2.
                    cv.create_text(x, y, text=u'指标', font=('sans-serif', 11, 'bold'))
                else:
                    cv.create_text(x, y, text=myCols[i - 1], font=('sans-serif', 11, 'bold'))
            else:
                if not i:
                    x = 10. + colGap/2.
                    cv.create_text(x, y, text=myIndex[j - 1], font=('sans-serif', 11, 'bold'))
                else:
                    cv.create_text(x, y, text=df.iloc[j - 1, i - 1], font=('sans-serif', 11, 'bold'))
            y = y + rowGap
        x = x + colGap
    cv.pack()
    root.mainloop()
    root.image_names()

def geoMean(df, windows):
    ''' pandas版本移动几何平均 '''
    finalDf = df.rolling(windows).apply(sta.gmean)
    return finalDf

def distRange(df, c_level, method=None):
    '''
    收益率区间范围
    :param c_level: 置信水平
    :param method: {None:默认历史模拟法}
    :return:
    '''
    if not method:
        a = df.describe(percentiles=[c_level, 1-c_level])
        leftTail = a[4]
        rightTail = a[6]
        return leftTail, rightTail

def standardization(df):
    ''' 标准变化函数 '''
    finalDf = (df - df.mean())/df.std()
    return finalDf

def maxDrawdown(series):
    '''
    :param series:
    :return: 最大回撤率, 最大回测持续期
    '''
    x = series.values
    length = series.index.size
    x.shape = (x.shape[0], 1)
    drawdown, exDrawdown, = np.zeros(length), np.zeros(length)  # drawdown_timing = np.zeros(length)
    for i in range(length):
        dd = (x[:1 + i].max() - x[i]) / x[:1 + i].max()
        drawdown[i] = dd
    maxDd = drawdown.max()                  # 最大回撤
    maxDd_index = drawdown.argmax()
    maximum = x[:maxDd_index + 1].argmax()  # 最大的index
    maxDd_duration = maxDd_index - maximum  # 最大回撤持续期
    return maxDd, maxDd_duration

def drawHist(heights, xList, plotTitle, path):
    ''' 绘制分布直方图 '''
    # 创建直方图
    loc = xList.index(u'myFund')
    targetValue = heights[loc]
    fig = plt.figure()
    histResult = plt.hist(heights, bins=min(len(heights), 17), normed=0, histtype='bar', color='skyblue')
    rectangleList = histResult[2]
    for i in range(1, len(rectangleList)):
        if targetValue >= rectangleList[i - 1]._x and targetValue < rectangleList[i]._x:
            rectangele = rectangleList[i - 1]
        else:
            rectangele = rectangleList[i]

    egBar = plt.bar(rectangele._x, width=rectangele._width, height=int(rectangele._height), color='yellowgreen',
                    align='edge', label=u'目标基金')
    plt.yticks(list(range(int(np.max(histResult[0]) + 1))))
    plt.legend(loc='upper left')   # 图例:第一个参数-Artist，第二个参数-Artist的标签
    # plt.legend([egBar], [u'目标基金'], loc='upper left')   # 图例:第一个参数-Artist，第二个参数-Artist的标签
    plt.xlabel(u'日频收益率')
    plt.ylabel(u'频数')
    plt.title(u'%s同策略收益分布图' % plotTitle)
    # plt.show()
    fig.savefig(path)
    plt.close(fig)

def scenarioGraph(seriesDict, background_array, scenarioToColorDict, title, toPath):
    '''
    :param seriesDict: 时间轴对齐后的数据字典
    :param backgroundDict:
    :param title:
    :return:
    '''
    for tempI in seriesDict:
        data = copy.deepcopy(seriesDict[tempI])
        if isinstance(data, pd.DataFrame):
            seriesDict[tempI] = pd.Series(data.iloc[:, 0])

    if len(seriesDict.values()) == 1:
        num_list =seriesDict.values()[0]
        up = max(num_list)  # 图例位置
        down = min(num_list)  # 图例位置
    else:
        for k in range(1, len(seriesDict.values())):
            num_list = seriesDict.values()[k]
            prevNum_list = seriesDict.values()[k -1]

            up = max(max(num_list), max(prevNum_list))     # 图例位置
            down = min(min(num_list), min(prevNum_list))     # 图例位置

    # 创建图像
    size = (14, 8)
    fig = plt.figure(figsize=(size[0], size[1]))
    tempNameList = seriesDict.values()[0].index
    plt.xlim(0, tempNameList.size)
    plt.ylim(down, up)
    ylim = copy.deepcopy(plt.ylim())
    xlim = copy.deepcopy(plt.xlim())
    xtickActuallyLen = 0.35
    gapNum = size[0] / xtickActuallyLen
    xtickGap = max(1, int(round(len(tempNameList) / float(gapNum))))
    plt.xticks(range(0, len(tempNameList), xtickGap), tempNameList[::xtickGap], rotation=45, fontsize=8)

    for color in range(len(background_array)):  # 背景色块
        plt.axvspan(0 + color, 1 + color, facecolor=background_array[color], edgecolor=background_array[color])

    # 色块标注
    annotateList = [(i + u'●') for i in scenarioToColorDict.keys()]
    textPos = [xlim[0] + (xlim[1] - xlim[0]) * 0.10, ylim[0] + (ylim[1] - ylim[0]) * (1 - 0.1)]
    xyPos = xlim[0] + (xlim[1] - xlim[0]) * 0.05, ylim[0] + (ylim[1] - ylim[0]) * (1 - 0.1)
    for q in range(len(annotateList)):
        annoataKey = annotateList[q]
        corlor = scenarioToColorDict[annoataKey.strip(u'●')]
        plt.annotate(annoataKey, color=corlor,
                     xy=(xyPos[0], xyPos[1]),
                     xytext=(textPos[0], textPos[1]),
                     size=10, va="center", ha="center",
                     bbox=dict(boxstyle="square", fc="k"))
        textPos[-1] = textPos[-1] - (ylim[1] - ylim[0]) * 0.05

    # 净值柱状图
    width = plt.xlim()[-1] / len(background_array)
    for i in range(len(seriesDict.keys())):
        seriesName = seriesDict.keys()[i]
        num_list = seriesDict[seriesName]
        x = list(range(len(num_list)))
        name_list = num_list.index
        plt.bar(x, num_list, width=width, label=seriesName, tick_label=name_list, fc='red', edgecolor='k', align='edge')
        plt.legend(fontsize=12, loc='lower center')

        pass
    plt.title(title, fontsize='large', fontweight='bold')
    fig.savefig(toPath)

def seriesPercentage(pdData, pctLevel=0.05, tailDirection=u'left'):
    '''
    按列进行统计
    :param pdData:
    :param pctLevel:
    :param tailDirection: {取分布左尾部分， 取分布右尾部分}
    :return:
    '''

    if isinstance(pdData, pd.Series):
        tempSeries = pdData.rank(pct=True, ascending=True).sort_values(ascending=True)
    tempSeries = tempSeries.dropna()

    if tailDirection == u'left':
        tempSeries2 = tempSeries[tempSeries <= pctLevel]
    else:
        tempSeries2 = tempSeries[tempSeries >= 1 - pctLevel]
    finalSeries = pdData.loc[tempSeries2.index]
    return finalSeries, tempSeries2

def seriesTop(pdData, num, ascending=True):
    '''
    按列进行排序
    :param pdData:
    :param num:
    :param ascending:
    :return:
    '''
    if isinstance(pdData, pd.Series):
        tempSeries = pdData.rank(ascending=True).sort_values(ascending=ascending)
    tempSeries = tempSeries.dropna()
    finalSeries = tempSeries.iloc[:num]
    return finalSeries, tempSeries

def rptDataToDailyData(rptData, rptDateDf, tradeDayIndex, toName):
    '''
    披露日财报数据转换为日线数据
    :param rptData: 只要含季末数据
    :param rptDateDf: rptDateSeries -- 对应wind stm_issuingdate 指标
    :param tradeDayIndex:
    :return:
    '''
    finalDf = pd.DataFrame(index=tradeDayIndex, columns=rptData.columns)
    quarterIndex = copy.deepcopy(rptData.index)
    for i in finalDf.columns:
        print('is generating column %s' % i)
        rptDateSereis = rptDateDf[i]
        dataSeries = rptData[i]
        for j in range(1, len(quarterIndex)):
            lastQuarter = quarterIndex[j - 1]
            quarter = quarterIndex[j]
            # print('is runing at %s' % quarter)
            lastRptDate = str(pd.to_datetime(rptDateSereis.loc[lastQuarter]).date())
            rptDate = str(pd.to_datetime(rptDateSereis.loc[quarter]).date())
            if rptDate == 'NaT' or lastRptDate =='NaT':
                continue
            value = dataSeries.loc[lastQuarter]
            if i == '000166.SZ' and str(quarter.date()) == '2015-03-31':
                pass
            cond = pd.eval('lastRptDate <= tradeDayIndex < rptDate')
            finalDf[i][cond] = value
    finalDf.to_csv(toName)

def getTradeDayList(startDate, endDate):
    ''' 从wind获取交易日期 '''
    w.start()
    tradedayList = w.tdays(startDate, endDate, u"").Data[0]
    w.stop()
    strDayList = [str(i.date()) for i in tradedayList]
    return tradedayList, strDayList

def getTradeDayList_New(startDate, endDate, freq=u'D', calender=False):
    '''
    从wind获取交易日期, 支持分频率
    freq: {'W'， 'M', 'Q'， 'S':半年， 'A':年}
    '''
    w.start()
    if freq == u'D':
        if calender ==True:
            formulaLeft = '"Days=Alldays")'
        else:
            formulaLeft = '"")'
    else:
        if calender == True:
            formulaLeft = '"Days=Alldays;Period=%s")' % freq
        else:
            formulaLeft = '"Period=%s")' % freq

    downLoadFormula = 'w.tdays(startDate, endDate,'
    tradedayList = eval(downLoadFormula + formulaLeft).Data[0]
    w.stop()
    strDayList = [str(i.date()) for i in tradedayList]
    return tradedayList, strDayList

def getSetCode(st, en, setType = 'SW', subType=1):
    ''' 获取行业数据 '''
    w.start()
    codeQuery = 'w.wset("sectorconstituent", "date=%s;sectorid=a001010100000000;field=wind_code")' % (en)
    joinCode = ','.join(eval(codeQuery).Data[0])
    if setType == 'SW':
        industyStr = "industry_sw"
    queryStr = 'w.wsd(joinCode, "%s", "%s", "%s", "industryType=%s")' % (industyStr, st, en, subType)
    resutl = eval(queryStr).Data
    w.stop()
    return

def getSetToCsv(codeList, dateList, path):
    ''' 从WIND获取指定日期的集合代码 '''
    w.start()
    for i in range(len(codeList)):
        sectorCode = codeList[i]
        dataSet = {}
        for p in range(len(dateList)):  # 周期迭代
            tempDate = dateList[p]
            print(tempDate)
            # 下载数据
            result = eval('w.wset("sectorconstituent", "date=' + tempDate + '; sectorid=' + sectorCode + '")')
            dataSet[tempDate] = result
        print(u'代码全时间段下载完毕')
        strFilename = path + sectorCode + u'-' + dateList[0] + u'-' + dateList[-1] + u'.csv'

        # 保存数据至csv文件
        # tempDf = pd.DataFrame()
        csvFile = file(strFilename, u'wb')
        csvFile.write(codecs.BOM_UTF8)
        csvWriter = csv.writer(csvFile)
        for j in range(len(dateList)):
            tempDate = dateList[j]
            row = dataSet[tempDate].Data[1]  # j 时间段所有代码数据
            row.insert(0, tempDate)
            csvWriter.writerow(row)
            # tempDf = tempDf.append(pd.Series(row), ignore_index=True)
        csvFile.close()
        # tempDf.index = pd.Index(dateList)
        # tempDf.transpose()
        # tempDf.to_csv(strFilename, header=False)
    w.stop()

def getDataToCsv(indexNameList, name, stockCodelist, uniPeriodList, periodList, strDayList, path, dataType=0):
    '''
    :param dataType: {0: 不复权， 1:前复权, 2:后复权}
    :return: 从WIND获取数据
    '''
    formulaLeft = '"")'
    if dataType == 1:
        formulaLeft = '"PriceAdj=F")'
    elif dataType == 2:
        formulaLeft = '"PriceAdj=B")'
    elif dataType == 3:
        formulaLeft = '"Period=Q;Days=Alldays")'

    lenLimit = 3000
    if len(stockCodelist) > lenLimit:
        splitedCodelist = []
        for k in range(lenLimit, len(stockCodelist) + lenLimit, lenLimit):
            splitedCodelist.append(stockCodelist[k - lenLimit: k])
    else:
        splitedCodelist = [stockCodelist]

    w.start()

    for i in range(len(indexNameList)):
        indexCode = indexNameList[i]
        dataSet = {}
        dateRangeDict = {}

        for q in range(len(splitedCodelist)):  # 分批次对代码的数据进行下载
            codeList = splitedCodelist[q]
            joinCode = u','.join(codeList)
            dataSet[q] = {}
            dateRangeDict[q] = {}

            for p in range(len(uniPeriodList)):  # 周期循环
                period = uniPeriodList[p]
                print(u'period is %s and block is %s ' % (period, q))
                periodLocSer = pd.Series(periodList.loc[period])
                tempSlice = periodList.index.get_loc(period)
                if len(periodLocSer) >= 2:
                    stLoc = tempSlice.start
                    enLoc = tempSlice.stop - 1
                else:
                    stLoc = tempSlice
                    enLoc = tempSlice
                # stLoc = enLoc     # 只取period期末数据
                dateRange = [strDayList[stLoc], strDayList[enLoc]]

                downLoadFormula = 'w.wsd(joinCode, indexNameList[i], dateRange[0], dateRange[-1],'
                result = eval(downLoadFormula + formulaLeft)
                dataSet[q][period] = result
                dateRangeDict[q][period] = [stLoc, enLoc]

        print(u'代码全时间段下载完毕')
        strFilename = path + indexCode + u'_' + name + u'-' + strDayList[0] + u'-' + strDayList[-1] + u'.csv'
        # 保存数据至csv文件
        finalDf = pd.DataFrame([])
        for q in range(len(splitedCodelist)):
            dfList = []
            for periodKey in uniPeriodList:  # 周期
                st = dateRangeDict[q][periodKey][0]
                end = dateRangeDict[q][periodKey][-1]
                csvTradedayList = strDayList[st:end + 1]
                for j in range(len(csvTradedayList)):  # 不同交易日
                    if st == end:
                        row = dataSet[q][periodKey].Data[0]
                    else:
                        row = [i[j] for i in dataSet[q][periodKey].Data]  # j 时间段所有股票代码数据
                    row.insert(0, csvTradedayList[j])
                    dfList.append(row)
            splitDf = pd.DataFrame(dfList)
            splitDf = pd.DataFrame(splitDf.iloc[:, 1:].values, index=splitDf.iloc[:, 0], columns=splitedCodelist[q])
            finalDf = pd.concat([finalDf, splitDf], axis=1)
        finalDf.to_csv(strFilename)
    w.stop()

def getDataFromWind(indexNameList, name, stockCodelist, uniPeriodList, periodList, strDayList, path, forwardType=1, calender=False, periodLocType=0):

    '''
    :param forwardType: {0: 不复权， 1:前复权, 2:后复权}
    :param calender: {True:日历日, False:交易日}
    :param periodLocType: {0: 周期全取到， 1:取期初, 2:取期末}
    :return: 从WIND获取数据
    '''

    if calender:
        formulaLeft = '"Days=Alldays"'
    else:
        formulaLeft = ''

    if forwardType == 1:
        formulaLeft = formulaLeft + '; "PriceAdj=F"'
    elif forwardType == 2:
        formulaLeft = formulaLeft + '; "PriceAdj=B"'
    else:
        formulaLeft = formulaLeft + ''
    formulaLeft = formulaLeft + ')'

    lenLimit = 3000
    if len(stockCodelist) > lenLimit:
        splitedCodelist = []
        for k in range(lenLimit, len(stockCodelist) + lenLimit, lenLimit):
            splitedCodelist.append(stockCodelist[k - lenLimit: k])
    else:
        splitedCodelist = [stockCodelist]
    w.start()

    dataDict = {}
    for i in range(len(indexNameList)):
        indexCode = indexNameList[i]
        dataSet = {}
        dateRangeDict = {}

        for q in range(len(splitedCodelist)):  # 分批次对代码的数据进行下载
            codeList = splitedCodelist[q]
            joinCode = u','.join(codeList)
            dataSet[q] = {}
            dateRangeDict[q] = {}

            for p in range(len(uniPeriodList)):  # 周期循环
                period = uniPeriodList[p]
                print(u'period is %s and block is %s ' % (period, q))
                periodLocSer = pd.Series(periodList.loc[period])
                tempSlice = periodList.index.get_loc(period)
                if len(periodLocSer) >= 2:
                    stLoc = tempSlice.start
                    enLoc = tempSlice.stop - 1
                else:
                    stLoc = tempSlice
                    enLoc = tempSlice
                if periodLocType == 1:
                    enLoc = stLoc       # 只取period期初数据
                elif periodLocType == 2:
                    stLoc = enLoc       # 只取period期末数据
                dateRange = [strDayList[stLoc], strDayList[enLoc]]
                downLoadFormula = 'w.wsd(joinCode, indexCode, dateRange[0], dateRange[-1],'
                if indexCode == 'mkt_cap_float':
                    downLoadFormula = downLoadFormula + '"unit=1;currencyType="'
                elif indexCode in ['turn']:
                    downLoadFormula = downLoadFormula + '"Period=W"'
                finalStr = downLoadFormula + formulaLeft
                result = eval(finalStr)
                dataSet[q][period] = result
                dateRangeDict[q][period] = [stLoc, enLoc]

        print(u'%s代码全时间段下载完毕' % indexCode)
        if path:
            strFilename = path + indexCode + u'_' + name + u'-' + strDayList[0] + u'-' + strDayList[-1] + u'.csv'
        # 转换数据结构
        finalDf = pd.DataFrame([])
        for q in range(len(splitedCodelist)):
            dfList = []
            for periodKey in uniPeriodList:  # 周期
                st = dateRangeDict[q][periodKey][0]
                end = dateRangeDict[q][periodKey][-1]
                csvTradedayList = strDayList[st:end + 1]
                for j in range(len(csvTradedayList)):  # 不同交易日
                    if st == end:
                        row = dataSet[q][periodKey].Data[0]
                    else:
                        row = [i[j] for i in dataSet[q][periodKey].Data]  # j 时间段所有股票代码数据
                    row.insert(0, csvTradedayList[j])
                    dfList.append(row)
            splitDf = pd.DataFrame(dfList)
            splitDf = pd.DataFrame(splitDf.iloc[:, 1:].values, index=splitDf.iloc[:, 0], columns=splitedCodelist[q])
            finalDf = pd.concat([finalDf, splitDf], axis=1)
        if path:        # 保存数据至csv文件
            finalDf.to_csv(strFilename)
        dataDict[indexCode] = finalDf
    w.stop()
    return dataDict

def getAllIndex(fileName):
    ''' 取index中所有的元素, 支持各列长度不一致 '''
    with open(fileName, 'r') as csvfile:
        reader = csv.reader(csvfile)
        finalDict = {}
        for row in reader:
            finalDict[row[0]] = pd.Index(row[1:])
        return finalDict

def getUnionIndex(fileName):
    ''' 取index中不重复的元素的并集 '''
    with open(fileName, 'r') as csvfile:
        reader = csv.reader(csvfile)
        finalIndex = pd.Index([])
        for row in reader:
            finalIndex = finalIndex | pd.Index(row[1:])
    return finalIndex

def pickleF(data=None, toPath=None, fromPath=None):
    '''
    将data输出至本地 或者读取pickle文件
    :param data:
    :param toPath: 文件输出路径，带文件名(一般取.txt)
    :param fromPath: 文件读取路径，带文件名(一般取.txt)
    :return:
    '''
    if toPath:
        # 将对象保存至本地文件
        with open(toPath, u'w+') as f:
            pickle.dump(data, f)
            f.close()
        print ('已将数据导入至%s' %toPath)

    elif fromPath:
        # 导入
        with open(fromPath, u'r') as f:
            loadedData = pickle.load(f)
            return loadedData

def getCurDir():
    ''' 返回上层文件路径 '''
    return os.path.abspath(os.path.dirname(__file__))

def loadJson(path):
    ''' 读取json文件为df结构'''
    f = open(path, encoding='utf-8')
    dictData = json.load(f)
    return dictData

# 树形结构
from collections import defaultdict

def tree():
    return defaultdict(tree)

if __name__== u"__main__":
    # a = strMarkFilter(u'exe_price_options', -1)
    # t1 = fileChecking(u'../Data/Main/', u'volume_stocks.csv')
    # t1 = lastTradedayEx(read_csvEx(u'../Data/Main/lasttradingdate_options.csv', indexCol=0).iloc[0], u'M')
    # tempTime = df.index[0]
    # tempResult = strToDatetime(tempTime, timeFreq=u'D')
    # t2 = lastTradeday(df, u'M')
    # t3 = timeSeriesChecking(u'../Data/Main/', u'D')
    # df = df.pct_change().fillna(method='bfill')
    # t2 = seriesPercentage(df.iloc[0], tailDirection='right')
    getSetCode('2018-09-28', '2018-09-28')
    pass
