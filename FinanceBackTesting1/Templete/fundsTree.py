#!/usr/bin/python
# -*- coding: utf-8 -*-
# 读取excel基金库数据，返回基金分类的树形结构
# 一次性获取基金数据和基金代码

import sys
if sys.getdefaultencoding() != 'utf-8':# 重新设置python 编码为utf-8
    reload(sys)
    sys.setdefaultencoding('utf-8')
import pandas as pd
import copy
import csv
import datetime
from TempleteTools import getTradeDayList, getPeriodDate, toPeriodList, getSetToCsv, getDataToCsv
from collections import defaultdict

def tree():
    ''' 树形结构 '''
    return defaultdict(tree)

def toUnicode(code):
    if isinstance(code, float):
        code = unicode(int(code))
    return code

def backFindingDict(allDictList, curCode):
    ''' 从当前key 回溯至第一个key, 并返回第一个key至当前的所有key'''
    keyList = []
    for i in range(-1, -len(allDictList)-1, -1):
        key = allDictList[i].keys()[0]
        values = allDictList[i].values()[0]
        if curCode == values:
            keyList.append(key)
            curCode = copy.deepcopy(key)
    keyList.reverse()
    return keyList

def listToMykeymethod(keyIndex):
    ''' List转化为自定义str用于字典的key'''
    myStr = u''
    for i in range(len(keyIndex)):
        key = keyIndex[i]
        myStr = myStr + u'["%s"]' % key
    return myStr

def getTreeFromExcel(path):
    df = pd.read_excel(path, header=None, encoding='gbk')
    df = df.fillna(0)
    codeDict = {}
    finalDict = tree()
    dictList = []
    lastTreeList = []
    for i in range(0, len(df.columns), 2):
        # print('i is %s' % i)
        # 转换浮点数
        for q in range(len(df.iloc[:, i + 1])):
            if df.iloc[q, i + 1]:
                df.iloc[q, i + 1] = toUnicode(df.iloc[q, i + 1])
        series1 = copy.deepcopy(df.iloc[:, i])
        series2 = copy.deepcopy(df.iloc[:, i + 1])
        nameSeries = copy.deepcopy(series1[series1 != 0])
        codeSeries = copy.deepcopy(series2[series2 != 0])
        for j in range(nameSeries.size):
            if i >= 2:
                name = nameSeries.iloc[j]
                code = codeSeries.iloc[j]
                # print('code is %s' % code)
                codeDict[code] = name
                curCodeLoc = pd.Index(series2).get_loc(code)
                lastTest = df.iloc[:, i - 1].iloc[curCodeLoc - 1]
                if lastTest != 0:
                    lastCopy = copy.deepcopy(lastTest)
                lastCode = lastCopy
                dictList.append({lastCode: code})
                previousKey = backFindingDict(dictList, code)
                previousKey.append(code)
                if name[0] != u'+':                             # 分类不能再细分
                    lastTreeList.append({code: previousKey})
                    eval('finalDict' + listToMykeymethod(previousKey))
    return finalDict, codeDict, lastTreeList

def getUnionIndex(fileName):
    ''' 取index中不重复的元素 '''
    with open(fileName, 'r') as csvfile:
        reader = csv.reader(csvfile)
        finalIndex = pd.Index([])
        for row in reader:
            finalIndex = finalIndex | pd.Index(row[1:])
    return finalIndex


if __name__ == '__main__':
    # 调用实例函数
    def testF1(codeList, st, en, freq, path):
        tradedayList, strdayList = getTradeDayList(st, en)       # 获取交易日
        uniPeriodList, periodList = toPeriodList(freq, tradedayList)         # 转化周期频率
        dateList = getPeriodDate(uniPeriodList, periodList, strdayList, type=2)
        getSetToCsv(codeList, dateList, path)

    def testF2(indexNameList, name, stockCodelist, st, en, freq, path):
        tradedayList, strdayList = getTradeDayList(st, en)  # 获取交易日
        uniPeriodList, periodList = toPeriodList(freq, tradedayList)  # 转化周期频率

        for i in uniPeriodList:
            periodLocSer = pd.Series(periodList.loc[i])
            tempSlice = periodList.index.get_loc(i)
            if len(periodLocSer) >= 2:
                stLoc = tempSlice.start
                enLoc = tempSlice.stop - 1
            else:
                stLoc = tempSlice
                enLoc = tempSlice

            # 内置下载周期
            inPutUniPeriodList, inPutPeriodList, inPutStrdayList, inputTradedayList = \
                [i], periodList.loc[i], strdayList[stLoc: enLoc + 1], tradedayList[stLoc: enLoc + 1]
            downloadFreq = u'W'
            downloadUniPeriodList, downloadPeriodList = toPeriodList(downloadFreq, inputTradedayList)  # 转化周期频率
            getDataToCsv(indexNameList, name, stockCodelist, downloadUniPeriodList, downloadPeriodList, inPutStrdayList, path, dataType=0)
            pass

    path = u'../Data/Main/'
    # myTree, cnDict, lastTreeList = getTreeFromExcel('../Data/Main/FundDict.xlsx')
    pass
    # codeList = [i.keys()[0] for i in lastTreeList[:2]]
    # codeList = ['1000023114000000']     # 基金代码
    # codeList = ['1000023121000000']     # 混合型
    # codeList = ['a001010100000000']     # 全市场
    # codeList = ['1000000090000000']     # HS300
    # codeList = ['XT125149.XT', 'ZG147323.OF', 'ZG144526.OF', 'XT1500307.XT', 'XT1500313.XT', 'XT1500310.XT', 'XT1500311.XT', 'XT1500308.XT', 'XT1500306.XT', 'XT1500312.XT', 'XT1500304.XT', 'XT1500314.XT', 'XT1500309.XT', 'XT1500305.XT', 'XT1504203.XT', 'XT1508381.XT', 'XT1616156.XT']     # 基金代码
    # st, en = '2010-01-01', '2018-08-15'
    # testF1(codeList, st, en, 'W', path)
    # # 合并代码
    # setIndex = pd.Index([])
    # for i in range(len(codeList)):
    #     codeName = codeList[i]
    #     setIndex = setIndex | getUnionIndex(path + codeName + u'-' + u'1998-03-31' + u'-' + en + u'.csv')
    #     if i == len(codeList) - 1:
    #         setIndex = setIndex.sort_values()
    #         a = pd.DataFrame(setIndex)
    #         a.to_csv(codeName + u'_' + u'2001-03-30' + u'_' + en + u'.csv')
    # targetIndex = setIndex.sort_values().tolist()
    #
    # 丢取部分代码
    tempPath = u'D:\Sanyuanjing\OftenUsedPythonCode\MomentumStrategy\FinanceBackTesting1\Data\基金\私募基金/'
    # fileList = [u'股票型基金', u'混合型基金', u'另类投资基金', u'债券型基金']
    fileList = [u'tempFund']
    targetEx = u'日频'
    for fileName in fileList:
        fullName = fileName + u'-' + targetEx
        # fullName = fileName
        print('Is producing:', fullName)
        # dropDf = pd.read_excel(tempPath + fullName + u'.xlsx', encoding='gbk', header=None)
        # tempSeries = dropDf[u'基金成立日']
        # tempSeries = dropDf.iloc[:, 1]
        # st, en = tempSeries.min(), datetime.date.today() - datetime.timedelta(days=1)
        # st, en = tempSeries[tempSeries != 0].min(), u'2018-07-16'
        st, en = u'2010-01-01', u'2018-08-15'
        # targetIndex = dropDf.iloc[:, 0].sort_values().tolist()
        targetIndex = ['XT125149.XT', 'ZG147323.OF', 'ZG144526.OF', 'XT1500307.XT', 'XT1500313.XT', 'XT1500310.XT', 'XT1500311.XT', 'XT1500308.XT', 'XT1500306.XT', 'XT1500312.XT', 'XT1500304.XT', 'XT1500314.XT', 'XT1500309.XT', 'XT1500305.XT', 'XT1504203.XT', 'XT1508381.XT', 'XT1616156.XT']     # 基金代码
        testF2([u'NAV_adj'], fullName, targetIndex, st, en, u'A', u'../Data/Main/')
        testF2([u'netasset_total'], fileName, targetIndex, st, en, u'A', u'../Data/Main/')


