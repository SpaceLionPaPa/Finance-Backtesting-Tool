#!/usr/bin/python
# -*- coding: utf-8 -*-

# 网站_基金组合回测功能
# writer:Qijie LI
# date:2018-07-19

import pandas as pd
import copy
from Templete.TempleteTools import toPeriodDF, toPeriodList, read_csvEx

def getCustomIndex(formula, indicesDict):
    ''' 依据formula 生成指标 '''
    tempKey = indicesDict.keys()
    # 生成局部变量
    for name in tempKey:
        locals()[name] = indicesDict[name]
    customIndex = pd.eval(formula)
    return customIndex

def backTesting(netvalueDf, customIndexDf, freq, num=5, locType = 1):
    ''' 基金回测系统 '''

    # period化
    timeList = copy.deepcopy(netvalueDf.index)
    uniquePeriod, periodList = toPeriodList(freq, timeList)
    periodIndex = copy.deepcopy(periodList.index)
    goupIndex = {str(q): {} for q in range(num)}
    goupRet = {str(q): {} for q in range(num)}

    # 回测
    for i in range(len(uniquePeriod)):

        # 返回周期范围
        eachPeriod = uniquePeriod[i]
        print()
        print('period is: %s' % eachPeriod)
        periodSlice = periodIndex.get_loc(eachPeriod)
        if isinstance(periodSlice, slice):
            stLoc, enLoc = periodSlice.start, periodSlice.stop -1
        else:
            stLoc = enLoc = periodSlice
        # 返回周期数据
        if locType == 0:
            netvalue = netvalueDf.iloc[stLoc]
            customIndex = customIndexDf.iloc[stLoc]
        elif locType == 1:
            netvalue = netvalueDf.iloc[enLoc]
            customIndex = customIndexDf.iloc[enLoc]

        if not i:
            retSeries = pd.Series(index=netvalue.index).fillna(0)
        else:
            retSeries = netvalue/lastNetvalue - 1

        retSeries = retSeries.dropna()
        goupSize = int(round(float(retSeries.index.size) / num))

        rankIndex = customIndex[retSeries.index].sort_values(ascending=False).index   # 由大到小

        for j in range(num):
            print('gourp num is: %s' % j)
            if j == (num - 1):
                portIndex = rankIndex[j * (goupSize):]
            else:
                portIndex = rankIndex[j * (goupSize): (j + 1) * goupSize]

            goupRet[str(j)][eachPeriod] = float(retSeries[portIndex].sum())/len(portIndex)
            goupIndex[str(j)][eachPeriod] = portIndex
        lastNetvalue = copy.deepcopy(netvalue)
    pass #
    goupRetDf = pd.DataFrame(goupRet)
    goupNetValueDf = (1 + goupRetDf).cumprod()
    return goupNetValueDf, goupIndex

if __name__ == u"__main__":
    import numpy as np
    import time
    st, en = '2016-01-01', '2018-06-01'
    st, en = pd.to_datetime(st), pd.to_datetime(en)
    path = u'D:\Sanyuanjing\OftenUsedPythonCode\MomentumStrategy\FinanceBackTesting1\Data\Main/'
    df = read_csvEx(path + 'stockFund.csv', 0)  # prase_date =True
    df = df.sort_index(axis=0)
    df = df.sort_index(axis=1)
    tempTimeIndex = copy.deepcopy(df.index)
    timeIndex = tempTimeIndex[pd.eval('st <= tempTimeIndex <= en')]
    df = df.loc[timeIndex]          # 数据的列也要对齐

    # 指标数据
    tempDf1 = pd.DataFrame(np.random.random(size=(df.index.size, df.columns.size)), index=df.index, columns=df.columns)
    tempDf2 = pd.DataFrame(np.random.random(size=(df.index.size, df.columns.size)), index=df.index, columns=df.columns).fillna(2)
    tempDf3 = pd.DataFrame(np.random.random(size=(df.index.size, df.columns.size)), index=df.index, columns=df.columns).fillna(3)
    indicesDict = {'index1': tempDf1, 'index2': tempDf2, 'index3': tempDf3}
    stList, enList = [], []
    tempList = indicesDict.values() + [df]
    for eachDf in tempList:
        stList.append(eachDf.index.min())
        enList.append(eachDf.index.max())

    # 时间对齐
    st, en = max(stList), min(enList)
    timeIndex = tempTimeIndex[pd.eval('st <= tempTimeIndex <= en')]
    df = df.loc[timeIndex]
    a = indicesDict.keys()
    for eachKey in indicesDict.keys():
        indicesDict[eachKey] = indicesDict[eachKey].loc[timeIndex]

    # 按公式生成数据
    formula = '(0.3* index1 + 0.5 * index2 - 0.2 * index3)/3'
    customIndexDf = getCustomIndex(formula, indicesDict)

    # 回测
    time1 = time.clock()
    portNetvalue, protIndex = backTesting(df, customIndexDf, 'M', 5, 1)
    time2 = time.clock()
    print('从%s至%s, 回测时间耗时%s秒' % (st, en, (time2 -time1)))
    pass