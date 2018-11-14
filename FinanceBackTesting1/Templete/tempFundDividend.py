#!/usr/bin/python
# -*- coding: utf-8 -*-

# 更新基金数据（支持复权数据的更新）

import pandas as pd
import numpy as np
import copy
from TempleteTools import read_csvEx

# 原始数据
path = u'../Data/Main/'
# indexList = forwardData.columns.tolist()
# tradedayList = forwardData.index.astype(str).tolist()
# downloadFreq = 'A'
# downloadUniPeriodList, downloadPeriodList = toPeriodList(downloadFreq, tradedayList)  # 转化周期频率
# unForwardData1 = getDataToCsv([u"nav"], u'公募股票_不复权净值', indexList, downloadUniPeriodList, downloadPeriodList, tradedayList, path, dataType=0)
# unForwardData2 = getDataToCsv([u"div_accumulatedperunit"], u'公募股票_单位累计分红', indexList, downloadUniPeriodList, downloadPeriodList, tradedayList, path, dataType=0)


# 手动净值复权
forwardData = read_csvEx(path + u'NAV_adj_WIND开放式基金分类_股票型基金-2016-05-13-2018-05-15' + u'.csv', 0)
div_accumulatedperunit = read_csvEx(path + u'div_accumulatedperunit_公募股票_单位累计分红-2016-05-13-2018-05-15' + u'.csv', 0)
div_accumulatedperunit = div_accumulatedperunit.fillna(0)
nav = read_csvEx(path + u'nav_公募股票_不复权净值-2016-05-13-2018-05-15' + u'.csv', 0)
# newForwardNav = pd.DataFrame(index=forwardData.index, columns=forwardData.columns)
# length = len(forwardData.index)
# for i in nav.columns:
#     fund_nav = nav[i]
#     fund_div = div_accumulatedperunit[i]
#     fund_fnav = forwardData[i]
#     eachDiv = fund_div - fund_div.shift(1)
#     eacRet = fund_nav/(fund_nav.shift(1) - eachDiv)
#     eachProcessF = fund_fnav.shift(1) * eacRet
#     unForwardIndex = eacRet[np.isnan(eacRet)].index
#     eachProcessF[unForwardIndex] = fund_nav[unForwardIndex]
#     newForwardNav[i] = pd.Series(eachProcessF)
#
# newForwardNav.iloc[0] = np.nan
# newForwardNav.to_csv(path + u'生成的复权数据.csv')
# testDiff = newForwardNav - forwardData
# pass

def fundDivUpdater(startForwardNav, navDf, div_accumulatedperunitDf):
    '''
    对基金净值数据进行复权处理
    :param startForwardNav: 起始时刻的复权单位净值
    :param navDf: 不复权单位净值
    :param div_accumulatedperunitDf: 单位累计分红--与navDf结构一致
    :return: 复权处理后的净值pandas.Dataframe
    '''
    dfIndex = copy.deepcopy(navDf.index)
    finalDf = pd.DataFrame(index=dfIndex[1:], columns=navDf.columns)
    eachDivDf = div_accumulatedperunitDf - div_accumulatedperunitDf.shift(1)
    retDf = navDf/(navDf.shift(1) - eachDivDf)

    for i in range(1, len(dfIndex)):
        ret = retDf.iloc[i]
        if i == 1:
            finalDf.iloc[i - 1] = startForwardNav * ret
        else:
            finalDf.iloc[i - 1] = finalDf.iloc[i - 2] * ret
    finalDf.to_csv(path + u'复权数据.csv')
    return finalDf

startForwardNav = forwardData.iloc[0]
navDf = nav
a = fundDivUpdater(startForwardNav, navDf, div_accumulatedperunit)
pass
