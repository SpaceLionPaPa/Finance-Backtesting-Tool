#!/usr/bin/python
# -*- coding: utf-8 -*-

# 更新基金数据（支持复权数据的更新）

import pandas as pd
import datetime
from TempleteTools import read_csvEx, getTradeDayList, getPeriodDate, toPeriodList, getSetToCsv, getDataToCsv, getUnionIndex

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
        downloadFreq = 'W'
        downloadUniPeriodList, downloadPeriodList = toPeriodList(downloadFreq, inputTradedayList)  # 转化周期频率
        getDataToCsv(indexNameList, name, stockCodelist, downloadUniPeriodList, downloadPeriodList, inPutStrdayList,
                     path, dataType=0)
        pass

# 原始数据
path = u'../Data/Main/'
previousData = read_csvEx(path + u'NAV_adj_WIND开放式基金分类_股票型基金-2016-05-13-2018-05-15' + u'.csv', 0)
previousSt, previousDataEn = previousData.index[0], previousData.index[-1]
previousColumns = previousData.columns
stop = 1

codeList = ['2001010100000000']
st, en = str(previousDataEn.date()), str(datetime.date.today() - datetime.timedelta(days=1))
tradedayList, strdayList = getTradeDayList(st, en)  # 获取交易日

# 下载新数据
# 新基金代码
uniPeriodList, periodList = toPeriodList('W', tradedayList)  # 转化周期频率
dateList = getPeriodDate(uniPeriodList, periodList, strdayList, type=2)
getSetToCsv(codeList, dateList, path)
# 合并代码
setIndex = pd.Index([])
for i in codeList:
    setIndex = setIndex | getUnionIndex(path + i + u'-' + st + u'-' + en + u'.csv')
    df = pd.DataFrame(setIndex)
    df.to_csv(i + '.csv')
targetIndex = setIndex.sort_values().tolist()

# # 丢取部分代码
# dropDf = pd.read_excel(u'../Data/Main/基金日期-期货型.xlsx', index_col=0, encoding='gbk')
# series1 = dropDf.iloc[:, 1]
# series2 = dropDf.iloc[:, 2]
# targetIndex = dropDf.index.difference(series1[series1 == 0].index | series2[series2 == 0].index) & \
#               (series2[series2 == u'周'].index | series2[series2 == u'日'].index)
targetIndex = targetIndex.sort_values().tolist()

# targetIndex = ['XT1526141.XT', 'XT1704763.XT']
# downloadDate = '2016-05-13', '2018-05-14'
downloadDate = '2016-05-13', '2018-05-14'
# downloadDate = '2016-05-13', '2018-05-11'
testF2(['NAV_adj'], '私募期货', targetIndex, downloadDate[0], downloadDate[-1], 'W', '../Data/Main/')