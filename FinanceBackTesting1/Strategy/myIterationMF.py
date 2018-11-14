#!/usr/bin/python
# -*- coding: utf-8 -*-

# 神奇公式组合因子测试
from magicalFormula4 import MyStrategy
import itertools
import time
from Templete.Constant import *


list1 = ['grossprofitmargin', 'pcf_ocf_ttm', 'roa', 'peg']
for i in itertools.permutations(list1, len(list1)):
    rankList = [i for i in i]
    print(rankList)
    time1 = time.clock()
    benchmarkName = u'hs300_stocks'
    timeSeriesFile = u'volume_cleaned_stocks.csv'
    fund = int(10000000)
    feeRatio = [0.002, 0.002]
    impactCostRatio = [0., 0.]
    timeFreq = u'D'
    indexNameList = [u'volume_stocks', u'open_stocks', u'high_stocks',
                     u'low_stocks', u'close_stocks', u'roa_stocks', u'estpeg_FTM_stocks',
                     u'estpeg_FY1_stocks', u'estpeg_FY2_stocks', u'roe_ttm2_stocks', u'grossprofitmargin_stocks',
                     u'yiziban_stocks', u'mvStd_stocks', u'mvMean_stocks', u'is_new_stock_504_stocks',
                     u"pcf_ocf_ttm_stocks", u"pcf_ncf_ttm_stocks", u"pcf_ocflyr_stocks", u"pcf_nflyr_stocks"]
    stockCodelist = []
    tempStr = rankList[0][:3]
    for i in rankList[1:]:
        tempStr = tempStr + u'_' + i[:3]
    strategyNamelist = [u'magicalFormula4' + u'_' + tempStr]
    tester = u'李琦杰'
    testContent = u'财务数据均值'
    dataSourceType = DATA_SOURCE_CSV

    primaryDate, startTime, endTime = u'2013-09-30', u'2014-04-30', u'2017-07-31'
    t1 = MyStrategy(tester, strategyNamelist, testContent, feeRatio, timeSeriesFile,
                    fund, startTime, dataSourceType, timeFreq, impactCostRatio, rankList)
    t1.loadData(primaryDate, endTime, stockCodelist, indexNameList, benchmarkName)
    t1.TrimData()
    t1.Trading()
    t1.outPut()
    print('回测完成')
    timeCost = (time.clock() - time1) / 60.
    print("回测共耗时 %.1f 分钟" % timeCost)
pass