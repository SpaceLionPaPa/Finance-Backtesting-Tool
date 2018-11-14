# -*- coding: utf-8 -*-
import sys
if sys.getdefaultencoding() != 'utf-8':# 重新设置python 编码为utf-8
    reload(sys)
    sys.setdefaultencoding('utf-8')
import pandas as pd
import os

# 整合数据
# from WindPy import w
# startDate, endDate = '2016-05-13', '2018-05-11'
# w.start()
# tradedayList = w.tdays(startDate, endDate, "Period=W").Data[0]
# w.stop()
# strDayList = [str(i.date()) for i in tradedayList]
# fileName = u'NAV_adj_temp-2015-01-05-2015-12-31'
# tempDf = pd.read_csv(u'../Data/Main/' + fileName + u'.csv').transpose()
# tempDf = pd.DataFrame(tempDf.iloc[1:].values, index=tempDf.index[1:], columns=tempDf.iloc[0])
# # newDf = pd.DataFrame()
# newDf = tempDf
# for i in tempDf.index:
#     if i in strDayList:
#         newDf = newDf.append(tempDf.loc[i])
# newDf.to_csv(u'../Data/Main/' + fileName + u'.csv')
# tempDf2 = newDf.transpose()
# tempDf2.to_csv(u'../Data/Main/' + fileName + u'_tranposed.csv')
stop  =1
