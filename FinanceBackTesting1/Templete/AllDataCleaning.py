# encoding: UTF-8
from Templete.Utilities import *
from Templete import TempleteTools
import os
import copy
import pandas as pd

def allCleaning(targetPath, fileList = []):
    ''' 清洗targetPath路径下所有dataframe数据 '''
    #  从CSV文件加载指标数据
    pathList = list(os.walk(targetPath))
    csvFile = [i for i in pathList[0][2] if os.path.splitext(i)[1] == '.csv']
    if fileList:
        filename = copy.deepcopy(fileList)
    else:
        filename = [os.path.splitext(each)[0] for each in csvFile]
    cleanedFileName = copy.deepcopy(filename)
    dataSet = {}
    targetDf = pd.read_csv(targetPath + u'volume.csv', parse_dates=True,
        index_col=0, encoding=u'gbk')
    dataSet[u'volume'] = targetDf
    timeData = targetDf.index
    timeData = timeData.astype(str).tolist()
    try:
        for i in range(len(filename)):
            indexName = copy.deepcopy(filename[i])
            if indexName in [u'ipo_date', u'tradedayList']:
                cleanedFileName.remove(indexName)
            elif indexName[-7:] == 'Cleaned':
                cleanedFileName.remove(indexName)
            else:
                dataSet[indexName] = TempleteTools.read_csvEx(
                    fileName=targetPath + indexName + u'.csv',
                    indexCol=0, startTime=timeData[0], endTime=timeData[-1])
                Log(u'已加载 [%s] 指标文件. 长度: %d' %
                    (indexName, len(dataSet[indexName])))
    except Exception, e:
        Log(u'%s从CSV文件加载指标数据, 产生异常: ' % indexName)
        Log(str(e))

    for i in range(len(cleanedFileName)):
        indexName = copy.deepcopy(cleanedFileName[i])
        print(u'%s 清洗开始' % indexName)
        if len(dataSet[indexName].columns) == len(dataSet[u'volume'].columns):
            cleanedDf = TempleteTools.cleaningDfEx(dataSet[indexName], dataSet[u'volume'])
            cleanedDf.to_csv(targetPath + indexName + u'Cleaned.csv')

def allDFCleaning(dataSet):
    ''' 清洗dataSet里的Dataframe'''
    # TODO dataSet必须包含volume
    indexList = dataSet.keys()
    cleanDataSet = {}
    for i in range(len(indexList)):
        indexName = indexList[i]
        print(u'%s 清洗开始' % indexName)
        if len(dataSet[indexName].columns) == len(dataSet[u'volume'].columns):
            cleanDataSet[indexName] = TempleteTools.cleaningDfEx(dataSet[indexName], dataSet[u'volume'])
    return cleanDataSet

if __name__== "__main__":
    allCleaning(u'../Data/Main/', fileList=[])

