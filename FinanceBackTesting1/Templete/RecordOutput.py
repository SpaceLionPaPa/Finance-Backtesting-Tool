# -*- coding:utf-8 -*-
# Program: 这是一个实现交易记录的脚本程序
# Writer: Qijie Li
# Date: Oct.23.2017import csv
import csv
import copy
import codecs
from datetime import date
import pandas as pd
from Templete.TempleteTools import read_csvEx

def transitionWriter(timeFreq, dateFreq, strategyName, orderDict):
    '''
    :param fullTimeIndex: 测试时间 --[time]
    :param strategyName: 策略名称--str
    :param orderDict:交易记录--{}
    :return:交易明细csv文件，持仓明细csv文件
    待添加功能--持仓成本价格，总成本
    '''
    # 参数设置
    orderRecord = orderDict[u'orderRecord']
    PLRecord = orderDict[u'PLRecord']
    positionRecord = orderDict[u'positionRecord']
    todayDate = date.today().isoformat()

    colName1 = ['Date', '动作', '方向', '种类', '代码', '名称', '乘数', '保证金比例%/期权维持保证金', '成交价',
                '成交量', '费率', '成交金额(不包含成本)', '费用', '成交金额(包含成本)', '备注1', '备注2']
    colName2 = ['Date', '当日盈亏(未扣手续费)', '手续费', '当日盈亏(扣除手续费)', '股票盈亏',
                '基金盈亏', '债券盈亏', '金融衍生品盈亏', '其他资产盈亏', '累计盈亏',
                '股票累计盈亏', '基金累计盈亏', '债券累计盈亏', '金融衍生品累计盈亏',
                '其他资产累计盈亏']
    colName3 = ['Date', '方向', '种类', '代码', '名称', '成本价', '数量']
    csvFile1 = file(u'../Output/' + strategyName + u'-Trading' + todayDate + u'.csv', u'wb')
    csvFile1.write(codecs.BOM_UTF8)
    csvWriter1 = csv.writer(csvFile1)
    csvWriter1.writerow(colName1)
    csvFile2 = file(u'../Output/' + strategyName + u'-ProfitAndLoss' + todayDate + u'.csv', u'wb')
    csvFile2.write(codecs.BOM_UTF8)
    csvWriter2 = csv.writer(csvFile2)
    csvWriter2.writerow(colName2)
    csvFile3 = file(u'../Output/' + strategyName + u'-Position' + todayDate + u'.csv', u'wb')
    csvFile3.write(codecs.BOM_UTF8)
    csvWriter3 = csv.writer(csvFile3)
    csvWriter3.writerow(colName3)
    costSum = 0
    actionCN = {1: '开仓', -1: '平仓'}
    directionCN = {1: '买入', -1: '卖出'}
    for i in range(len(timeFreq)):
        # 交易明细
        currentTime = timeFreq[i]
        if currentTime in orderRecord:
            for j in orderRecord[currentTime]:
                # 单个股票成交记录
                orderNum     = j
                orderDcit    = orderRecord[currentTime][orderNum]
                objectName   = ''
                objectCode   = orderDcit[u'objectCode']
                action       = actionCN[orderDcit[u'action']]
                direction    = directionCN[orderDcit[u'direction']]
                objectType   = orderDcit[u'objectType']
                multiplier   = orderDcit[u'multiplier']
                marginRatio  = orderDcit[u'marginRatio']
                dealPrice    = orderDcit[u'price']
                dealVolume   = orderDcit[u'volume']
                feeRatio     = orderDcit[u'feeRatio']
                amountBfCost = orderDcit[u'amount before cost']
                feeCost      = orderDcit[u'feeCost']
                amountAfCost = orderDcit[u'amount after cost']
                if u'note1' in orderDcit.keys():
                    note1    = orderDcit[u'note1'].encode("utf-8")
                else:
                    note1    = None
                if u'note2' in orderDcit.keys():
                    note2 = orderDcit[u'note2'].encode("utf-8")
                else:
                    note2    = None
                row1         = [currentTime, action, direction, objectType,  objectCode,
                                objectName, multiplier, marginRatio, dealPrice, dealVolume,
                                feeRatio, amountBfCost, feeCost, amountAfCost, note1, note2]
                csvWriter1.writerow(row1)
                costSum += feeCost

        if currentTime in dateFreq:
            # 组合盈亏
            # todo 细分项目的盈亏后期添加
            cost    = abs(copy.deepcopy(costSum))       # 手续费
            netPL   = PLRecord[currentTime]             # 当期盈亏（净额）
            PL      = netPL + cost                      # 当期盈亏 (当期交易项目加总）
            stockPL = netPL                             # 股票盈亏
            fundPL  = 0                                 # 基金盈亏
            bondPL  = 0                                 # 债券盈亏
            deriPL  = 0                                 # 金融衍生品盈亏
            otherPL = 0                                 # 其他资产盈亏
            if currentTime == dateFreq[0]:
                sumNetPL    = netPL     # 累计盈亏
                sumStockPL  = stockPL   # 股票累计盈亏
                sumFundPL   = fundPL    # 基金累计盈亏
                sumBondPL   = bondPL    # 债券累计盈亏
                sumDeriPL   = deriPL    # 金融衍生品累计盈亏
                sumOtherPL  = otherPL   # 其他资产累计盈亏
            else:
                sumNetPL    += netPL
                sumStockPL  += stockPL
                sumFundPL   += fundPL
                sumBondPL   += bondPL
                sumDeriPL   += deriPL
                sumOtherPL  += otherPL
            row2 = [currentTime, PL, cost, netPL, stockPL, fundPL, bondPL, deriPL, otherPL,
                    sumNetPL, sumStockPL, sumFundPL, sumBondPL, sumDeriPL, sumOtherPL]
            costSum = 0.
            csvWriter2.writerow(row2)

            # 持仓记录
            position = positionRecord[currentTime]
            row3 = [currentTime]
            for direction in position:
                for objectType in position[direction]:
                    positionDict = position[direction][objectType]
                    if positionDict:
                        for code in positionDict:
                            objectCode = code
                            volume = positionDict[code][u'volume']
                            costPrice = positionDict[code][u'costPrice']
                            name = None
                            row3 = [currentTime, direction, objectType, objectCode, name, costPrice, volume]
                            csvWriter3.writerow(row3)

    csvFile1.close()
    csvFile2.close()
    csvFile3.close()

def evaluation_F():
    '''
    依据交易记录生成评价指标
    :return: 资金使用效率大致估值
    '''
    # todayDate = date.today().isoformat()
    # # 胜率
    # trading = csv.reader(open(u'../Output/' + u'Towl' + u'-Position' + todayDate + u'.csv', u'rb'))
    # winRate
    # stop = 1

    # todayDate = date.today().isoformat()
    # position = csv.reader(open(u'../Output/' + strategyName + u'-Position' + todayDate + u'.csv', u'rb'))
    # close = read_csvEx(u'../Data/Main/' + u'close.csv', indexCol=0)
    # positionList = []
    # n = 0
    # for line in position:
    #     if n:
    #         tempDate = line[0]
    #         code = line[1]
    #         if code == 'no position':
    #             continue
    #         volume = float(line[-1])
    #         positionList.append((tempDate, code, volume))
    #     n += 1
    # positionDF = pd.DataFrame(positionList)
    # positionDF = pd.DataFrame(positionDF.iloc[:, 1:].values, index=positionDF.iloc[:, 0])
    # csvFile1 = file(u'../Output/' + strategyName + u'-Evaluation' + todayDate + u'.csv', u'wb')
    # csvFile1.write(codecs.BOM_UTF8)
    # csvWriter1 = csv.writer(csvFile1)
    # csvWriter1.writerow(['Date', '资金使用率(市值估计)'])
    # for i in fullTimeIndex:
    #     tempDate = i
    #     if tempDate not in positionDF.index:
    #         row = [tempDate, 'no position']
    #     else:
    #         tempDF = positionDF.loc[tempDate]
    #         if len(tempDF.shape) >= 2:
    #             code = tempDF.iloc[:, 0]
    #             price = close.loc[tempDate][code]
    #             volume = tempDF.iloc[:, 1]
    #             volume.index = code
    #             value = price * volume
    #             value = value.sum()
    #             pass
    #         else:
    #             code = tempDF[0]
    #             volume = tempDF[1]
    #             price = close.loc[tempDate][code]
    #             value = price * volume
    #         row = [tempDate, float(value) / initial_Capital]
    #     csvWriter1.writerow(row)
    # csvFile1.close()

if __name__ == '__main__':
    t1= evaluation_F()


