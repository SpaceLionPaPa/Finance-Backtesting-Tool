# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

# indicesDict = {'成立以来': ['近一年', '000300']}
indicesDict= {u'\u4e0b\u884c\u6807\u51c6\u5dee': [u'\u8fd1\u4e09\u4e2a\u6708', u'000300'], u'\u6536\u76ca\u7387': [u'\u6210\u7acb\u4ee5\u6765', u'000300']}
formula = u'收益率/下行标准差'

cnIndiceDict = {u'Spearman\u79e9\u76f8\u5173': 'spearman', u'\u98ce\u9669\u6536\u76ca(99%)': 'var99', u'\u7d22\u63d0\u8bfa\u6bd4\u7387': 'sortino_ratio', u'\u590f\u666e\u6bd4\u7387': 'sharp_ratio', u'Calmar\u6bd4\u7387': 'carlmar', u'\u8a79\u68ee\u963f\u5c14\u6cd5': 'jensen_alpha', u'\u7279\u96f7\u8bfa\u6307\u6570': 'treynor', u'\u6700\u5927\u56de\u64a4\u7387': 'maxdrawdown', u'beta': 'beta', u'\u4fe1\u606f\u6bd4\u7387': 'ir', u'\u5e74\u5316\u6536\u76ca\u7387': 'annualreturn', u'\u6295\u8d44\u80dc\u7387': 'winrate', u'\u5e74\u5316\u6ce2\u52a8\u7387': 'annualvolatility', u'alpha': 'alpha', u'\u98ce\u9669\u6536\u76ca(95%)': 'var95', u'\u4e0b\u884c\u6807\u51c6\u5dee': 'downsidestd', u'\u6700\u5927\u56de\u64a4\u6301\u7eed\u671f': 'maxdrawdown_duration', u'\u6536\u76ca\u7387': 'alltimereturn', u'\u5e73\u5747\u635f\u76ca\u6bd4': 'lossprofitratio'}


codeDict = {u'2018-06-29~2018-08-31':
                [u'001230.OF', u'000711.OF', u'000884.OF', u'000960.OF', u'000751.OF', u'399011.OF', u'000831.OF', u'002300.OF', u'000913.OF', u'001766.OF', u'161035.OF', u'004075.OF', u'000780.OF'],
            u'2017-12-29~2018-06-29':
                [u'001230.OF', u'000711.OF', u'000884.OF', u'000960.OF', u'000751.OF', u'399011.OF', u'000831.OF', u'002300.OF', u'000913.OF', u'001766.OF', u'161035.OF', u'004075.OF', u'000780.OF'],
            u'2017-06-30~2017-12-29':
                [u'001230.OF', u'000711.OF', u'000884.OF', u'000960.OF', u'000751.OF', u'399011.OF', u'000831.OF', u'002300.OF', u'000913.OF', u'001766.OF', u'161035.OF', u'004075.OF', u'000780.OF'],
            u'2016-12-30~2017-06-30':
                [u'001542.OF', u'001047.OF', u'001878.OF', u'000991.OF', u'001605.OF', u'001208.OF', u'000854.OF', u'001703.OF', u'110022.OF', u'110003.OF', u'001186.OF', u'001048.OF'],
            u'2016-05-31~2016-12-30':
                [u'001542.OF', u'540006.OF', u'000991.OF', u'001178.OF', u'519714.OF', u'000457.OF', u'000974.OF', u'000866.OF', u'110022.OF', u'519606.OF', u'000628.OF', u'000418.OF']}

# codeDict = {u'2017-12-29~2018-08-31':
#                 [u'001230.OF', u'000711.OF', u'000884.OF', u'000960.OF', u'000751.OF', u'399011.OF', u'000831.OF', u'002300.OF', u'000913.OF', u'001766.OF', u'161035.OF', u'004075.OF', u'000780.OF'],
#             u'2016-12-30~2017-12-29':
#                 [u'001542.OF', u'001047.OF', u'001878.OF', u'000991.OF', u'001605.OF', u'001208.OF', u'000854.OF', u'001703.OF', u'110022.OF', u'110003.OF', u'001186.OF', u'001048.OF'],
#             u'2015-12-30~2016-12-30':
#                 [u'001542.OF', u'540006.OF', u'000991.OF', u'001178.OF', u'519714.OF', u'000457.OF', u'000974.OF', u'000866.OF', u'110022.OF', u'519606.OF', u'000628.OF', u'000418.OF']}

mktCN_Dict = {u'000300.SH': u'沪深300',
              u'NH0100.NHF': u'南华商品指数',
              u'H11008.CSI': u'中证企业债',
              u'CBA00601.CS': u'中债-国债总财富(总值)指数',
              u'CBA01201.CS': u'中债-金融债券总财富(总值)指数',
              u'M0039354': u'GDP: 当季同比',
              u'M0061673': u'预测GDP: 当季同比',
              u'M0000612': u'CPI: 当月同比',
              u'M0061676': u'预测CPI: 当月同比',
              u'DR001.IB': u'银行间质押式1日回购利率'}
