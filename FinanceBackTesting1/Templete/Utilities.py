# encoding: UTF-8

"""
常用工具
潘卓夫 2017-08-15 11:46:44
"""
import sys
import os
from os import path
import decimal
import json
from importlib import import_module
import time
from datetime import datetime


if "日期时间":

    def todayDate():
        """获取当前本机电脑日期"""
        return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)    

    def timeString(microsecond = False, tm = None):
        """ 获取当前时间 不输入time参数,则为本机时间 """

        if not tm:
            tm = time.time()
        strDtm = time.strftime('%H:%M:%S', time.localtime(tm))
      
        if microsecond:
            return '%s.%03d' % (strDtm, (tm - int(tm)) * 1000 )
        else:
            return strDtm

    def dateString(tm = time.time()):
        """ 获取当前日期 不输入time参数,则为本机日期 """

        #tm = time.time()
        return time.strftime('%Y-%m-%d', time.localtime(tm))

    def dateTimeString(microsecond = False, offsetSecond = 0):
        """获取当前日期时间, microsecond:显示毫秒, offsetSecond:偏移秒数"""
        tm = time.time()
        strDtm = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(tm + offsetSecond))
      
        if microsecond:
            return '%s.%03d' % (strDtm, (tm - int(tm)) * 1000 )
        else:
            return strDtm

if "调试":

    def print_dict(d):
        """按照键值打印一个字典"""
        for key,value in d.items():
            print key + ':' + str(value)

    def print_func(func):
        """简单装饰器用于输出函数名
           调用方式:     
                @print_func
                def functionXXX()
        """
        def wrapper(*args, **kw):
            print ""
            print str(func.__name__)
            return func(*args, **kw)
        return wrapper

    def print_log(level, txt):
        """ 日志服务未初始化前使用 """
        print u'%s [%+5s] - %s' % (dateTimeString(True), level, unicode(txt))

    def Log(txt):
        """ 日志服务未初始化前使用 """
        print_log(u'INFO', txt)

    def Warn(txt):
        """ 日志服务未初始化前使用 """
        print_log(u'WARN', txt)

    def Error(txt):
        """ 日志服务未初始化前使用 """
        print_log(u'ERROR', txt)

    def Debug(txt):
        """ 日志服务未初始化前使用 """
        print_log(u'DEBUG', txt)

    __xxxpyxxxProgress = 0
    def ShowProgress(info, progress, total):
        """ 显示进度 """
        global __xxxpyxxxProgress  
        scale = int(progress / int(total / 10))
        if  progress == 0:
            __xxxpyxxxProgress = 0
            print u'%s [ INFO] - %s ==>' % (dateTimeString(True), info), 
        elif scale > __xxxpyxxxProgress:
            __xxxpyxxxProgress = scale
            if scale > 9:
                print u'■'
            else:
                print u'■',

if "对象序列化反序列化为字典":
    def obj2dict(obj):
        d = {}
        d['__class__'] = obj.__class__.__name__
        d['__module__'] = obj.__module__
        d.update(obj.__dict__)
        return d

    def dict2obj(d):
        if '__class__' in d:
            class_name = d.pop('__class__')
            module_name = d.pop('__module__')
            module =  import_module(module_name)
            class_ = getattr(module, class_name)
            instance = class_()
            for key, value in d.items():
                setattr(instance, key.encode('ascii'), value)
        else:
            instance = d
        return instance

if "命令行":
    def getCommandLine():
        """ 获取命令行 """
        cmdLine = []
        if len(sys.argv) > 1:
            for s in sys.argv[1:]:
                cmdLine.append(s.upper())
        
        return cmdLine

if "配置":
    def loadSetting(fileName):
        """ 加载单配置文件 """

        with open(fileName, 'r') as f:
            setting = json.load(f)

        return setting

    def saveSetting(fileName, cfg):
        """ 加载单配置文件 """
        with open(fileName, 'w') as f:
             jsonL = json.dumps(cfg, indent=4, default = obj2dict, ensure_ascii=False)
             f.write(jsonL)