# encoding: UTF-8

# Program: This is a tool script for options strategy.
# Writer: Qijie Li
# Date: Feb.05.2018

import scipy.stats as st
norminv = st.distributions.norm.ppf
norm = st.distributions.norm.cdf
from numpy import sqrt, exp, log
import math
pai = math.pi

def timeValue(price, ua_Price, exe_Price, callput = 1):
    '''
    :param price:       期权价格
    :param ua_Price:    标的资产价格
    :param exe_Price:   行权价
    :param callput:     {1: call, -1: put}
    :return:            期权时间价值
    '''
    k = exe_Price
    s = ua_Price
    p = price
    if callput == 1:
        iv = max(s - k, 0)                                                  # iv: Intrinsic Value内在价值
    else:
        iv = max(k - s, 0)
    tv = p - iv
    return tv

def bsformula(callput, S0, K, r, t, sigma, q = 0.):
    '''
    :param callput:     {1: call, -1: put}
    :param S0:          underlying price
    :param K:           strike price
    :param r:           continuously compounded risk-free interest rate
    :param T:           time to expiration (% of year)
    :param sigma:       volatility (% p.a.)
    :param q:           continuously compounded dividend yield (% p.a.)
    :return:            greek values
    '''
    d1 = 1. / (sigma * sqrt(t)) * (log(S0 / K) + (r - q + 0.5 * (sigma ** 2)) * t)
    d2 = d1 - sigma * sqrt(t)
    stdNormal_f = 1./sqrt(2 * pai) * exp(-d1 ** 2/2)                                    # the standard normal probability density function

    # calculate options' values and greeks values for options
    f1 = S0 * sigma * exp(-q * t) / (2. * sqrt(t))
    f2 = r * K * exp(-r * t)
    f3 = q * S0 * exp(-q * t)
    if callput == 1:
        optionValues = S0 * exp(-q * t) * norm(d1)-K*exp(-r * t) * norm(d2)             # specification of call option
        delta = exp(-q * t) * norm(d1)
        theta = (-f1 * stdNormal_f - f2 * norm(d2) + f3 * norm(d1))                     # 年度theta
        rho = K * t * exp(-r * t) * norm(d2)
    elif callput == -1:
        optionValues = K * exp(-r * t) * norm(-d2)-S0 * exp(-q * t) * norm(-d1)
        delta = exp(-q * t)*(norm(d1)-1)
        theta = (-f1 * stdNormal_f + f2 * norm(-d2) - f3 * norm(-d1))
        rho = -K * t * exp(-r * t) * norm(-d2)
    vega = S0 * exp(-q * t) * sqrt(t) * stdNormal_f
    gamma = exp(-q * t) / (S0 * sigma * sqrt(t)) * stdNormal_f
    return (optionValues, delta, gamma, theta, vega, rho)

def callPutIndexEx(callput, exe_Mode):
    '''
    :param callput: {1: call, -1: put}
    :return: 全市场call或put的代码index
    '''
    if callput == 1:
        targetIndex = exe_Mode[exe_Mode == u'认购'].index
    elif callput == -1:
        targetIndex = exe_Mode[exe_Mode == u'认沽'].index
    return targetIndex


if __name__ == "__main__":
    callput, S0, K, r, T, sigma, q = (1, 50.0, 50.0, 0.1, 2.0, 0.05, 0.0)
    optionsValue, delta, gamma, theta, vega, rho = bsformula(callput, S0, K, r, T, sigma, q)
    stop = 1
