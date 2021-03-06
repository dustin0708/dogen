#-*-coding:utf-8-*-

import dogen
from lxml import etree
from dogen.stock.constant import *

def filter_thsgn_black_list(cnpt, blacklist):

    for temp in range(0, len(blacklist)):
        try:
            cnpt.remove(blacklist[temp])
        except Exception:
            pass
    
    return cnpt

def parse_thsgn_file(filename, blackfile):

    ### 解析黑名单
    blacklist = []
    parser = etree.HTMLParser(encoding='utf-8')
    eledoc = etree.parse(blackfile, parser=parser)
    trecod = eledoc.xpath('//tr')
    for i in range(1, len(trecod)):
        tds  = trecod[i].findall('td')
        blacklist.extend(tds[0].text.strip().split(';'))

    ### 解析概念
    codelist = []
    parser = etree.HTMLParser(encoding='utf-8')
    eledoc = etree.parse(filename, parser=parser)
    trecod = eledoc.xpath('//tr')
    for i in range(1, len(trecod)):
        tds  = trecod[i].findall('td')

        code = tds[0].text.split('.')[0]
        name = tds[1].text
        cnpt = tds[4].text.strip().split(';')
        indt = tds[7].text.split('-')[0]

        code = {CODE: code, NAME: name, CONCEPT: filter_thsgn_black_list(cnpt, blacklist), INDUSTRY: indt}
        codelist.append(code)

    return codelist


def lookup_cnpt(database, code):
    return database.lookup_stock_concept(cond={CODE:code})[0]

def lookup_industry(database, code):
    try:
        indt = database.lookup_stock_concept(cond={CODE:code})
        if len(indt)>0:
            indt = indt[0][INDUSTRY]
    except Exception:
        indt = None
    return indt

def lookup_concept(database, code):
    try:
        indt = database.lookup_stock_concept(cond={CODE:code})
        if len(indt)>0:
            indt = indt[0][CONCEPT]
    except Exception:
        indt = None
    return indt
