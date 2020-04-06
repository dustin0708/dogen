#-*-coding:utf-8-*-

import dogen
from lxml import etree
from dogen.stock.constant import *

def filter_from_black_list(cnpt):
    black =["转融券标的",\
            "融资融券",\
            "标普道琼斯A股",\
            "年报预增",\
            "富时罗素概念股",\
            "股权转让",\
            "深股通",\
            "参股券商",\
            "沪股通",\
            "机构重仓",\
            "MSCI概念",\
            "证金持股",\
            "创业板重组松绑"]

    for temp in range(0, len(black)):
        try:
            cnpt.remove(black[temp])
        except Exception:
            pass
    
    return cnpt

def parse_thsgn_file(filename):

    parser = etree.HTMLParser(encoding='utf-8')
    eledoc = etree.parse(filename, parser=parser)

    codelist = []
    trecod = eledoc.xpath('//tr')
    for i in range(1, len(trecod)):
        tds  = trecod[i].findall('td')

        code = tds[0].text.split('.')[0]
        name = tds[1].text
        cnpt = tds[4].text.strip().split(';')
        indt = tds[7].text.split('-')[0]

        code = {CODE: code, NAME: name, CONCEPT: filter_from_black_list(cnpt), INDUSTRY: indt}
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
