#-*-coding:utf-8-*-

from lxml import etree
from dogen.stock.constant import *


def parse_thsgn_file(filename='thsgn.html'):

    parser = etree.HTMLParser(encoding='utf-8')
    eledoc = etree.parse(filename, parser=parser)

    codelist = []
    trecod = eledoc.xpath('//tr')
    for i in range(1, len(trecod)):
        tds  = trecod[i].findall('td')

        code = tds[0].text.split('.')[0]
        name = tds[1].text
        cnpt = tds[4].text.split(';')
        indt = tds[7].text.split('-')[0]

        code = {CODE: code, NAME: name, CONCEPT: cnpt, INDUSTRY: indt}
        codelist.append(code)

    return codelist