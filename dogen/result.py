#-*-coding:utf-8-*-

import pandas

RST_COL_INDEX = 'index'
RST_COL_CODE = 'code'
RST_COL_NAME = 'name'
RST_COL_SCORE = 'score'
RST_COL_INDUSTRY = 'industry'
RST_COL_TAKE_TRADE = 'take-trade'
RST_COL_LAST_CLOSE = 'last-close'
RST_COL_OUTSTANDING = 'outstanding'
RST_COL_START = 'start'
RST_COL_END = 'end'
RST_COL_RISE_RATE = 'rise-rate'
RST_COL_INC_HL = 'inc_hl'
RST_COL_START_CLOSE = 'start-close'
RST_COL_MATCH_TIME = 'match-time'


def result_dict2dataframe(result):
    data = None
    if len(result) > 0:
        data = pandas.DataFrame.from_dict(result, orient='columns')
        data.sort_values(by=RST_COL_SCORE, ascending=False, inplace=True)
    return data
