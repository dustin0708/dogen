#-*-coding:utf-8-*-

###############################################################################
##                          支持的事件定义
###############################################################################
EVT_UPDATE_KDATA            = 'evt_update_kdata'            ## 更新股票数据
EVT_POLICY_HL_FALLRAPID     = 'evt_policy_HL_FallRapid'     ## 涨停快速回调
EVT_POLICY_HL_RISETREND     = 'evt_policy_HL_RiseTrend'     ## 涨停上涨趋势
EVT_POLICY_NEWSTOCK         = 'evt_policy_NewStock'         ## 开板新股
EVT_POLICY_IMMOTALGUIDE     = 'evt_policy_ImmotalGuide'     ## 仙人指路
EVT_POLICY_MANDARIN         = 'evt_policy_Mandarin'         ## 鸳鸯戏水
EVT_POLICY_REBOUND          = 'evt_policy_Rebound'          ## 绝低反弹
EVT_POLICY_SERIALRISE       = 'evt_policy_SerialRise'       ## 串阳上涨


###############################################################################
##                          策略参数定义
###############################################################################
PARAM_FROM_TRADE        = "from_trade"                  ## 起始日期
PARAM_LAST_TRADE        = "last_trade"                  ## 截止日期
PARAM_MAXI_TRADE        = "maxi_trade"                  ## 最多交易日数
PARAM_MINI_TRADE        = "mini_trade"                  ## 最少交易日数

PARAM_MINI_HEAP_FALLEN  = "mini_heap_fallen"            ## 最小累积跌幅
PARAM_MINI_HEAP_RISEUP  = "mini_heap_riseup"            ## 最小累积涨幅
PARAM_MAXI_HEAP_FALLEN  = "maxi_heap_fallen"            ## 最大累积跌幅
PARAM_MAXI_HEAP_RISEUP  = "maxi_heap_riseup"            ## 最大累积涨幅

PARAM_MAXI_PRICE        = "maxi_price"                  ## 最大收盘价
PARAM_MINI_PRICE        = "mini_price"                  ## 最小收盘价

def GetParamValue(dparam, name, default=None):
    try:
        value = dparam[name]
    except Exception:
        value = default
    
    return value


if __name__ == "__main__":
    print("Welcome to " +  sys.argv[0] + " package.")