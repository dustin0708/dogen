#-*-coding:utf-8-*-

####################### 以下为celery任务配置参数 #######################
from celery.task.schedules import crontab

### 中间件, Rabbitmq
broker_url = "pyamqp://127.0.0.1:5672"

### 存放执行结果的数据库地址
result_backend = "mongodb://127.0.0.1:27017"

### 结果序列化方式
result_serializer = 'json'

### 时区
timezone = 'Asia/Shanghai'

imports = (
    'celery_dogen.tasks_of_kdata',     # 股票数据更新
    'celery_dogen.tasks_of_policy'  # 股票匹配策略
)


beat_schedule = {
    ### 交易数据更新
    'exec-kdata-daily-pull': {
        'task': 'celery_dogen.tasks_of_kdata.dispatcher_of_daily_pull_update_kdata',
        'schedule': crontab(day_of_week='2-6', hour='0', minute='01')
    },

    ### 更新热点
    'exec-update-hot-concept':{
        'task': 'celery_dogen.tasks_of_kdata.daily_update_hot_concept',
        'schedule': crontab(day_of_week='2-6', hour='0', minute='20'),
        'args': (None, 1, True)
    },

    ### 涨停回调策略
    'exec-policy-hl-fallheap': {
        'task': 'celery_dogen.tasks_of_policy.dispatcher_of_hl_fallheap_match',
        'schedule': crontab(day_of_week='2-6', hour='1', minute='0'),
        'args': (None, None, None, True, 1000)
    },

    ### 涨停上涨策略
    'exec-policy-hl-risekeep': {
        'task': 'celery_dogen.tasks_of_policy.dispatcher_of_hl_risekeep_match',
        'schedule': crontab(day_of_week='2-6', hour='1', minute='0'),
        'args': (None, None, None, True, 1000)
    },

    ### 两连板策略
    'exec-policy-hl-twoserial': {
        'task': 'celery_dogen.tasks_of_policy.dispatcher_of_hl_twoserial_match',
        'schedule': crontab(day_of_week='2-6', hour='1', minute='0'),
        'args': (None, None, None, True, 1000)
    },

    ### 反弹策略
    'exec-policy-rb-risekeep': {
        'task': 'celery_dogen.tasks_of_policy.dispatcher_of_rb_risekeep_match',
        'schedule': crontab(day_of_week='2-6', hour='1', minute='0'),
        'args': (None, None, None, True, 1000)
    },

    ### 上涨回调策略
    'exec-policy-up-fallback': {
        'task': 'celery_dogen.tasks_of_policy.dispatcher_of_up_fallback_match',
        'schedule': crontab(day_of_week='2-6', hour='1', minute='0'),
        'args': (None, None, None, True, 1000)
    },

    ### 蓄势上涨策略
    'exec-policy-up-riseheap': {
        'task': 'celery_dogen.tasks_of_policy.dispatcher_of_up_riseheap_match',
        'schedule': crontab(day_of_week='2-6', hour='1', minute='0'),
        'args': (None, None, None, True, 1000)
    },

    ### 上涨鸳鸯策略
    'exec-policy-up-mandarin': {
        'task': 'celery_dogen.tasks_of_policy.dispatcher_of_up_mandarin_match',
        'schedule': crontab(day_of_week='2-6', hour='1', minute='0'),
        'args': (None, None, None, True, 1000)
    },

    ### 上涨20日均线策略
    'exec-policy-up-ma20touch': {
        'task': 'celery_dogen.tasks_of_policy.dispatcher_of_up_ma20touch_match',
        'schedule': crontab(day_of_week='2-6', hour='1', minute='0'),
        'args': (None, None, None, True, 1000)
    },

    ### 新股开板策略
    'exec-policy-new-ipo': {
        'task': 'celery_dogen.tasks_of_policy.dispatcher_of_new_ipo_match',
        'schedule': crontab(day_of_week='2-6', hour='1', minute='0'),
        'args': (None, None, None, True, 1000)
    },
}

####################### 业务配置，独立于celery #######################
mongo_server = "mongodb://127.0.0.1:27017"
mongo_database = "Dogen"