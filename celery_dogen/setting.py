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

    ### 涨停回调策略
    'exec-policy-hl-fallback': {
        'task': 'celery_dogen.tasks_of_policy.dispatcher_of_hl_fallback_match',
        'schedule': crontab(day_of_week='2-6', hour='0', minute='21'),
        'args': (None, None, None, True, 1000)
    },
}

####################### 业务配置，独立于celery #######################
mongo_server = "mongodb://127.0.0.1:27017"
mongo_database = "Dogen"