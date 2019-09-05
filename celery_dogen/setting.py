#-*-coding:utf-8-*-

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
    'exec-kdata-daily-pull': {
        'task': 'celery_dogen.tasks_of_kdata.dispatcher_of_daily_pull_update_kdata',
        'schedule': crontab(day_of_week='1-6', hour='0', minute='01')
    },

    'exec-policy-hl-fallback': {
        'task': 'celery_dogen.tasks_of_policy.dispatcher_of_hl_fallback_match',
        'schedule': crontab(day_of_week='1-6', hour='5', minute='11'),
        'args': {'save_result': True}
    }
}
