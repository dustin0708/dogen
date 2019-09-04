#-*-coding:utf-8-*-

from celery.task.schedules import crontab

### 中间件, Rabbitmq
broker_url = "pyamqp://127.0.0.1:5672"

### 任务结果保存数据库地址
result_backend = ''

### 时区
timezone = 'Asia/Shanghai'

imports = (
    ###>>>> 以下是定时任务模块
    'dogen.crontab.trigger_daily_pull',     # 周期性触发股票数据更新
    'dogen.crontab.trigger_hl_fallback',     # 周期性触发涨停回调策略
    

    ###>>>> 以下是celery worker模块
    'dogen.kdata.daily_pull',   # 股票数据更新

    'dogen.policy.hl_fallback', # 股票匹配策略
)


beat_schedule = {
    'exec-kdata-daily-pull': {
        'task': 'dogen.crontab.trigger_daily_pull.dispatch_tasks_of_daily_pull',
        'schedule': crontab(day_of_week='1-6', hour='0', minute='30')
    },

    'exec-policy-hl-fallback': {
        'task': 'dogen.crontab.trigger_hl_fallback.dispatch_tasks_of_hl_fallback',
        'schedule': crontab(day_of_week='1-6', hour='5', minute='0')
    }
}