#-*-coding:utf-8-*-

from celery import Celery

app = Celery('celery_dogen')

app.config_from_object('celery_dogen.setting')
