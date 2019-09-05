#-*-coding:utf-8-*-

from celery import Celery

app = Celery('celery_dogen')

app.config_from_object('celery_dogen.setting')

from .tasks_of_kdata        import *
from .tasks_of_policy       import *
from .tasks_of_dispatcher   import *