#-*-coding:utf-8-*-

from celery import Celery
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

app = Celery('celery_dogen')

app.config_from_object('celery_dogen.setting')

####################### 业务配置，独立于celery #######################
mongo_server = "mongodb://127.0.0.1:27017"
mongo_database = "Dogen"

from .tasks_of_kdata        import *
from .tasks_of_policy       import *