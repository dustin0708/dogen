#-*-coding:utf-8-*-

### 外部设置日志句柄
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

mongo_server = "mongodb://127.0.0.1:27017"
mongo_database = "Dogen"
