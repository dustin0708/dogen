#-*-coding:utf-8-*-

from Script.Core import Kdata
from Script.Handler import UpdateKdata
import logging,sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


db = Kdata.DbH5file('./data/kdata_dbfile.h5')
tmp=Kdata.DbMemory()

KdataUpdate.main([db,tmp], start='2018-01-01')

