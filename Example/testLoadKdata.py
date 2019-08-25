#-*-coding:utf-8-*-

from Script.Core import Kdata
from Script.Task import LoadKdata
import logging,sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

bs=Kdata.download_basics()
db = Kdata.DbH5file('./tmp.h5')
db.write_basics(bs)

code='600687'
gt=Kdata.download_kdata(bs.loc[code], start='2018-10-01')
db.write_kdata(code, gt)

code='603629'
gt=Kdata.download_kdata(bs.loc[code], start='2018-11-15')
db.write_kdata(code, gt)

tmp=Kdata.DbMemory()
LoadKdata.main(db,tmp)

logging.error("helo")