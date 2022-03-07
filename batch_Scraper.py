##########################################################
# データ加工の基礎となる部分
##########################################################
import glob

import consts as con
from torch import classes 
from Pipeline import *
from OfficialScraping import TenjiConverter
from utils import opdt_list_
from utils import unpack_, filter_opdt_until
from dataProcess_PastData import *
# from OfficialScraping import download_official_chokuzen_bulk
from OfficialScraping import TenjiScraper, TenjiConverter
from OfficialScraping import ZenkenScraper
from batch_Base import BatchBase
from abc import ABC, abstractmethod
from multiprocessing import Pool, dummy


def excute(Cls):
    Cls.main()


D_FMT = '%Y%m%d'
class PastTenjiBase(BatchBase):
    INPUT_FILE = ''
    OUTPUT_FILE = ''
    SCRAPE_KEY = ['OPDT', 'RCOURSECD', 'RNO']

    def _createRaceList(self):
        sql = con.QUERY_OPDT_FMT(tbl='RACEMST', op1=self.opdt_from, op2=self.opdt_until)
        rs = pd.read_sql(sql, engine)
        self.race_list = rs[self.SCRAPE_KEY].drop_duplicates()
        print(len(self.race_list))

    def __init__(self, opdt_from=None, opdt_until=None, isPool=True):
        super().__init__(opdt_from, None)
        self.isPool = isPool
        if opdt_until is None:
            self.opdt_until = opdt_from
            self.opdt_from = (datetime.strptime(opdt_from, D_FMT) - timedelta(days=3)).strftime(D_FMT)
        else:
            self.opdt_from = opdt_from
            self.opdt_until = opdt_until
            
    def SCRAPER(self):
        pass

    def _execute(self):
        gs = pd_readSheet('choose_model', 'proxy')
        Classes = [self.SCRAPER(r, False, gs) for i, r in self.race_list.iterrows()]

        if len(Classes)>14 and self.isPool:
            with Pool(14) as p:
                p.map(excute, Classes)
        else:
            [excute(Cls) for Cls in Classes]

    def _main(self):
        print(1)
        self._createRaceList()
        self._execute()

class PastTenjiScraper(PastTenjiBase):
    SCRAPER = TenjiScraper

class PastZenkenScraper(PastTenjiBase):
    SCRAPE_KEY = ['SOPDT', 'RCOURSECD']
    SCRAPER = ZenkenScraper


class PastTenjiConverter(PastTenjiBase):
    def _execute(self):
        opdt_list = np.unique(self.race_list.OPDT)
        Classes = [TenjiConverter(self.race_list, opdt, False) for opdt in opdt_list]

        print(len(opdt_list))
        # print(un)
        # if len(Classes)>15:
        #     with dummy.Pool(15) as p:
        #         p.map(excute, Classes)
        # else:
        [excute(Cls) for Cls in Classes]



