##########################################################
# データ加工の基礎となる部分
##########################################################
import glob

import consts as con
from Pipeline import *
from OfficialScraping import TenjiConverter
from dataProcess_PastData import *
from batch_Base import BatchBase
from abc import ABC, abstractmethod
from multiprocessing import Pool, dummy


class BaseCreatorBase(BatchBase):
    pass


class DayCreator(BaseCreatorBase):
    INPUT_FILE = ''
    OUTPUT_FILE = con.PIPE_DAYS

    def _readInput(self):
        self.df_in = download_data_daily(self.opdt)

    def _main(self):
        self.df_out = main_v1(self.df_in)

class DayCreatorChokuzen(BaseCreatorBase):
    INPUT_FILE = con.PIPE_DAYS
    OUTPUT_FILE = con.PIPE_CHOKUZEN

    def __init__(self, opdt, mid=None, rcd=None, rno=None):
        super().__init__(opdt, mid)
        self.r = [opdt, rcd, rno]
        if rcd is None:
            self.is_daily = True
        else:
            self.is_daily = False
        
    def _load_tenji(self):
        concat_list = glob.glob(con.PATH_TENJI_DF_FMT(y=self.opdt[:4], op=self.opdt))
        with Pool() as p:
            self.df_tenji = pd.concat(map(pdreadpickle, concat_list))
        self.df_tenji =  data_cleanse_tenji(self.df_tenji)

    def _mergeBaseTenji(self):
        df_input = self.df_in.drop(columns=['WGHT', 'SHOWTM'])
        self.df_out = pd.merge(self.df_tenji, df_input, on=self.LAKE_MGR_KEYS)

    def _main(self):
        self._load_tenji()
        self._mergeBaseTenji()

