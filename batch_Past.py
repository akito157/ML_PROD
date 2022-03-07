##########################################################
# データ加工の基礎となる部分
##########################################################
import glob
import consts as con
from Pipeline import *
from dataProcess_PastData import *
from batch_Base import BatchBase
from abc import ABC, abstractmethod
from multiprocessing import Pool, dummy


class PastCreatorBase(BatchBase):
    INPUT_FILE = ''
    OUTPUT_FILE = ''

    @abstractmethod
    def SAVE_FOLDER(self):
        pass

    def _saveOutput(self):
        self.df_out.to_pickle(con.SAVE_PATH_FMT(tbl=self.SAVE_FOLDER, y=self.year))


class PastBaseCreator(PastCreatorBase):
    SAVE_FOLDER = con.GAL_BASE

    def _update_yearly(self):
        df_m = pd.read_sql(con.QUERY_YEAR_FMT(tbl='RACEMST', y=self.year), engine)
        df_d = pd.read_sql(con.QUERY_YEAR_FMT(tbl='RACEDTL', y=self.year), engine)
        df_m.to_pickle(con.SAVE_PATH_FMT(tbl='RACEMST', y=self.year))
        df_d.to_pickle(con.SAVE_PATH_FMT(tbl='RACEDTL', y=self.year))

    def _readInput(self):
        self._update_yearly()
        self.df_in = download_data_multi(self.year, self.year)

    def _main(self):
        self.df_out = main_v1(self.df_in)


class PastTenjiCreator(PastCreatorBase):
    SAVE_FOLDER = con.GAL_TENJI

    def _readInput(self):
        concat_list = glob.glob(con.PATH_TENJI_DF_FMT(y=self.year, op=''))
        self.df_in = pd.concat(map(pdreadpickle, concat_list))

    def _main(self):
        self.df_out =  data_cleanse_tenji(self.df_in)


class PastZenkenCreator(PastCreatorBase):
    SAVE_FOLDER = con.GAL_ZENKEN

    def _readInput(self):
        concat_list = glob.glob(con.PATH_ZENKEN_DF_FMT(sop=str(self.year)+'*', rcd=''))
        self.df_in = pd.concat(map(pdreadpickle_zenken, concat_list))

    def _main(self):
        self.df_out =  data_cleanse_zenken(self.df_in)



class PastMerger(PastCreatorBase):
    SAVE_FOLDER = con.GAL_PAST
    TENJI_MGR_KEY = ['TEINO', 'OPDT', 'RCOURSECD', 'RNO']
    ZENKEN_MGR_KEY = ['SOPDT','RCOURSECD', 'MOTORNO']

    def _load(self):
        self.base  = pd.read_pickle(con.SAVE_PATH_FMT(tbl=con.GAL_BASE, y=self.year))
        self.tenji = pd.read_pickle(con.SAVE_PATH_FMT(tbl=con.GAL_TENJI, y=self.year))
        self.zenken = pd.read_pickle(con.SAVE_PATH_FMT(tbl=con.GAL_ZENKEN, y=self.year))

    def _mergeBaseTenji(self):
        df = self.base.drop(columns=['WGHT', 'SHOWTM'])
        df = pd.merge(df, self.tenji, on=[*self.TENJI_MGR_KEY])
        self.df_out = pd.merge(df, self.zenken, on=[*self.ZENKEN_MGR_KEY])

    def _main(self):
        self._load()
        self._mergeBaseTenji()


