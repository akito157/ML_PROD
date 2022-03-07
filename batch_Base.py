##########################################################
# データバッチ運用見据えて
# - Contents
# - 更新履歴
#   - 20220220 初版作成
##########################################################
import consts as con
from abc import ABC, abstractmethod
from numpy import empty
from Pipeline import *
from utils import unpack_
from utils_req import pd_readSheet, notify_error
from ml_utils import engine
import gc
import os

class InfoPrinter():
    FMT = '#-- {:17}'.format
    def _info(self, m, v):
        print(f'{self.FMT(m)}  {v}')
        
    def _timerStart(self):
        self.timer_st = time.time()

    def _timerEnd(self, message=""):
        print(message, time.time()-self.timer_st)

    def _infoStart(self):
        self.st = time.time()
        # print(f'#------------- START   {cls_name}')
        print(f'#------------- START   {self.cls_name}')
        self._info('OPDT', self.opdt)

    def _infoEnd(self):
        self._summary()
        elapsed_time = np.round(time.time() - self.st, 1)
        print(f'#------------- END     {elapsed_time} sec')

    def _summary(self):
        self._info('df_in shape', self.df_in.shape)
        self._info('df_out shape', self.df_out.shape)
        
    
class CONSTS():
    CAT_NAMES = ['is_absent', 'Rフラグ', '前FIXPLC', 'RNO', '前R', 'RCOURSECD',
                 'WINDCD', 'DIRECTION']
    DF_BASE3 = 'df_tmp/df_base3.pkl'
    MODEL_SHEET = ('choose_model', 'models')
    
    LAKE_LIST = ['kihon',  'waku', 'setsu', 'tenji',
    'conditional', 'motor', 'time', 'jyo', 'sinnyu'
    ]
    LAKE_PATH =  "C:/GAL/Kyotei/biyori"
    MERGE_BASE =  'kihon'
    MERGE_KEYS =  ['id', 'TEINO']
    MERGE_ZENKEN_KEYS =  ['SOPDT', 'RCOURSECD', 'MOTORNO']
    EMPTY =  pd.DataFrame()
    
    # LakeCreator
    PATH_PAST_BASE = 'df_base.pkl'
    PATH_PAST_TENJI = 'df_tenji.pkl'
    PATH_TEMP = 'df_tmp.pkl'
    PATH_PIPE = 'pipe_data'

    LAKE_MGR_KEYS = ['TEINO', 'OPDT', 'RCOURSECD', 'RNO']
    ISSAVE = True
    SHOW_TIME = False



class GspreadReader():
    def _getModelInfo(self, id_):
        SQL = f'SELECT * FROM a_MODELS WHERE id = {str(id_)}'
        model = pd.read_sql_query(SQL,  engine)
        return model
        
    def read_modelSheet(self):
        return  pd_readSheet(*self.MODEL_SHEET)



class BatchBase(ABC, GspreadReader, CONSTS, InfoPrinter):
    def __init__(self, opdt=None, mid=None, batch_id=None):
        self.opdt = opdt
        self.year = int(opdt[:4])
        self.mid = mid
        self.bid = 0
        self.df_in = self.EMPTY
        self.df_out = self.EMPTY
        self.input_file = self._setFileName(self.INPUT_FILE)
        self.output_file = self._setFileName(self.OUTPUT_FILE)
        self.cls_name = self.__class__.__name__

        if mid is not None:
            model = self._getModelInfo(mid)
            self.df_id = model.a_DATAFRAME_id[0]
            self.col_to_use = f'col_to_use_{model.model_type[0]}'

        # self.col_to_use = f'col_to_use_{model.model_type[0]}'
        

        if self.INPUT_FILE!="":
            if self._checkExistance(self.input_file) is False:
                print(f'Not Exist Required Files {self.input_file}')
        gc.collect()


    def _setFileName(self, FILE):
        # if FILE==con.PIPE_EXPECT:
        #     file_name = f'{self.opdt}_{self.odds_table[2]}'
        # else:
        file_name = f'{self.opdt}'
        return f'{self.PATH_PIPE}/{FILE}/{file_name}.pkl'



    def _readInput(self):
        if self.INPUT_FILE!="":
            self.df_in = pd.read_pickle(self.input_file)

    def _saveOutput(self):
        if self.OUTPUT_FILE!="":
            self.df_out.to_pickle(self.output_file)

    def _checkExistance(self, input_file):
        return os.path.exists(input_file)

    def _readModelSheetProperty(self, bid):
        gs = self.read_modelSheet()
        return gs[gs['batch_id']==bid].reset_index(drop=True)

    # TODO: 手動モード
    @classmethod
    def read_from_path(self):
        pass

    @classmethod
    def main_manually(self):
        pass

    @abstractmethod
    def INPUT_FILE(self):
        pass
        
    @abstractmethod
    def OUTPUT_FILE(self):
        pass

    @abstractmethod
    def _main(self):
        pass

    # TODO: propertyメソッドとかにしたい
    def errorCather(self,func):
        try:
            func()
        except Exception as e:
            message = f'''error! {self.cls_name}
             {e}'''
            notify_error(message)
            raise

    def main(self):
        self._infoStart()
        self._readInput()
        self.errorCather(self._main)
        self._saveOutput()
        self._infoEnd()
        gc.collect()
