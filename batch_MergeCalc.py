##########################################################
# データバッチ運用見据えて
# - Contents
# - 更新履歴
#   - 20220220 初版作成
##########################################################
import consts as con
from abc import ABC, abstractmethod
from Pipeline import *
from utils import unpack_
from dataProcess_LastCalc import *
from batch_Base import BatchBase


class DataMerger(BatchBase):
    # TODO: CONSTを整理する, Excelへ移管できないか検討する
    # TODO: 本来Pastなどで実行する処理が入っているかと思われる（前走など）
    RACE_COLS = ['id', 'YEAR', 'MONTH', 'DAY', 'SOPDT', 'RCOURSECD', 'RNO', 'GCD', 'RKINDF', 'OPDT', 'INFIXF']
    DTL_COLS  = ['TEINO', 'ALLWINP', 'LCLWINP', 'LCLFUKP', 'BOATFUKP',  'SEXCD', 'BMI', 'CLS', 'NKI', 'AGE', 'is_absent']
    TENJI_RACE_COL = ['WEATHERCD', 'WINDCD', 'WINDPOWER', 'WAVE', 'TEMP', 'WTEMP', 'DIRECTION']
    TENJI_DTL_COL = ['WGHT', 'ADJ', 'TILT', 'SHOWTM','SHOWST', 'SHOWCOURSE', '前FIXPLC', '前R', '前ST', '前INCOURSE']
    # BASE_FILTER_COLS = [*RACE_COLS,  *DTL_COLS, *TENJI_RACE_COL, *TENJI_DTL_COL, *Y_COLS]
    # ZENKEN_COLS = ['2連対率_MOT_s0', '2連対率_BOT_s0','前検タイム_MOT_s0', '順位_2連対率_MOT_s0', '順位_2連対率_BOT_s0', '順位_前検タイム_MOT_s0',
    #    '差@2連対率_MOT-BOT', '順位差@2連対率_MOT-BOT', '順位差@2連対率_MOT-前検']
    BASE_FILTER_COLS = [*RACE_COLS,  *DTL_COLS, *TENJI_RACE_COL, *TENJI_DTL_COL, *Y_COLS, 'MOTORNO']
    # BASE_FILTER_COLS = [*RACE_COLS,  *DTL_COLS, *TENJI_RACE_COL, *TENJI_DTL_COL, *Y_COLS, *ZENKEN_COLS]
    FIXPLC_DICT = { '１':1, '２':2, '３':3, '４':4, '５':5, '６':6,
                    '転':7,  'Ｆ':8, '落':9, '失':10, 'エ':11, '妨':12, '欠':13, '不':14, 'Ｌ':15, '沈':16 }
    MAE_COLS = ['前R', '前ST', '前INCOURSE']

    INPUT_FILE = con.PIPE_CHOKUZEN
    OUTPUT_FILE = con.PIPE_MERGE

    def __init__(self, opdt=None, mid=None):
        super().__init__(opdt, mid)
        self.df_lake = self.EMPTY
        self.dfs_lake = {}

    def __merge_df(self, df1, df2, key, drop_cols):
        drop_cols.extend(MST_KEYS)
        new_cols1 = [x for x in df1.columns if x not in drop_cols]
        new_cols2 = [x for x in df2.columns if x not in drop_cols]
        df1 = df1[[*key, *new_cols1]]
        df2 = df2[[*key, *new_cols2]]
        return pd.merge(df1, df2, on=key, how="inner")
        
    def _readLakes(self):
        for lake_name in self.LAKE_LIST:
            lake_path = f'{self.LAKE_PATH}/{lake_name}/{self.opdt}.pkl'
            if lake_name==self.MERGE_BASE:
                self.df_lake = pd.read_pickle(lake_path)
            else:
                self.dfs_lake[lake_name] = pd.read_pickle(lake_path)

    def _mergeEachLakes(self):
        drop_cols_before = LastCalculator(self.df_lake, self.col_to_use).removeColumnsBefore()
        for df_to_merge in self.dfs_lake.values():
            self.df_lake = self.__merge_df(self.df_lake, df_to_merge, self.MERGE_KEYS, drop_cols_before)


    def _createLakes(self):
        self._readLakes()
        self._mergeEachLakes()

    def __mergeZenken(self, df):
        df = df[self.BASE_FILTER_COLS]
        df_zenken = pd.read_pickle(con.SAVE_PATH_FMT(tbl=con.GAL_ZENKEN, y='2022'))
        df = pd.merge(df, df_zenken, on=self.MERGE_ZENKEN_KEYS, how='left')
        return df.drop(columns='MOTORNO')

    def __mergeLake(self, df):
        return pd.merge(df, self.df_lake, on=self.MERGE_KEYS, how='left')


    def __cleanse(self, df):
        df = float64to32(df).sort_values(self.MERGE_KEYS)
        df['前FIXPLC'] = df['前FIXPLC'].map(self.FIXPLC_DICT).astype('category').astype('float')
        for mae in self.MAE_COLS:
            df[mae] = df[mae].str.strip().replace("", np.nan).astype(np.float32)
        return df

    def _createLastBase(self):
        df_b = self.__mergeZenken(self.df_in)
        df_b = self.__mergeLake(df_b)
        df_b = self.__cleanse(df_b)
        self.df_out = df_b.copy()
    

    def _main(self):
        self._createLakes()
        self._createLastBase()

    
class DataCalculater(BatchBase):
    INPUT_FILE = con.PIPE_MERGE
    OUTPUT_FILE = con.PIPE_TEST

    def __init__(self, opdt=None, mid=None):
        super().__init__(opdt, mid)
        self.df_new = self.EMPTY
        self.df_yoko = self.EMPTY
        self.df_final = self.EMPTY

    def _lastCalc(self):
        self.lc = LastCalculator(self.df_in, self.col_to_use)
        self.lc.setCalcList()
        self.lc.excelCalc()
        self.lc.bulkCalc()
        df_new = self.lc.removeColumns()
        self.df_new = pd.concat([df_new, self.lc.df_new], axis=1)
        
    def _to_yoko(self):
        # self.df_new.
        df = self.df_new.replace(np.inf, np.nan)
        df = df.fillna(-999)
        except_cols = [*Y_COLS, *self.lc.race_cols]
        self.df_yoko = to_yokomochi(df, except_cols, showTime=False)
        
    def __katahen(self, df, cat_cols=[]):
        df = float64to32(df)
        cat_cols = unpack_([sh_col(df, cat_name) for cat_name in self.CAT_NAMES])
        df[cat_cols] = df[cat_cols].astype('category')
        return df
        
    def _finalize(self):
        df = self.df_yoko.drop(columns=dropCols(self.df_yoko))
        df = calc_periods_day(df)
        df = self.__katahen(df)
        self.df_out = df[sorted(df.columns)]

    def _main(self):
        self._lastCalc()
        self._to_yoko()
        self._finalize()


        
class BulkDataCalculater(DataCalculater):
    def read_bulk(self):
        self.df_base =pd.read_pickle(self.DF_BASE3)
        

class BatchLastCalc():
    def __init__(self, opdt=None, mid=None, isTrain=False):
        self.opdt = opdt
        self.mid = mid
        self.DM_ = DataMerger(mid, opdt)
        if isTrain:
            self.DC_ = BulkDataCalculater(mid, opdt)
            self.DC_.read_bulk()
        else:
            self.DC_ = DataCalculater(mid, opdt)

    def main(self):
        self.DM_.mergeProcess()
        self.DC_.calcProcess()

