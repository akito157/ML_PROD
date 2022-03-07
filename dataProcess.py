##########################################################
# データ加工メイン
# - Contents
# - 更新履歴
#   - 20220115 初版作成
#   - 20220122 オブジェクト継承の形に変更
##########################################################
from concurrent import futures
future_list = []
import time
import os
import pandas as pd

from utils import sh_col
from dataProcess_Calculator import *
from dataProcess_CalculatorOriginal import *
from dataProcess_PastData import *
from dataProcess_Base import CalculaterBase
from dataProcess_Base import CalculaterChokuzen
from dataProcess_Base import MST_KEYS
import gc

# TODO: 事故フラグ削除の統一化を図る(事故フラグを残したほうが良いパターンを整理する)
# TODO: DataLakeの保存先（枝分かれ）の管理方法について検討する

REMOVE_ALL = ['F', 'K0', 'K1', 'L0', 'L1', 'S0', 'S1', 'S2']

#---------------------  CLASS
class KihonCalculater(CalculaterBase):
    NAME = 'kihon'
    KEYS = ['ENTNO']
    SUFFIX = ""
        
    def calcMain(self):
        self.dfs['l1'] = last_rolling(self.dfs['y2'], self.KEYS, 10)
        self.calc(get_past_value, [], ['p1', 'p2'])
        self.calc(calculate_fixplc, ['S0', 'S1', 'S2'], ['m6','m3', 'p1', 'p2', 'p0', 'y1', 'l1'])
        self.calc(calculate_mean, REMOVE_ALL, ['m1', 'm3', 'm6', 'y1', 'p2', 'l1'])
        self.calc(calculate_std, REMOVE_ALL, ['m1', 'm3', 'm6', 'y1', 'p2', 'l1'])
        self.calc(calculate_point, ['S0', 'S1', 'S2'], ['m1', 'm3', 'l1'])
        self.calc(calculate_biyori_p, REMOVE_ALL, ['m6', 'y1', 'l1'])
    
    
    
class WakuCalculater(CalculaterBase):
    NAME = 'waku'
    KEYS = ['ENTNO', 'INCOURSE']
    SUFFIX = "_INC"
    LEFT_ON = ['ENTNO', 'TEINO']
        
    def update_dfs(self):
        for key, value in self.dfs.items(): # Filter Past Data
            if key!='opdt':
                self.dfs[key] = add_filter_to_past(value, self.df_days, self.KEYS)
        
    def calcMain(self):
        st = time.time()
        self.update_dfs()
        self.dfs['l1'] = last_rolling(self.dfs['y2'], self.KEYS, 10)
        self.dfs['l2'] = last_rolling(self.dfs['y2'], self.KEYS, 20)
        self.calc(calculate_new_concept, ['S0', 'S1', 'S2', 'F'], ['m6','y1', 'p2', 'l1', 'l2','y2'])
        self.calc(calculate_point, ['S0', 'S1', 'S2'], ['y1', 'p2', 'l1', 'l2'])
        self.calc(calculate_fixplc, ['S0', 'S1', 'S2'], ['m6','m3', 'p2', 'y1', 'y2', 'l1', 'l2'])
        self.calc(calculate_mean, REMOVE_ALL, ['m6','m3', 'y1', 'p2', 'l1'])
        self.calc(calculate_std, REMOVE_ALL, ['m6','m3', 'y1', 'p2', 'l1'])
    

    
class MotorCalculater(CalculaterBase):
    NAME = 'motor'
    KEYS = ['RCOURSECD', 'MOTORNO']
    SUFFIX = "_MOT"
    
    def calcMain(self):
        self.dfs['z0'] = motor_data(self.dfs['y1'], self.df_today)
        self.dfs['z1'] = motor_data(self.dfs['m1'], self.df_today)
        self.calc(calculate_fixplc, ['K0', 'K1'], ['z0', 'z1'])
        self.calc(calculate_point, [], ['z1', 'z0'])
        self.calc(calculate_mean, REMOVE_ALL, ['z0', 'z1'])
        self.calc(calculate_start, [], ['z0'])
        self.calc(calculate_std, REMOVE_ALL, ['z0', 'z1'])
        self.calc(calculate_gyakuten, [], ['z0'])
        
    
    
class SetsuCalculater(CalculaterChokuzen):
    NAME = 'setsu'
    KEYS = ['ENTNO']
    SUFFIX = ""
        
    def calcMain(self):
        self.calc(calculate_point, [], ['s1'])
        self.calc(calculate_fixplc, [], ['s1'])
        self.calc(calculate_mean, REMOVE_ALL, ['s1'])
        self.calc(calculate_tenji, [], ['s1'])
        self.calc(calculate_std, REMOVE_ALL, ['s1'])
        
        

class JyoCalculater(CalculaterBase):
    NAME = 'jyo'
    KEYS = ['RCOURSECD']
    SUFFIX = "_JYO"
    LEFT_ON = None
    
    def calcMain(self):
        self.calc(calculate_gyakuten, [], ['y1'])
        
        self.SUFFIX = "_JYI"
        self.KEYS = ['RCOURSECD', 'INCOURSE']
        self.LEFT_ON = ['RCOURSECD', 'TEINO']
        self.calc(calculate_fixplc, [], ['y1'])
        self.calc(calculate_new_concept, ['S0', 'S1', 'S2', 'F'], ['y1'])
        self.calc(calculate_gyakuten, [], ['y1'])
        self.calc(calculate_start, [], ['y1'])
        self.calc(calculate_mean, REMOVE_ALL, ['y1'])
        self.calc(calculate_tenji, ['K0', 'K1'], ['y1'])
        self.calc(calculate_std, REMOVE_ALL, ['m6','m3', 'y1', 'p2'])
        
        
class TimeCalculater(CalculaterBase):
    NAME = 'time'
    SUFFIX = ''
    KEYS = ['ENTNO']

    def _calcEach(self, keys, left_on, suffix):
        self.KEYS = keys
        self.LEFT_ON = left_on
        self.SUFFIX = suffix
        
        st = time.time()
        if 'RCOURSECD' not in self.KEYS:
            # self.calc(calculate_start, ['F', 'L0', 'L1', 'K0', 'K1', 'K2'], ['y2'])
            self.calc(calculate_start, ['F', 'L0', 'L1', 'K0', 'K1', 'K2'], ['y1', 'y2'])

        # self.calc(calculate_zenso, ['K0', 'K1'], ['y2'])
        self.calc(calculate_zenso, REMOVE_ALL, ['y2'])
        self.calc(calculate_gyakuten, [], ['y1', 'y2'])
        
    def calcMain(self):
        # self.update_dfs()
        st = time.time()
        self._calcEach(['ENTNO'], left_on= None, suffix='')
        self._calcEach(['ENTNO', 'INCOURSE'], left_on=['ENTNO', 'TEINO'], suffix='_INC')
        self._calcEach(['ENTNO', 'RCOURSECD'], left_on=None, suffix='_JYO')
        
        
class TenjiCalculater(CalculaterBase):
    NAME = 'tenji'
    SUFFIX = ''
    KEYS = ['ENTNO']
    LEFT_ON =None
    SHOW_TIME = True
    
        
    def _calcEach(self, keys, left_on, suffix):
        self.KEYS = keys
        self.LEFT_ON = left_on
        self.SUFFIX = suffix
        
        self.add_MST_KEYS()
        if 'SHOWCOURSE' in self.KEYS:
            self.calc(calculate_new_concept, ['S0', 'S1', 'S2', 'F'], ['m6','y1', 'y2'])
            self.calc(calculate_point, ['S0', 'S1', 'S2'], ['m3', 'm6', 'y1'])
        self.calc(calculate_fixplc, ['K0', 'K1'], ['m6', 'y1'])
        
    def calcMain(self):
        st = time.time()
        self.calc(calculate_tenji, ['K0', 'K1'], ['m1', 'm3','m6', 'y1'])
        self._calcEach(['ENTNO', 'SHOWCOURSE'], left_on=None, suffix='_SIN')
        self._calcEach(['ENTNO', 'SSTORD'], left_on=None, suffix='_SST')
        self._calcEach(['ENTNO', 'STMORD'], left_on=None, suffix='_STM')

        # print('show cod', time.time() -st)
        # print('sstord cod', time.time() -st)
        # print('stmord cod', time.time() -st)
        # print('tenji', time.time() -st)


class ConditionCalculater(CalculaterBase):
    NAME = 'conditional'
    SUFFIX = ''
    KEYS = ['ENTNO']
    LEFT_ON =None
    SHOW_TIME = True
    
    def _calcEach(self, keys, left_on, suffix):
        self.KEYS = keys
        self.LEFT_ON = left_on
        self.SUFFIX = suffix
        
        self.calc(calculate_point, ['S0', 'S1', 'S2'], ['y1'])
        self.calc(calculate_fixplc, [], ['y1'])
        
    def _calcEach2(self, keys, left_on, suffix):
        self._calcEach(keys, left_on, suffix)
        
        self.calc(calculate_mean, REMOVE_ALL, ['y1'])
        self.calc(calculate_std, REMOVE_ALL, ['y1'])
        
        
    def calcMain(self):
        self._add_cols(['WINDBIN', 'WINDPBIN', 'JIMOTO', 'SNDAYS', 'SHOWSTBIN', 
                        'WAVEBIN', 'TILTBIN', 'GIRLS', 'TEI1CLS', 'CLSAs'])
        
        self._calcEach2(['ENTNO', 'WINDBIN'], left_on=None, suffix='_WID')

        self._calcEach2(['ENTNO', 'WINDPBIN'], left_on=None, suffix='_WIP')
        self._calcEach2(['ENTNO', 'WAVEBIN'], left_on=None, suffix='_WAV')
        self._calcEach2(['ENTNO', 'SHOWSTBIN'], left_on=None, suffix='_SSB')
        self._calcEach2(['ENTNO', 'TILTBIN'], left_on=None, suffix='_TIL')
        self._calcEach(['ENTNO', 'SNDAYS'], left_on=None, suffix='_DAY')
        self._calcEach(['ENTNO', 'JIMOTO'], left_on=None, suffix='_JMT')
        self._calcEach(['ENTNO', 'GIRLS'], left_on=None, suffix='_GRL')
        self._calcEach(['ENTNO', 'TEI1CLS'], left_on=None, suffix='_TEC')
        self._calcEach(['ENTNO', 'CLSAs'], left_on=None, suffix='_CLA')
        
        self._drop_cols('(TEINO|INCOURSE|FIXPLC)(平均|偏差)|3連対率|回数|Cnt|(TILT|SHOWST|WAVE|CLS|WIND(CD|POWER))$')
    

class INCOURSECalculater(CalculaterBase):
    NAME = 'sinnyu'
    KEYS = ['ENTNO', 'TEINO']
    SUFFIX = "_TEI"
    SHOW_TIME = True
    
        
    def __createNewCols(self):
        df_list = [self.df_days, self.dfs['y2']]
        
        for df in df_list:
            cond = df['TEINO'] != df['SHOWCOURSE'].fillna(0).astype(int).astype(str)
            df.loc[cond, 'SHCnotTEI'] = 1

        
    def calcMain(self):
        self._add_cols(['INFIXF', 'SHOWCOURSE'])
        self.__createNewCols()
        
        self.KEYS = ['ENTNO', 'TEINO', 'INFIXF']
        self.LEFT_ON = ['ENTNO', 'TEINO', 'INFIXF']
        self.SUFFIX = "_TEI"
        self.calc(calculate_changeINC, [], ['y2'])
        
        
        self.KEYS = ['ENTNO', 'TEINO', 'INFIXF']
        self.LEFT_ON = ['ENTNO', '非枠なり時進_TEI_y2', 'INFIXF']
        self.SUFFIX = "_INC2"
        self.calc(calculate_fixplc, ['K0', 'K1'], ['y2'])
        
        self.KEYS = ['ENTNO', 'TEINO', 'SHCnotTEI']
        self.LEFT_ON = ['ENTNO', 'TEINO', 'SHCnotTEI']
        self.SUFFIX = "_SHC"
        self.calc(calculate_changeINC, [], ['y2'])
        self.calc(calculate_fixplc, ['K0', 'K1'], ['y2'])
        
        
        # self.KEYS = ['ENTNO', 'TEI''SHCnotTEI']
        # self.LEFT_ON = ['ENTNO', 'SHCnotTEI']
        # self.LEFT_ON = ['ENTNO', '非枠なり時進_TEI_y2', 'INFIXF']
        # self.SUFFIX = "_SHC2"
        
        
        # self.KEYS = ['ENTNO', 'TEINO', 'SHOWCOURSE', 'INFIXF']
        
        
        
#     def __stdEachRace(self,s):
#         s2 = s / s.sum().sum()
#         return  s2 / s2.sum()
        
#     def __additionalCalc(self):
#         cols = sh_col(self.df_days, '\d{1}_TEI_', 0)
#         new_cols = ['std_'+c for c in cols]
#         self.df_days[new_cols] = self.df_days.groupby('id')[cols].apply(lambda s: self.__stdEachRace(s))
        
#         df_g = choice_not_wakunari(self.df_days[['TEINO', *new_cols]], add_prefix=True)
#         display(df_g)

        
# class SetsuMaeCalculater(WakuCalculater):
#     pass
# def get_biyori_setsu_mae(type_, df_days, df, periods=[]):
#     df = df[df.RCOURSECD.isin(df_days.RCOURSECD.unique())]
#     df = df[df.SOPDT.isin(df_days.SOPDT.unique())]
#     df = df[df.ENTNO.isin(df_days.ENTNO.unique())]
    
#     df = df.sort_values(['RCOURSECD', 'OPDT', 'RNO'])
#     df['ENTNO_th'] = df.groupby(['RCOURSECD','ENTNO']).cumcount() + 1
#     df = df[df.ENTNO_th==1]
#     df = df[["SOPDT", 'RCOURSECD', 'OPDT', 'ENTNO',  'ENTNO_th']]
    
#     keys = ['SOPDT', 'RCOURSECD', 'OPDT', 'ENTNO', 'ENTNO_th']
#     a = pd.DataFrame()
#     for opdt, df_opdt in df.groupby('OPDT'):
#         print(opdt)
#         # display(df_opdt[:10])
#         file_name = f"C:/GAL/Kyotei/biyori/{type_}/{opdt}.pkl"
#         df_1st = pd.read_pickle(file_name)
#         cols = df_1st.columns[df_1st.columns.str.contains("|".join(periods))]
#         df_1st = add_ENTNO_th(df_1st)
#         df_1st = df_1st[[*keys, *cols]]
        
#         df_opdt = pd.merge(df_opdt, df_1st, on=keys, how='left')
#         a = pd.concat([a, df_opdt])
        
#     drop_cols = list(set(keys) - set(["ENTNO"]))
#     a = a.drop(columns=drop_cols)
#     print(len(df_days))
#     df_days = pd.merge(df_days, a, on="ENTNO")
#     display(df_days)
#     print(len(df_days))


# class AcuratePointCalculator(WakuCalculater):
    # pass
    
    
    
# class OthersCalculater(WakuCalculater):