##########################################################
# データ加工メイン
# - Contents
# - 更新履歴
#   - 20200115 初版作成
##########################################################
from concurrent import futures
future_list = []
import time
import os
import pandas as pd
import numpy as np
import re

from dataProcess_PastData import *
from abc import ABC, abstractmethod
from utils import *

#---------------------  CONSTS
MST_KEYS = ['id', "SOPDT", "OPDT", "RCOURSECD", "RNO","ENTNO", "TEINO", 'MOTORNO', 'ENTNO_th']

#---------------------  CLASS
class CalculaterABC(ABC):
    @abstractmethod
    def NAME():
        pass
    
    @abstractmethod
    def KEYS():
        pass
    
    @abstractmethod
    def SUFFIX():
        pass
    
    @abstractmethod
    def calcMain():
        pass
    
class CalculaterBase(CalculaterABC):
    SHOW_TIME = False
    LEFT_ON = None
    
    def __init__(self, df_days, dfs, isSave=False, isTest=False):
        self.df_days = df_days.copy()
        self.dfs = dfs.copy()
        self.isSave = isSave
        self.df_today = dfs['today']
        self.file_name = f"C:/GAL/Kyotei/biyori/{self.NAME}/{self.dfs['opdt']}.pkl"
        self.isTest = isTest
        self.SHOW_TIME = True if isTest else False
        self.MST_KEYS = MST_KEYS.copy()
        
    def add_MST_KEYS(self):
        for key in self.KEYS:
            if key not in self.df_days.columns:
                mgr_key = ['id', 'TEINO']
                self.df_days = pd.merge(self.df_days, self.df_today[[*mgr_key, key]], on=mgr_key, how='left')
                self.MST_KEYS.append(key)
            
    def __merge(self, df_tm):
        if self.LEFT_ON is None:
            self.LEFT_ON = self.KEYS
        
        if 'TEINO' in self.LEFT_ON:
            df_tm = df_tm.reset_index()
            df_tm[self.KEYS[1]] = df_tm[self.KEYS[1]].astype(int).astype(str)
            df_tm.set_index(self.KEYS, inplace=True)
        
        if len(df_tm)==0:
            self.df_days[df_tm.columns] = None
        else:
            try:
                self.df_days= pd.merge(self.df_days, df_tm,
                                       how='left', left_on=self.LEFT_ON, right_on=self.KEYS)
            except ValueError:
                df_tm = df_tm.reset_index()
                df_tm[self.KEYS[1]] = df_tm[self.KEYS[1]].astype('float')
                df_tm.set_index(self.KEYS, inplace=True)
                
                self.df_days= pd.merge(self.df_days, df_tm,
                                       how='left', left_on=self.LEFT_ON, right_on=self.KEYS)
        

    def _calcEachPast(self, f, df_past, past):
        SUFFIX = f'{self.SUFFIX}_{past}'
        df_past = df_past.dropna(subset=self.KEYS, how='any')
        if len(df_past)>0:
            return f(df_past, self.KEYS).add_suffix(SUFFIX)
        else:
            return self.df_days[self.KEYS]
        
    def calc(self, f, rmflg, periods):
        st = time.time()
        for p in periods:
            self.__merge(self._calcEachPast(f, rmv_jiko(self.dfs[p], rmflg), p))
        if self.SHOW_TIME: print(self.NAME, f.__name__, st-time.time())
    
    def main(self):
        self.calcMain()
        self._afterCleanse()
        self._option()
        return self.df_days
        
    def _add_cols(self, cols):
        self.df_days[cols] = self.df_today[cols]
        
    def _drop_cols(self, ptn):
        cols = sh_col(self.df_days, ptn)
        self.df_days.drop(columns=cols, inplace=True)
    
    def _afterCleanse(self):
        cols = sorted([x for x in self.df_days.columns if x not in self.MST_KEYS])
        self.df_days[cols] = self.df_days[cols].astype('float32').round(3)
        self.df_days = self.df_days[[*self.MST_KEYS, *cols]]
    
    def _option(self):
        if self.isSave: self.save()
        if self.isTest: show_describe(self.df_days)
        
    def save(self):
        file_name = f"C:/GAL/Kyotei/biyori/{self.NAME}/{self.dfs['opdt']}.pkl"
        self.df_days.to_pickle(self.file_name)
    
    
def show_describe(df):
    reg = '(.*?)_\w{1}\d{1}'
    new_cols =  [x for x in df.columns if x not in MST_KEYS]
    unique_col =  np.unique([re.findall(reg, s) for s in new_cols])

    nulls = pd.DataFrame(df.isnull().sum(), columns=['NULL']).T.drop(columns=MST_KEYS).round(0)
    desc = df.describe().round(2)

    df_des = pd.concat([nulls, desc]).style.set_precision(2)
    display(df_des)
        

from inspect import signature
class CalculaterChokuzen(CalculaterBase):
    def update_dfs(self):
        df_s = self.dfs['m1']
        sopdts = self.df_days.SOPDT.unique()
        df_s = df_s[df_s.SOPDT.isin(sopdts)]
        self.dfs['s1'] = df_s.copy()
        
    def _calcEachPast(self, func, df_past, past):
        args = [df_past, self.KEYS]
        if 'min_cnt' in str(signature(func)):
            args.append(0)
            if 'is_setsu' in str(signature(func)):
                args.append(True)
        return func(*args).add_suffix(f'{self.SUFFIX}_{past}')
        
    def setNth(self, nth, df_days_tmp):
        td_nth = df_days_tmp.ENTNO_th==nth
        self.df_days = df_days_tmp[td_nth].copy()
        if nth==2:
            td_1st = df_days_tmp.ENTNO_th==1
            df_past = self.dfs['s1']
            df_1st = self.df_today[td_1st].drop(columns='ENTNO_th')
            self.dfs['s1'] = pd.concat([df_past, df_1st])
        
    def main(self):
        self.update_dfs()
        df_days_tmp = self.df_days.copy()
        
        df_all = pd.DataFrame()
        for nth in [1,2]:
            self.setNth(nth, df_days_tmp)
            self.calcMain()
            df_all = pd.concat([df_all, self.df_days])
            
        self.df_days = df_all.copy()
        
        self._afterCleanse()
        self._option()
        return self.df_days