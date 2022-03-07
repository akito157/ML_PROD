##########################################################
# 日和データ計算ロジック
# - Contents
#   - calculate_fixplc 着率データ
#   - calculate_new_cocenpt 新概念
#   - calculate_point 勝率 
#   - calculate_mean 各種平均（ST、着順、展示ST） 
#   - calculate_biyori_p 日和独自Ｐ（ST、着順、展示ST） 
#   - add_cloud_data KyoteiCloudから取得したALLWINP等
# - 更新履歴
#   - 20200115 初版作成
##########################################################
import operator
import pandas as pd
import numpy as np
import time
import re
import gc
from abc import ABC, abstractmethod
from utils import *
from utils_req import pd_readSheet, pd_updateSheet
MST_KEYS = ['id', "SOPDT", "OPDT", "RCOURSECD", "RNO","ENTNO",
            "TEINO",  'MOTORNO', 'ENTNO_th']

#---------------------  Consts


class Calcs():
    def _calcEnzan(self, x1, x2, func):
        ops = { '+':operator.add, "-": operator.sub,
               "/": operator.truediv, "*": operator.mul} 
        return ops[func](self.df[x1] , self.df[x2])
    
    def _calcStd(self, x1, x2):
        return  self.df.groupby('id')[x1].transform('std')
    
    def _calcStandize(self, x1, x2):
        return self.df[x1] /self.df.groupby('id')[x1].transform('sum')
    
    def _calcRank(self, x1, x2):
        return self.df.groupby('id')[x1].transform('rank')
        
    def _calcAverage(self, x1, x2):
        if x1[0]=='[':
            xlis = re.sub("\[|\]|\s","", x1).split(',')
            if len(x2)>0:
                weight = [float(s) for s in re.sub("\[|\]|\s","", x2).split(',')]
                data = self.df[xlis]
                masked_data = np.ma.masked_array(data, np.isnan(data))
                return np.ma.average(masked_data, weights=weight, axis=1).filled(np.nan)
            
            else:
                return self.df[xlis].mean(axis=1)
        else:
            return  self.df.groupby('id')[x1].transform('mean')
            
    def _calcSum(self, x1, x2):
        if x1[0]=='[':
            xlis = re.sub("\[|\]|\s","", x1).split(',')
            sum_val = self.df[xlis].sum(axis=1)
            return sum_val
        else:
            return self.df.groupby('id')[x1].transform('sum')
        
    def _calcDiff(self, x1, x2):
        avg = self.df.groupby('id')[x1].transform('mean')
        return  self.df[x1] - avg 
        
    def _calcVSIN(self, x1, x2):
        shift = self.df.groupby('id')[x1].shift()
        return self.df[x1] - shift 
    
    def _calcVSNo1(self, x1, x2):
        no1_val = self.df.groupby('id')[x1].transform('first')
        
        if len(x2)>0:
            return no1_val - self.df[x2]
        else:
            return self.df[x1] - no1_val

    def _calcCond(self, x1, x2):
        ops = { "==": operator.eq, "!=": operator.ne, ">": operator.gt, "<": operator.lt} 
        agg, opr, val2, ins_val = re.sub("\[|\]|\s","", x2).split(',')
        cond = ops[opr](self.df.groupby('id')[x1].transform(agg), int(val2))
        return np.where(cond, int(ins_val), 0)
    
    def _calc(self, f):
        st = time.time()
        if f.func in '+-/*' : y = self._calcEnzan(f.x1, f.x2, f.func)
        if f.func=='vsin' : y = self._calcVSIN(f.x1, f.x2)
        if f.func=='vsno1' : y = self._calcVSNo1(f.x1, f.x2)
        if f.func=='std' : y = self._calcStd(f.x1, f.x2)
        if f.func=='rank' : y = self._calcRank(f.x1, f.x2)
        if f.func=='stdd' : y = self._calcStandize(f.x1, f.x2)
        if f.func=='diff' : y = self._calcDiff(f.x1, f.x2)
        if f.func=='avg' : y = self._calcAverage(f.x1, f.x2)
        if f.func=='sum' : y = self._calcSum(f.x1, f.x2)
        if f.func=='cond' : y = self._calcCond(f.x1, f.x2)
        
        self.df_new[f.y] = y
        if f.y in self.reuse_y:
            self.df[f.y] = y
        gc.collect()
        return time.time() - st
    
    
class LastCalculator(ABC, Calcs):
    MAX_COL = 24

    def __init__(self, df, file_key='col_to_use'):
        gc.collect()
        self.df = df.copy()
        self.df_new = pd.DataFrame()
        self.calcList = None
        self.race_cols = []
        self.reuse_y = []
        self.file_key = file_key
        self.df.TEINO = self.df.TEINO.astype('int')
        
    def _findValueList(self, gs, num):
        iter_ = range(0, self.MAX_COL, 2)
        return unpack_([list(gs[gs[i+1]==num][i]) for i in iter_])

    def removeColumnsBefore(self):
        gs = pd_readSheet(self.file_key, 'select', 2, set_header=False)
        not_use_col = self._findValueList(gs, '0')
        return not_use_col
        
    def removeColumns(self, use_col=['TEINO']):
        gs = pd_readSheet(self.file_key, 'select', 2, set_header=False)
        self.race_cols.extend(self._findValueList(gs, '9'))
        use_col.extend(self._findValueList(gs, '1'))
        use_col.extend(self._findValueList(gs, '9'))
        return self.df[list(set(use_col))]

        
    def __bulkCalc(self, gs, name, lis_, func):
        new_lis = [name + x for x in lis_]
        self.df_new[new_lis] = self.df[lis_] - self.df.groupby('id')[lis_].transform(func)
        
    BULK_DICT = {
        '2':['R平均差@','mean'],
        '3':['内艇差@','shift'],
        '4':['1号艇差@','first'],
    }

    def _bulkCalc(self, gs, num):
        lis_ = self._findValueList(gs, num)
        self.__checkExists(lis_, fillna=True)
        self.bulk_cols.extend(lis_)

        nums = [num] if len(num)==1 else [num[0], num[-1]]
        for n in nums:
            self.__bulkCalc(gs, self.BULK_DICT[n][0], lis_, self.BULK_DICT[n][1])

    def bulkCalc(self):
        self.bulk_cols = []
        gs = pd_readSheet(self.file_key, 'select', 2, set_header=False)
        for num in ['2', '3', '4', '4.2', '4.3']:
            self._bulkCalc(gs, num)
        
        
    def __checkExists(self, col_to_calc, fillna=False):
        isOK = True
        for col in col_to_calc:
            if col not in self.df.columns and col!="" and col[0]!='[':
                isOK = False
                print(f'{col} does not exist!')
                if fillna:
                    self.df[col] = None
                    print(f'{col} replaced with NaN')
        return isOK

    def setCalcList(self):
        self.df_new = pd.DataFrame()
        self.race_cols = []
        df = pd_readSheet(self.file_key, 'LastCalc')
        
        test = df[df['test']=='1']
        if len(test)>0:
            df = test.copy()
        else:
            df = df[df['not_use']!='1']
        self.race_cols.extend(list(df[(df['race_col']=='1')&(df['not_use']!='1')]['y']))
        col_to_calc = list(df.x1) + list(df.x2)
        self.reuse_y = [c for c in col_to_calc if c in list(df.y)]
        col_to_calc = [c for c in col_to_calc if c not in self.reuse_y]
        
        if self.__checkExists(col_to_calc):
            self.calcList = df

    def excelCalc(self):
        gc.collect()
        time_dic = {x:0 for x in np.unique(self.calcList.func)}
        for i, calc in self.calcList.iterrows():
            st = self._calc(calc)
            time_dic[calc.func] += st
            
        self.df = self.df.drop(columns=self.reuse_y)
        print(time_dic)
    
    
    

def dropCols(df):
    FILE = 'col_to_use'
    SHEET = 'drop_cols'
    gs = pd_readSheet(FILE, SHEET, 1)
    drop_ptns = list(gs.drop_ptn)
    drop_cols = []
    for drop_ptn in drop_ptns:
        drop_col = sh_col(df, drop_ptn)
        drop_cols.append(drop_col)
    gs['drop_col'] = [str(x) for x in drop_cols]
    gs['n'] = [str(len(x)) for x in drop_cols]
    pd_updateSheet(FILE, SHEET, gs)
    
    drop_list = [y for x in drop_cols for y in x]
    return drop_list
    
    
    
        
def outputCooumns(df, removeMST=True, show=True):
    cols = df.columns
    cols = sorted([x for x in cols if x not in MST_KEYS])
    
    times = ['p0', 'p1', 'p2', 'z0', 'z1', 's0', 's1', 'm0', 'm1', 'm3', 'm6', 'y1', 'a1', 'l1']
    unique_cols = np.unique([re.sub("_[a-z]{1}\d{1}","", x) for x in cols])
    

    if show:
        for x in times:
            print(x, end=", ")
        print()
        print()

        for x in cols:
            print(x, end=", ")

        print()
        print()
        for x in unique_cols:
            print(x, end=", ")
        
    return unique_cols


def calc_periods_day(df):
    arr  = list(range(1,13))
    zenki = (df.MONTH>=5)&(df.MONTH<11)
    kouki = zenki==False

    days_calc = [[zenki,4], [kouki,-2]]
    for cond, n in days_calc:
        result = arr[n:] + arr[:n]
        n_day = np.array([result.index(m) for m in df.MONTH])*30 + df.DAY
        df.loc[cond, 'NDAYS'] = n_day
    return df


def hokan():
    uni_cols = outputCooumns(df_base3, show=False)
    fillna_list = {
        '_m1':['m3', 'm6', 'y1', 'y2'],
        '_m3':['m6', 'y1', 'y2'],
        '_m6':['y1', 'y2'],
        '_y1':['y2'],
        '_p0':['p1', 'p2', 'y1', 'y2'],
        '_p1':['p2', 'y1', 'y2'],
        '_p2':['y1', 'y2'],
    }

    df_base4 = df_base3[:].copy()
    for uni_col in uni_cols[:300]:
        reg  = '^' + uni_col +  "_\w{1}\d{1}"
        cal_cols = sh_col(df_base3, reg)

        for tm, fill_vals in fillna_list.items():
            target_col = uni_col+tm

            if target_col in cal_cols:
                n_null = df_base4[target_col].isnull().sum()

                if n_null>0:
                    # print(target_col, 'start', n_null)

                    for fill_val in fill_vals:

                        # ゼロになるまでクエリ返す
                        if n_null>0:
                            fill_val_col = uni_col +'_'+fill_val
                            if fill_val_col in cal_cols:

                                df_base4[target_col].fillna(df_base4[fill_val_col], inplace=True)
                                n_null = df_base4[target_col].isnull().sum()
                                print(target_col, fill_val_col, n_null)
                        else:
                            break



        # fillna_list

        # print(df_base3[cal_cols].isnull().sum())


