##########################################################
# 過去データ作成に関わる関数
# - Contents
#   - データクレンズ
#   - 期特定
#   - 今季データ
#   - 枠別過去フィルター
# - 更新履歴
#   - 20200115 初版作成
##########################################################
import time
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
D_FMT = '%Y%m%d'

# TODO: PipeLineとの統合棲み分けを行う


def _create_STMin(df):

    jikoflg = ['F', 'L0', 'L1', 'K0', 'K1', 'K2']
    not_jiko = (~df['FIXPLC'].isin(jikoflg))
    df['STMIN'] = 0
    df.loc[(df.STORD==1)&not_jiko, 'STMIN'] = df['ST']
    df['STMIN'] = df.groupby('id')['STMIN'].transform('max')

    df['STMIN2nd'] = 0
    df.loc[(df.STORD==2)&not_jiko, 'STMIN2nd'] = df['ST']
    df['STMIN2nd'] = df.groupby('id')['STMIN2nd'].transform('max')
    return df


def data_cleanse(df):
    df['STMORD'] = df.groupby('id')['SHOWTM'].rank(method='min')
    df['is_yusho'] = 0
    df.loc[(df.DAYS==df.NITIME)&(df.RNO=='12'), 'is_yusho'] = 1
    df.loc[(df.RKINDCD==21)&(df.is_yusho==0), 'is_yusho'] = 1
    return _create_STMin(df)


def __filterENTNO(df, filter_):
    return df[df.ENTNO.isin(filter_)]

def all_past_data(df, opdt, filter_=[]):
    if len(filter_)>0:
        df = __filterENTNO(df, filter_)
    dfs = {}
    dfs['today'] = add_ENTNO_th(df[df.OPDT==opdt])
    dfs['y2'] = past_data(24, opdt, df)
    dfs['y1'] = past_data(12, opdt, dfs['y2'])
    dfs['m6'] = past_data(6, opdt, dfs['y1'])
    dfs['m3'] = past_data(3, opdt, dfs['m6'])
    dfs['m1'] = past_data(1, opdt, dfs['m3'])
    dfs['p0'] = konki_data(dfs['y1'], opdt, 0)
    dfs['p1'] = konki_data(df, opdt, 1)
    dfs['p2'] = konki_data(df, opdt, 2)
    dfs['opdt'] = opdt
    return dfs


# N走データ
def last_rolling(df, keys, n_roll=10):
    REMOVE = ['F', 'K0', 'K1', 'L0', 'L1', 'S0', 'S1', 'S2']
    df = df[~df.FIXPLC.isin(REMOVE)].sort_values(['OPDT', 'RNO'])
    return df.groupby(keys, as_index=False).tail(n_roll)
    
    
def rmv_jiko(df, jikoflg=[]):
    return df[~df['FIXPLC'].isin(jikoflg)]


def str2dt(opdt):
    return datetime.strptime(opdt, D_FMT)

def dt2str(opdt):
    return opdt.strftime(D_FMT)

def konki_data(df, opdt, offset=0):
    m6 = relativedelta(months=6)
    
    if offset==0:
        opdt_from = judgeSemesterStart(opdt)
        opdt_until = opdt
    else:
        offset_m = m6 * offset
        opdt_offset = dt2str(str2dt(opdt) - offset_m)
        opdt_from = judgeSemesterStart(opdt_offset)
        opdt_until = dt2str(str2dt(opdt_from) + m6)
        
    return past_data(0, opdt_from, df, opdt_until=opdt_until)


    
def past_data(month, opdt, df, opdt_until=None):
    if opdt_until is None:
        before = (datetime.strptime(opdt, D_FMT) - relativedelta(months=month)).strftime(D_FMT)
        
        ## 現在31日かつX月前が30日の月（9月等）は次月の1日とする
        if opdt[-2:]=='31' and before[-2:]=='30':
            before = (datetime.strptime(before, D_FMT) + timedelta(days=1)).strftime(D_FMT)
        
        return df[(df['OPDT']>=before)&(df['OPDT']<opdt)]
    else:
        return df[(df['OPDT']>=opdt)&(df['OPDT']<opdt_until)]
    


import numpy as np
def motor_data(df, df_today):
    rcds = np.unique(df_today['RCOURSECD'])
    df = df[df.RCOURSECD.isin(rcds)]
    
    df_m = pd.DataFrame()
    for rcd, df in df.groupby('RCOURSECD'):
        df_g = df.groupby('OPDT')[['MOTORFUKP']].mean().reset_index()[1:]
        
        if len(df_g[df_g.MOTORFUKP==0])>0:
            from_ = df_g[df_g.MOTORFUKP==0].iloc[0].OPDT
            df_m = df_m.append(df[df.OPDT>=from_])
        else:
            df_m = df_m.append(df)
    return df_m

def add_ENTNO_th(df):
    df = df.sort_values(['RCOURSECD', 'RNO'])
    df['ENTNO_th'] = df.groupby(['RCOURSECD','ENTNO']).cumcount() + 1
    return df


def add_filter_to_past(df_past, df_today, keys):
    df_past = df_past.set_index(keys)
    df_today_idx = df_today.set_index([keys[0], 'TEINO']).index.set_names(keys)
    df_past = df_past.loc[df_past.index.intersection(df_today_idx)].reset_index()
    return df_past

def judgeSemesterStart(opdt):
    year = int(opdt[0:4])
    md = int(opdt[4:])
    if md >= 1101: # 今年の11月
        m = '11'
    elif md >= 501: # 今年の5月
        m = '05'
    elif md <501: #去年の11月
        year -= 1
        m = '11'
    return str(year) + m + "01"


def create_year_data(year):
    print(year)
    file_name = f"C:/GAL/Kyotei/RACEMST/RACEMST.pkl" 
    df_m = pd.read_pickle(file_name)
    
    df_d = pd.DataFrame()
    rng = range(year-1,year+1)
    for year in rng:
        file_name = f"C:/GAL/Kyotei/RACEDTL/RACEDTL_{year}.pkl" 
        df_tmp = pd.read_pickle(file_name)
        df_d = df_d.append(df_tmp)

    df_d = df_d.drop(columns='id')
    df_m['unique_key'] = df_m['OPDT'] + df_m['RCOURSECD'] + df_m['RNO']
    df_d['unique_key'] = df_d['OPDT'] + df_d['RCOURSECD'] + df_d['RNO']
    df = pd.merge(df_m, df_d.drop(columns=['SOPDT','OPDT', 'RCOURSECD', 'RNO']), on = 'unique_key', how='inner')
    
    gc.collect()
    return df


def batch_process(year, df=None):
    if df is None:
        df = create_year_data(year)

    date_index = pd.date_range(f"{year}-01-01", f"{year}-12-31", freq="D")
    opdts = list(date_index.to_series().dt.strftime("%Y%m%d"))
    
    for opdt in opdts:
        create_days_data(df, opdt)

def batch_process_speacial(year, df):
    for opdt in opdts:
        create_days_data_special(df, opdt)