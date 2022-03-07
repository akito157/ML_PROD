##########################################################
# オリジナルデータ計算ロジック
# - Contents
#   - calculate_start スタート関連データ
#   - sinnyuuhennkouji 進入変更データ
#   - calculate_tenji 展示関係
#   - condition
# - 更新履歴
#   - 20200115 初版作成
##########################################################
import pandas as pd
import numpy as np
import time
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from dataProcess_PastData import str2dt, dt2str, judgeSemesterStart
from Pipeline import numpy_where

REMOVE = ['F', 'K0', 'K1', 'L0', 'L1', 'S0', 'S1', 'S2']


def choice_not_wakunari(df_g, keys, add_prefix=False):
    if add_prefix:
        df_g = df_g.set_index('TEINO')
        df_g = df_g.dropna(how='any', axis=0)
        teinos = df_g.index.values
    else:
        teinos = df_g.index.get_level_values(keys[1])
        
    maxINC = df_g.idxmax(axis=1)
    
    def __nth(s, n):
        ss=np.sort(s)
        return np.where(s==ss[-n])[0][0] + 1
    
    def __nth_val(s, n):
        return np.sort(s)[-n]
    
    top1INC = df_g.apply(lambda s :__nth(s, 1), axis=1)
    top2INC = df_g.apply(lambda s :__nth(s, 2), axis=1)
    top1INCProb = df_g.apply(lambda s :__nth_val(s, 1), axis=1)
    top2INCProb = df_g.apply(lambda s :__nth_val(s, 2), axis=1)
    
    wakunari = teinos==top1INC
    maxINCisnotTEINO = (maxINC!=teinos)
    
    df_g.loc[wakunari, '非枠なり時進'] = top2INC
    df_g.loc[wakunari, '非枠なり時進確率'] = top2INCProb
    
    df_g.loc[top1INCProb<0.81, '非枠なり時進'] = top2INC
    df_g.loc[top1INCProb<0.81, '非枠なり時進確率'] = top2INCProb
    
    df_g.loc[maxINCisnotTEINO, '非枠なり時進'] = top1INC
    df_g.loc[maxINCisnotTEINO, '非枠なり時進確率'] = top1INCProb
    
    return df_g
    

    
def calculate_changeINC(df, keys, min_cnt=5):
    df = df[df.INFIXF==0]
    
    if 'SHCnotTEI' in keys:
        df = df[df.SHCnotTEI==1]
    
    st = time.time()
    inc_cols = ['1','2','3','4','5','6']
    
    df['TEIisINC'] = df[keys[1]]==df.INCOURSE
    df_t = df.groupby([*keys, 'TEIisINC'])['id'].count().unstack(fill_value=0)
    wakunariProb = df_t.div(df_t.sum(axis=1), axis=0)[True]
    
    df_g = df.groupby([*keys, 'INCOURSE'])['id'].count().unstack(fill_value=0)[inc_cols]
    shusso = df_g.sum(axis=1)
    
    df_g = df_g.div(df_g.sum(axis=1), axis=0)
    
    df_g = df_g.dropna(how='all', axis=0)
    
    #############
    morethanNrace= df.groupby(keys)['id'].count() > min_cnt
    df_g = choice_not_wakunari(df_g, keys)
    
    df_g['非枠なり確率'] = 1 - wakunariProb
    df_g['枠別出走回数'] = shusso
    df_g = df_g[morethanNrace]
    df_g = df_g.drop(columns=inc_cols)
    # display(df_g)
    # print(unko)
    
    return df_g


#---------------------  GET
def get_past_value(df, keys):
    COLS = ['ALLWINP', 'ALLFUKP', 'CLS']
    df_1st = df[[*keys,*COLS]].groupby(keys)[COLS].first().add_suffix('過去')
    return df_1st

#---------------------  Consts
def calculate_tenji(df, keys, min_cnt=5):
    st = time.time()
    tenji_cols = ['SHOWTM', 'STMORD', 'SHOWST', 'SSTORD', 'SHOWCOURSE', 
                  'TILT', 'WGHT', 'ADJ']
    
    df_g = df.groupby(keys)[tenji_cols]
    df_g = pd.merge(df_g.mean().add_suffix('平均'), df_g.std().add_suffix('偏差'), on=keys)

    # スピアマン
    df_c = df[~df.FIXPLC.isin(REMOVE)]
    df_c.FIXPLC = df_c.FIXPLC.replace('',0).astype(int)

    # ######################
    target = ['FIXPLC', 'STMORD', 'SSTORD', 'STORD']
    df_c[target] = df_c[target].rank()

    target = ['FIXPLC', 'STMORD', 'SSTORD']
    df_corr = df_c.groupby(keys)[target].corr(method='pearson').round(3).unstack().iloc[:,1:3]
    df_corr.columns = ['STMORD対FIX相関', 'SSTORD対FIX相関']
    df_g = pd.merge(df_g, df_corr, on=keys)

    target = ['STORD', 'SSTORD']
    df_corr = df_c.groupby(keys)[target].corr(method='pearson').round(3).unstack().iloc[:,1].rename('SSTORD対ST相関')
    df_g = pd.merge(df_g, df_corr, on=keys)
    
    return df_g[_morethanNrace(df, keys, min_cnt)]



    
def _morethanNrace(df, keys, min_cnt):
    return df.groupby(keys)['id'].count() > min_cnt

def calculate_gyakuten(df, keys, min_cnt=5):
    INIT_COLS = [*keys, 'id', 'TEINO', 'WINTECHCD', 'TANNO1', 'RTM']
    df = df[INIT_COLS]
    df.loc[df.RTM==0, 'RTM'] = None

    #---------------------  決まり手抜き
    nuki = (df.TEINO==df.TANNO1)&(df.WINTECHCD=='1')
    col = '抜き率'
    df[col] = 0
    df[col] = numpy_where(df, col, nuki, 1)

    df_g = df.groupby(keys)[['抜き率', 'RTM']].agg(
        抜き率 =('抜き率','mean'),
        抜き回数 =('抜き率','sum'),
        RTM平均 = ('RTM','mean'),
    )
    df_g['RTM平均'] = df_g['RTM平均'].round(0)
    return df_g[_morethanNrace(df, keys, min_cnt)]

    
def calculate_zenso(df, keys):
    cols = ['OPDT', 'FIXPLC', 'ST', 'GCD', 'RKINDF', 'STORD', 'TEINO']
    df = df[[*keys, *cols]]

    
    # 前走データ取得
    # df_g = df.groupby(keys).nth(-1)[cols]
    df_g = df.groupby(keys)[cols].nth(-1)

    # 前走からの日付を取得
    opdt = str2dt(df.OPDT.max()) + timedelta(days=1)
    df_g['AGO'] = (opdt - pd.to_datetime(df_g.OPDT)).dt.days
    df_g = df_g.drop(columns='OPDT')
    
    return df_g.add_prefix(f'前{1}_')



def calculate_start(df, keys, min_cnt=20):
    st = time.time()
    INIT_COLS = [*keys, 'id', 'ST', 'STORD', 'FIXPLC', 'STMIN', 'STMIN2nd']
    df = df[INIT_COLS]
    
    STORD1st = (df.STORD==1)
    STORDnot1st = (df.STORD!=1)
    
    FIX1st = df.FIXPLC=='01'
    FIX2ren = df.FIXPLC.isin(['01','02'])
    
    STMIN_diff = df.ST -df.STMIN
    df['STMIN_diff'] = STMIN_diff

    STMIN2nd_diff = df.ST -df.STMIN2nd
    Tobidashi = (STMIN2nd_diff<= -5)
    
    notDeokure = (STMIN_diff< 5)
    littleDeokure = ((5 <= STMIN_diff) & (STMIN_diff < 10))
    deokure = (10 <= STMIN_diff)
    
    # print('  start cod', time.time() -st)
    cond_list = {
        'ST飛出': STORD1st&Tobidashi,
        'ST最速': STORD1st,
        'ST安定': STORDnot1st&notDeokure,
        'ST少出遅': STORDnot1st&littleDeokure,
        'ST出遅': deokure,
        
        'ST飛出1着率': STORD1st&Tobidashi&FIX1st,
        'ST最速1着率': STORD1st&FIX1st,
        'ST安定1着率': STORDnot1st&notDeokure&FIX1st,
        'ST少出遅1着率': STORDnot1st&littleDeokure&FIX1st,
        'ST出遅1着率': deokure&FIX1st,
    
        'ST飛出2連対率': STORD1st&Tobidashi&FIX2ren,
        'ST最速2連対率': STORD1st&FIX2ren,
        'ST安定2連対率': STORDnot1st&notDeokure&FIX2ren,
        'ST少出遅2連対率': STORDnot1st&littleDeokure&FIX2ren,
        'ST出遅2連対率': deokure&FIX2ren,
    }
    
    for key, cond in cond_list.items():
        df[key] = 0
        df[key] = numpy_where(df, key, cond, 1)
    # print('  start set_cond', time.time() -st)
    
    cols = ['STMIN_diff', *list(cond_list.keys())]
    df = df[['id',*keys, *cols]].fillna(0)
    df_g = df.groupby(keys)[cols].mean()
    
    # print('  start groupby', time.time() -st)
    st_types = ['ST飛出','ST最速', 'ST安定','ST少出遅', 'ST出遅']
    for st_type in st_types:
        df_g[st_type+'1着率'] = df_g[st_type+'1着率'] /df_g[st_type]
        df_g[st_type+'2連対率'] = df_g[st_type+'2連対率'] /df_g[st_type]
    
    return df_g[_morethanNrace(df, keys, min_cnt)]
    
    
def calculate_F(df, keys):
    
    opdt_before = df.groupby(keys).OPDT.shift(1)
    df['OPDT_INTERVAL'] = (pd.to_datetime(df.OPDT) - pd.to_datetime(opdt_before) ).dt.days
    
    
    opdt = dt2str(str2dt(df.OPDT.max()) + timedelta(days=1))
    opdt_zenki = dt2str(str2dt(df.OPDT.max()) - relativedelta(month=6))
    
    zenki_start = judgeSemesterStart(opdt_zenki)
    konki_start = judgeSemesterStart(opdt)
    
    df_z = df[(df.OPDT<konki_start)&(df.OPDT>=zenki_start)]
    df_F_z = df_z[df_z.FIXPLC=='F'][['OPDT', 'ENTNO']].rename(columns={'OPDT':'FOPDT'})
    
    
    df_F_z['F前期累計'] = df_F_z.groupby(keys)['ENTNO'].transform(lambda x: x.count())
    df_F_z['F前期Nth'] = df_F_z.groupby(keys)['ENTNO'].cumcount() + 1
    
    
    df_F_z = pd.merge(df_F_z, df_z, on=['ENTNO'], how='left')
    
    afterF = df_F_z.FOPDT < df_F_z.OPDT
    yasumi30 = df_F_z['OPDT_INTERVAL'] >= 30
    
    F1 = df_F_z['F前期Nth']==1
    TOTALF1 = df_F_z['F前期累計']==1
    
    df_F_z = df_F_z[afterF]
    df_F1_shoka = df_F_z[F1&yasumi30][['ENTNO', 'F前期Nth', 'OPDT']].rename(columns={'OPDT':'F1消化日'})
    
    
    # df_tmp = df_F_z[['ENTNO', 'F前期Nth', 'F1消化日']].drop_duplicates().dropna(axis=0)
    # df_tmp = df_F_z[['ENTNO', 'F前期Nth', 'F1消化日']].drop_duplicates().dropna(axis=0)
    
    df_F_z = pd.merge(df_F_z, df_F1_shoka, on=['ENTNO', 'F前期Nth'], how='left')
    
    cols = ['ENTNO', 'OPDT', 'FOPDT', 'F前期累計', 'F前期Nth', 'F1消化日', 'OPDT_INTERVAL']
    
    
    F1Mishoka = df_F_z['F1消化日']!=df_F_z['F1消化日']
    
    
    F1Shokazumi = df_F_z['F1消化日'] is not None
    
    # afterFshoka = df_F_z['F1消化日'] <= df_F_z.OPDT
    
    # df_F_z = df_F_z[~(TOTALF1)&F1Shokazumi]
    
    df_F_z = df_F_z[TOTALF1&F1Mishoka]
    
    # df_F_z = df_F_z[~(F1&beforeFshoka)]
    display(df_F_z[cols].sort_values(['ENTNO', 'OPDT']))
    # df_F_z = df_F_z[~(F1)&afterFshoka]
    
    
    # df_F_z = df_F_z[F1&yasumi30]
    
    
    display(df_F_z)
    
    # display(df_F_z['F前期Nth'].value_counts())
    display(df_F_z[['F前期Nth', 'F前期累計']].value_counts())
    
    
    # tmp
    F_cnt_zenki = F_cnt_zenki[F_cnt_zenki==1]
    
    # 前期
    df_F_player = pd.merge(F_cnt_zenki, df_zenki, on='ENTNO', how='left')
    display(df_F_player)
    
    
    # df_F = pd.merge(F_cnt_konki, F_cnt_zenki, on='ENTNO', how='outer')
    
    
    
    #############　今期
    konki_start = judgeSemesterStart(opdt)
    df_konki = df[df.OPDT>=konki_start]
    df_F_konki = df_konki[df_konki.FIXPLC=='F'][['id', 'OPDT', 'ENTNO']]
    F_cnt_konki = df_F_konki.groupby(keys)['id'].count().rename('F今期')
    
    display(df_F)
    
    display(df_F)
    
    
    display(F_cnt_zenki.value_counts(dropna=False))
    
    
    
    display(df_F_konki)
    display(df_F_zenki)
    
# import statsmodels.api as sm