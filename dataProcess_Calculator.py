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
import pandas as pd
import numpy as np
import time

# TODO: 高速化を検討する

#---------------------  Consts
# 勝率
FIXPLCS= ['01', '02', '03', '04', '05', '06']
IP_IPPAN = [10,8,6,4,2,1]
IP_YUSHO = [11,9,7,6,4,3]

G1_IPPAN = [11,9,7,5,3,2]
G1_YUSHO = [12,10,8,7,5,4]
SG_IPPAN = [12,10,8,6,4,3]
SG_YUSHO = [13,11,9,8,6,5]

TRIAL = [10,9,7,6,5,4]
DREAM = [12,10,9,7,6,5]
TOKUSEN = [11,9,7,5,3,2]

    
#---------------------  CALC
def calculate_fixplc(df, keys, min_cnt=10):
    
    FIX_PLC_CATEGORIES= ['01', '02', '03', '04', '05', '06', 'S0', 'S1', 'S2', 'F', 'L0', 'L1', 'K0', 'K1']
    order_cols = set(list(df.FIXPLC.unique())) & set(FIX_PLC_CATEGORIES)
    df_t3 = df.groupby([*keys, 'FIXPLC'])['id'].count().unstack(fill_value=0)[order_cols]
    df_t3 = df_t3.reindex(columns=FIX_PLC_CATEGORIES)
    df_t3 = df_t3.fillna(0)
    
    df_t4 = pd.DataFrame()
    df_t4['出走回数'] = df_t3.sum(axis=1)
    df_t4['1着率'] = df_t3['01']
    df_t4['2着率'] = df_t3['02']
    df_t4['3着率'] = df_t3['03']
    df_t4['2連対率'] = df_t3['01'] + df_t3['02']
    df_t4['3連対率'] = df_t3['01'] + df_t3['02'] + df_t3['03']
    df_t4['5位以下率'] = df_t3['05'] + df_t3['06']
    df_t4['F回数'] = df_t3['F']

    if 'INCOURSE' in keys:
        # se_sum = (df_t4['出走回数'] - df_t3['S0'] - df_t3['F'])
        se_sum = (df_t4['出走回数'] - df_t3['S0'] - df_t3['F'] - df_t3['S1'])
    else:
        se_sum = (df_t4['出走回数'] - df_t3['S0'] - df_t3['F'] - df_t3['S1'])
        
    RATE_COLS = [x for x in df_t4.columns if x not in ('F率','出走回数')]
    for x in RATE_COLS:
        df_t4[x] = df_t4[x] / se_sum

    if df_t4['出走回数'].max()!=10:
        morethan10 = df_t4['出走回数']>=min_cnt
        df_t4 = df_t4[morethan10]
    return df_t4[RATE_COLS]




def calculate_point(df, keys, is_setsu=False, min_cnt=10):
    if is_setsu:
        df['GCD'] = ['T' if x==11 and y[4:6]=='12' else z for x,y,z in zip(df.RKINDF, df.OPDT, df.GCD)]
        df['GCD'] = ['D' if x==10 else z for x,z in zip(df.RKINDF, df.GCD)] 
        df['GCD'] = ['S' if x in (6,7) else z for x,z in zip(df.RKINDF, df.GCD)] #特選
    
    GCDs = pd.Index([0,1,2,3,4, 'T', 'D', 'S'], name='GCD')
    is_yushos = pd.Index([0,1], name='is_yusho')
    fixplcs = set(list(df.FIXPLC.unique())) & set(FIXPLCS)
    
    # データ整形
    grp_col = ['GCD', 'is_yusho', *keys, 'FIXPLC']
    a = df.groupby(grp_col)['id'].count().unstack(fill_value=0)[fixplcs].reindex(columns=FIXPLCS)
    a = a.reindex(pd.MultiIndex.from_product([GCDs, is_yushos, *a.index.levels[2:]]), fill_value=0)
    df_time = df[keys].drop_duplicates().set_index(keys)
    df_point = df_time.copy()
    
    g1 = a.loc[(1,)] + a.loc[(2,)]
    ip = a.loc[(3,)] + a.loc[(4,)]
    sg = a.loc[(0,)]

    # 得点計算表
    point_hyo = {
        'IP_IP':{'hyo':IP_IPPAN,  'data':ip.loc[0]},
        'IP_YU':{'hyo':IP_YUSHO,  'data':ip.loc[1]},
        'G1_IP':{'hyo':G1_IPPAN,  'data':g1.loc[0]},
        'G1_YU':{'hyo':G1_YUSHO,  'data':g1.loc[1]},
        'SG_IP':{'hyo':SG_IPPAN,  'data':sg.loc[0]},
        'SG_YU':{'hyo':SG_YUSHO,  'data':sg.loc[1]},
        'TRIAL':{'hyo':TRIAL,     'data':a.loc[('T',)].loc[0]},
        'DREAM':{'hyo':DREAM,     'data':a.loc[('D',)].loc[0]},
        'TOKUSEN':{'hyo':TOKUSEN, 'data':a.loc[('S',)].loc[0]},
                         }
    # 勝率計算
    for key, value in point_hyo.items():
        df_time[key] = value['data'].sum(axis=1)
        df_point[key] = (value['data'] * value['hyo']).sum(axis=1)
        
    df_time['all'] = df_time.sum(axis=1)
    df_point['all'] = df_point.sum(axis=1)
    df_result = (df_point/df_time).dropna(axis=0, how='all')
    
    if df_time['all'].max()!=10:
        morethan10 = df_time['all']>=min_cnt
        df_result = df_result[morethan10]
    return df_result[['all']]


def _init_mean_std(df, keys, target_col):
    INIT_COLS = list(set(['id', *keys, *target_col]))
    TO_INT =  ['TEINO', 'FIXPLC', 'INCOURSE']
    df[TO_INT] = df[TO_INT].replace('', 0).apply(np.int32)
    return df[INIT_COLS]

def __check10(df, keys, df_g, min_cnt):
    cnt = df.groupby(keys)['id'].count()
    if cnt.max()!=10:
        return df_g[cnt>=min_cnt]
    else:
        return df_g


def calculate_mean(df, keys, min_cnt=10):
    TARGET_COL = ['TEINO', 'FIXPLC', 'INCOURSE', 'STORD', 'ST']
    df = _init_mean_std(df, keys, TARGET_COL)

    target = list(set(TARGET_COL) - set(keys))
    df_g = df.groupby(keys)[target].mean().add_suffix('平均')
    return __check10(df, keys, df_g, min_cnt)

def calculate_std(df, keys, min_cnt=10):
    TARGET_COL = ['TEINO', 'FIXPLC', 'INCOURSE', 'STORD', 'ST']
    df = _init_mean_std(df, keys, TARGET_COL)

    std_col = list(set(TARGET_COL) - set(keys))
    df_g = df.groupby(keys)[std_col].std().add_suffix('偏差')
    return __check10(df, keys, df_g, min_cnt)


def calculate_biyori_p(df, keys, min_cnt=10):
    calc_col = ['FIXPLC', 'INCOURSE']
    df[calc_col] = df[calc_col].apply(np.int32)
    df_g = df[[*keys, *calc_col]].groupby(keys).mean()
    df_g['順位P'] = df_g['INCOURSE'] - df_g['FIXPLC']
    return __check10(df, keys, df_g[['順位P']], min_cnt)


def calculate_new_concept(df, keys, min_cnt=5):
    tech_cols = ['id', 'ENTNO', 'RCOURSECD', 'TANNO1', 'TEINO', 'WINTECHCD', keys[1]]
    
    df = df[tech_cols]
    teino = df['TEINO']
    incourse = df[keys[1]]
    no1 = df['TANNO1']
    wintech = df['WINTECHCD']
    
    sashi = (no1==teino) & (wintech=="4")
    makuri = (no1==teino) & (wintech=="3")
    makurizashi = (no1==teino) & (wintech=="5")
    
    ###########
    df.loc[(teino=='1') & (no1==teino), '逃げ'] = 1
    df.loc[(teino=='1') & (no1!=teino) & ((wintech=="4")|(wintech=='5')), '差れ'] = 1
    df.loc[(teino=='1') & (no1!=teino) & (wintech=="3"), '捲れ'] = 1
    df.loc[(teino=='1'), '出走回数'] = 1

    df.loc[(incourse=='2') & (no1!=teino) & (wintech=="2"), '逃し'] = 1

    for no in ['2', '3', '4', '5', '6']:
        df.loc[(teino==no), '出走回数'] = 1
        df.loc[(teino==no) & sashi, '差し'] = 1
        df.loc[(teino==no) & makuri, '捲り'] = 1
        df.loc[(teino==no) & makurizashi, '捲差'] = 1

        
    #########
    df_g = df.groupby(keys).count().reset_index()
    df_f = df_g[keys].copy()

    NEW_COLS = [x for x in df_g.columns if x not in ['出走回数', *tech_cols]]
    df_f[NEW_COLS]  = df_g[NEW_COLS].copy()
    df_f['出走回数'] = df_g['出走回数']

    for x in NEW_COLS:
        df_f[x+"率"] =df_f[x] / df_g['出走回数']
    df_new = df_f.fillna(0)

    cnt = df_new['出走回数']
    if cnt.max()!=10 or cnt.max()!=20:
        df_new = df_new[cnt>=10]
        
    df_new = df_new.set_index(keys)
    del_col = [x for x in df_new.columns if '出走回数' in x ]
    df_new = df_new.drop(columns=del_col)
    return df_new.add_prefix('新_')



    

def add_cloud_cols(df_days, df_today):
    cloud_col_d = {
                   'ENTNO':'ENTNO', 
                   'RNO':'RNO', 
                   'ALLWINP':'all_a1', 
                   'LCLWINP':'all_l1', 
                   'ALLFUKP':'2連対率_a1', 
                   'LCLFUKP':'2連対率_l1',
                  }
    cloud_col = list(cloud_col_d.keys())
    
    df_cloud = df_today[cloud_col]
    df_cloud[['ALLWINP', 'LCLWINP']] = df_cloud[['ALLWINP', 'LCLWINP']]/100
    df_cloud[['ALLFUKP', 'LCLFUKP']] = df_cloud[['ALLFUKP', 'LCLFUKP']]/10000
    
    df_days = pd.merge(df_days, df_cloud, on=["ENTNO", 'RNO'], how="left")
    df_days.rename(columns=cloud_col_d, inplace=True)
    return df_days
