##########################################################
# データ加工メイン
# - Contents
# - 更新履歴
#   - 20200115 初版作成
##########################################################
from concurrent import futures
future_list = []
import os
import sys
import time
import pandas as pd
import gc
from utils_req import notify_line

from dataProcess import *
from Pipeline import _create_conditional_tenji
from dataProcess_Calculator import *
from dataProcess_Calculator import *
MST_KEYS = ['id', "SOPDT", "OPDT", "RCOURSECD", "RNO","ENTNO", "TEINO", "WEATHERCD", 'MOTORNO']

#---------------------  Consts
args = sys.argv
if len(args)==4:
    from_ = int(args[1])
    until = int(args[2])
    is_con = args[3]
else:
    from_ = int(args[1])
    until = int(args[1])
    is_con = int(args[2])

if is_con=='1':
    is_concurrent = True
else:
    is_concurrent = False
    

def bulk_update(opdt, df_base):
    
    # name = 'tenji'
    # name = 'sinnyu'
    name = 'conditional'
    Calculaters = [
        # KihonCalculater,
        # WakuCalculater,
        # SetsuCalculater,
        # INCOURSECalculater,
        ConditionCalculater,
        # TenjiCalculater,
        # TimeCalculater,
        # JyoCalculater,
        # MotorCalculater,
    ]
    
    file_name = f"C:/GAL/Kyotei/biyori/{name}/{opdt}.pkl"
    
    # if os.path.exists(file_name) is False:
    if True:
        print(opdt)
        df = data_cleanse(df_base)
        dfs = all_past_data(df, opdt)
        df_days = dfs['today'][[*MST_KEYS, 'ENTNO_th']]


        for Calculater in Calculaters:
            Calculater(df_days, dfs, isSave=True).main()
        gc.collect()

    

def main():
    st = time.time()
    opdts = []
    for year in range(from_,until+1):
        date_index = pd.date_range(f"{year}-01-01", f"{year}-12-31", freq="D")
        opdts.extend(list(date_index.to_series().dt.strftime("%Y%m%d")))
        
    df = pd.read_pickle('df_base.pkl')
    
    if True:
        df_tenji = pd.read_pickle('df_tenji.pkl')
        df = df.drop(columns=['WEATHERCD', 'WINDPOWER', 'WAVE', 'WGHT', 'SHOWTM'])
        df = pd.merge(df, df_tenji, on=['id', 'TEINO', 'OPDT', 'RCOURSECD', 'RNO'])
        df = _create_conditional_tenji(df)
    
    if is_concurrent:
        print('並列稼働')
        with futures.ThreadPoolExecutor(max_workers=16) as executor:
            for opdt in opdts:
                future = executor.submit(bulk_update, opdt, df)
                future_list.append(future)
                gc.collect()
    else:
        print('単稼働')
        for opdt in opdts:
            bulk_update(opdt, df)
            
            if opdt[4:] in ["0101", '0110', '0201','0301', '0401', '0701', '1001', '1231']:
                cst = time.time() - st
                # message = opdt[:4] + ' ' + str(round(cst))
                message = f'{opdt[:4]} {opdt[4:]} {str(round(cst))}}}'
                notify_line(message)

    print(st - time.time())


if __name__ == "__main__":
    # try:
    main()
    # except Exception as e:
        # notify_line(e)
        
    