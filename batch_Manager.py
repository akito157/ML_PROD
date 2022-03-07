##########################################################
# データバッチ運用見据えて
# - Contents
# - 更新履歴
#   - 20220220 初版作成
##########################################################
from ast import Constant
from numpy import disp
from torch import batch_norm_elemt
from batch_Past import *
from batch_Scraper import *
from batch_Day import *
from batch_Lake import *
from batch_MergeCalc import *
from batch_Predict import *
import gc


class CONSTS():
    MODEL_SHEET =  ('choose_model', 'models')
    BATCH_ALL =  {
        'PastBase'  : PastBaseCreator,
        'PastTenji'  : PastTenjiCreator,
        'PastMerge'  : PastMerger,

        'LakeDaily'  : LakeCreatorDaily,
        'LakeChokuzen'  : LakeCreatorChokuzen,
        'DataMerger'  : DataMerger,
        'DataCalc'  : DataCalculater,
        'predict'  : Predicter,
        'betPicker'  : BetPicker,
        'result'  : ResultCalculater,
    }

class GspreadReader(CONSTS):
    def read_modelSheet(self):
        gs  = pd_readSheet(*self.MODEL_SHEET)
        self.modelSheet = gs[gs['is_run']=='1']

        
class BatchExecuterBase(CONSTS):
    NOTIFY_FILE = 'df_result.pkl'
    def __init__(self, batch_info, opdt=None):
        gc.collect()
        self.batch_info = batch_info
        self.mid = batch_info.model_id
        self.bid = batch_info.batch_id
        self.opdt = opdt
        self.batch_list = self.setBatch()

    def FILTER(self):
        pass

    def setBatch(self):
        batch_list = []
        set_mid = self.mid
        for col, CLASS_ in self.BATCH_ALL.items():
            if self.batch_info[col]=='':
                continue
            if self.batch_info[col]=='1':
                if col in self.FILTER:
                    batch_list.append(CLASS_(self.opdt, set_mid, batch_id=self.bid))
                set_mid = self.mid
            else:
                set_mid = self.batch_info[col]
        print(f'batch_id {self.bid}: n_batch:{len(batch_list)}')
        return batch_list

    def _notify(self):
        file_path = f'{"pipe_data"}/{self.NOTIFY_FILE}'
        self.df_notify = pd.read_pickle(file_path)
        notify_dataframe(self.df_notify)


class BatchExecuterMorning(BatchExecuterBase):
    FILTER = ['LakeDaily']

class BatchExecuterEachRace(BatchExecuterBase):
    FILTER = ['LakeChokuzen', 'LastCalc', 'predict']

class BatchExecuterAll(BatchExecuterBase):
    FILTER = list(CONSTS.BATCH_ALL.keys())

class BatchManager(GspreadReader):
    def __init__(self, opdt):
        self.opdt = opdt

    def setMorning(self):
        self.BEs = []
        for i, batch_info in self.modelSheet.iterrows():
            self.BEs.append(BatchExecuterMorning(batch_info, self.opdt))

    def setEachRace(self):
        self.BEs = []
        for i, batch_info in self.modelSheet.iterrows():
            self.BEs.append(BatchExecuterEachRace(batch_info, self.opdt))

    def setAll(self):
        self.BEs = []
        for i, batch_info in self.modelSheet.iterrows():
            self.BEs.append(BatchExecuterAll(batch_info, self.opdt))

    def executeBatch(self):
        for BE in self.BEs:
            for cls_ in BE.batch_list:
                cls_.main()
            BE._notify()


D_FMT = '%Y%m%d'
class BatchPast():
    Batchers =[
        PastTenjiScraper,
        PastTenjiConverter,

        PastBaseCreator,
        PastTenjiCreator,
        PastTenjiCreator,
        PastMerger,
    ]

    def __init__(self, opdt):
        # opdt = (datetime.strptime(opdt, D_FMT) - timedelta(days=1)).strftime(D_FMT)
        self.opdt = opdt 

    def main(self):
        for Batcher in self.Batchers:
            Batcher(self.opdt).main()



class BatchDay():
    Batchers =[
        DayCreator,
        LakeCreatorDaily,
        LakeCreatorMotor,
    ]

    def __init__(self, opdt):
        self.opdt = opdt

    def main(self):
        for Batcher in self.Batchers:
            Batcher(self.opdt, self.mid).main()



class BatchEach():
    Batchers =[
        DayCreatorChokuzen,
        LakeCreatorChokuzen,
        LakeCreatorJyo,

    ]
    def __init__(self, opdt):
        self.opdt = opdt


    def main(self):
        for Batcher in self.Batchers:
            Batcher(self.opdt).main()


class BatchPredict():
    Batchers =[
        DataMerger,
        DataCalculater,
        Predicter,
    ]

    def __init__(self, opdt, mid):
        self.opdt = opdt
        self.mid = mid

    def main(self):
        for Batcher in self.Batchers:
            Batcher(self.opdt, self.mid).main()




class BatchResult():
    def __init__(self, opdt, mid, isChokuzen=True):
        self.opdt = opdt
        self.mid = mid
        self.isChokuzen =isChokuzen

    def main(self):
        try:
            # Expecter(self.opdt, self.mid).main()
            Expecter(self.opdt, self.mid, isChokuzen=self.isChokuzen).main()
            for bet_thresh in [1, 1.5, 2]:
                BetPicker(opdt=self.opdt, mid=self.mid, bet_thresh=bet_thresh).main()
            ResultCalculater(self.opdt, self.mid).main()
        except:
            pass


def show_all_result(pipe='result'):

    df = pd.concat(map(pdreadpickle,glob.glob(f'pipe_data/{pipe}/*.pkl')))

    return df
