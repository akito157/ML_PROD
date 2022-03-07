##########################################################
# データ加工の基礎となる部分
##########################################################
import consts as con
from Pipeline import *
from dataProcess_PastData import *
from dataProcess import *
from batch_Base import BatchBase
from abc import ABC, abstractmethod
from multiprocessing import Pool, dummy
from dateutil.relativedelta import relativedelta
from utils import filter_opdt

class LakeCreatorBase(BatchBase):
    OUTPUT_FILE = ''
    FILTER_COL = []
    OFFSET = relativedelta(months=25)
    DROP_COLS = [
        'HGHT', 'BMI', 'AGE', 'NKI','FUKNO1', 'BOATNO', 'YEAR', 'MONTH', 'DAY', 
    ]

    def __init__(self, opdt=None, mid=None, isPool=False):
        super().__init__(opdt, mid)
        self.isPool  = isPool
        self.opdt_from  = dt2str(str2dt(self.opdt) - self.OFFSET)
        self.past_path = [con.SAVE_PATH_FMT(tbl=con.GAL_PAST, y=y) for y in range(self.year-2, self.year+1)]
        self.output_paths = []

    @abstractmethod
    def CALCULATERS():
        pass

    def _filter(self, df):
        return df[df[self.FILTER_COL].isin(np.unique(self.df_in[self.FILTER_COL].values))]

    def _readPast(self):
        USE_COL = list(set(self.df_in.columns) - set(self.DROP_COLS))
        self.df_in = self.df_in[USE_COL]
        self.past = filter_opdt(pd.concat(map(pdreadpickle, self.past_path))[USE_COL], self.opdt_from, self.opdt)

    def __mergeTodayPast(self):
        return pd.concat([self.df_in, self.past]).reset_index(drop=True)
 
    def _createPast(self):
        self._readPast()
        all_past = self._filter(data_cleanse(self.__mergeTodayPast()))
        self.dfs = all_past_data(all_past, self.opdt)
        self.df_days = self.dfs['today'][[*MST_KEYS]]

    def __poolExecute(self, Classes):
        self._info('Pooly Excute')
        # with dummy.Pool(len(Classes)) as p:
        with Pool(len(Classes)) as p:
            p.map(excute, Classes)

    def __eachExecute(self, Classes):
        for Cls in Classes:
            Cls.main()
            self._info(Cls.__class__.__name__, Cls.df_days.shape)
            self.output_paths.append(Cls.file_name)

    def _excuteLakeCreator(self):
        Classes = [Cls(self.df_days, self.dfs, isSave=self.ISSAVE) for Cls in self.CALCULATERS]
        if self.isPool:
            self.__poolExecute(Classes)
        else:
            self.__eachExecute(Classes)
        
    def _main(self):
        self._createPast()
        self._excuteLakeCreator()
        

class LakeCreatorDaily(LakeCreatorBase):
    INPUT_FILE = con.PIPE_DAYS
    FILTER_COL = 'ENTNO'
    CALCULATERS = [
        KihonCalculater,
        WakuCalculater,
        TimeCalculater,
    ]



class LakeCreatorMotor(LakeCreatorDaily):
    CALCULATERS = [
        MotorCalculater,
    ]

    def _filter(self, df):
        return motor_data(df, self.df_in)


class LakeCreatorChokuzen(LakeCreatorBase):
    INPUT_FILE = con.PIPE_CHOKUZEN
    FILTER_COL = 'ENTNO'
    CALCULATERS = [
        SetsuCalculater,
        ConditionCalculater,
        INCOURSECalculater,
        TenjiCalculater,
    ]
    
    def __init__(self, opdt, mid=None, rcd=None, rno=None, isPool=False):
        super().__init__(opdt, mid, isPool)
        self.rcd = rcd
        self.rno = rno


class LakeCreatorJyo(LakeCreatorChokuzen):
    FILTER_COL = 'RCOURSECD'
    CALCULATERS = [
        JyoCalculater,
    ]


def excute(Cls):
    print('# execute', Cls.__class__.__name__)
    Cls.main()
