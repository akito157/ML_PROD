##########################################################
# バッチ予測
##########################################################
import consts as con
from abc import ABC, abstractmethod
from Pipeline import *
from utils import unpack_
from utils_req import pd_readSheet
import gc
from ml_utils import engine
from ml_utils import load_model
from batch_Base import BatchBase


class Predicter(BatchBase):
    INPUT_FILE = con.PIPE_TEST
    OUTPUT_FILE = con.PIPE_PREDICT

    def __init__(self, opdt=None, mid=None):
        super().__init__(opdt, mid)
        self.model = load_model(mid)

    def _preCleanse(self):
        df = self.df_in.reset_index(drop=True)
        KEYS = ['id', 'SOPDT', 'RCOURSECD', 'RNO']
        df[KEYS] = df[KEYS].astype(int)
        self.X = df.drop(columns=[*Y_COLS, 'id'])

    def _predict(self):
        pred = self.model.predict(self.X)
        df = self.df_in[['id']].reset_index(drop=True)
        df_prob = pd.DataFrame(pred)
        self.df_out = pd.concat([df, df_prob], axis=1)

    def _main(self):
        self._preCleanse()
        self._predict()


class Expecter(BatchBase):
    INPUT_FILE = con.PIPE_PREDICT
    OUTPUT_FILE = con.PIPE_EXPECT
    BET_TYPE = 'TANNO1'

    QUERY_FMT = '''
        select
            t_ODDS2.id,
            t_ODDS2.RACEMST_id,
            t_RETURN.bet_type,
            t_RETURN.comb,
            t_RETURN.odds,
            t_ODDS2.odds as fixed_odds
        from t_RETURN
        left join t_ODDS2
            on t_RETURN.ODDS_id = t_ODDS2.id
        left join RACEMST
            on t_ODDS2.RACEMST_id = RACEMST.id
        where t_RETURN.bet_type='{type_}'
            and RACEMST.OPDT ="{op}"
    '''.format

    COLS = ['id', 'bet_type', 'prob', 'comb', 
            'odds', 'expected_return',
            'fixed_odds', 'fixed_expected_return'
            ]

    def __init__(self, opdt=None, mid=None, isChokuzen=True):
        if isChokuzen:
            self.odds_table = 't_RETURN'
        else:
            self.odds_table = 't_ODDS2'
        super().__init__(opdt, mid)

    def _readOdds(self):
        # sql = self.QUERY_FMT(op=self.opdt, type_=self.BET_TYPE)
        # sql = self.QUERY_FMT(odds_table=self.odds_table, op=self.opdt, type_=self.BET_TYPE )
        sql = self.QUERY_FMT(op=self.opdt, type_=self.BET_TYPE )
        self.df_odds = cx.read_sql(engine_cx, sql).drop(columns='id')
        self.df_odds = self.df_odds.rename(columns={'RACEMST_id':'id'})

    def __mergeProbOdds(self):
        df_c = self.df_in.melt(id_vars='id', var_name='comb', value_name='prob')
        df_c['comb'] = (df_c['comb'] +1).astype(str)
        return pd.merge(df_c,  self.df_odds , on=['id','comb'])

    def _calcExpectedReturn(self):
        df_r = self.__mergeProbOdds()
        df_r['expected_return'] = df_r['odds'] * df_r['prob']
        df_r['fixed_expected_return'] = df_r['fixed_odds'] * df_r['prob']
        self.df_out = df_r[self.COLS]

    def _main(self):
        self._readOdds()
        self._calcExpectedReturn()


class BetPicker(BatchBase):
    INPUT_FILE = con.PIPE_EXPECT
    OUTPUT_FILE = con.PIPE_BET

    def __init__(self, opdt=None, mid=None, bet_thresh=None):
        super().__init__(opdt, mid)
        self.bet_thresh = bet_thresh
        if self.bet_thresh is None:
            self.bet_thresh = self._getThresh()

    def _saveOutput(self):
        repr = f'_{str(int(self.bet_thresh*100))}.pkl'
        self.output_file = self.output_file.replace('.pkl', repr)
        self.df_out.to_pickle(self.output_file)


    def _getThresh(self):
        model_prop = self._readModelSheetProperty(self.bid)
        self.bet_thresh = int(model_prop['bet_thresh'][0]) /100

    def _setThresh(self):
        overThresh = self.df_in['expected_return']>self.bet_thresh
        overThreshFixed = self.df_in['fixed_expected_return']>self.bet_thresh

        self.df_out =  self.df_in[overThresh|overThreshFixed]
        self.df_out.loc[overThresh, 'bet_thresh'] = 1
        self.df_out.loc[overThreshFixed, 'fixed_bet_thresh'] = 1

    def _main(self):
        self._setThresh()
        # display(self.df_out)
        # display(self.df_out[['bet_thresh', 'fixed_bet_thresh']].value_counts(dropna=False))


# TODO: bidではなく。。。。日時単位で実行できるようにする
# TODO: df_styleして表示する
class ResultCalculater(BatchBase):
    INPUT_FILE = con.PIPE_BET
    OUTPUT_FILE = con.PIPE_RESULT
    COLS = ['id', 'TANNO1', 'TANRFD1']
    QUERY_FMT = 'SELECT * FROM RACEMST WHERE OPDT="{op}"'.format
    OUTPUT_COLS =['OPDT', '閾値',
        '払戻獲得率', '全的中払戻率' ,'回収率', '投票的中率',
        '母数', '投票数', '的中数', '投票率', 
        ]

    def _readInput(self):
        import glob
        file_ptn = self.input_file.replace('.pkl', '*.pkl')
        self.df_in = pd.concat([pd.read_pickle(f) for f in glob.glob(file_ptn)])

    def _loadActualResult(self):
        self.actual = cx.read_sql(engine_cx, self.QUERY_FMT(op=self.opdt))[self.COLS]
        self.actual['TANRFD1'] = self.actual['TANRFD1'].replace('', 0).fillna(0).astype(int)
        self.n_test = len(self.actual)
        self.all_return = round(self.actual.TANRFD1.sum() /self.n_test, 2)

    def __setFlg(self, df_r):
        df_r[['is_hit', 'actual_return']] = None
        df_r.loc[(df_r['comb']==df_r['TANNO1']), 'is_hit'] = True
        df_r.loc[df_r['is_hit']==True, 'actual_return'] = df_r['TANRFD1']
        return df_r

    def _calculateResult(self):
        self.result = pd.merge(self.df_each, self.actual, on='id', how='left')
        self.result = self.__setFlg(self.result)
        self.n_vote = len(self.df_each)
        self.n_hit = self.result['is_hit'].sum()
        
        try:
            self.vote_accuracy = round(self.n_hit / self.n_vote, 3)
            self.return_ = round(self.result['actual_return'].sum() / self.n_vote, 2)
        except:
            self.vote_accuracy = 0
            self.return_ = 0

    def _toDataFrame(self):
        val = [ self.opdt, self.df_each.bet_thresh.values[0],
                round(self.return_/self.all_return,3), self.all_return, self.return_, self.vote_accuracy,
                self.n_test, self.n_vote, self.n_hit, round(self.n_vote /self.n_test, 3)
        ]
        se = pd.Series(val, index=self.OUTPUT_COLS, name=self.bid)
        self.df_out = self.df_out.append(se)


    def _main(self):
        self.df_out = pd.DataFrame(columns=self.OUTPUT_COLS)
        self._loadActualResult()
        for bet_thresh, df_input in self.df_in.groupby('bet_thresh'):
            self.df_each = df_input
            self._calculateResult()
            self._toDataFrame()
