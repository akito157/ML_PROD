
IP_CONFIRM_URL = "https://www.cman.jp/network/support/go_access.cgi"
LINE_NOTIFY_API = 'https://notify-api.line.me/api/notify'
LINE_BOAT_ACCESS_TOKEN = '757537pt1KWnIJN3uTqywE0xd9IJRQlsX3vyTspbMZc'
LINE_ERROR_ACCESS_TOKEN = 'ASzxhZXDQIMEiaQwrx6Xmc3Q04AuakUPVz7aDc01YBU'

SCOPE = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
JSON_PATH = "json/river-bedrock-161104-fd3420f65861.json"
GSPREAD = {
    'Memo': '1HuYpMDk5s704hv2_SRcZZGUIZB1MYw0UcbZFdS6mVYo',
    'col_to_use': '1JajPn1YT3yZWjToqAJhVH4UeC9zW0yu3MBy_CVgLKzA',
    'TESTcol_to_use': '1tlZbwfSbOv-4YWO_cpNJsbIpaIJ1FQmGPq0lzg07y0Q',
    'choose_model': '1JuVMMJ75-kYvFrLtNNePLCJCEYewd_Nj_kcd7zri9Kc',
    # 'col_to_use_0220_MINIMUM_188':'1iwiDtHis7BJoq-6iK-ASOPFY5CXgQ0KYSRl4o4pYhy8',
    'col_to_use_0220_MINIMUM_188': '1reYbUmCxF5gRMm_jRoJaCtt2uXf4htoLxpZVk3BVQTo',
}


QUERY_OPDT_FMT = 'SELECT * FROM {tbl} WHERE OPDT>="{op1}" and OPDT<="{op2}"'.format
QUERY_YEAR_FMT = 'SELECT * FROM {tbl} WHERE OPDT>="{y}0000" and OPDT<="{y}9999"'.format

LIST_TENJI_PTN ={
    'DIRECTION' : 'weather1_bodyUnitImage is-direction(\d{1,2})',
    'WINDCD'    : 'weather1_bodyUnitImage is-wind(\d{1,2})',    # 0無風, 1234 東西南北
    'WEATHERCD' : 'weather1_bodyUnitImage is-weather(\d{1})',
    'WINDPOWER' : 'weather1_bodyUnitLabelData.*?(\d{1})m',
    'TEMP'  : '気温.*?weather1_bodyUnitLabelData.*?(\d{1,2}.\d{1})℃',
    'WTEMP' : '水温.*?weather1_bodyUnitLabelData.*?(\d{1,2}.\d{1})℃',
    'WAVE'  : '波高.*?weather1_bodyUnitLabelData.*?(\d{1,3})cm', 
    }


URL_TENJI_FMT = 'https://www.boatrace.jp/owpc/pc/race/beforeinfo?rno={rno}&jcd={rcd}&hd={op}'.format
URL_ZENKEN_FMT = ' https://www.boatrace.jp/owpc/pc/race/rankingmotor?jcd={rcd}&hd={op}'.format

PATH_TENJI_DF_FMT = 'C:/GAL/Kyotei/scraping/official_chokuzen_df/{y}/{op}*pkl'.format
PATH_ZENKEN_DF_FMT = 'C:/GAL/Kyotei/scraping/official_zenken/{sop}_{rcd}*pkl'.format
SAVE_PATH_FMT = "C:/GAL/Kyotei/{tbl}/{y}.pkl".format

GAL_BASE  = '01_BASE'
GAL_TENJI = '02_TENJI'
GAL_ZENKEN= '04_ZENKEN'
GAL_PAST  = '03_PAST'

PIPE_PAST     = 'past'
PIPE_DAYS     = 'days'
PIPE_CHOKUZEN = 'days_chokuzen'
PIPE_MERGE    = 'merge'
PIPE_TEST     = 'test'
PIPE_PREDICT  = 'prob'
PIPE_EXPECT   = 'expect'
PIPE_BET      = 'bet'
PIPE_RESULT   = 'result'
