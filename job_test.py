# from kyotei.utils import notify_line
from OfficialScraping import convertHtml2pandasList

from utils_req import notify_line
# TODO: 効率的なテスト方法を検討する
if __name__ == "__main__":
    # scrape_year(2021)
    opdt_from = '20190101'
    opdt_until = '20190101'
    convertHtml2pandasList(opdt_from, opdt_until)
    # convertHtml2pandasList('20170101', '20171231')
    # print('nya-')
    # notify_line('nya-')
