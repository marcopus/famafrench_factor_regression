import pandas as pd
import statsmodels.api as sm
from glob import glob
import ntpath
import pandas_datareader.data as web
import os
import runcurl

pd.options.display.max_columns = None
pd.options.display.width = None


def get_morningstar_fund_name(fund_isin):
    # curl string to obtain some morningstar fund info
    curlstr = "curl 'https://www.morningstar.com/api/v1/search/securities?q=" + fund_isin + "&region=international&limit=50' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0' -H 'Accept: application/json, text/plain, */*' -H 'Accept-Language: en-US,en;q=0.5' --compressed -H 'Referer: https://www.morningstar.com/search?query=IE00BFY0GT14' -H 'x-api-key: Nrc2ElgzRkaTE43D5vA7mrWj2WpSBR35fvmaqfte' -H 'DNT: 1' -H 'Connection: keep-alive' -H 'Cookie: ASP.NET_SessionId=zdgbrkbblfiepa45gm4wjffg; _gcl_au=1.1.1480557771.1600715785; _uetsid=c33fb5d1faf382624c557dd389f438fb; _uetvid=11b9521be1a452a32efc4091dbb6616e; overlay_hibernation=Wed%2C%2023%20Sep%202020%2018:51:51%20GMT; intro_hibernation=Tue%2C%2022%20Sep%202020%2019:51:51%20GMT' -H 'TE: Trailers'"
    req = runcurl.execute(curlstr)

    # extract the morningstar fund info
    if req.status_code == 200:
        fund_name = req.json()['results'][0]['name']
        print('Fund name Morningstar: ' + fund_name)
        return fund_name
    else:
        print('Cannot find ' + fund_isin + ' on Morningstar!')
        return None


def get_morningstar_fund_category(fund_isin):
    # curl string to obtain the morningstar fund id
    curlstr = "curl 'https://www.morningstar.com/api/v1/search/securities?q=" + fund_isin + "&region=international&limit=50' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0' -H 'Accept: application/json, text/plain, */*' -H 'Accept-Language: en-US,en;q=0.5' --compressed -H 'Referer: https://www.morningstar.com/search?query=IE00BFY0GT14' -H 'x-api-key: Nrc2ElgzRkaTE43D5vA7mrWj2WpSBR35fvmaqfte' -H 'DNT: 1' -H 'Connection: keep-alive' -H 'Cookie: ASP.NET_SessionId=zdgbrkbblfiepa45gm4wjffg; _gcl_au=1.1.1480557771.1600715785; _uetsid=c33fb5d1faf382624c557dd389f438fb; _uetvid=11b9521be1a452a32efc4091dbb6616e; overlay_hibernation=Wed%2C%2023%20Sep%202020%2018:51:51%20GMT; intro_hibernation=Tue%2C%2022%20Sep%202020%2019:51:51%20GMT' -H 'TE: Trailers'"
    req = runcurl.execute(curlstr)

    # extract the morningstar fund id
    if req.status_code == 200:
        secId = req.json()['results'][0]['secId']
    else:
        print('Cannot find ' + fund_isin + ' on Morningstar!')
        return None

    # curl string to obtain the morningstar fund info
    curlstr = "curl 'https://api-global.morningstar.com/sal-service/v1/etf/process/asset/v2/" + secId + "/data?locale=en&clientId=MDC&benchmarkId=category&version=3.31.0' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:81.0) Gecko/20100101 Firefox/81.0' -H 'Accept: application/json, text/plain, */*' -H 'Accept-Language: en-US,en;q=0.5' --compressed -H 'Referer: https://www.morningstar.com/etfs/xetr/exsa/portfolio' -H 'X-API-REALTIME-E: eyJlbmMiOiJBMTI4R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.XmuAS3x5r-0MJuwLDdD4jNC6zjsY7HAFNo2VdvGg6jGcj4hZ4NaJgH20ez313H8An9UJrsUj8ERH0R8UyjQu2UGMUnJ5B1ooXFPla0LQEbN_Em3-IG84YPFcWVmEgcs1Fl2jjlKHVqZp04D21UvtgQ4xyPwQ-QDdTxHqyvSCpcE.ACRnQsNuTh1K_C9R.xpLNZ8Cc9faKoOYhss1CD0A4hG4m0M7-LZQ0fISw7NUHwzQs2AEo9ZXfwOvAj1fCbcE96mbKQo8gr7Oq1a2-piYXM1X5yNMcCxEaYyGinpnf6PGqbdr6zbYZdqyJk0KrxWVhKSQchLJaLGJOts4GlpqujSqJObJQcWWbkJQYKG9K7oKsdtMAKsHIVo5-0BCUbjKVnHJNsYwTsI7xn2Om8zGm4A.nBOuiEDssVFHC_N68tDjVA' -H 'X-SAL-ContentType: e7FDDltrTy+tA2HnLovvGL0LFMwT+KkEptGju5wXVTU=' -H 'X-API-RequestId: ae855627-097b-9bb0-a2dd-92925520bce1' -H 'ApiKey: lstzFDEOhfFNMLikKa0am9mgEKLBl49T' -H 'Origin: https://www.morningstar.com' -H 'Connection: keep-alive' -H 'TE: Trailers'"
    req = runcurl.execute(curlstr)

    if req.status_code == 200:
        fund_category = req.json()['categoryName']
        print('Fund category Morningstar: ' + fund_category)
        return fund_category
    else:
        print('Cannot get info on ' + fund_isin + ' on Morningstar!')
        return None


def get_yahoo_fund_symbol(fund_isin, fund_exchange=None):
    '''
    >EXPERIMENTAL< It does not work very well...
    Fund_exchange is typically one of 'AMS', 'LSE', 'GER', 'MIL', 'FRA'. If
    omitted, it will prompt for interactive selection.
    '''

    # curl string to obtain the yahoo symbols given the fund name
    curlstr = "curl 'https://query1.finance.yahoo.com/v1/finance/search?q=" + fund_isin + "&lang=en-US&region=US&quotesCount=6&newsCount=4&enableFuzzyQuery=false&quotesQueryId=tss_match_phrase_query&multiQuoteQueryId=multi_quote_single_token_query&newsQueryId=news_cie_vespa&enableCb=true&enableNavLinks=true&enableEnhancedTrivialQuery=true' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0' -H 'Accept: */*' -H 'Accept-Language: en-US,en;q=0.5' --compressed -H 'Referer: https://finance.yahoo.com/screener?.tsrc=fin-srch' -H 'Origin: https://finance.yahoo.com' -H 'Connection: keep-alive' -H 'Cookie: B=bvg32mtd9ql9o&b=3&s=uq; GUC=AQABAgFfO-NgIUIgrgSo; PRF=t%3DVGWL.F%252BVDVA.L%252BVVAL.AS%252BVVAL.L%252BVVL.TO%252BZPRX.DE%252BEURUSD%253DX%252B%255EGSPC%252BES%253DF%252BBRK-B%252BBRKB%252BWORK%252BVEIEX%252BXDEV.DE%252BVVAL.SW%26qct%3DtrendArea; ucs=eup=2; A1=d=AQABBPVPhl4CEJQlhU7jN3yvYRgayeLumysFEgABAgHjO18hYO2Nb2UBACAAAAcIOFWdWrdiwL8&S=AQAAAji0MRIc7MuC98UsHfOepqk; A3=d=AQABBPVPhl4CEJQlhU7jN3yvYRgayeLumysFEgABAgHjO18hYO2Nb2UBACAAAAcIOFWdWrdiwL8&S=AQAAAji0MRIc7MuC98UsHfOepqk; A1S=d=AQABBPVPhl4CEJQlhU7jN3yvYRgayeLumysFEgABAgHjO18hYO2Nb2UBACAAAAcIOFWdWrdiwL8&S=AQAAAji0MRIc7MuC98UsHfOepqk&j=GDPR; thamba=1' -H 'Pragma: no-cache' -H 'Cache-Control: no-cache' -H 'TE: Trailers'"
    req = runcurl.execute(curlstr)

    # extract the yahoo fund symbol
    if req.status_code == 200:
        n = len(req.json()['quotes'])
    else:
        print('Fund not found on Yahoo!')
        return None

    # search the exchange when not provided
    if not fund_exchange:
        # interactive exchange selection
        if n > 1:
            print('Multiple funds found:')
            funds = [(quote['symbol'], quote['shortname'], quote['exchange']) for quote in req.json()['quotes']]
            print(*enumerate(funds, 1), sep='\n')
            fund_number = input('Please make a selection: ')
            fund_exchange = req.json()['quotes'][int(fund_number) - 1]['exchange']
        else:
            fund_exchange = req.json()['quotes'][0]['exchange']
        print('Fund exchange Yahoo: ' + fund_exchange)

    # select the symbol corresponding to the desired exchange
    fund_symbol = list(filter(lambda quote: quote['exchange'] == fund_exchange, req.json()['quotes']))[0]['symbol']
    print('Fund symbol Yahoo: ' + fund_symbol)

    return fund_symbol


def get_yahoo_fund_currency(fund_symbol):
    json = pd.read_json('https://query1.finance.yahoo.com/v8/finance/chart/' + fund_symbol)
    fund_currency = json['chart']['result'][0]['meta']['currency'].upper()
    print('Fund currency Yahoo: ' + fund_currency)

    return fund_currency


def get_yahoo_price_data(fund_symbol, cache_dir='price\\'):
    try:
        price = pd.read_pickle(cache_dir + fund_symbol)
    except:
        price = pd.read_csv('https://query1.finance.yahoo.com/v7/finance/download/' +
                            fund_symbol + '?period1=0&period2=10000000000&interval=1d&events=history&includeAdjustedClose=true',
                            header=0, names=['Date', 'Open', 'High', 'Low', 'Close', 'NAV', 'Volume'],
                            usecols=['Date', 'NAV'], index_col=['Date'])
        price.index = pd.to_datetime(price.index).to_period("B")
        price.to_pickle(cache_dir + fund_symbol)

    print('Price data interval: ' + str(price.index[0]) + ' to ' + str(price.index[-1]))
    return price


def get_av_price_data(fund_symbol, cache_dir='price\\'):
    try:
        price = pd.read_pickle(cache_dir + fund_symbol)
    except:
        if not os.getenv('ALPHAVANTAGE_API_KEY'):
            raise Exception("Please set 'ALPHAVANTAGE_API_KEY' environment variable!")
        price = web.DataReader(fund_symbol, 'av-daily-adjusted')['adjusted close'].to_frame(name='NAV')
        price.index = pd.to_datetime(price.index).to_period("B")
        price.index.name = 'Date'
        price.to_pickle(cache_dir + fund_symbol)

    print('Price data interval: ' + str(price.index[0]) + ' to ' + str(price.index[-1]))
    return price


def get_av_forex_data(base_currency='EUR', to_currency='USD', cache_dir='forex\\'):
    # get the EUR/USD rate data
    try:
        fx = pd.read_pickle(cache_dir + base_currency + '-' + to_currency)
    except:
        if not os.getenv('ALPHAVANTAGE_API_KEY'):
            raise Exception("Please set 'ALPHAVANTAGE_API_KEY' environment variable!")
        fx = web.DataReader(base_currency + '/' + to_currency,
                            'av-forex-daily')['close'].to_frame(name='FX')
        fx.index = pd.to_datetime(fx.index).to_period("B")
        fx.index.name = 'Date'
        fx.to_pickle(cache_dir + base_currency + '-' + to_currency)

    return fx


def get_excel_price_data(file):
    # read the price data from file
    price = pd.read_excel(file, sheet_name='Price_Daily', index_col=0)

    # convert the index to date period format
    price.index = pd.to_datetime(price.index, format='%Y%m%d').to_period("B")

    return price


def get_csv_price_data(file):
    # read the price data from file
    price = pd.read_csv(file, index_col=0)

    # convert the index to date period format
    price.index = pd.to_datetime(price.index, format='%Y%m%d').to_period("B")

    return price


def convert_price_to_usd(price, fund_currency):
    fx = get_av_forex_data(base_currency=fund_currency)
    price = price.merge(fx, left_on='Date', right_on='Date')
    price.NAV = price.NAV * price.FX
    price.drop('FX', axis=1, inplace=True)

    return price


def calc_return(price, freq):
    if freq == 'daily':
        # calculate daily returns
        ret = price.NAV.pct_change()[1:].to_frame(name='Return')
        return ret[ret.all(1)]
    elif freq == 'monthly':
        # calculate monthly returns
        return price.resample("M").last().NAV.pct_change()[1:].to_frame(name='Return')
    else:
        return None


def get_famafrench_data(name_factor_data, name_mom_data, cache_dir='famafrench\\'):
    if name_factor_data:
        try:
            factor_data = pd.read_pickle(cache_dir + name_factor_data)
        except:
            factor_data = web.DataReader(name_factor_data, 'famafrench')[0] / 100
            factor_data.to_pickle(cache_dir + name_factor_data)
        if name_mom_data:
            try:
                mom_data = pd.read_pickle(cache_dir + name_mom_data)
            except:
                mom_data = web.DataReader(name_mom_data, 'famafrench')[0] / 100
                mom_data.to_pickle(cache_dir + name_mom_data)
            if 'WML' not in mom_data.columns:
                mom_data = mom_data.iloc[:, 0].to_frame(name='WML')
            factor_data = factor_data.merge(mom_data, left_index=True, right_index=True)
        if 'period' not in str(factor_data.index.dtype):
            factor_data.index = factor_data.index.to_period("B")
    else:
        factor_data = pd.DataFrame()
    return factor_data


def calc_famafrench_regression(factor_data, fund_data, fund_symbol, quiet=False):
    if not factor_data.empty:
        x = fund_data.merge(factor_data, left_index=True, right_index=True)
        x['Return-RF'] = x['Return'] - x['RF']
        y = x['Return-RF']
        x = x.drop(['RF', 'Return', 'Return-RF'], axis=1)
        x = sm.add_constant(x)

        model = sm.OLS(y, x).fit(cov_type='HAC', cov_kwds={'maxlags': 1})
        model.predict(x)
        res = model.params.copy()
        res[abs(model.tvalues) < 1.96] = None
        res.name = fund_symbol
        reg = res.to_frame().transpose()
        reg.insert(0, 'R2_adj', model.rsquared_adj)
        if not quiet:
            print(model.summary())
        return reg
    else:
        return None


def get_fund_factor_data(fund_isin, freq):
    # obtain the fund category
    fund_category = get_morningstar_fund_category(fund_isin)

    if freq == 'daily':
        # assign the name of daily factor data
        if 'US' in fund_category:
            name_factor_data = 'F-F_Research_Data_5_Factors_2x3_daily'
            name_mom_data = 'F-F_Momentum_Factor_daily'
        elif 'Global' in fund_category and 'Emerging' not in fund_category:
            name_factor_data = 'Developed_5_Factors_Daily'
            name_mom_data = 'Developed_Mom_Factor_Daily'
        elif 'Europe' in fund_category or 'Eurozone' in fund_category:
            name_factor_data = 'Europe_5_Factors_Daily'
            name_mom_data = 'Europe_Mom_Factor_Daily'
        else:
            name_factor_data = None
            name_mom_data = None
    elif freq == 'monthly':
        if 'Emerging' in fund_category:
            name_factor_data = 'Emerging_5_Factors'
            name_mom_data = 'Emerging_MOM_Factor'
        elif 'US' in fund_category:
            name_factor_data = 'F-F_Research_Data_5_Factors_2x3'
            name_mom_data = 'F-F_Momentum_Factor'
        elif 'Global' in fund_category and 'Emerging' not in fund_category:
            name_factor_data = 'Developed_5_Factors'
            name_mom_data = 'Developed_Mom_Factor'
        elif 'Europe' in fund_category or 'Eurozone' in fund_category:
            name_factor_data = 'Europe_5_Factors'
            name_mom_data = 'Europe_Mom_Factor'
        else:
            name_factor_data = None
            name_mom_data = None
    else:
        raise Exception("`freq´ must be one of 'daily' or 'monthly'")

    if name_factor_data:
        print('Factor data: ' + name_factor_data)
    else:
        print('Factor data not found!')

    # retrieve fama-french daily factor data
    return get_famafrench_data(name_factor_data, name_mom_data)


def run_fund_regression(fund_symbol, fund_isin, freq):
    # retrieve fama-french daily factor data
    factor_data = get_fund_factor_data(fund_isin, freq=freq)

    # get the fund price data
    price = get_av_price_data(fund_symbol)

    # get the fund currency
    fund_currency = get_yahoo_fund_currency(fund_symbol)

    # currency conversion of non USD price
    if fund_currency != 'USD':
        price = convert_price_to_usd(price, fund_currency)

    # calculate daily returns
    ret = calc_return(price, freq=freq)

    # calculating regression
    return calc_famafrench_regression(factor_data, ret, fund_symbol)


def main():
    # get fund price data file
    fund_info = pd.DataFrame(glob('nav data\\*.csv'), columns=['FilePath'])

    # extract ISIN from filename
    fund_info.index = fund_info.apply(lambda row: ntpath.splitext(ntpath.basename(row.FilePath))[0].split('-')[0], axis=1)

    # initialize dataframe for daily regression results
    reg_daily = pd.DataFrame()

    # initialize dataframe for monthly regression results
    reg_monthly = pd.DataFrame()

    for fund in fund_info.itertuples():

        print('\nNow processing ' + fund.Index)

        # retrieve fama-french daily factor data
        factor_data_daily = get_fund_factor_data(fund.Index, freq='daily')

        # retrieve fama-french monthly factor data
        factor_data_monthly = get_fund_factor_data(fund.Index, freq='monthly')

        # read the price data
        price = get_csv_price_data(fund.FilePath)

        fund_currency = ntpath.splitext(ntpath.basename(fund.FilePath))[0].split('-')[1]

        # currency conversion of non USD price
        if fund_currency != 'USD':
            price = convert_price_to_usd(price, fund_currency)

        # calculate daily returns
        ret_daily = calc_return(price, freq='daily')

        # calculate monthly returns
        ret_monthly = calc_return(price, freq='monthly')

        # calculating regression
        reg_daily = reg_daily.append(
            calc_famafrench_regression(factor_data_daily, ret_daily, fund.Index, quiet=True))

        # calculating regression
        reg_monthly = reg_monthly.append(
            calc_famafrench_regression(factor_data_monthly, ret_monthly, fund.Index, quiet=True))

    print('\nDaily Factor Regression Results')
    print(reg_daily)
    print('\nMonthly Factor Regression Results')
    print(reg_monthly)

    # append monthly regression results when daily results are missing
    reg_daily.append(reg_monthly[~reg_monthly.index.isin(reg_daily.index)]).to_csv('results\\reg_daily.csv',
                                                                                   encoding='utf-8', index_label='ISIN')

    # export regression results to csv
    reg_monthly.to_csv('results\\reg_monthly.csv', encoding='utf-8', index_label='ISIN')


if __name__ == '__main__':
    main()
