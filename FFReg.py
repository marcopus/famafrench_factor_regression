import pandas as pd
from math import prod
import statsmodels.api as sm
from glob import glob
import ntpath
import pandas_datareader.data as web


def get_forex_data(base_currency='EUR', to_currency='USD'):

    # get the EUR/USD rate data
    fx = web.DataReader(base_currency + '/' + to_currency, 'av-forex-daily',
                        api_key='6QAL8820E2NU81J2')['close'].to_frame(name='FX')
    fx.index = pd.to_datetime(fx.index).to_period("B")
    fx.index.name='Date'

    return fx


def get_price_data(fund_id, fund_currency, convert_currency=True):
    
    # read the price data from file
    price = pd.read_excel('Price Data\\' + fund_id + '.xlsx', sheet_name='Price_Daily', index_col=0)
    
    # convert the index to date period format
    price.index = pd.to_datetime(price.index, format='%Y%m%d').to_period("B")

    # convert NAV to USD
    if not fund_currency==None and not fund_currency=='USD' and convert_currency:
        fx = get_forex_data(base_currency=fund_currency)
        price = price.merge(fx, left_on='Date', right_on='Date')
        price.NAV = price.NAV * price.FX
        price.drop('FX', axis=1, inplace=True)
        
    return price
    


def calc_return(fund_id, freq, fund_currency=None):
    
    #get the fund price data
    price = get_price_data(fund_id, convert_currency=True, fund_currency=fund_currency)

    # calculate daily returns
    if freq=='daily':
        ret = price.NAV.pct_change()[1:].to_frame(name='Return')

    # calculate monthly returns
    if freq=='monthly':
        ret = price.resample("M").last().NAV.pct_change()[1:].to_frame(name='Return')

    return ret


def get_famafrench_data(name_factor_data, name_mom_data, cache_dir='famafrench\\'):
    
    if name_factor_data:
        print('Retrieving ' + name_factor_data + '...')
        try:
            factor_data = pd.read_pickle(cache_dir + name_factor_data)
        except:
            factor_data = web.DataReader(name_factor_data, 'famafrench')[0]/100
            factor_data.to_pickle(cache_dir + name_factor_data)
        if name_mom_data:
            try:
                mom_data = pd.read_pickle(cache_dir + name_mom_data)
            except:
                mom_data = web.DataReader(name_mom_data, 'famafrench')[0]/100
                mom_data.to_pickle(cache_dir + name_mom_data)
            if not 'WML' in mom_data.columns:
                mom_data = mom_data.iloc[:,0].to_frame(name='WML')
            factor_data = factor_data.merge(mom_data, left_index=True, right_index=True)
        if not 'period' in str(factor_data.index.dtype):
            factor_data.index = factor_data.index.to_period("B")
    else:
        factor_data = pd.DataFrame()
    return factor_data


def calc_famafrench_regression(factor_data, fund_data, fund_id):
    
    if not(factor_data.empty):
        X = fund_data.merge(factor_data, left_index=True, right_index=True)
        X['Return-RF'] = X['Return'] - X['RF']
        y = X['Return-RF']
        X = X.drop(['RF', 'Return', 'Return-RF'], axis=1)
        X = sm.add_constant(X)
        
        model = sm.OLS(y, X).fit(cov_type='HAC',cov_kwds={'maxlags':1})
        predictions = model.predict(X)
        model.summary()
        res = model.params.copy()
        res[abs(model.tvalues)<1.96]=None
        res.name = fund_id
        reg = res.to_frame().transpose()
    else:
        print('No daily factor data!')
        reg = pd.DataFrame()
    return reg


def main():

    # get fund price data file
    fund_files = glob('Price Data\\*.xlsx')

    # extract fund ISIN from file path
    fund_ISINs = []
    for file in fund_files:
        fund_ISINs.append(ntpath.splitext(ntpath.basename(file))[0])

    # read the morningstar fund data
    fund_data = pd.read_excel('..\\Instruments.xlsx', sheet_name='Equity_MS', index_col=0)

    # create a dataframe with ISIN and path to fund price data
    funds = pd.DataFrame(fund_files, index = fund_ISINs, columns = ['FilePath'])
    funds.index.name = 'ISIN'

    # obtain the Morningstar fund category
    funds = funds.merge(fund_data[['Name', 'Category', 'Currency']],
                        left_index=True, right_index=True)

    # add a column with the name of the factor data file to use for each fund
    funds['FF5_monthly'] = funds.apply(lambda row:
        'Emerging_5_Factors' if 'Emerging' in row.Category else
        ('F-F_Research_Data_5_Factors_2x3' if 'US' in row.Category else
        ('Developed_5_Factors' if 'Global' in row.Category and not 'Emerging' in row.Category else
        ('Europe_5_Factors' if 'Europe' in row.Category or 'Eurozone' in row.Category else
        ''))), axis = 1)

    # add a column with the name of the factor data file to use for each fund
    funds['FF5_daily'] = funds.apply(lambda row:
        'F-F_Research_Data_5_Factors_2x3_daily' if 'US' in row.Category else
        ('Developed_5_Factors_Daily' if 'Global' in row.Category and not 'Emerging' in row.Category else
        ('Europe_5_Factors_Daily' if 'Europe' in row.Category or 'Eurozone' in row.Category else
        '')), axis = 1)

    # add a column with the name of the factor data file to use for each fund
    funds['MOM_monthly'] = funds.apply(lambda row:
        'Emerging_MOM_Factor' if 'Emerging' in row.Category else
        ('F-F_Momentum_Factor' if 'US' in row.Category else
        ('Developed_Mom_Factor' if 'Global' in row.Category and not 'Emerging' in row.Category else
        ('Europe_Mom_Factor' if 'Europe' in row.Category or 'Eurozone' in row.Category else
        ''))), axis = 1)

    # add a column with the name of the factor data file to use for each fund
    funds['MOM_daily'] = funds.apply(lambda row:
        'F-F_Momentum_Factor_daily' if 'US' in row.Category else
        ('Developed_Mom_Factor_Daily' if 'Global' in row.Category and not 'Emerging' in row.Category else
        ('Europe_Mom_Factor_Daily' if 'Europe' in row.Category or 'Eurozone' in row.Category else
        '')), axis = 1)

    # initialize dataframe for daily regression results
    reg_daily = pd.DataFrame()
    reg_daily.index.name = 'ISIN'
    
    # initialize dataframe for monthly regression results
    reg_monthly = pd.DataFrame()
    reg_monthly.index.name = 'ISIN'

    for fund in funds.itertuples():

        # retrieve fama-french monthly factor data
        FF5_monthly = get_famafrench_data(fund.FF5_monthly, fund.MOM_monthly)

        # retrieve fama-french daily factor data
        FF5_daily = get_famafrench_data(fund.FF5_daily, fund.MOM_daily)
        
        print('\nNow processing ' + fund.Name)
        
        # calculate daily returns
        ret_daily = calc_return(fund.Index, freq='daily', fund_currency=fund.Currency)

        # calculate monthly returns
        ret_monthly = calc_return(fund.Index, freq='monthly', fund_currency=fund.Currency)
        
        # calculating regression
        reg_daily = reg_daily.append(calc_famafrench_regression(FF5_daily, ret_daily, fund.Index))
        
        # calculating regression
        reg_monthly = reg_monthly.append(calc_famafrench_regression(FF5_monthly, ret_monthly, fund.Index))

    print('\nDaily Factor Regression Results')
    print(reg_daily)
    print('\nMonthly Factor Regression Results')
    print(reg_monthly)

    # append monthly regression results when daily results are missing
    reg_daily.append(reg_monthly[~reg_monthly.index.isin(reg_daily.index)]).to_csv('results\\reg_daily.csv', encoding='utf-8')

    # export regression results to csv
    reg_monthly.to_csv('results\\reg_monthly.csv', encoding='utf-8')


if __name__ == '__main__':
    main()
