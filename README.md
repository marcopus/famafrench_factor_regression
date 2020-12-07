# famafrench_factor_regression
Run regression analysis of funds against Fama-French factors

## Usage
`run_fund_regression(fund_symbol, fund_isin, freq, currency, quiet=False)`
* `fund_symbol`: ticker symbol (Reuters)
* `fund_isin`: only used to retrieve the fund category from Morningstar.com
* `freq`: `daily`|`monthly` run regression on daily or monthly returns
* `currency`: `USD`|`EUR` run the regression analysis on USD or EUR returns
* `quiet`: `True`|`False`

Example:
`run_fund_regression('SLY', 'IE00B2QWCY14', freq='daily', currency='EUR', log_return=False, quiet=True)`
