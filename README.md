# Stock Info Crawler
 A stock crawler can update stock data to Atlas Mongo DB

## Environment setting
This script required `Python 3` or higher version.

This script use `Pipenv` as packaging tool. Please install `Pipenv` first though Python first.
```
pip install pipenv
```

```
pip3 install pipenv
```

Before running script, install dependency through pipenv first!
```
cd stock-info-crawler
pipenv install
```

## Before usage
Follow `dbCredential.config.example` and set up your Atlas Mongo DB information.

Then rename `dbCredential.config.example` to `dbCredential.config`

*Note: You may need to download `X509` licence for usage*

## Update Price and Volume
Required argument:

| Switch | Description |
| - | - |
| -s, --start | Set up update start date. Format: "yyyy-mm-dd". |
| -e, --end | Set up update end date. Format: "yyyy-mm-dd". |
| -l, --latest | Update database to latest date. |

*Note: `[-s, -e]` or `[-l]` is required*

Example:
```
pipenv run update_pricevolume.py -s 2011-01-01 -e 2021-06-30
```

```
pipenv run update_pricevolume.py -l
```

## Update Income Statements
Required argument:

| Switch | Description |
| - | - |
| -s, --start | Set up update start year and season. Format: "yyyy-s". |
| -e,  --end | Set up update end year and season. Format: "yyyy-s". |
| -l, --latest | Update database to latest date. |

*Note: `[-s, -e]` or `[-l]` is required*

Example:
```
pipenv run update_income_statements.py -s 2011-1 -e 2021-2
```

```
pipenv run update_income_statements.py -l
```