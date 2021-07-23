# Stock Info Crawler
 A stock crawler can update stock data to Atlas Mongo DB

## Environment setting
This script required `Python 3` or higher version.

This script use `Pipenv` as packaging tool. Please install `Pipenv` first though Python first.
```
pip install pipenv

or

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

## Usage
Required argument:
```
-sd --start_date : Set up update start date. Format: %Y-%m-%d.
-ed --end_date : Set up update end date. Format: %Y-%m-%d.

-l --latest : Update database to latest date
```
*Note: `[-sd, -ed]` or `[-l]` is required*

Example:
```
pipenv run update_pricevolume.py -sd 2011-01-01 -ed 2021-06-30

or

pipenv run update_pricevolume.py -l
```