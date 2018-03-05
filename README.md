# mixpanel3
MixPanel handy scripts (Python3)

[![Build Status](https://travis-ci.org/miti0/mixpanel3.svg?branch=master)](https://travis-ci.org/miti0/mixpanel3)


## About
API for [MixPanel](https://mixpanel.com) with main focus to data export.

Currently available features:
 - Export People to csv
 - Export Events to csv


## Install

```
pip install -r requirements.txt
```


## Run

### Exporting Users/People data
Script exports all users data to csv

About:
```
usage: people2csv.py [-h] --api_secret API_SECRET

optional arguments:
  -h, --help            show this help message and exit
  --api_secret API_SECRET
                        Mixpanel API secret (default: None)
```


Run:
```
python3 people2csv.py --api_secret [API_SECRET]
```


### Exporting Events data
Script exports events data to csv

About:
```
usage: events2csv.py [-h] --api_secret API_SECRET --from_date FROM_DATE
                     --to_date TO_DATE [--events EVENTS] --out_dir OUT_DIR

optional arguments:
  -h, --help            show this help message and exit
  --api_secret API_SECRET
                        Mixpanel API secret (default: None)
  --from_date FROM_DATE
                        Export starting date (for ex. 2018-01-01) (default:
                        None)
  --to_date TO_DATE     Export ending date (for ex. 2018-01-01) (default:
                        None)
  --events EVENTS       Events to be exported (comma separated) (default:
                        None)
  --out_dir OUT_DIR     Output directory (default: None)
```


Run:
```
python3 events2csv.py --api_secret [API_SECRET] --from_date 2018-01-01 --to_date 2018-01-01 --out_dir out/20180101/
```
Above example will export ALL user defined events in mixpanel for 2018-01-01 and save output csv files to out/20180101/ directory.


## Credits

people2csv & events2csv:
 - Credits: [gist by wgins](https://gist.github.com/wgins/b21f4f0c2e160f7f95af)
 