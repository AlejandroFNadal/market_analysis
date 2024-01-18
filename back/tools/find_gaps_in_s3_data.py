"""
Find missing days and day/hours
We cannot find the missing blocks because we are not storing empty files for the empty blocks.
Later we would need some way to get the start block of the hour and start reprocessing from there.
The list of missing days/hours is important for airflow, as we need those dags tasks to be rerun.
"""
import boto3
import argparse
from datetime import datetime, timedelta

s3 = boto3.client('s3')

paginator = s3.get_paginator('list_objects_v2')
parser = argparse.ArgumentParser(prog="s3 Ethereum Gap Finder")
parser.add_argument('start_date')
parser.add_argument('stop_date')

args = parser.parse_args()
start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
moving_date = datetime.strptime(args.start_date, '%Y-%m-%d')
stop_date = datetime.strptime(args.stop_date, '%Y-%m-%d')
only_keys = []
# Each page in a list operation is max a thousand elements. We have about 7000 blocks per day in Eth. We can then use the Prefix
# parameter to reduce the amount of elements we get. We can do a query per day and it will still paginate. This tool should not be used a lot
# it can incurr in some costs

while moving_date <= stop_date:
    print(f"Obtaining data from {moving_date}")
    year, month, day = moving_date.strftime('%Y-%m-%d').split('-')
    page_iterator = paginator.paginate(Bucket='eth-analysis-databucket-1h76jq2x0s25g',
                                       Prefix=f'gas_averages/{year}/{month}/{day}')
    for page in page_iterator:
        if 'Contents' in page:
            only_keys.extend([elem['Key'] for elem in page['Contents']])
    moving_date += timedelta(days=1)
    print(f"Currently at {len(only_keys)} keys")

dict_keys = {}
print("Generating dict helper")
for elem in only_keys:
    subelems = elem.split('/')[1:]
    year, month, day, hour, block = subelems
    if year not in dict_keys:
        dict_keys[year] = {}
    if month not in dict_keys[year]:
        dict_keys[year][month] = {}
    if day not in dict_keys[year][month]:
        dict_keys[year][month][day] = {}
    if hour not in dict_keys[year][month][day]:
        dict_keys[year][month][day][hour] = []
    dict_keys[year][month][day][hour].append(block)

start_block = [only_keys[0].split('/')[5]]
missing = []
# print keys of dict_keys
print(dict_keys['2024'].keys())
while start_date <= stop_date:
    print(f"Checking if date {start_date} is in dict_helper")
    key = start_date.strftime('%Y/%m/%d')
    year, month, day = key.split('/')
    for i in range(0, 24):
        try:
            _ = dict_keys[year][month][day][f"{i:02}"]
            print(f"Found {year}/{month}/{day}/{i:02}, len: {len(dict_keys[year][month][day][f'{i:02}'])}")
        except KeyError:
            missing.append(f"{year}/{month}/{day}/{i:02}")
    start_date += timedelta(days=1)

print(missing)

