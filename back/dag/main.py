"""
Airflow DAG. Reads from a set of CSV files that are inside a folder/S3 bucket.

The files are stored with a datetime partitions, so the DAG will read a year/month/day/hour at a time.
It will take the csv, calculate the average of them all, and store the results in a DynamoDB table.
"""
from __future__ import annotations

import json
import pendulum
import os
import csv
import boto3
from airflow.decorators import dag, task
from decimal import Decimal

s3_bucket = os.environ['S3_BUCKET_NAME']


@dag(
    schedule='@hourly',
    start_date=pendulum.datetime(2021, 1, 1),
    catchup=False,
    tags=['ETH Analysis']
)
def pipeline():
    @task()
    def extract(ts=None):
        """Access to the given folder based on the execution date and reads the CSV files."""
        # get context variable to see if we want to target a s3 bucket or a local folder
        try:
            # local = "{{ dag_run.conf['local'] }}"
            env_place = os.environ['AIRFLOW_ENV_PLACE']
            year = ts[:4]
            month = ts[5:7]
            day = ts[8:10]
            hour = ts[11:13]
            files = []
            if env_place == 'local':
                path = f'/tmp/{year}/{month}/{day}/{hour}/'
                # get all the files in the folder
                files = os.listdir(path)
            else:
                # get all the files in the key
                s3 = boto3.client('s3')
                print(f"Looking for files in bucket: {s3_bucket} with prefix: /gas_averages/{year}/{month}/{day}/{hour}/")
                response = s3.list_objects_v2(Bucket=s3_bucket,
                                              Prefix=f'gas_averages/{year}/{month}/{day}/{hour}/')
                print(f"Get files from bucket gave response: {response}")
                if response['KeyCount'] == 0:
                    print('No files found')
                    raise Exception('No files found')
                else:
                    files = [x['Key'].split('/')[-1] for x in response['Contents']]
            return files
        except Exception as e:
            print(e)
            raise e

    @task()
    def transform(files, ts=None):
        # read each file one by one as a csv, and add up the value of the third column
        year = ts[:4]
        month = ts[5:7]
        day = ts[8:10]
        hour = ts[11:13]
        env_place = os.environ['AIRFLOW_ENV_PLACE']
        total = 0
        if len(files) == 0:
            return {'avg': 0, 'amount': 0}
        for file in files:
            if env_place == 'local':
                path = '/tmp/back/'
                with open(path+file) as f:
                    reader = csv.reader(f)
                    for row in reader:
                        total += float(row[2])
            else:
                s3 = boto3.client('s3')
                response = s3.get_object(Bucket=s3_bucket,
                                         Key=f'gas_averages/{year}/{month}/{day}/{hour}/{file}')
                data = response['Body'].read().decode('utf-8')
                reader = csv.reader(data.splitlines())
                for row in reader:
                    try:
                        total += float(row[2])
                    except Exception as e:
                        print(f"Could not parse row: {row}")
        # calculate the average
        average = total / len(files)
        return {'avg': average, 'amount': len(files)}

    @task()
    def load(dict_vals, ts=None):
        """
        Saves the average into a DynamoDB table.

        The table contains a date partition key and an hour sort key.
        The average is stored in a column called 'average'.
        We also store the amout of transactions in a column called 'transactions'.
        """
        average = dict_vals['avg']
        transaction_count = dict_vals['amount']
        if transaction_count == 0:
            print('No transactions found')
            return
        # get the date and hour from the execution date
        date = ts[:10]
        hour = ts[11:13]
        # create the item
        item = {
            'DATE': date,
            'HOUR': hour,
            'average': Decimal(str(average)),
            'transactions': transaction_count
        }
        # save the item into the table
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('GasCostPerHour')
        table.put_item(Item=item)

    files = extract()
    transform_res = transform(files)
    load(transform_res)


pipeline()
