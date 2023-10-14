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
from airflow.decorators import dag, task


@dag(
    schedule='@hourly',
    start_date=pendulum.datetime(2021, 1, 1),
    catchup=False,
    tags=['ETH Analysis']
)
def pipeline():
    @task()
    def extract(ts=None):
        """Accesses to the given folder based on the execution date and reads the CSV files."""
        # get context variable to see if we want to target a s3 bucket or a local folder
        try:
            #local = "{{ dag_run.conf['local'] }}"
            local = "true"
            year = ts[:4]
            month = ts[5:7]
            day = ts[8:10]
            hour = ts[11:13]
            if local == 'true':
                path = f'/home/ale/Documents/market_analysis/back/{year}/{month}/{day}/{hour}/'
            # get all the files in the folder
            files = os.listdir(path)
            return files
        except Exception as e:
            print(e)
            raise e

    @task()
    def transform(files):
        # read each file one by one as a csv, and add up the value of the third column
        total = 0
        for file in files:
            path = '/home/ale/Documents/market_analysis/back/'
            with open(path+file) as f:
                reader = csv.reader(f)
                for row in reader:
                    total += float(row[2])
        # calculate the average
        average = total / len(files)
        return {'avg': average, 'amount': len(files)}

    @task()
    def load(dict_vals):
        """
        Saves the average into a DynamoDB table.

        The table contains a date partition key and an hour sort key.
        The average is stored in a column called 'average'.
        We also store the amout of transactions in a column called 'transactions'.
        """
        average = dict_vals['avg']
        transaction_count = dict_vals['amount']
        # get the date and hour from the execution date
        date = "{{ execution_date.strftime('%Y-%m-%d') }}"
        hour = "{{ execution_date.strftime('%H') }}"
        # create the item
        item = {
            'date': date,
            'hour': hour,
            'average': average,
            'transactions': transaction_count
        }
        # save the item into the table
        table.put_item(Item=item)

    files = extract()
    transform_res = transform(files)
    load(transform_res)


pipeline()
