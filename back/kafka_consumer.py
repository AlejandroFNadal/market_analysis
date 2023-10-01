
import json
from loguru import logger
import csv
import boto3
from datetime import datetime
from confluent_kafka import Consumer
from utils import wei_to_eth
import os

SAVE_TO_S3 = False   # Set this flag to True to save to S3

api_key = os.environ['ETHERSCAN_API_KEY']
def average(lst):
    return sum(lst) / len(lst)

def save_to_csv(timestamp, batch_key, avg_gas, usd_price):
    with open(f'{batch_key}.csv', 'w') as file:
        writer = csv.writer(file)
        writer.writerow([timestamp, batch_key, avg_gas, usd_price])

def save_to_s3(timestamp, batch_key, avg_gas):
    # Ensure you have AWS credentials set up, either as environment variables or in a config file.
    s3 = boto3.client('s3')
    bucket_name = 'YOUR_S3_BUCKET_NAME'  # replace with your bucket name
    key = f"gas_averages/{timestamp}.csv"

    # Create CSV content
    csv_content = f"timestamp,batch_key,avg_gas\n{timestamp},{batch_key},{avg_gas}"
    
    # Upload to S3
    s3.put_object(Body=csv_content, Bucket=bucket_name, Key=key)

def process_batch(batch_key, transactions):
    gas_values = [tx["gas"] for tx in transactions]
    avg_gas = average(gas_values)
    usd_price = wei_to_eth(avg_gas) * transactions[0]["usd_price"]
    timestamp = datetime.utcnow().isoformat()

    save_to_csv(timestamp, batch_key, avg_gas, usd_price)

    if SAVE_TO_S3:
        save_to_s3(timestamp, batch_key, avg_gas)

    logger.info(f"Processed batch with key {batch_key}. Average gas: {avg_gas}, USD price: {usd_price}")

def main():
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'eth_consumer_group',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe(['eth'])

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                logger.error(f"Error in message consumption: {message.error()}")
            else:
                transactions = json.loads(message.value().decode('utf-8'))
                batch_key = message.key().decode('utf-8')
                process_batch(batch_key, transactions)

    except KeyboardInterrupt:
        logger.info("Consumer interrupted. Closing connection...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()


