
import json
from loguru import logger
import csv
import boto3
from datetime import datetime
from confluent_kafka import Consumer
from utils import wei_to_eth
import os
from dotenv import load_dotenv

# if this envvar flag is enabled, we are in AWS and we need to write to S3
SAVE_TO_S3 = os.environ.get('SAVE_TO_S3', False)
load_dotenv()
api_key = os.environ['ETHERSCAN_API_KEY']


def average(lst):
    """Return the average of a list of numbers."""
    return sum(lst) / len(lst)


def save_to_csv(timestamp, batch_key, avg_gas, usd_price):
    """Save the data to a CSV file, partitioned by year/month/day/hour."""
    # save with partition year/month/day/hour
    path = f"{timestamp[:4]}/{timestamp[5:7]}/{timestamp[8:10]}/{timestamp[11:13]}"
    os.makedirs(path, exist_ok=True)
    with open(f'{path}/{batch_key}.csv', 'w') as file:
        writer = csv.writer(file)
        writer.writerow([timestamp, batch_key, avg_gas, usd_price])


def save_to_s3(timestamp, batch_key, avg_gas, usd_price):
    """Save the data to an S3 bucket."""
    # Ensure you have AWS credentials set up, either as environment variables or in a config file.
    s3 = boto3.client('s3')
    bucket_name = os.environ['S3_BUCKET_NAME']
    key = f"gas_averages/{timestamp[:4]}/{timestamp[5:7]}/{timestamp[8:10]}/{timestamp[11:13]}/{batch_key}.csv"
    # Create CSV content
    csv_content = f"timestamp,batch_key,avg_gas,usd_price\n{timestamp},{batch_key},{avg_gas},{usd_price}"
    # Upload to S3
    s3.put_object(Body=csv_content, Bucket=bucket_name, Key=key)


def process_batch(batch_key, block):
    transactions = block['transactions']
    gas_values = [tx["gas"] for tx in transactions]
    avg_gas = average(gas_values)
    usd_price = wei_to_eth(avg_gas) * transactions[0]["usd_price"]
    # timestamp arrives as a string, convert to datetime object
    timestamp = block['timestamp']

    save_to_csv(timestamp, batch_key, avg_gas, usd_price)

    if SAVE_TO_S3:
        save_to_s3(timestamp, batch_key, avg_gas)

    logger.info(f"Processed batch with key {batch_key}. Average gas: {avg_gas}, USD price: {usd_price}")

def get_ec2_ip_by_tag(tag_name, tag_value):
    """Get the public IP of an EC2 instance by its tag."""
    ec2_client = boto3.client('ec2')
    response = ec2_client.describe_instances(Filters=[{'Name': f'tag:{tag_name}', 'Values': [tag_value]}])
    # Assuming only one instance with the given tag
    instance = response['Reservations'][0]['Instances'][0]
    return instance['PublicIpAddress']

def main():
    if os.environ['KAFKA_SERVER_AWS'] == 'True':
        ip = get_ec2_ip_by_tag('Name', 'KafkaAirflowServer')
    else:
        ip = 'localhost'
    conf = {
        'bootstrap.servers': f'{ip}:9092',
        'group.id': 'eth_consumer_group',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe(['eth'])

    try:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            logger.error(f"Error in message consumption: {message.error()}")
        else:
            block = json.loads(message.value().decode('utf-8'))
            batch_key = message.key().decode('utf-8')
            process_batch(batch_key, block)

    except KeyboardInterrupt:
        logger.info("Consumer interrupted. Closing connection...")
    finally:
        consumer.close()


def handler():
    main()


if __name__ == "__main__":
    while True:
        main()

