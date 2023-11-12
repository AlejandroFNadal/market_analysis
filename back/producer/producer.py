import requests
import sys
import json
from confluent_kafka import Producer
import time
import boto3
from loguru import logger
import os
from dotenv import load_dotenv

# create logger with 'DEBUG' level
logger.add(sys.stderr, format="{time} {level} {message}", filter="my_module", level="TRACE")

load_dotenv()
# Replace with your Etherscan API Key
api_key = os.environ['ETHERSCAN_API_KEY']
root_url = 'https://api.etherscan.io/api'


def get_latest_block_number(api_key):
    """Get the latest block number in the chain."""
    url = f'{root_url}?module=proxy&action=eth_blockNumber&apikey={api_key}'
    response = requests.get(url)
    result = int(response.json()['result'], 16)  # Convert hex to decimal
    logger.info(f'Latest block number in chain: {result}')
    return result


def get_block_transactions(block_number, api_key):
    """Get all transactions from a block."""
    hex_block_number = hex(int(block_number))
    url = f'{root_url}?module=proxy&action=eth_getBlockByNumber&tag={hex_block_number}&boolean=true&apikey={api_key}'
    response = requests.get(url)
    transactions = response.json()['result']['transactions']
    timestamp = int(response.json()['result']['timestamp'], 16)
    # using GMT timezone, convert to string
    string_timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(timestamp))
    return transactions, string_timestamp


def wei_to_eth(wei):
    """Convert Wei to Ether."""
    return wei / 10**18  # 1 Ether equals 10^18 Wei


def get_eth_price(api_key):
    """Get the current ETH price in USD."""
    url = f'{root_url}?module=stats&action=ethprice&apikey={api_key}'
    response = requests.get(url)
    data = response.json()
    eth_price_usd = float(data['result']['ethusd'])
    return eth_price_usd


def get_ec2_ip_by_tag(tag_name, tag_value):
    """Get the public IP of an EC2 instance by tag."""
    ec2_client = boto3.client('ec2')
    # do not get terminated instances
    response = ec2_client.describe_instances(Filters=[{'Name': f'tag:{tag_name}', 'Values': [tag_value]},
                                                      {'Name': 'instance-state-name', 'Values': ['running']}])
    # Assuming only one instance with the given tag
    instance = response['Reservations'][0]['Instances'][0]
    logger.info(f"Found EC2 instance: {instance}")
    logger.info(f'Public IP of EC2 instance with tag {tag_name}={tag_value}: {instance["PublicIpAddress"]}')
    return instance['PublicIpAddress']

# If the following variable is true, we need to try to reach the kafka server on aws. 
# Otherwise, asume it is local


if os.environ['KAFKA_SERVER_AWS'] == 'true':
    ip = get_ec2_ip_by_tag('Name', 'KafkaAirflowServer')
else:
    ip = 'localhost'
conf = {
    'bootstrap.servers': f'{ip}:9092',  # Kafka broker 
}
try:
    producer = Producer(conf)
except Exception as e:
    logger.error(f'Failed to connect to Kafka broker: {e}')
    sys.exit(1)


def main():
    """Fetch data from Etherscan."""
    try:
        latest_block_number = get_latest_block_number(api_key)
        # check what is the last block number in dynamoDB, it is a single value
        # if the latest_block_number is greater than the last block number in dynamoDB, then 
        # we need to fetch the transaction from the last block number in dynamoDB to the latest_block_number
        boto3.setup_default_session(region_name='us-east-1')
        try:
            if os.environ['DYNAMODB_SERVER_AWS'] == 'true':
                dynamodb = boto3.resource('dynamodb')
            else:
                dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
            table = dynamodb.Table('BlockConfig')
            # if we dont find it, just accept the latest_block_number as the last_processed_block
            response = table.get_item(
                Key={
                    'config_type': 'last_processed_block'
                }
            )
        except Exception as e:
            logger.error(f'Failed to connect to DynamoDB: {e}')
            raise Exception(f'Failed to connect to DynamoDB: {e}')
        try:
            last_processed_block = response['Item']['last_block_number']
            logger.info(f"Last processed block: {last_processed_block}")
        except KeyError:
            # this is the first execution, no data in dynamoDB
            logger.info('No data in dynamoDB, setting last_processed_block to latest_block_number - 6')
            last_processed_block = latest_block_number-6
        # we want 10 confirmed blocks before we start processing. This is to avoid any reorgs
        # we are running in Lambda, so we should not overextend the execution
        start_time = time.time()
        while last_processed_block < (latest_block_number - 5) and (time.time() - start_time) < 20:
            block_to_process = last_processed_block + 1
            logger.info(f'Processing block number: {block_to_process}')
            transactions, timestamp = get_block_transactions(block_to_process, api_key)
            usd_price = get_eth_price(api_key)
            logger.info(f'Current ETH price in USD: {usd_price}')
            filtered_transactions = []
            for tx in transactions:
                val = wei_to_eth(int(tx["value"], 16))
                input_val = tx["input"]
                gas_units = tx["gas"]
                # gas_val = wei_to_eth(int(tx["gas"], 16))
                if val > 0 and input_val == '0x' and gas_units == '0x5208':
                    message = {
                        'from': tx["from"],
                        'to': tx["to"],
                        'value': val,
                        'gas': int(tx["gas"], 16) * int(tx["gasPrice"], 16),
                        'usd_price': usd_price,
                    }
                    filtered_transactions.append(message)
                    # print(f'From: {tx["from"]}, To: {tx["to"]}, Value: {val}, USD Price: {val * usd_price}')
            batch_key = str(block_to_process).encode('utf-8')
            object_to_send = {
                'timestamp': timestamp,
                'transactions': filtered_transactions
            }
            # create byte from bytearray
            message_bytes = bytes(json.dumps(object_to_send).encode('utf-8'))
            # write the filtered_transactions to kafka
            try:
                logger.debug(f"Sending message to Kafka")
                producer.produce('eth', key=batch_key, value=message_bytes)
                producer.flush()
            except Exception as e:
                logger.error(f'Failed to send message to Kafka: {e}')
                raise Exception(f'Failed to send message to Kafka: {e}')
            logger.info(f'Block Number: {block_to_process}, Transactions: {len(filtered_transactions)}')
            # update the last_processed_block in dynamoDB
            try:
                table.update_item(
                    Key={
                        'config_type': 'last_processed_block'
                    },
                    UpdateExpression='SET last_block_number = :val1',
                    ExpressionAttributeValues={
                        ':val1': block_to_process
                    }
                )
            except Exception as e:
                logger.error(f'Failed to update last_processed_block in dynamoDB: {e}')
            last_processed_block = block_to_process
    except Exception as e:
        logger.error(f'Sth has failed - : {e}')


def handler(event, context):
    """Lambda handler function."""
    return main()


if __name__ == "__main__":
    logger.warning("Running locally")
    # lambda simulation, every 30 seconds
    while True:
        main()
        time.sleep(30)


