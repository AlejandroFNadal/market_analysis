import requests
import sys
import json
from confluent_kafka import Producer
import time
import boto3
from loguru import logger


# Replace with your Etherscan API Key
api_key = os.environ['ETHERSCAN_API_KEY']

def get_latest_block_number(api_key):
    url = f'https://api.etherscan.io/api?module=proxy&action=eth_blockNumber&apikey={api_key}'
    response = requests.get(url)
    result = int(response.json()['result'], 16)  # Convert hex to decimal
    logger.info(f'Latest block number in chain: {result}')
    return result

def get_block_transactions(block_number, api_key):
    url = f'https://api.etherscan.io/api?module=proxy&action=eth_getBlockByNumber&tag={hex(int(block_number))}&boolean=true&apikey={api_key}'
    response = requests.get(url)
    transactions = response.json()['result']['transactions']
    return transactions

def wei_to_eth(wei):
    return wei / 10**18  # 1 Ether equals 10^18 Wei

def get_eth_price(api_key):
    url = f'https://api.etherscan.io/api?module=stats&action=ethprice&apikey={api_key}'
    response = requests.get(url)
    data = response.json()
    eth_price_usd = float(data['result']['ethusd'])
    return eth_price_usd


def main():
    """Infinite loop to continuously fetch data from Etherscan."""
    conf = {
            'bootstrap.servers': 'localhost:9092',  # replace with your Kafka broker(s)
        }

    producer = Producer(conf)

    while True:
        latest_block_number = get_latest_block_number(api_key)
        # check what is the last block number in dynamoDB, it is a single value
        # if the latest_block_number is greater than the last block number in dynamoDB, then 
        # we need to fetch the transaction from the last block number in dynamoDB to the latest_block_number
        boto3.setup_default_session(region_name='us-east-1')
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
        table = dynamodb.Table('BlockConfig')
        # if we dont find it, just accept the latest_block_number as the last_processed_block
        response = table.get_item(
            Key={
                'config_type': 'last_processed_block'
            }
        )
        try:
            last_processed_block = response['Item']['last_block_number']
        except KeyError:
            # this is the first execution, no data in dynamoDB
            logger.info('No data in dynamoDB, setting last_processed_block to latest_block_number - 10')
            last_processed_block = latest_block_number-11
        # we want 10 confirmed blocks before we start processing. This is to avoid any reorgs
        while last_processed_block < (latest_block_number - 10):
            block_to_process = last_processed_block + 1
            transactions = get_block_transactions(block_to_process, api_key)
            usd_price = get_eth_price(api_key)
            filtered_transactions = []
            for tx in transactions:
                val = wei_to_eth(int(tx["value"], 16))
                # gas_val = wei_to_eth(int(tx["gas"], 16))
                if val > 0:
                    message = {
                        'from': tx["from"],
                        'to': tx["to"],
                        'value': val,
                        'gas': int(tx["gas"], 16) * int(tx["gasPrice"], 16),
                        'usd_price': usd_price
                    }
                    filtered_transactions.append(message)
                    # print(f'From: {tx["from"]}, To: {tx["to"]}, Value: {val}, USD Price: {val * usd_price}')
            # create byte from bytearray
            message_bytes = bytes(json.dumps(filtered_transactions).encode('utf-8'))
            batch_key = str(block_to_process).encode('utf-8')
            # write the filtered_transactions to kafka
            producer.produce('eth', key=batch_key, value=message_bytes)
            producer.flush()
            print(f'Block Number: {block_to_process}, Transactions: {len(filtered_transactions)}')
            # update the last_processed_block in dynamoDB
            table.update_item(
                Key={
                    'config_type': 'last_processed_block'
                },
                UpdateExpression='SET last_block_number = :val1',
                ExpressionAttributeValues={
                    ':val1': block_to_process
                }
            )
            last_processed_block = block_to_process


if __name__ == "__main__":
    # lambda simulation, every 30 seconds
    while True:
        main()
        time.sleep(2)


