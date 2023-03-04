# pull stock data from SPY Alpha Vantage API
import os
import requests
from flask import Flask
from dotenv import load_dotenv
app = Flask(__name__)

current_data = {
    "SPY":{}
}

load_dotenv()
@app.route("/")
def get_tickers():
    return current_data

@app.route("/price")
def get_price():
    all_data = []
    for elem in current_data:
        root_url = "https://www.alphavantage.co/query" 
        params={
            "function":"TIME_SERIES_INTRADAY",
            "symbol":elem,
            "interval":"60min",
            "adjusted":"true",
            "apikey": os.getenv('API_KEY')
        }
        r = requests.get(root_url, params)
        data = r.json()
        all_data.append(data) 
    return all_data
   

