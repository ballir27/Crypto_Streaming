import hashlib
import json
import os
import threading
import time
from pathlib import Path

import jwt
import websocket
from dotenv import dotenv_values
from loguru import logger

# Derived from your Coinbase CDP API Key
# SIGNING_KEY: the signing key provided as a part of your API key. Also called the "SECRET KEY"
# API_KEY: the api key provided as a part of your API key. also called the "API KEY NAME"
PROJECT_ROOT = Path(__file__).resolve().parent
config = dotenv_values(os.path.join(PROJECT_ROOT, "../.env"))

COINBASE_API_KEY = config.get('COINBASE_API_KEY')
COINBASE_SIGNING_KEY = config.get("COINBASE_SIGNING_KEY").encode().decode('unicode_escape')

ALGORITHM = "ES256"

if not COINBASE_SIGNING_KEY or not COINBASE_API_KEY:
    raise ValueError("Missing mandatory environment variable(s)")

CHANNEL_NAMES = {
    "level2": "level2",
    "user": "user",
    "tickers": "ticker",
    "ticker_batch": "ticker_batch",
    "status": "status",
    "market_trades": "market_trades",
    "candles": "candles",
}

#Market Data Endpoint
WS_API_URL = "wss://advanced-trade-ws.coinbase.com"

def sign_with_jwt(message, channel, products=[]):
    payload = {
        "iss": "coinbase-cloud",
        "nbf": int(time.time()),
        "exp": int(time.time()) + 120,
        "sub": COINBASE_API_KEY,
    }
    headers = {
        "kid": COINBASE_API_KEY,
        "nonce": hashlib.sha256(os.urandom(16)).hexdigest()
    }
    token = jwt.encode(payload, COINBASE_SIGNING_KEY, algorithm=ALGORITHM, headers=headers)
    message['jwt'] = token
    return message

def on_message(ws, message):
    data = json.loads(message)
    with open("Output1.txt", "a") as f:
        f.write(json.dumps(data) + "\n")

def subscribe_to_products(ws, products, channel_name):
    message = {
        "type": "subscribe",
        "channel": channel_name,
        "product_ids": products
    }
    signed_message = sign_with_jwt(message, channel_name, products)
    ws.send(json.dumps(signed_message))

def unsubscribe_to_products(ws, products, channel_name):
    message = {
        "type": "unsubscribe",
        "channel": channel_name,
        "product_ids": products
    }
    signed_message = sign_with_jwt(message, channel_name, products)
    ws.send(json.dumps(signed_message))

def on_open(ws):
    products = ["BTC-USD"]
    subscribe_to_products(ws, products, CHANNEL_NAMES["level2"])

def start_websocket():
    ws = websocket.WebSocketApp(WS_API_URL, on_open=on_open, on_message=on_message)
    ws.run_forever()

def main():
    ws_thread = threading.Thread(target=start_websocket)
    ws_thread.start()

    sent_unsub = False
    start_time = time.time()

    try:
        while True:
            if (time.time() - start_time) > 5 and not sent_unsub:
                # Unsubscribe after 5 seconds
                ws = websocket.create_connection(WS_API_URL)
                unsubscribe_to_products(ws, ["BTC-USD"], CHANNEL_NAMES["level2"])
                ws.close()
                sent_unsub = True
            time.sleep(1)
    except Exception as e:
        logger.error(f"Exception: {e}")
        raise e

if __name__ == "__main__":
    main()
