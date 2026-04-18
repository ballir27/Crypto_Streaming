import hashlib
import json
import os
import threading
import time
from pathlib import Path

import jwt
import pika
import websocket
from dotenv import dotenv_values
from loguru import logger

import consumer

# --- Configuration & Auth Setup ---
PROJECT_ROOT = Path(__file__).resolve().parent
config = dotenv_values(os.path.join(PROJECT_ROOT, "../.env"))

COINBASE_API_KEY = config.get('COINBASE_API_KEY')
COINBASE_SIGNING_KEY = config.get("COINBASE_SIGNING_KEY").encode().decode('unicode_escape')
ALGORITHM = "ES256"

if not COINBASE_SIGNING_KEY or not COINBASE_API_KEY:
    raise ValueError("Missing mandatory environment variable(s)")

# --- RabbitMQ Setup ---
def get_rabbitmq_channel():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='coinbase_stream', durable=True)
    logger.info("Successfully connected to RabbitMQ and declared 'coinbase_stream'")
    return connection, channel

# Initialize global connection and channel
# Note: For massive scale, you'd handle connection drops more robustly
rmq_conn, rmq_channel = get_rabbitmq_channel()

CHANNEL_NAMES = {
    "level2": "level2",
    "tickers": "ticker",
    "market_trades": "market_trades",
    # ... rest of your channels
}

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

# --- The Key Change ---
def on_message(ws, message):
    """Instead of writing to a file, we publish to RabbitMQ."""
    try:
        # Publish message to the queue
        rmq_channel.basic_publish(
            exchange='',
            routing_key='coinbase_stream',
            body=message, # 'message' is already a JSON string from Coinbase
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent on disk
            )
        )
        logger.debug("Message buffered to RabbitMQ")
    except Exception as e:
        logger.error(f"Failed to publish to RabbitMQ: {e}")

def subscribe_to_products(ws, products, channel_name):
    message = {
        "type": "subscribe",
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
    ws_thread = threading.Thread(target=start_websocket, daemon=True)
    ws_thread.start()

    consumer.mq_start()  # Start the consumer to process messages from RabbitMQ

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Closing connections...")
        rmq_conn.close()

if __name__ == "__main__":
    main()
