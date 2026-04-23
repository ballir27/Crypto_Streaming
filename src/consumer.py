import json
import os
from pathlib import Path

import pika
import psycopg2
from dotenv import dotenv_values
from loguru import logger

# from psycopg2 import extras

# --- Configuration & Auth Setup ---
PROJECT_ROOT = Path(__file__).resolve().parent
config = dotenv_values(os.path.join(PROJECT_ROOT, "../.env"))

DB_CONFIG = {
    "host": config.get("POSTGRES_HOST"),
    "database": config.get("POSTGRES_DB"),
    "user": config.get("POSTGRES_USER"),
    "password": config.get("POSTGRES_PASSWORD"),
    "port": config.get("POSTGRES_PORT")
}
BATCH_SIZE = 100  # Number of messages to collect before pushing to DB
msg_batch = []

# --- Database Setup ---
def get_db_connection():
    pg_conn = psycopg2.connect(**DB_CONFIG)
    # Ensure your table exists and has a JSONB column for the raw data
    with pg_conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.coinbase_raw (
                id SERIAL PRIMARY KEY,
                data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        pg_conn.commit()
    return pg_conn

db_conn = get_db_connection()

def process_batch(ch, method):
    global msg_batch
    if not msg_batch:
        return

    try:
        with db_conn.cursor() as cur:
            # Efficient bulk insert using psycopg2's execute_values
            # We wrap the dicts in json.dumps to store as JSONB
            data_to_insert = [(json.dumps(m),) for m in msg_batch]
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO raw.coinbase_raw (data) VALUES %s",
                data_to_insert
            )
            db_conn.commit()

            # Acknowledge all messages in the batch at once
            ch.basic_ack(delivery_tag=method.delivery_tag, multiple=True)
            logger.info(f"Saved batch of {len(msg_batch)} to Postgres")
            msg_batch = []
    except Exception as e:
        db_conn.rollback()
        logger.error(f"Database error: {e}")

def callback(ch, method, properties, body):
    global msg_batch
    msg_batch.append(json.loads(body))

    if len(msg_batch) >= BATCH_SIZE:
        process_batch(ch, method)

# --- RabbitMQ Setup ---
def mq_start():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='coinbase_stream', durable=True)

    # Don't dispatch more than BATCH_SIZE to this worker at a time
    channel.basic_qos(prefetch_count=BATCH_SIZE)
    channel.basic_consume(queue='coinbase_stream', on_message_callback=callback)

    logger.info("Worker started. Waiting for messages...")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        db_conn.close()
