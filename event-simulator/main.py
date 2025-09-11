# event-simulator/main.py

import asyncio
import aiohttp
import json
from aiokafka import AIOKafkaProducer
import logging
import os

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wikipedia.changes")
WIKI_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

# --- Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("event-simulator")


async def produce_events():
    # Create Kafka producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    logger.info("Kafka producer started. Streaming from Wikipedia...")

    # Connect to Wikipedia EventStream
    async with aiohttp.ClientSession() as session:
        async with session.get(WIKI_STREAM_URL) as response:
            async for line in response.content:
                if not line:
                    continue
                line = line.decode("utf-8").strip()
                if not line.startswith("data: "):
                    continue
                try:
                    event_data = json.loads(line[6:])

                    # Skip bots and non-edit events
                    if event_data.get("bot") or event_data.get("type") != "edit":
                        continue

                    await producer.send_and_wait(
                        topic=KAFKA_TOPIC,
                        value=json.dumps(event_data).encode("utf-8")
                    )
                    logger.info(f"Sent edit: {event_data['title']} by {event_data['user']}")
                except Exception as e:
                    logger.warning(f"Failed to process line: {e}")

    await producer.stop()
    logger.info("Kafka producer stopped.")


if __name__ == "__main__":
    asyncio.run(produce_events())