# Rabbiteer

A friendly and robust RabbitMQ client library for Python 🐰

## Features

- Simple and intuitive interface
- Automatic retries with exponential backoff
- Comprehensive error handling
- Support for SSL, message priorities, and TTL
- Full typing support
- Extensive logging

## Installation

```bash
pip install rabbiteer
```

## Quick Start

```bash
from rabbiteer import RabbitMQ

# Initialize connection
rabbit = RabbitMQ(
    host='localhost',
    user='guest',
    password='guest',
    queue_name='my_queue'
)

# Publish a message
rabbit.publish({"hello": "world"})

# Consume messages
def process_message(message):
    print(f"Received: {message}")

rabbit.consume(process_message)
```
