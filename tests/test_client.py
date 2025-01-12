import pytest
import json
from unittest.mock import patch, MagicMock, PropertyMock
from src.rabbiteer.client import RabbitMQ, ConnectionType
import logging

logger = logging.getLogger(__name__)

@pytest.fixture
def rabbitmq():
    with patch('pika.BlockingConnection') as mock_conn:
        mock_channel = MagicMock()
        mock_conn.return_value.channel.return_value = mock_channel
        mock_conn.return_value.is_closed = False
        mock_conn.return_value.is_open = True

        return RabbitMQ(
            host='localhost',
            port=5672,
            username='guest',
            password='guest',
            exchange='test_exchange',
            queue='test_queue',
        )

def test_start_server(rabbitmq):
    with patch.object(rabbitmq, 'create_channel') as mock_create_channel, \
         patch.object(rabbitmq, 'create_exchange') as mock_create_exchange, \
         patch.object(rabbitmq, 'create_queue') as mock_create_queue:
        
        rabbitmq.start_server()

        mock_create_channel.assert_called_once()
        mock_create_exchange.assert_called_once()
        mock_create_queue.assert_called_once()

def test_create_exchange(rabbitmq):
    rabbitmq._channel = MagicMock()
    rabbitmq.create_exchange()
    rabbitmq._channel.exchange_declare.assert_called_once_with(
        exchange='test_exchange',
        exchange_type='direct',
        durable=True,
        auto_delete=False
    )

def test_create_channel(rabbitmq):
    rabbitmq.create_channel()
    assert rabbitmq._channel is not None

def test_create_queue(rabbitmq):
    rabbitmq._channel = MagicMock()
    rabbitmq.create_queue()
    rabbitmq._channel.queue_declare.assert_called_once_with(
        queue='test_queue',
        durable=True,
        arguments=None
    )
    rabbitmq._channel.queue_bind.assert_called_once_with(
        queue='test_queue',
        exchange='test_exchange',
        routing_key='test_queue'
    )

def test_publish(rabbitmq):
    rabbitmq._channel = MagicMock()
    message = {"key": "value"}
    rabbitmq.publish(message=message)
    import pika
    rabbitmq._channel.basic_publish.assert_called_once_with(
        exchange='test_exchange',
        routing_key='test_queue',
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ),
        mandatory=False
    )

def test_get_message(rabbitmq):
    rabbitmq._channel = MagicMock()
    rabbitmq._channel.basic_get.return_value = (MagicMock(), MagicMock(), json.dumps({"key": "value"}).encode())
    message = rabbitmq.get_message()
    assert message == {"key": "value"}
    rabbitmq._channel.basic_ack.assert_called_once()

def test_purge_queue(rabbitmq):
    rabbitmq._channel = MagicMock()
    rabbitmq.purge_queue()
    rabbitmq._channel.queue_purge.assert_called_once_with(queue='test_queue')

def test_delete_queue(rabbitmq):
    rabbitmq._channel = MagicMock()
    rabbitmq.delete_queue()
    rabbitmq._channel.queue_delete.assert_called_once_with(queue='test_queue')

def test_consume(rabbitmq):
    rabbitmq._channel = MagicMock()
    callback = MagicMock()
    rabbitmq.consume(callback)
    rabbitmq._channel.basic_consume.assert_called_once()
    rabbitmq._channel.start_consuming.assert_called_once()

def test_close(rabbitmq):
    assert not rabbitmq._connection.is_closed

    rabbitmq.close()
    assert rabbitmq._connection.is_closed

def test_already_closed(rabbitmq):
    rabbitmq._connection = MagicMock()
    rabbitmq._connection.is_closed = True
    rabbitmq.close()
    assert rabbitmq._connection.is_closed