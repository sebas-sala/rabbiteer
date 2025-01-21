import json 
import pika
import pika.exceptions
import pika.spec
from pika.exchange_type import ExchangeType
from pika.spec import BasicProperties
from pika import BlockingConnection, SelectConnection
import logging
from typing import Dict, Optional, Any, Callable
from enum import Enum
from retry import retry

class ConnectionType(Enum):
    BLOCKING = "blocking"
    SELECT = "select"

class Parameters(object):
    """Base connection parameters class definition
    """
    __slots__ = ('_blocked_connection_timeout', '_channel_max', '_client_properties', 
                 '_connection_attempts', '_credentials', '_frame_max', '_heartbeat', 
                 '_host', '_locale', '_port', '_retry_delay', '_socket_timeout', 
                 '_stack_timeout', '_ssl_options', '_virtual_host', '_tcp_options',
                 '_username', '_password')

    DEFAULT_USERNAME = 'guest'
    DEFAULT_PASSWORD = 'guest'

    DEFAULT_EXCHANGE = ""
    DEFAULT_EXCHANGE_TYPE = ExchangeType.direct

    DEFAULT_LOG_LEVEL = logging.INFO

    DEFAULT_BLOCKED_CONNECTION_TIMEOUT = None
    DEFAULT_CHANNEL_MAX = pika.channel.MAX_CHANNELS
    DEFAULT_CLIENT_PROPERTIES = None
    DEFAULT_CREDENTIALS = pika.credentials.PlainCredentials(DEFAULT_USERNAME, DEFAULT_PASSWORD)
    DEFAULT_CONNECTION_ATTEMPTS = 1
    DEFAULT_CONNECTION_TYPE = ConnectionType.BLOCKING
    DEFAULT_FRAME_MAX = pika.spec.FRAME_MAX_SIZE
    DEFAULT_HEARTBEAT_TIMEOUT = None
    DEFAULT_HOST = 'localhost'
    DEFAULT_LOCALE = 'en_US'
    DEFAULT_PORT = 5672
    DEFAULT_RETRY_DELAY = 2.0
    DEFAULT_SOCKET_TIMEOUT = 10.0
    DEFAULT_STACK_TIMEOUT = 15.0
    DEFAULT_SSL = False
    DEFAULT_SSL_OPTIONS = None
    DEFAULT_SSL_PORT = 5671
    DEFAULT_VIRTUAL_HOST = '/'
    DEFAULT_TCP_OPTIONS = None
    DEFAULT_START_SERVER = True

    def __init__(self):
        self._blocked_connection_timeout = self.DEFAULT_BLOCKED_CONNECTION_TIMEOUT
        self._channel_max = self.DEFAULT_CHANNEL_MAX
        self._client_properties = self.DEFAULT_CLIENT_PROPERTIES
        self._connection_attempts = self.DEFAULT_CONNECTION_ATTEMPTS
        self._credentials = self.DEFAULT_CREDENTIALS
        self._frame_max = self.DEFAULT_FRAME_MAX
        self._heartbeat = self.DEFAULT_HEARTBEAT_TIMEOUT
        self._host = self.DEFAULT_HOST
        self._locale = self.DEFAULT_LOCALE
        self._port = self.DEFAULT_PORT
        self._retry_delay = self.DEFAULT_RETRY_DELAY
        self._socket_timeout = self.DEFAULT_SOCKET_TIMEOUT
        self._stack_timeout = self.DEFAULT_STACK_TIMEOUT
        self._ssl_options = self.DEFAULT_SSL_OPTIONS
        self._virtual_host = self.DEFAULT_VIRTUAL_HOST
        self._tcp_options = self.DEFAULT_TCP_OPTIONS
        self._username = self.DEFAULT_USERNAME
        self._password = self.DEFAULT_PASSWORD

class RabbitMQError(Exception):
    """Custom exception for RabbitMQ client errors"""

    def __init__(self, message: str):
        super(RabbitMQError, self).__init__(message)        

class RabbitMQ(Parameters):
    """
    Enhanced RabbitMQ client with additional features and better error handling.
    
    Attributes:
        host (str): RabbitMQ server host
        username (str): RabbitMQ server username
        password (str): RabbitMQ server password
        queue (str): Queue name
        exchange (str): Exchange name
        exchange_type (ExchangeType): Exchange type
        connection_attempts (int): Connection attempts
        connection_type (ConnectionType): Connection type
        virtual_host (str): Virtual host
        log_level (int): Log level
        retry_delay (float): Retry delay
        port (int): Port
        ssl_options (Dict): SSL options
        channel_max (int): Maximum number of channels
        client_properties (Dict): Client properties
        frame_max (int): Maximum frame size
        heartbeat (int): Heartbeat timeout
        locale (str): Locale
        socket_timeout (float): Socket timeout
        stack_timeout (float): Stack timeout
        tcp_options (Dict): TCP options
    """

    class _DEFAULT(object):
        """Designates default parameter value; internal use"""

    def __init__(
        self, 
        host: str = _DEFAULT, 
        username: str = _DEFAULT,
        password: str = _DEFAULT, 
        queue: Optional[str] = _DEFAULT, 
        exchange: Optional[str] = _DEFAULT, 
        exchange_type: Optional[ExchangeType] = _DEFAULT,
        connection_attempts: int = _DEFAULT,
        connection_type: ConnectionType = _DEFAULT,
        virtual_host: str = _DEFAULT,
        log_level: int = _DEFAULT,
        retry_delay: float = _DEFAULT,
        port: int = _DEFAULT,
        ssl_options: Optional[Dict] = _DEFAULT,
        channel_max: int = _DEFAULT,
        client_properties: Optional[Dict] = _DEFAULT,
        frame_max: int = _DEFAULT,
        heartbeat: Optional[int] = _DEFAULT,
        locale: str = _DEFAULT,
        socket_timeout: float = _DEFAULT,
        stack_timeout: float = _DEFAULT,
        tcp_options: Optional[Dict] = _DEFAULT,
        start_server: bool = _DEFAULT,
        **kwargs: Any
    ):
        super(RabbitMQ, self).__init__()

        self._host = host if host is not self._DEFAULT else self.DEFAULT_HOST
        self._username = username if username is not self._DEFAULT else self.DEFAULT_USERNAME
        self._password = password if password is not self._DEFAULT else self.DEFAULT_PASSWORD
        self._queue = queue if queue is not self._DEFAULT else None
        
        self._exchange = exchange if exchange is not self._DEFAULT else self.DEFAULT_EXCHANGE
        self._exchange_type = exchange_type if exchange_type is not self._DEFAULT else self.DEFAULT_EXCHANGE_TYPE
        
        self._connection_attempts = connection_attempts if connection_attempts is not self._DEFAULT else self.DEFAULT_CONNECTION_ATTEMPTS
        self._virtual_host = virtual_host if virtual_host is not self._DEFAULT else self.DEFAULT_VIRTUAL_HOST
        self._connection_type = connection_type if connection_type is not self._DEFAULT else self.DEFAULT_CONNECTION_TYPE
        self._retry_delay = retry_delay if retry_delay is not self._DEFAULT else self.DEFAULT_RETRY_DELAY
        self._port = port if port is not self._DEFAULT else self.DEFAULT_PORT
        self._ssl_options = ssl_options if ssl_options is not self._DEFAULT else self.DEFAULT_SSL_OPTIONS
        self._channel_max = channel_max if channel_max is not self._DEFAULT else self.DEFAULT_CHANNEL_MAX
        self._client_properties = client_properties if client_properties is not self._DEFAULT else self.DEFAULT_CLIENT_PROPERTIES
        self._frame_max = frame_max if frame_max is not self._DEFAULT else self.DEFAULT_FRAME_MAX
        self._heartbeat = heartbeat if heartbeat is not self._DEFAULT else self.DEFAULT_HEARTBEAT_TIMEOUT
        self._locale = locale if locale is not self._DEFAULT else self.DEFAULT_LOCALE
        self._socket_timeout = socket_timeout if socket_timeout is not self._DEFAULT else self.DEFAULT_SOCKET_TIMEOUT
        self._stack_timeout = stack_timeout if stack_timeout is not self._DEFAULT else self.DEFAULT_STACK_TIMEOUT
        self._tcp_options = tcp_options if tcp_options is not self._DEFAULT else self.DEFAULT_TCP_OPTIONS
        self._start_server = start_server if start_server is not self._DEFAULT else self.DEFAULT_START_SERVER

        if kwargs:
            raise TypeError('unexpected kwargs: %r' % (kwargs,))

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level if log_level is not self._DEFAULT else self.DEFAULT_LOG_LEVEL)
        
        self._connection = None
        self._channel = None

        if self._start_server:
            self.start_server()
    
    @retry(tries=5, delay=2)
    def start_server(self):
            """Initialize server connection with retry mechanism"""
            self.create_channel()
            self.create_exchange()
            self.create_queue()

    def create_exchange(self, durable: bool = True, auto_delete: bool = False):
            """Declare exchange with specified parameters

                Args:   
                    durable: Whether exchange is durable
                    auto_delete: Whether exchange is auto-deleted
            """

            if not self._exchange:
                    self.logger.info("No exchange specified, skipping exchange declaration")
                    return

            if not self._channel:
                self.logger.info("Channel created successfully")
                return

            try:
                self._channel.exchange_declare(
                    exchange=self._exchange,
                    exchange_type=self._exchange_type.value,
                    durable=durable,
                    auto_delete=auto_delete
                )
                self.logger.info(f"Exchange '{self._exchange}' declared successfully")
            except Exception as e:
                self.logger.error(f"Failed to declare exchange: {str(e)}")
                raise RabbitMQError(f"Exchange declaration error: {str(e)}")

    def create_channel(self):
        """Create a new channel and connection to the RabbitMQ server"""
        try: 
            credentials = pika.PlainCredentials(self._username, self._password)
            params = pika.ConnectionParameters(
                host=self._host, 
                credentials=credentials,
                connection_attempts=self._connection_attempts,
                retry_delay=self._retry_delay,
                virtual_host=self._virtual_host,
                port=self._port,
                ssl_options=self._ssl_options,
                channel_max=self._channel_max,
                client_properties=self._client_properties,
                frame_max=self._frame_max,
                heartbeat=self._heartbeat,
                locale=self._locale,
                socket_timeout=self._socket_timeout,
                stack_timeout=self._stack_timeout,
                tcp_options=self._tcp_options,
            )
          
            self.__set_connection(params)
            self._channel = self._connection.channel()
        except pika.exceptions.AMQPConnectionError as e:
            self.logger.error(f"Failed to create channel: {str(e)}")
            raise RabbitMQError(f"Channel creation error: {str(e)}")  

    def __set_connection(self, params: pika.ConnectionParameters):
        try: 
            if self._connection_type == ConnectionType.BLOCKING:
                self._connection = BlockingConnection(params)
            elif self._connection_type == ConnectionType.SELECT:
                self._connection = SelectConnection(params)
                self._connection.ioloop.start()
            else:
                raise RabbitMQError("Unsupported connection type")    
            
            if not self._connection.is_open:
                raise RabbitMQError("Connection not open")
        except KeyboardInterrupt:
            self.logger.info("Connection interrupted")
            self.close()

    def create_queue(self, queue_arguments: Optional[Dict] = None, durable: bool = True, routing_key: Optional[str] = None):
            """
            Declare queue with specified parameters
            
            Args:
                durable: Whether queue is durable
                queue_arguments: Optional arguments for queue declaration (e.g., message TTL)
            """

            if not self._channel:
                self.logger.info("Channel not created, skipping queue declaration")
                return

            try:
                self._channel.queue_declare(
                    queue=self._queue,
                    durable=durable,
                    arguments=queue_arguments
                )
                if self._exchange:
                    self._channel.queue_bind(
                        queue=self._queue,
                        exchange=self._exchange,
                        routing_key=routing_key or self._queue
                    )
                self.logger.info(f"Queue '{self._queue}' declared successfully")
            except Exception as e:
                self.logger.error(f"Failed to declare queue: {str(e)}")
                raise RabbitMQError(f"Queue declaration error: {str(e)}")
    
    @retry(tries=5, delay=2)
    def publish(
        self,
        message: Any,
        routing_key: Optional[str] = None,
        properties: Optional[BasicProperties] = None,
        mandatory: bool = False,
        priority: Optional[int] = None
    ):
        """
        Publish message to queue with retry mechanism
        
        Args:
            message: Message content (will be JSON serialized)
            routing_key: Optional routing key (defaults to queue name)
            properties: Message properties
            mandatory: Mandatory message flag
            priority: Message priority (0-9, if supported by queue)
        """
        try:
            if properties is None:
                properties = pika.BasicProperties(
                    delivery_mode=2,
                    priority=priority
                )

            routing_key = routing_key or self._queue
            self._channel.basic_publish(
                exchange=self._exchange,
                routing_key=routing_key,
                body=json.dumps(message),
                properties=properties,
                mandatory=mandatory
            )
            self.logger.debug(f"Published message: {message}")
        except Exception as e:
            self.logger.error(f"Failed to publish message: {str(e)}")
            raise RabbitMQError(f"Publishing error: {str(e)}")

    def get_message(self, auto_ack: bool = False) -> Optional[Dict]:
        """
        Get a single message from the queue
        
        Args:
            auto_ack: Whether to automatically acknowledge the message
            
        Returns:
            Optional[Dict]: Message content if available, None otherwise
        """
        try:
            method_frame, header_frame, body = self._channel.basic_get(
                queue=self._queue,
                auto_ack=auto_ack
            )
            
            if method_frame:
                message = json.loads(body)
                if not auto_ack:
                    self._channel.basic_ack(method_frame.delivery_tag)
                return message
            return None
        except Exception as e:
            self.logger.error(f"Failed to get message: {str(e)}")
            raise RabbitMQError(f"Get message error: {str(e)}")

    def purge_queue(self):
            """Purge all messages from the queue"""
            try:
                self._channel.queue_purge(queue=self._queue)
                self.logger.info(f"Purged queue '{self._queue}'")
            except Exception as e:
                self.logger.error(f"Failed to purge queue: {str(e)}")
                raise RabbitMQError(f"Purge error: {str(e)}")

    def delete_queue(self):
        """Delete the queue"""
        try:
            self._channel.queue_delete(queue=self._queue)
            self.logger.info(f"Deleted queue '{self._queue}'")
        except Exception as e:
            self.logger.error(f"Failed to delete queue: {str(e)}")
            raise RabbitMQError(f"Delete error: {str(e)}")

    @retry(tries=5, delay=2)
    def consume(
        self,
        callback: Callable,
        auto_ack: bool = False,
        exclusive: bool = False,
        consumer_tag: Optional[str] = None
    ):
        """
            Start consuming messages from the queue
            
            Args:
                callback: Callback function to process messages
                auto_ack: Whether to automatically acknowledge messages
                exclusive: Whether to use exclusive consumer
                consumer_tag: Optional consumer tag
        """
        try: 
            if not self._channel:
                self.logger.error("Channel not created")
                return
        
            if self._channel.is_closed:
                self.logger.error("Channel is closed")
                return

            def wrapped_callback(ch, method, properties, body):
                try:
                    decoded_message = json.loads(body)
                    callback(decoded_message)
                    if not auto_ack:
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                except json.JSONDecodeError as e:
                    self.logger.error(f"Failed to decode message: {str(e)}")
                    if not auto_ack:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                except Exception as e:
                    self.logger.error(f"Error in callback: {str(e)}")
                    if not auto_ack:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            self._channel.basic_consume(
                queue=self._queue,
                on_message_callback=wrapped_callback,
                auto_ack=auto_ack,
                exclusive=exclusive,
                consumer_tag=consumer_tag
            )
            self.logger.info("Started consuming messages")
            self._channel.start_consuming()

        except Exception as e:
                self.logger.error(f"Failed to start consuming: {str(e)}")
                raise RabbitMQError(f"Consume error: {str(e)}")

    def close(self):
        try:
            self.logger.info("Closing connection...")
            self.logger.info(self._connection.is_closed)

            if self._connection and not self._connection.is_closed:
                self._connection.close()
                self.logger.info("Connection closed successfully")
            else:
                self.logger.info("Connection already closed")
        except Exception as e:
            print(f"Error: {e}")
            raise e
        
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()