import json 
import pika
import pika.exceptions
from pika.exchange_type import ExchangeType
from pika.spec import BasicProperties
import logging
from functools import wraps
import time
from typing import Dict, Optional, Any, Callable

class RabbitMQError(Exception):
    pass

def retry_operation(max_attempts: int = 3, delay: int = 5):
    """Decorator for retrying operations with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        sleep_time = delay * (2 ** attempt)
                        logging.warning(f"Attempt {attempt + 1} failed. Retrying in {sleep_time} seconds...")
                        time.sleep(sleep_time)
            raise RabbitMQError(f"Operation failed after {max_attempts} attempts") from last_exception
        return wrapper
    return decorator

class RabbitMQ():
  """
    Enhanced RabbitMQ client with additional features and better error handling.
    
    Attributes:
        host (str): RabbitMQ server host
        user (str): RabbitMQ username
        password (str): RabbitMQ password
        queue_name (str): Default queue name
        exchange (str): Default exchange name
        exchange_type (ExchangeType): Type of exchange (direct, fanout, topic, headers)
        prefetch_count (int): Number of messages to prefetch
        logger (logging.Logger): Logger instance
    """

  def __init__(
      self, 
      host: str, 
      user: str, 
      password: str, 
      queue_name: str, 
      exchange: str = '', 
      exchange_type: ExchangeType = ExchangeType.direct,
      connection_timeout: int = 10,
      connection_attempts: int = 10,
      virtual_host: str = '/',
      log_level: int = logging.INFO
    ):
    self._host = host
    self._user = user
    self._password = password
    self._queue_name = queue_name
    self._exchange = exchange
    self._exchange_type = exchange_type
    self._connection_timeout = connection_timeout
    self._connection_attempts = connection_attempts
    self._virtual_host = virtual_host

    self.logger = logging.getLogger(__name__)
    self.logger.setLevel(log_level)

    self.connection = None
    self._channel = None
    self.start_server()
  
  @retry_operation(max_attempts=5, delay=2)
  def start_server(self):
      """Initialize server connection with retry mechanism"""
      self.create_channel()
      if self._exchange: 
        self.create_exchange()
      self.create_queue()

  def create_exchange(self):
      """Declare exchange with specified parameters"""
      try:
          self._channel.exchange_declare(
              exchange=self._exchange,
              exchange_type=self._exchange_type.value,
              durable=True,
              auto_delete=False
          )
          self.logger.info(f"Exchange '{self._exchange}' declared successfully")
      except Exception as e:
          self.logger.error(f"Failed to declare exchange: {str(e)}")
          raise RabbitMQError(f"Exchange declaration error: {str(e)}")

  def create_channel(self):
    try: 
      credentials = pika.PlainCredentials(self._user, self._password)
      params = pika.ConnectionParameters(
        host=self._host, 
        credentials=credentials,
        connection_attempts=10,
        retry_delay=5
      )

      self.connection = pika.BlockingConnection(params)
      self._channel = self._connection.channel()
    except pika.exceptions.AMQPConnectionError as e:
      print(f"Error: {e}")
      raise e
    
  def create_queue(self, queue_arguments: Optional[Dict] = None):
      """
      Declare queue with specified parameters
      
      Args:
          queue_arguments: Optional arguments for queue declaration (e.g., message TTL)
      """
      try:
          self._channel.queue_declare(
              queue=self._queue_name,
              durable=True,
              arguments=queue_arguments
          )
          if self._exchange:
              self._channel.queue_bind(
                  queue=self._queue_name,
                  exchange=self._exchange,
                  routing_key=self._queue_name
              )
          self.logger.info(f"Queue '{self._queue_name}' declared successfully")
      except Exception as e:
          self.logger.error(f"Failed to declare queue: {str(e)}")
          raise RabbitMQError(f"Queue declaration error: {str(e)}")

  def create_exchange(self):
      """Declare exchange with specified parameters"""
      try:
          self._channel.exchange_declare(
              exchange=self._exchange,
              exchange_type=self._exchange_type.value,
              durable=True,
              auto_delete=False
          )
          self.logger.info(f"Exchange '{self._exchange}' declared successfully")
      except Exception as e:
          self.logger.error(f"Failed to declare exchange: {str(e)}")
          raise RabbitMQError(f"Exchange declaration error: {str(e)}")
  
  @retry_operation(max_attempts=5, delay=2)
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

          routing_key = routing_key or self._queue_name
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

      self._channel.basic_publish(
          exchange=self._exchange,
          routing_key=self._queue_name,
          body=json.dumps(message),
          properties=properties
      )
      print(f"Sent message: {message}")

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
              queue=self._queue_name,
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
            self._channel.queue_purge(queue=self._queue_name)
            self.logger.info(f"Purged queue '{self._queue_name}'")
        except Exception as e:
            self.logger.error(f"Failed to purge queue: {str(e)}")
            raise RabbitMQError(f"Purge error: {str(e)}")

  def delete_queue(self):
      """Delete the queue"""
      try:
          self._channel.queue_delete(queue=self._queue_name)
          self.logger.info(f"Deleted queue '{self._queue_name}'")
      except Exception as e:
          self.logger.error(f"Failed to delete queue: {str(e)}")
          raise RabbitMQError(f"Delete error: {str(e)}")

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
              queue=self._queue_name,
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
        if self._connection and not self._connection.is_closed:
            self._connection.close()
            self.logger.info("Connection closed successfully")
    except Exception as e:
        print(f"Error: {e}")
        raise e
    
  def __enter__(self):
    return self
  
  def __exit__(self, exc_type, exc_val, exc_tb):
      """Context manager exit"""
      self.close()