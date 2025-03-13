import logging
import json
import pika
from os import environ as env
import ssl
from pika import ConnectionParameters, SSLOptions, PlainCredentials


class MessageService:
    def __init__(self):
        self.connection = None
        self.channel = None
        self._initialize_connection()

    def _initialize_connection(self):
        """Initialize connection to RabbitMQ"""
        # Check required environment variables
        required_vars = ["RABBITMQ_HOST", "RABBITMQ_USER", "RABBITMQ_PASSWORD"]
        missing_vars = [var for var in required_vars if not env.get(var)]
        if missing_vars:
            logging.error(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )
            return

        ssl_options = None
        if env.get("RABBITMQ_SSL", "").lower() == "true":
            context = ssl.create_default_context()
            ssl_options = SSLOptions(context)

        parameters = ConnectionParameters(
            ssl_options=ssl_options,
            host=env.get("RABBITMQ_HOST"),
            credentials=PlainCredentials(
                env.get("RABBITMQ_USER"), env.get("RABBITMQ_PASSWORD")
            ),
        )

        try:
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            logging.info("Successfully connected to RabbitMQ for publishing messages")
        except Exception as e:
            logging.error(f"Failed to connect to RabbitMQ: {str(e)}")
            self.connection = None
            self.channel = None

    def _ensure_connection(self):
        """Ensure connection is established"""
        if not self.connection or self.connection.is_closed:
            self._initialize_connection()

        if not self.channel or self.channel.is_closed:
            if self.connection and not self.connection.is_closed:
                self.channel = self.connection.channel()
            else:
                self._initialize_connection()

        return self.channel is not None

    def publish_task(self, routing_key, body):
        """Publish a task message to the insight exchange"""
        if isinstance(body, dict):
            body = json.dumps(body)

        if self._ensure_connection():
            try:
                self.channel.basic_publish(
                    exchange="insight",
                    routing_key=routing_key,
                    body=body,
                    properties=pika.BasicProperties(
                        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
                    ),
                )
                return True
            except Exception as e:
                logging.error(f"Failed to publish task message: {str(e)}")
                return False
        return False

    def publish_user_notification(self, user_id, is_public, message):
        """Publish a notification message to the user exchange"""
        if isinstance(message, dict):
            message = json.dumps(message)

        if self._ensure_connection():
            try:
                self.channel.basic_publish(
                    exchange="user",
                    routing_key="public" if is_public else f"user-{user_id}",
                    body=message,
                )
                return True
            except Exception as e:
                logging.error(f"Failed to publish user notification: {str(e)}")
                return False
        return False

    def close(self):
        """Close connection to RabbitMQ"""
        if self.connection and not self.connection.is_closed:
            try:
                self.connection.close()
                logging.info("RabbitMQ connection closed")
            except Exception as e:
                logging.error(f"Error closing RabbitMQ connection: {str(e)}")
