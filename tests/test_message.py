import pytest
from unittest.mock import patch, MagicMock, call
from insight_worker.message import MessageService
import ssl


@pytest.fixture
def message_service():
    with patch("insight_worker.message.env") as mock_env:
        mock_env.get.side_effect = lambda key, default=None: {
            "RABBITMQ_SSL": "false",
            "RABBITMQ_HOST": "rabbitmq.example.com",
            "RABBITMQ_USER": "test_user",
            "RABBITMQ_PASSWORD": "test_password",
        }.get(key, default)
        
        with patch("insight_worker.message.pika") as mock_pika:
            # Mock connection and channel
            mock_connection = MagicMock()
            mock_channel = MagicMock()
            mock_pika.BlockingConnection.return_value = mock_connection
            mock_connection.channel.return_value = mock_channel
            
            service = MessageService()
            yield service


def test_init_with_ssl():
    # First examine the MessageService initialization to understand what's happening
    with patch("insight_worker.message.env") as mock_env:
        mock_env.get.side_effect = lambda key, default=None: {
            "RABBITMQ_SSL": "true",
            "RABBITMQ_HOST": "rabbitmq.example.com",
            "RABBITMQ_USER": "test_user",
            "RABBITMQ_PASSWORD": "test_password",
        }.get(key, default)
        
        # Create a real SSL context for testing
        real_context = ssl.create_default_context()
        
        with patch("insight_worker.message.ssl.create_default_context", return_value=real_context):
            with patch("insight_worker.message.SSLOptions") as mock_ssl_options_class:
                mock_ssl_options = MagicMock(name="SSLOptionsInstance")
                mock_ssl_options_class.return_value = mock_ssl_options
                
                with patch("insight_worker.message.ConnectionParameters") as mock_connection_params:
                    with patch("insight_worker.message.pika.BlockingConnection"):
                        # Create the service instance which calls _initialize_connection
                        service = MessageService()
                        
                        # Verify SSLOptions was created with a context
                        mock_ssl_options_class.assert_called_once()
                        
                        # Verify ConnectionParameters was called with expected args
                        mock_connection_params.assert_called_once()
                        call_kwargs = mock_connection_params.call_args[1]
                        assert call_kwargs["host"] == "rabbitmq.example.com"
                        assert call_kwargs["ssl_options"] is mock_ssl_options


def test_init_without_ssl():
    with patch("insight_worker.message.env") as mock_env:
        mock_env.get.side_effect = lambda key, default=None: {
            "RABBITMQ_SSL": "false",
            "RABBITMQ_HOST": "rabbitmq.example.com",
            "RABBITMQ_USER": "test_user",
            "RABBITMQ_PASSWORD": "test_password",
        }.get(key, default)
        
        with patch("insight_worker.message.PlainCredentials") as mock_credentials_class:
            # Mock the credentials object
            mock_credentials = MagicMock(name="PlainCredentialsInstance")
            mock_credentials_class.return_value = mock_credentials
            
            with patch("insight_worker.message.ConnectionParameters") as mock_connection_params:
                with patch("insight_worker.message.pika.BlockingConnection"):
                    # Create the service which calls _initialize_connection
                    service = MessageService()
                    
                    # Verify credentials were created
                    mock_credentials_class.assert_called_once_with("test_user", "test_password")
                    
                    # Verify connection parameters
                    mock_connection_params.assert_called_once()
                    call_kwargs = mock_connection_params.call_args[1]
                    assert call_kwargs["host"] == "rabbitmq.example.com"
                    assert call_kwargs["ssl_options"] is None
                    assert call_kwargs["credentials"] == mock_credentials


def test_init_connection_failure():
    with patch("insight_worker.message.env") as mock_env:
        mock_env.get.side_effect = lambda key, default=None: {
            "RABBITMQ_SSL": "false",
            "RABBITMQ_HOST": "rabbitmq.example.com",
            "RABBITMQ_USER": "test_user",
            "RABBITMQ_PASSWORD": "test_password",
        }.get(key, default)
        
        with patch("insight_worker.message.pika") as mock_pika:
            # Mock connection failure
            mock_pika.BlockingConnection.side_effect = Exception("Connection failed")
            
            with patch("insight_worker.message.logging") as mock_logging:
                service = MessageService()
                
                assert service.connection is None
                assert service.channel is None
                mock_logging.error.assert_called_once()
                assert "Connection failed" in mock_logging.error.call_args[0][0]


def test_ensure_connection_when_closed(message_service):
    # Reset any previous calls to initialize_connection
    with patch.object(message_service, "_initialize_connection") as mock_init:
        # Set connection as closed
        message_service.connection.is_closed = True
        
        # Test _ensure_connection
        result = message_service._ensure_connection()
        
        # Only verify that initialization was called at least once
        assert mock_init.called
        assert result == (message_service.channel is not None)


def test_ensure_connection_channel_closed(message_service):
    # Reset the connection.channel call counter
    message_service.connection.channel.reset_mock()
    
    # Mock connection is open but channel is closed
    message_service.connection.is_closed = False
    message_service.channel.is_closed = True
    
    result = message_service._ensure_connection()
    
    # Verify channel was obtained from connection
    assert message_service.connection.channel.called
    assert result == (message_service.channel is not None)


def test_publish_task_success(message_service):
    # Ensure connection is established
    message_service.connection.is_closed = False
    message_service.channel.is_closed = False
    
    result = message_service.publish_task("task.process", {"id": "123"})
    
    assert result is True
    message_service.channel.basic_publish.assert_called_once_with(
        exchange="insight",
        routing_key="task.process",
        body='{"id": "123"}',
        properties=message_service.channel.basic_publish.call_args[1]["properties"],
    )


def test_publish_task_string_message(message_service):
    # Ensure connection is established
    message_service.connection.is_closed = False
    message_service.channel.is_closed = False
    
    result = message_service.publish_task("task.process", "string message")
    
    assert result is True
    message_service.channel.basic_publish.assert_called_once_with(
        exchange="insight",
        routing_key="task.process",
        body="string message",
        properties=message_service.channel.basic_publish.call_args[1]["properties"],
    )


def test_publish_task_connection_failure(message_service):
    # Mock _ensure_connection failure
    with patch.object(message_service, "_ensure_connection", return_value=False):
        result = message_service.publish_task("task.process", {"id": "123"})
        
        assert result is False
        message_service.channel.basic_publish.assert_not_called()


def test_publish_task_publish_failure(message_service):
    # Ensure connection is established
    message_service.connection.is_closed = False
    message_service.channel.is_closed = False
    
    # Mock publish failure
    message_service.channel.basic_publish.side_effect = Exception("Publish failed")
    
    with patch("insight_worker.message.logging") as mock_logging:
        result = message_service.publish_task("task.process", {"id": "123"})
        
        assert result is False
        mock_logging.error.assert_called_once()
        assert "Publish failed" in mock_logging.error.call_args[0][0]


def test_publish_user_notification_success(message_service):
    # Ensure connection is established
    message_service.connection.is_closed = False
    message_service.channel.is_closed = False
    
    result = message_service.publish_user_notification(
        "user123", False, {"message": "Test notification"}
    )
    
    assert result is True
    message_service.channel.basic_publish.assert_called_once_with(
        exchange="user",
        routing_key="user-user123",
        body='{"message": "Test notification"}',
    )


def test_publish_user_notification_public(message_service):
    # Ensure connection is established
    message_service.connection.is_closed = False
    message_service.channel.is_closed = False
    
    result = message_service.publish_user_notification(
        "user123", True, {"message": "Public notification"}
    )
    
    assert result is True
    message_service.channel.basic_publish.assert_called_once_with(
        exchange="user",
        routing_key="public",
        body='{"message": "Public notification"}',
    )


def test_publish_user_notification_string_message(message_service):
    # Ensure connection is established
    message_service.connection.is_closed = False
    message_service.channel.is_closed = False
    
    result = message_service.publish_user_notification(
        "user123", False, "String notification"
    )
    
    assert result is True
    message_service.channel.basic_publish.assert_called_once_with(
        exchange="user",
        routing_key="user-user123",
        body="String notification",
    )


def test_publish_user_notification_failure(message_service):
    # Ensure connection is established
    message_service.connection.is_closed = False
    message_service.channel.is_closed = False
    
    # Mock publish failure
    message_service.channel.basic_publish.side_effect = Exception("Publish failed")
    
    with patch("insight_worker.message.logging") as mock_logging:
        result = message_service.publish_user_notification(
            "user123", False, {"message": "Test"}
        )
        
        assert result is False
        mock_logging.error.assert_called_once()
        assert "Publish failed" in mock_logging.error.call_args[0][0]


def test_close_connection_success(message_service):
    # Mock connection is open
    message_service.connection.is_closed = False
    
    with patch("insight_worker.message.logging") as mock_logging:
        message_service.close()
        
        message_service.connection.close.assert_called_once()
        mock_logging.info.assert_called_once_with("RabbitMQ connection closed")


def test_close_connection_failure(message_service):
    # Mock connection is open but close fails
    message_service.connection.is_closed = False
    message_service.connection.close.side_effect = Exception("Close failed")
    
    with patch("insight_worker.message.logging") as mock_logging:
        message_service.close()
        
        message_service.connection.close.assert_called_once()
        mock_logging.error.assert_called_once()
        assert "Close failed" in mock_logging.error.call_args[0][0]


def test_close_connection_already_closed(message_service):
    # Mock connection is already closed
    message_service.connection.is_closed = True
    
    message_service.close()
    
    message_service.connection.close.assert_not_called()