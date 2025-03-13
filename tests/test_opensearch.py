import pytest
from unittest.mock import patch, MagicMock
from insight_worker.opensearch import OpenSearchService


@pytest.fixture
def opensearch_service():
    with patch("insight_worker.opensearch.httpx"):
        service = OpenSearchService(
            endpoint="https://opensearch.example.com",
            user="test_user",
            password="test_password",
            ca_cert="/path/to/ca.crt",
        )
        yield service


def test_init_with_params():
    with patch("insight_worker.opensearch.httpx"):
        service = OpenSearchService(
            endpoint="https://opensearch.example.com",
            user="test_user",
            password="test_password",
            ca_cert="/path/to/ca.crt",
        )
        
        assert service.endpoint == "https://opensearch.example.com"
        assert service.user == "test_user"
        assert service.password == "test_password"
        assert service.ca_cert == "/path/to/ca.crt"
        assert service.url.scheme == "https"
        assert service.token is not None


def test_init_with_env_vars():
    with patch("insight_worker.opensearch.env") as mock_env:
        mock_env.get.side_effect = lambda key, default=None: {
            "OPENSEARCH_ENDPOINT": "https://opensearch.example.com",
            "OPENSEARCH_USER": "env_user",
            "OPENSEARCH_PASSWORD": "env_password",
            "OPENSEARCH_CA_CERT": "/path/to/env/ca.crt",
        }.get(key, default)
        
        with patch("insight_worker.opensearch.httpx"):
            service = OpenSearchService()
            
            assert service.endpoint == "https://opensearch.example.com"
            assert service.user == "env_user"
            assert service.password == "env_password"
            assert service.ca_cert == "/path/to/env/ca.crt"


def test_request_https(opensearch_service):
    with patch("insight_worker.opensearch.httpx") as mock_httpx:
        opensearch_service._request("GET", "/test", {"key": "value"})
        
        mock_httpx.request.assert_called_once_with(
            "GET",
            "https://opensearch.example.com/test",
            headers={"Authorization": f"Basic {opensearch_service.token.decode()}"},
            verify="/path/to/ca.crt",
            json={"key": "value"},
        )


def test_request_http(opensearch_service):
    with patch("insight_worker.opensearch.httpx") as mock_httpx:
        # Change URL scheme to http
        opensearch_service.url = MagicMock()
        opensearch_service.url.scheme = "http"
        
        opensearch_service._request("POST", "/test")
        
        mock_httpx.request.assert_called_once_with(
            "POST",
            "https://opensearch.example.com/test",
            headers={"Authorization": f"Basic {opensearch_service.token.decode()}"},
            verify=None,
            json=None,
        )


def test_configure_index_success(opensearch_service):
    with patch("insight_worker.opensearch.httpx") as mock_httpx:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_httpx.request.return_value = mock_response
        
        result = opensearch_service.configure_index()
        
        assert result is True
        assert mock_httpx.request.call_count == 1
        assert mock_httpx.request.call_args[0][0] == "put"
        assert mock_httpx.request.call_args[0][1].endswith("/inodes")
        # Verify that JSON payload contains expected structure
        json_data = mock_httpx.request.call_args[1]["json"]
        assert "settings" in json_data
        assert "mappings" in json_data
        assert "properties" in json_data["mappings"]


def test_configure_index_already_exists(opensearch_service):
    with patch("insight_worker.opensearch.httpx") as mock_httpx:
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.json.return_value = {
            "error": {"type": "resource_already_exists_exception"}
        }
        mock_httpx.request.return_value = mock_response
        
        result = opensearch_service.configure_index()
        
        assert result is True


def test_configure_index_failure(opensearch_service):
    with patch("insight_worker.opensearch.httpx") as mock_httpx:
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal server error"
        mock_httpx.request.return_value = mock_response
        
        with pytest.raises(Exception, match="Internal server error"):
            opensearch_service.configure_index()


def test_delete_index_success(opensearch_service):
    with patch("insight_worker.opensearch.httpx") as mock_httpx:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_httpx.request.return_value = mock_response
        
        result = opensearch_service.delete_index()
        
        assert result is True
        mock_httpx.request.assert_called_once()
        assert mock_httpx.request.call_args[0][0] == "delete"
        assert mock_httpx.request.call_args[0][1].endswith("/inodes")


def test_delete_index_failure(opensearch_service):
    with patch("insight_worker.opensearch.httpx") as mock_httpx:
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = "Index not found"
        mock_httpx.request.return_value = mock_response
        
        with pytest.raises(Exception, match="Index not found"):
            opensearch_service.delete_index()


def test_index_document_success(opensearch_service):
    with patch("insight_worker.opensearch.httpx") as mock_httpx:
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_httpx.request.return_value = mock_response
        
        document = {"title": "Test Document", "content": "Test content"}
        result = opensearch_service.index_document("doc123", document)
        
        assert result == mock_response
        mock_httpx.request.assert_called_once_with(
            "put",
            "https://opensearch.example.com/inodes/_doc/doc123",
            headers={"Authorization": f"Basic {opensearch_service.token.decode()}"},
            verify="/path/to/ca.crt",
            json=document,
        )


def test_index_document_failure(opensearch_service):
    with patch("insight_worker.opensearch.httpx") as mock_httpx:
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "Bad request"
        mock_httpx.request.return_value = mock_response
        
        document = {"title": "Test Document", "content": "Test content"}
        with pytest.raises(Exception, match="Bad request"):
            opensearch_service.index_document("doc123", document)


def test_delete_document_success(opensearch_service):
    with patch("insight_worker.opensearch.httpx") as mock_httpx:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_httpx.request.return_value = mock_response
        
        result = opensearch_service.delete_document("doc123")
        
        assert result is True
        mock_httpx.request.assert_called_once_with(
            "delete",
            "https://opensearch.example.com/inodes/_doc/doc123",
            headers={"Authorization": f"Basic {opensearch_service.token.decode()}"},
            verify="/path/to/ca.crt",
            json=None,
        )


def test_delete_document_failure(opensearch_service):
    with patch("insight_worker.opensearch.httpx") as mock_httpx:
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_httpx.request.return_value = mock_response
        
        result = opensearch_service.delete_document("doc123")
        
        assert result is False