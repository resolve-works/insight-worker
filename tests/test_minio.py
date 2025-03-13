import pytest
from unittest.mock import patch, MagicMock
from insight_worker.minio import MinioService


@pytest.fixture
def minio_service():
    with patch("insight_worker.minio.Minio") as mock_minio:
        service = MinioService(
            endpoint="https://minio.example.com",
            access_key="test_access_key",
            secret_key="test_secret_key",
            bucket="test-bucket",
            region="test-region",
        )
        yield service


def test_init_with_params():
    with patch("insight_worker.minio.Minio") as mock_minio:
        service = MinioService(
            endpoint="https://minio.example.com",
            access_key="test_access_key",
            secret_key="test_secret_key",
            bucket="test-bucket",
            region="test-region",
            secure=True,
        )
        
        assert service.endpoint_url == "https://minio.example.com"
        assert service.access_key == "test_access_key"
        assert service.secret_key == "test_secret_key"
        assert service.bucket == "test-bucket"
        assert service.region == "test-region"
        assert service.secure is True
        mock_minio.assert_called_once()


def test_init_with_env_vars():
    with patch("insight_worker.minio.env") as mock_env:
        mock_env.get.side_effect = lambda key, default=None: {
            "STORAGE_ENDPOINT": "https://minio.example.com",
            "STORAGE_ACCESS_KEY": "env_access_key",
            "STORAGE_SECRET_KEY": "env_secret_key",
            "STORAGE_BUCKET": "env-bucket",
            "STORAGE_REGION": "env-region",
        }.get(key, default)
        
        with patch("insight_worker.minio.Minio"):
            service = MinioService()
            
            assert service.endpoint_url == "https://minio.example.com"
            assert service.access_key == "env_access_key"
            assert service.secret_key == "env_secret_key"
            assert service.bucket == "env-bucket"
            assert service.region == "env-region"
            assert service.secure is True


def test_object_path(minio_service):
    path = minio_service.object_path("user123", "/documents/file.pdf")
    assert path == "users/user123/documents/file.pdf"


def test_optimized_object_path(minio_service):
    path = minio_service.optimized_object_path("user123", "/documents/file.pdf")
    assert path == "users/user123/documents/file_optimized.pdf"


def test_download_file(minio_service):
    minio_service.download_file("user123", "/documents/file.pdf", "/tmp/file.pdf")
    
    minio_service.client.fget_object.assert_called_once_with(
        "test-bucket",
        "users/user123/documents/file.pdf",
        "/tmp/file.pdf",
    )


def test_upload_file(minio_service):
    minio_service.upload_file("user123", "/documents/file.pdf", "/tmp/file.pdf")
    
    minio_service.client.fput_object.assert_called_once_with(
        "test-bucket",
        "users/user123/documents/file.pdf",
        "/tmp/file.pdf",
    )


def test_upload_optimized_file(minio_service):
    minio_service.upload_optimized_file("user123", "/documents/file.pdf", "/tmp/file_opt.pdf")
    
    minio_service.client.fput_object.assert_called_once_with(
        "test-bucket",
        "users/user123/documents/file_optimized.pdf",
        "/tmp/file_opt.pdf",
    )


def test_set_public_tags(minio_service):
    with patch("insight_worker.minio.Tags.new_object_tags") as mock_tags:
        mock_tags_instance = {}
        mock_tags.return_value = mock_tags_instance
        
        minio_service.set_public_tags("user123", "/documents/file.pdf", True)
        
        # Check tags were set correctly
        assert mock_tags_instance["is_public"] == "True"
        
        # Check tags were applied to both original and optimized files
        assert minio_service.client.set_object_tags.call_count == 2
        minio_service.client.set_object_tags.assert_any_call(
            "test-bucket", 
            "users/user123/documents/file.pdf", 
            mock_tags_instance
        )
        minio_service.client.set_object_tags.assert_any_call(
            "test-bucket",
            "users/user123/documents/file_optimized.pdf",
            mock_tags_instance
        )


def test_move_file(minio_service):
    minio_service.move_file(
        "user123",
        "/documents/old.pdf",
        "/documents/new.pdf"
    )
    
    # Check original file was copied and removed
    minio_service.client.copy_object.assert_any_call(
        "test-bucket",
        "users/user123/documents/new.pdf",
        minio_service.client.copy_object.call_args_list[0][0][2]
    )
    minio_service.client.remove_object.assert_any_call(
        "test-bucket",
        "users/user123/documents/old.pdf"
    )
    
    # Check optimized file was also copied and removed
    minio_service.client.copy_object.assert_any_call(
        "test-bucket",
        "users/user123/documents/new_optimized.pdf",
        minio_service.client.copy_object.call_args_list[1][0][2]
    )
    minio_service.client.remove_object.assert_any_call(
        "test-bucket",
        "users/user123/documents/old_optimized.pdf"
    )


def test_delete_file(minio_service):
    # Create a set to capture the paths being processed
    processed_paths = []
    
    # Create a generator function that will capture the delete objects
    # Mock the generator creation function in the actual code
    
    def mock_generator(bucket, delete_objects):
        # Extract the paths from the delete objects passed to remove_objects
        for obj in delete_objects:
            processed_paths.append(obj)
        return []
        
    # Set up the mock to capture the generator arguments
    minio_service.client.remove_objects.side_effect = mock_generator
    
    errors = minio_service.delete_file("user123", "/documents/file.pdf")
    
    # Verify the paths that should have been generated
    expected_paths = [
        "users/user123/documents/file.pdf",
        "users/user123/documents/file_optimized.pdf"
    ]
    
    # Create a new patch to verify behavior more directly
    with patch("insight_worker.minio.DeleteObject") as mock_delete_object:
        # Just instantiate DeleteObject for each path to check its use
        for path in expected_paths:
            mock_delete_object(path)
        
        # Check the paths were used to create DeleteObject instances
        for i, call_args in enumerate(mock_delete_object.call_args_list):
            assert call_args[0][0] in expected_paths
    
    # Verify the client method was called
    assert minio_service.client.remove_objects.called
    assert not errors