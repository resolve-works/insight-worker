import re
from minio import Minio
from minio.commonconfig import CopySource, Tags
from minio.deleteobjects import DeleteObject
from os import environ as env
from urllib.parse import urlparse
from typing import Optional, List, Iterable, Dict, Any


class MinioService:
    """Service for handling MinIO object storage operations"""

    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        bucket: Optional[str] = None,
        region: Optional[str] = None,
        secure: Optional[bool] = None,
    ):
        self.endpoint_url = endpoint or env.get("STORAGE_ENDPOINT")
        self.access_key = access_key or env.get("STORAGE_ACCESS_KEY")
        self.secret_key = secret_key or env.get("STORAGE_SECRET_KEY")
        self.bucket = bucket or env.get("STORAGE_BUCKET")
        self.region = region or env.get("STORAGE_REGION", "insight")
        
        url = urlparse(self.endpoint_url)
        self.secure = secure if secure is not None else (url.scheme == "https")
        self.client = Minio(
            url.netloc,
            secure=self.secure,
            access_key=self.access_key,
            secret_key=self.secret_key,
            region=self.region,
        )

    def object_path(self, owner_id: str, path: str) -> str:
        """Generate the object path for a file

        Args:
            owner_id: User ID that owns the file
            path: Path to the file

        Returns:
            Full object path in storage
        """
        return f"users/{owner_id}{path}"

    def optimized_object_path(self, owner_id: str, path: str) -> str:
        """Generate the object path for an optimized file

        Args:
            owner_id: User ID that owns the file
            path: Path to the file

        Returns:
            Full object path for the optimized version
        """
        optimized_path = re.sub(r"(.+)(/[^/.]+)(\..+)$", r"\1\2_optimized\3", path)
        return f"users/{owner_id}{optimized_path}"

    def download_file(self, owner_id: str, path: str, target_path: str) -> None:
        """Download a file from object storage

        Args:
            owner_id: User ID that owns the file
            path: Path to the file
            target_path: Local filesystem path to download to
        """
        self.client.fget_object(
            self.bucket,
            self.object_path(owner_id, path),
            target_path,
        )

    def upload_file(self, owner_id: str, path: str, source_path: str) -> None:
        """Upload a file to object storage

        Args:
            owner_id: User ID that owns the file
            path: Path to store the file
            source_path: Local filesystem path to upload from
        """
        self.client.fput_object(
            self.bucket,
            self.object_path(owner_id, path),
            source_path,
        )

    def upload_optimized_file(self, owner_id: str, path: str, source_path: str) -> None:
        """Upload an optimized file to object storage

        Args:
            owner_id: User ID that owns the file
            path: Path to store the file
            source_path: Local filesystem path to upload from
        """
        self.client.fput_object(
            self.bucket,
            self.optimized_object_path(owner_id, path),
            source_path,
        )

    def set_public_tags(self, owner_id: str, path: str, is_public: bool) -> None:
        """Set public access tags on a file

        Args:
            owner_id: User ID that owns the file
            path: Path to the file
            is_public: Whether the file should be publicly accessible
        """
        tags = Tags.new_object_tags()
        tags["is_public"] = str(is_public)
        
        # Apply tags to both original and optimized files
        for object_path in [
            self.object_path(owner_id, path),
            self.optimized_object_path(owner_id, path),
        ]:
            self.client.set_object_tags(self.bucket, object_path, tags)

    def move_file(self, owner_id: str, old_path: str, new_path: str) -> None:
        """Move a file in object storage

        Args:
            owner_id: User ID that owns the file
            old_path: Current path of the file
            new_path: New path for the file
        """
        paths = [
            (
                self.object_path(owner_id, old_path),
                self.object_path(owner_id, new_path),
            ),
            (
                self.optimized_object_path(owner_id, old_path),
                self.optimized_object_path(owner_id, new_path),
            ),
        ]

        for old, new in paths:
            self.client.copy_object(
                self.bucket,
                new,
                CopySource(self.bucket, old),
            )
            self.client.remove_object(self.bucket, old)

    def delete_file(self, owner_id: str, path: str) -> List[Dict[str, Any]]:
        """Delete a file from object storage

        Args:
            owner_id: User ID that owns the file
            path: Path to the file

        Returns:
            List of any errors that occurred during deletion
        """
        paths = [
            self.object_path(owner_id, path),
            self.optimized_object_path(owner_id, path),
        ]
        delete_objects = (DeleteObject(path) for path in paths)
        
        errors = list(self.client.remove_objects(self.bucket, delete_objects))
        return errors