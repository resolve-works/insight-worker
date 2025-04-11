import logging
import httpx
from base64 import b64encode
from os import environ as env
from urllib.parse import urlparse
from typing import Dict, Optional, Any, Union


class OpenSearchService:
    """Service for handling OpenSearch operations"""

    def __init__(
        self,
        endpoint: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        ca_cert: Optional[Union[str, bool]] = None,
    ):
        self.endpoint = endpoint or env.get("OPENSEARCH_ENDPOINT")
        self.user = user or env.get("OPENSEARCH_USER")
        self.password = password or env.get("OPENSEARCH_PASSWORD")
        self.ca_cert = ca_cert if ca_cert is not None else env.get("OPENSEARCH_CA_CERT")
        self.url = urlparse(self.endpoint)
        self.token = b64encode(f"{self.user}:{self.password}".encode())

    def _request(self, method: str, path: str, json: Optional[Dict[str, Any]] = None):
        """Create a http request to OpenSearch with authentication

        :param method: GET, POST, PUT, DELETE
        :param path: the path to request
        :param json: json data to send
        :return: a `Response` object
        """
        return httpx.request(
            method,
            f"{self.endpoint}{path}",
            headers={"Authorization": f"Basic {self.token.decode()}"},
            verify=self.ca_cert if self.url.scheme == "https" else None,
            json=json,
        )

    def configure_index(self):
        """Create and configure the OpenSearch index with proper mappings and settings"""
        json = {
            "settings": {
                "analysis": {
                    "analyzer": {"path_analyzer": {"tokenizer": "path_tokenizer"}},
                    "tokenizer": {
                        "path_tokenizer": {
                            "type": "path_hierarchy",
                        }
                    },
                },
                "index.knn": True,
            },
            "mappings": {
                "properties": {
                    "join_field": {
                        "type": "join",
                        # Parent 'inode' has children of type 'page'
                        "relations": {"inode": "page"},
                    },
                    # Inode properties
                    "folder": {
                        "type": "text",
                        "analyzer": "path_analyzer",
                        "fielddata": True,
                    },
                    # Page properties
                    "contents": {
                        "type": "text",
                        "term_vector": "with_positions_offsets",
                    },
                    "embedding": {
                        "type": "knn_vector",
                        "dimension": 1536,
                        # "mode": "on_disk",
                        "method": {
                            "name": "hnsw",
                            "space_type": "l2",
                            "engine": "faiss",
                            "parameters": {"ef_construction": 400, "m": 16},
                        },
                    },
                }
            },
        }
        res = self._request("put", "/inodes", json)

        if res.status_code == 200:
            return True
        elif (
            res.status_code == 400
            and res.json()["error"]["type"] == "resource_already_exists_exception"
        ):
            return True
        else:
            raise Exception(res.text)

    def delete_index(self):
        """Delete the OpenSearch index"""
        res = self._request("delete", "/inodes")
        if res.status_code == 200:
            return True
        else:
            raise Exception(res.text)

    def index_document(
        self, id: str, document: Dict[str, Any], routing_key: str | None = None
    ):
        """Index a document in OpenSearch

        :param id: Document ID
        :param document: Document data
        :return: Response from OpenSearch
        """

        url = f"/inodes/_doc/{id}"
        if routing_key is not None:
            url += f"?routing={routing_key}"

        res = self._request("put", url, document)
        if res.status_code not in [200, 201]:
            raise Exception(res.text)
        return res

    def delete_document(self, id: str):
        """Delete a document from OpenSearch

        :param id: Document ID
        :return: Response from OpenSearch
        """
        delete_pages_query = {
            "query": {
                "has_parent": {"parent_type": "inode", "query": {"term": {"_id": id}}}
            }
        }

        # Delete all child pages first
        delete_pages_response = self._request(
            "post", f"/inodes/_delete_by_query?routing={id}", delete_pages_query
        )

        if delete_pages_response.status_code != 200:
            logging.error(delete_pages_response.json())

        # Then delete the parent document
        delete_inode_response = self._request("delete", f"/inodes/_doc/{id}")
        if delete_inode_response.status_code != 200:
            logging.error(delete_inode_response.json())
