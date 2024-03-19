import httpx
from base64 import b64encode
from os import environ as env
from urllib.parse import urlparse


def opensearch_request(method, path, json=None):
    url = urlparse(env.get("OPENSEARCH_ENDPOINT"))
    token = b64encode(
        f"{env.get('OPENSEARCH_USER')}:{env.get('OPENSEARCH_PASSWORD')}".encode()
    )

    return httpx.request(
        method,
        f"{env.get('OPENSEARCH_ENDPOINT')}{path}",
        headers={"Authorization": f"Basic {token.decode()}"},
        verify=env.get("OPENSEARCH_CA_CERT") if url.scheme == "https" else None,
        json=json,
    )
