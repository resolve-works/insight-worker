import httpx
import tiktoken
from os import environ as env
from itertools import islice

headers = {
    "Authorization": f"Bearer {env.get('OPENAI_API_KEY')}",
    "Content-Type": "application/json",
}

encoding = tiktoken.get_encoding("cl100k_base")


# Python 3.12 itertools provide this out of the box
def batched(iterable, n):
    iterator = iter(iterable)
    while batch := tuple(islice(iterator, n)):
        yield batch


def embed(strings):
    for batch in batched(strings, 64):
        # Send tokens to external service instead of the whole text
        # https://community.openai.com/t/embedding-tokens-vs-embedding-strings
        data = {
            # We split and join to remove all ocurrences of multiple whitespace
            # characters in sequence.
            # We slice by max length of embedding model here. Some files can
            # contain posters at A0 format with font-size 11pt...
            "input": [
                encoding.encode(" ".join(string.split()))[:8192] for string in batch
            ],
            "model": "text-embedding-3-small",
        }
        response = httpx.post(
            "https://api.openai.com/v1/embeddings",
            headers=headers,
            json=data,
            timeout=30,
        )
        if response.status_code == 200:
            for embedding in response.json()["data"]:
                yield embedding["embedding"]
        else:
            raise Exception(response.text)
