import click
import logging
import json
import ssl
from os import environ as env
from pika import ConnectionParameters, SelectConnection, PlainCredentials, SSLOptions
from .tasks import (
    analyze_file,
    ingest_document,
    index_document,
    embed_document,
    delete_file,
    delete_document,
    answer_prompt,
)
from .opensearch import opensearch_request
from sqlalchemy.exc import NoResultFound

logging.basicConfig(level=logging.INFO)


# Make elastic treat pages as nested objects
def create_mapping():
    logging.info("Creating index")

    json = {"mappings": {"properties": {"pages": {"type": "nested"}}}}
    res = opensearch_request("put", "/documents", json)

    if res.status_code == 200:
        logging.info("Index created succesfully")
        return
    elif (
        res.status_code == 400
        and res.json()["error"]["type"] == "resource_already_exists_exception"
    ):
        logging.info("Index creation skipped, index already exists")
        return
    else:
        raise Exception(res.text)


def on_message(channel, method_frame, header_frame, body):
    data = json.loads(body)

    try:
        match method_frame.routing_key:
            case "analyze_file":
                analyze_file(data["id"], channel)
            case "ingest_document":
                ingest_document(data["id"], channel)
            case "index_document":
                index_document(data["id"], channel)
            case "embed_document":
                embed_document(data["id"], channel)
            case "answer_prompt":
                answer_prompt(data["id"], channel)
            case "delete_file":
                delete_file(data["id"], channel)
            case "delete_document":
                delete_document(data["id"], channel)
            case _:
                raise Exception(f"Unknown routing key: {method_frame.routing_key}")

        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
    except Exception as e:
        logging.error("Could not process message", exc_info=e)
        channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)


def on_channel_open(channel):
    channel.basic_consume(env.get("QUEUE"), on_message)


def on_open(connection):
    connection.channel(on_open_callback=on_channel_open)


def on_close(connection, exception):
    connection.ioloop.stop()


@click.group()
def cli():
    pass


@cli.command()
def process_messages():
    create_mapping()

    ssl_options = None
    if env.get("RABBITMQ_SSL").lower() == "true":
        context = ssl.create_default_context()
        ssl_options = SSLOptions(context)

    parameters = ConnectionParameters(
        ssl_options=ssl_options,
        host=env.get("RABBITMQ_HOST"),
        credentials=PlainCredentials(
            env.get("RABBITMQ_USER"), env.get("RABBITMQ_PASSWORD")
        ),
    )

    connection = SelectConnection(
        parameters=parameters,
        on_open_callback=on_open,
        on_close_callback=on_close,
    )
    try:
        connection.ioloop.start()
    except SystemExit:
        # Gracefully close the connection
        connection.close()
        # Loop until we're fully closed.
        # The on_close callback is required to stop the io loop
        connection.ioloop.start()
