import click
import logging
import json
import ssl
from os import environ as env
from pika import ConnectionParameters, SelectConnection, PlainCredentials, SSLOptions
from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import Session
from .models import Inodes
from .tasks import (
    ingest_inode,
    embed_inode,
    index_inode,
    move_inode,
    share_inode,
    delete_inode,
)
from .opensearch import opensearch_request

logging.basicConfig(level=logging.INFO)


# Make elastic treat pages as nested objects
def configure_index():
    logging.info("Creating index")

    json = {
        "settings": {
            "analysis": {
                "analyzer": {"path_analyzer": {"tokenizer": "path_tokenizer"}},
                "tokenizer": {
                    "path_tokenizer": {
                        "type": "path_hierarchy",
                    }
                },
            }
        },
        "mappings": {
            "properties": {
                "pages": {"type": "nested"},
                "folder": {
                    "type": "text",
                    "analyzer": "path_analyzer",
                    "fielddata": True,
                },
            }
        },
    }
    res = opensearch_request("put", "/inodes", json)

    if res.status_code == 200:
        logging.info("Index created succesfully")
    elif (
        res.status_code == 400
        and res.json()["error"]["type"] == "resource_already_exists_exception"
    ):
        logging.info("Index creation skipped, index already exists")
    else:
        raise Exception(res.text)


def on_message(channel, method_frame, header_frame, body):
    body = json.loads(body)

    try:
        match method_frame.routing_key:
            case "ingest_inode":
                ingest_inode(body["after"]["id"], channel)
            case "embed_inode":
                embed_inode(body["after"]["id"], channel)
            case "index_inode":
                index_inode(body["after"]["id"], channel)
            case "move_inode":
                move_inode(body["after"]["id"], channel)
            case "share_inode":
                share_inode(body["after"]["id"], channel)
            case "delete_inode":
                delete_inode(body["before"], channel)
            case _:
                raise Exception(f"Unknown routing key: {method_frame.routing_key}")

        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
    except Exception as e:
        logging.error("Could not process message", exc_info=e)
        channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)


def on_channel_open(channel):
    # Prefetch is disabled because we send messages on the channel passed to
    # the on_message handler. When prefetch is enabled, these messages will
    # only be published after all prefetched messages have been acked.
    # A better solution would be to use a seperate publish channel
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(env.get("QUEUE"), on_message)


def on_open(connection):
    connection.channel(on_open_callback=on_channel_open)


def on_close(connection, exception):
    connection.ioloop.stop()


@click.group()
def cli():
    pass


@cli.command()
def delete_index():
    res = opensearch_request("delete", "/inodes")

    if res.status_code == 200:
        logging.info("Index destroyed succesfully")
    else:
        raise Exception(res.text)


@cli.command()
def rebuild_index():
    engine = create_engine(env.get("POSTGRES_URI"))

    with Session(engine) as session:
        stmt = update(Inodes).values(is_indexed=False)
        session.execute(stmt)
        session.commit()

        stmt = select(Inodes.id)
        inodes = session.scalars(stmt).all()

        for inode_id in inodes:
            index_inode(inode_id)


@cli.command()
def process_messages():
    configure_index()

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
