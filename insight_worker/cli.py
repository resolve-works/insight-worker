import click
import logging
import json
import ssl
from os import environ as env
from pika import ConnectionParameters, SelectConnection, PlainCredentials, SSLOptions
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from .models import Inodes
from .tasks import (
    ingest_file,
    embed_file,
    index_inode,
    delete_inode,
    move_inode,
    answer_prompt,
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
    id = json.loads(body)["id"]

    try:
        match method_frame.routing_key:
            case "ingest_file":
                inode = ingest_file(id)
                # Trigger next tasks
                for routing_key in ["index_inode", "embed_file"]:
                    channel.basic_publish(
                        exchange="insight",
                        routing_key=routing_key,
                        body=json.dumps({"id": str(inode.id)}),
                    )
                channel.basic_publish(
                    exchange="",
                    routing_key=f"user-{inode.owner_id}",
                    body=json.dumps({"id": str(inode.id), "task": "ingest_file"}),
                )
            case "index_inode":
                inode = index_inode(id)
                channel.basic_publish(
                    exchange="",
                    routing_key=f"user-{inode.owner_id}",
                    body=json.dumps({"id": str(inode.id), "task": "index_inode"}),
                )
            case "embed_file":
                inode = embed_file(id)
                channel.basic_publish(
                    exchange="",
                    routing_key=f"user-{inode.owner_id}",
                    body=json.dumps({"id": str(inode.id), "task": "embed_file"}),
                )
            case "answer_prompt":
                answer_prompt(id, channel)
            case "delete_inode":
                delete_inode(id, channel)
            case "move_inode":
                move_inode(id, channel)
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
