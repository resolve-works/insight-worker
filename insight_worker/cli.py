import click
import logging
import json
import ssl
from os import environ as env
from pika import ConnectionParameters, SelectConnection, PlainCredentials, SSLOptions
from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import Session
from .models import Inodes
from .opensearch import OpenSearchService
from .minio import MinioService
from .pdf import PdfService
from .worker import InsightWorker

logging.basicConfig(level=logging.INFO)

postgres_uri = env.get("POSTGRES_URI")
if not postgres_uri:
    logging.error("Missing required environment variable: POSTGRES_URI")
    raise ValueError("POSTGRES_URI environment variable is required")

connect_args = {"options": "-csearch_path=private,public"}
engine = create_engine(postgres_uri, connect_args=connect_args)

# Create global service instances
opensearch_service = OpenSearchService()
minio_service = MinioService()
pdf_service = PdfService()

# Create worker instance
worker = InsightWorker(engine, opensearch_service, minio_service, pdf_service)


def on_message(channel, method_frame, header_frame, body):
    body = json.loads(body)
    
    # Set the channel on the worker to use for publishing
    worker.channel = channel

    try:
        match method_frame.routing_key:
            case "ingest_inode":
                worker.ingest_inode(body["after"]["id"])
            case "embed_inode":
                worker.embed_inode(body["after"]["id"])
            case "index_inode":
                worker.index_inode(body["after"]["id"])
            case "move_inode":
                worker.move_inode(body["after"]["id"])
            case "share_inode":
                worker.share_inode(body["after"]["id"])
            case "delete_inode":
                worker.delete_inode(body["before"])
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
def create_index():
    logging.info("Creating index")
    try:
        opensearch_service.configure_index()
        logging.info("Index created successfully")
    except Exception as e:
        raise Exception(f"Failed to create index: {str(e)}")


@cli.command()
def delete_index():
    try:
        opensearch_service.delete_index()
        logging.info("Index destroyed successfully")
    except Exception as e:
        raise Exception(f"Failed to delete index: {str(e)}")


@cli.command()
def rebuild_index():
    try:
        opensearch_service.delete_index()
        logging.info("Index destroyed successfully")
    except Exception as e:
        pass

    try:
        opensearch_service.configure_index()
        logging.info("Index created successfully")
    except Exception as e:
        raise Exception(f"Failed to create index: {str(e)}")

    with Session(engine) as session:
        stmt = update(Inodes).values(is_indexed=False)
        session.execute(stmt)
        session.commit()

        stmt = select(Inodes.id)
        inodes = session.scalars(stmt).all()

        for inode_id in inodes:
            worker.index_inode(inode_id)


@cli.command()
def process_messages():
    # Configure index
    # This is here so we can just clear the dev environment and everything will still work
    opensearch_service.configure_index()

    ssl_options = None
    if env.get("RABBITMQ_SSL", "").lower() == "true":
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
