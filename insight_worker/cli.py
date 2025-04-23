import click
import logging
import asyncio
from os import environ as env
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

        # Use asyncio to run the indexing
        async def index_all():
            for inode_id in inodes:
                await worker.index_inode(inode_id)

        asyncio.run(index_all())


@cli.command()
def process_messages():
    # Configure index
    # This is here so we can just clear the dev environment and everything will still work
    opensearch_service.configure_index()

    async def main():
        # Setup RabbitMQ connection and start consuming messages
        queue = await worker.setup_rabbitmq()

        # Start consuming messages
        await queue.consume(worker.process_message)

        # Keep the connection open
        try:
            # This will keep the event loop running
            await asyncio.Future()
        except (KeyboardInterrupt, SystemExit):
            # Close the connection on exit
            await worker.connection.close()

    # Run the async main function
    asyncio.run(main())
