import logging
import json
import ssl
from os import environ as env
from tempfile import TemporaryDirectory
from pathlib import Path
from pikepdf import PdfError
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select, func
from sqlalchemy.orm import Session
import aio_pika

from insight_worker.opensearch import OpenSearchService
from insight_worker.minio import MinioService
from insight_worker.pdf import PdfService
from insight_worker.models import Pages, Inodes
from insight_worker.rag import embed


class IngestException(Exception):
    pass


class InsightWorker:
    def __init__(
        self,
        engine,
        opensearch_service: OpenSearchService,
        minio_service: MinioService,
        pdf_service: PdfService,
    ):
        self.engine = engine
        self.opensearch_service = opensearch_service
        self.minio_service = minio_service
        self.pdf_service = pdf_service
        self.connection = None
        self.channel = None
        self.insight_exchange = None
        self.user_exchange = None

    # Generate a OCRd and optimized version of a uploaded PDF. The resulting PDF is
    # optimized for "fast web view", meaning it is linearized, allowing us to load
    # only parts of it
    async def ingest_inode(self, id):
        logging.info(f"Ingesting inode {id}")

        with TemporaryDirectory() as dir:
            temp_path = Path(dir)
            original_path = temp_path / "original"

            with Session(self.engine) as session:
                stmt = select(Inodes).where(Inodes.id == id)
                inode = session.scalars(stmt).one()

                self.minio_service.download_file(
                    inode.owner_id,
                    inode.path,
                    original_path,
                )

                try:
                    # Is file actually PDF?
                    if not self.pdf_service.validate_pdf_mime_type(original_path):
                        raise IngestException("unsupported_file_type")

                    repaired_path = temp_path / "repaired"
                    optimized_path = temp_path / "optimized"

                    # Store the length of the PDF
                    if not inode.to_page:
                        try:
                            inode.to_page = self.pdf_service.get_pdf_page_count(
                                original_path
                            )
                        except PdfError:
                            raise IngestException("corrupted_file")

                    try:
                        # Repair and optimize the PDF
                        self.pdf_service.repair_pdf(original_path, repaired_path)
                        self.pdf_service.optimize_pdf(repaired_path, optimized_path)
                    except Exception:
                        raise IngestException("corrupted_file")

                    # Upload the optimized file
                    self.minio_service.upload_optimized_file(
                        inode.owner_id,
                        inode.path,
                        optimized_path,
                    )

                    # If this is a public inode, mark the optimized file also as a public file
                    if inode.is_public:
                        self.minio_service.set_public_tags(
                            inode.owner_id, inode.path, inode.is_public
                        )

                    # Extract text from the optimized PDF
                    page_texts = self.pdf_service.extract_pdf_pages_text(optimized_path)

                    # Create list of page records
                    page_values = [
                        {
                            "contents": text.replace("\x00", ""),
                            "index": inode.from_page + index,
                            "inode_id": inode.id,
                        }
                        for index, text in enumerate(page_texts)
                    ]

                    # Use PostgreSQL dialect insert with values and on_conflict_do_update
                    stmt = insert(Pages).values(page_values)

                    stmt = stmt.on_conflict_do_update(
                        constraint="pages_inode_id_index_key",
                        set_={
                            "contents": stmt.excluded.contents,
                        },
                    )

                    session.execute(stmt)
                except IngestException as e:
                    inode.error = str(e)
                except Exception as e:
                    logging.error(f"Error occurred during ingest of {id}", exc_info=e)
                finally:
                    inode.is_ingested = True
                    session.commit()

                    if self.channel:
                        # After ingest, trigger index & embed
                        body = json.dumps({"after": {"id": id}})
                        await self.insight_exchange.publish(
                            aio_pika.Message(
                                body=body.encode(),
                                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            ),
                            routing_key="embed_inode",
                        )

                        # Notify user when this inode had meaningful status changes
                        if inode.is_ready or inode.error:
                            # Also notify user
                            notification = json.dumps(
                                {"id": id, "task": "ingest_inode"}
                            )
                            routing_key = (
                                "public"
                                if inode.is_public
                                else f"user-{inode.owner_id}"
                            )
                            await self.user_exchange.publish(
                                aio_pika.Message(body=notification.encode()),
                                routing_key=routing_key,
                            )

    # Index inode into opensearch
    async def index_inode(self, id):
        logging.info(f"Indexing inode {id}")
        with Session(self.engine) as session:
            stmt = select(Inodes).where(Inodes.id == id)
            inode = session.scalars(stmt).one()
            owner_id = inode.owner_id
            stmt = (
                select(Pages)
                .where(func.length(Pages.contents) > 0)
                .where(Pages.inode_id == inode.id)
            )
            pages = session.scalars(stmt).all()

            try:
                # Index parent inode document
                parent_document = {
                    "id": inode.id,
                    "path": f"{inode.path}",
                    "type": inode.type,
                    "folder": str(Path(inode.path).parent),
                    "filename": inode.name,
                    "owner_id": str(owner_id),
                    "is_public": inode.is_public,
                    "readable_by": [str(owner_id)],
                    "join_field": {"name": "inode"},
                }

                # Index the parent inode first
                self.opensearch_service.index_document(id, parent_document)

                # Then index each page as a child document with proper routing
                for page in pages:
                    # Create a unique ID for each page
                    page_id = f"{id}_{page.index}"
                    page_document = {
                        "id": page.id,
                        "owner_id": str(owner_id),
                        "is_public": inode.is_public,
                        "readable_by": [str(owner_id)],
                        "index": page.index - inode.from_page,
                        "contents": page.contents,
                        "embedding": page.embedding.tolist(),
                        "join_field": {"name": "page", "parent": id},
                    }
                    self.opensearch_service.index_document(page_id, page_document, id)

                inode.is_indexed = True
                session.commit()

                if self.channel:
                    # Notify user when this inode had meaningful status changes
                    if inode.is_ready or inode.error:
                        notification = json.dumps({"id": id, "task": "index_inode"})
                        routing_key = (
                            "public" if inode.is_public else f"user-{owner_id}"
                        )
                        await self.user_exchange.publish(
                            aio_pika.Message(body=notification.encode()),
                            routing_key=routing_key,
                        )
            except Exception as e:
                logging.error(f"Error indexing document {id}: {str(e)}", exc_info=e)
                raise

    # Generate embeddings for inode pages
    async def embed_inode(self, id):
        logging.info(f"Embedding inode {id}")
        with Session(self.engine) as session:
            stmt = select(Inodes).where(Inodes.id == id)
            inode = session.scalars(stmt).one()

            if inode.error is not None:
                raise Exception("Cannot embed errored file")

            owner_id = inode.owner_id
            stmt = (
                select(Pages)
                .where(Pages.index >= inode.from_page)
                .where(Pages.index < inode.to_page)
                .where(Pages.embedding == None)
                .where(func.length(Pages.contents) > 0)
                .where(Pages.inode_id == inode.id)
            )
            pages = session.scalars(stmt).all()
            if pages:
                embeddings = embed([page.contents for page in pages])
                for embedding, page in zip(embeddings, pages):
                    page.embedding = embedding

            inode.is_embedded = True
            session.commit()

            if self.channel:
                body = json.dumps({"after": {"id": id}})
                await self.insight_exchange.publish(
                    aio_pika.Message(
                        body=body.encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    ),
                    routing_key="index_inode",
                )

                # Notify user when this inode had meaningful status changes
                if inode.is_ready or inode.error:
                    notification = json.dumps({"id": id, "task": "embed_inode"})
                    routing_key = "public" if inode.is_public else f"user-{owner_id}"
                    await self.user_exchange.publish(
                        aio_pika.Message(body=notification.encode()),
                        routing_key=routing_key,
                    )

    # Move file in object storage
    async def move_inode(self, id):
        logging.info(f"Moving inode {id}")

        with Session(self.engine) as session:
            stmt = select(Inodes).where(Inodes.id == id)
            inode = session.scalars(stmt).one()
            stmt = select(func.inode_path(Inodes.id)).where(Inodes.id == inode.id)
            path = session.scalars(stmt).first()

            # If paths didn't change, we don't have to update the storage backend
            if path != inode.path:
                # Move in storage backend if this is a file
                if inode.type == "file":
                    # Move file in storage
                    self.minio_service.move_file(inode.owner_id, inode.path, path)

                # Move succesful, save new path (object paths are inferred from path)
                inode.path = path
                inode.should_move = False
                session.commit()

                # After update, re-index
                if self.channel:
                    body = json.dumps({"after": {"id": id}})
                    await self.insight_exchange.publish(
                        aio_pika.Message(
                            body=body.encode(),
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        ),
                        routing_key="index_inode",
                    )

    # Make file accessible for non-owners on inode share
    async def share_inode(self, id):
        logging.info(f"Sharing inode {id}")

        with Session(self.engine) as session:
            stmt = select(Inodes).where(Inodes.id == id)
            inode = session.scalars(stmt).one()

            # For public files we use object tags to allow users access
            if inode.type == "file":
                self.minio_service.set_public_tags(
                    inode.owner_id, inode.path, inode.is_public
                )

        if self.channel:
            # Re-index this inode to change share status in opensearch to
            body = json.dumps({"after": {"id": id}})
            await self.insight_exchange.publish(
                aio_pika.Message(
                    body=body.encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key="index_inode",
            )

    # Remove files from object storage on inode deletion
    async def delete_inode(self, data):
        logging.info(f"Deleting inode {data['id']}")

        # Make sure all original and optimized files are destroyed
        if data["type"] == "file":
            errors = self.minio_service.delete_file(data["owner_id"], data["path"])
            for error in errors:
                logging.error(f"error occurred when deleting object", error)

        # Remove indexed contents of files that descend this inode
        try:
            self.opensearch_service.delete_document(data["id"])
        except Exception as e:
            # Record could be not found for whatever reason
            logging.error(f"Error deleting document {data['id']}: {str(e)}")

    async def setup_rabbitmq(self):
        # Configure SSL if enabled
        ssl_context = None
        if env.get("RABBITMQ_SSL", "").lower() == "true":
            ssl_context = ssl.create_default_context()

        # Connect to RabbitMQ
        self.connection = await aio_pika.connect_robust(
            host=env.get("RABBITMQ_HOST"),
            login=env.get("RABBITMQ_USER"),
            password=env.get("RABBITMQ_PASSWORD"),
            ssl=ssl_context is not None,
            ssl_context=ssl_context,
        )

        # Create channel
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=1)

        # Declare exchanges
        self.insight_exchange = await self.channel.declare_exchange(
            "insight", aio_pika.ExchangeType.DIRECT, durable=True
        )
        self.user_exchange = await self.channel.declare_exchange(
            "user", aio_pika.ExchangeType.TOPIC, durable=True
        )

        # Get queue name from environment
        queue_name = env.get("QUEUE")
        if not queue_name:
            raise ValueError("QUEUE environment variable is required")

        # Declare queue
        queue = await self.channel.declare_queue(queue_name, durable=True)

        return queue

    async def process_message(self, message: aio_pika.IncomingMessage):
        async with message.process():
            body = json.loads(message.body.decode())

            try:
                match message.routing_key:
                    case "ingest_inode":
                        await self.ingest_inode(body["after"]["id"])
                    case "embed_inode":
                        await self.embed_inode(body["after"]["id"])
                    case "index_inode":
                        await self.index_inode(body["after"]["id"])
                    case "move_inode":
                        await self.move_inode(body["after"]["id"])
                    case "share_inode":
                        await self.share_inode(body["after"]["id"])
                    case "delete_inode":
                        await self.delete_inode(body["before"])
                    case _:
                        raise Exception(f"Unknown routing key: {message.routing_key}")
            except Exception as e:
                logging.error(
                    f"Could not process message with routing key {message.routing_key}",
                    exc_info=e,
                )
                await message.nack(requeue=False)
