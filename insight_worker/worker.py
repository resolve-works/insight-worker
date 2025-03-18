import logging
import json
from tempfile import TemporaryDirectory
from pathlib import Path
from pikepdf import PdfError
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select, func
from sqlalchemy.orm import Session
from .models import Pages, Inodes
from .rag import embed


class IngestException(Exception):
    pass


class InsightWorker:
    def __init__(
        self,
        engine,
        opensearch_service,
        minio_service,
        pdf_service,
        message_service=None,
    ):
        self.engine = engine
        self.opensearch_service = opensearch_service
        self.minio_service = minio_service
        self.pdf_service = pdf_service
        self.message_service = message_service

    # Generate a OCRd and optimized version of a uploaded PDF. The resulting PDF is
    # optimized for "fast web view", meaning it is linearized, allowing us to load
    # only parts of it
    def ingest_inode(self, id):
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

                    if self.message_service:
                        # After ingest, trigger index & embed
                        body = json.dumps({"after": {"id": id}})
                        self.message_service.publish_task("embed_inode", body)

                        # Notify user when this inode had meaningful status changes
                        if inode.is_ready or inode.error:
                            # Also notify user
                            self.message_service.publish_user_notification(
                                inode.owner_id,
                                inode.is_public,
                                {"id": id, "task": "ingest_inode"},
                            )

    # Index inode into opensearch
    def index_inode(self, id):
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

            # Index file with pages and folder
            document = {
                "path": f"{inode.path}",
                "type": inode.type,
                "folder": str(Path(inode.path).parent),
                "filename": inode.name,
                "owner_id": str(owner_id),
                "is_public": inode.is_public,
                "readable_by": [str(owner_id)],
                "pages": [
                    {
                        "index": page.index - inode.from_page,
                        "contents": page.contents,
                        "embedding": page.embedding,
                    }
                    for page in pages
                ],
            }

            try:
                self.opensearch_service.index_document(id, document)
                inode.is_indexed = True
                session.commit()

                if self.message_service:
                    # Notify user when this inode had meaningful status changes
                    if inode.is_ready or inode.error:
                        self.message_service.publish_user_notification(
                            owner_id, inode.is_public, {"id": id, "task": "index_inode"}
                        )
            except Exception as e:
                logging.error(f"Error indexing document {id}: {str(e)}", exc_info=e)
                raise

    # Generate embeddings for inode pages
    def embed_inode(self, id):
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

            if self.message_service:
                body = json.dumps({"after": {"id": id}})
                self.message_service.publish_task("index_inode", body)

                # Notify user when this inode had meaningful status changes
                if inode.is_ready or inode.error:
                    self.message_service.publish_user_notification(
                        owner_id, inode.is_public, {"id": id, "task": "embed_inode"}
                    )

    # Move file in object storage
    def move_inode(self, id):
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
                if self.message_service:
                    body = json.dumps({"after": {"id": id}})
                    self.message_service.publish_task("index_inode", body)

    # Make file accessible for non-owners on inode share
    def share_inode(self, id):
        logging.info(f"Sharing inode {id}")

        with Session(self.engine) as session:
            stmt = select(Inodes).where(Inodes.id == id)
            inode = session.scalars(stmt).one()

            # For public files we use object tags to allow users access
            if inode.type == "file":
                self.minio_service.set_public_tags(
                    inode.owner_id, inode.path, inode.is_public
                )

        if self.message_service:
            # Re-index this inode to change share status in opensearch to
            body = json.dumps({"after": {"id": id}})
            self.message_service.publish_task("index_inode", body)

    # Remove files from object storage on inode deletion
    def delete_inode(self, data):
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
