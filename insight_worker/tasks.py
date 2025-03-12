import logging
import ocrmypdf
import json
import magic
import pika
import subprocess
from os import environ as env
from multiprocessing import Process
from tempfile import TemporaryDirectory
from pathlib import Path
from pikepdf import Pdf, PdfError
from itertools import chain
from sqlalchemy import create_engine, select, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextContainer
from .models import Pages, Inodes
from .opensearch import OpenSearchService
from .minio import MinioService
from .rag import embed

# Create global service instances
opensearch_service = OpenSearchService()
minio_service = MinioService()


logging.basicConfig(level=logging.INFO)

engine = create_engine(
    env.get("POSTGRES_URI"), connect_args={"options": "-csearch_path=private,public"}
)


# Helper functions for PDF processing


def repair_pdf(input_file, output_file):
    subprocess.check_call(
        [
            "/usr/bin/gs",
            "-dSAFER",
            "-dNOPAUSE",
            "-dBATCH",
            "-sDEVICE=pdfwrite",
            "-o",
            output_file,
            input_file,
        ]
    )


def ocrmypdf_process(input_file, output_file):
    ocrmypdf.ocr(
        input_file,
        output_file,
        # Default is pdf-a, but the PDF/a spec for some reason does not support
        # pdf alignment. PDFJS refuses to use range-requests on a pdf/a.
        output_type="pdf",
        color_conversion_strategy="RGB",
        progress_bar=False,
        # https://github.com/ocrmypdf/OCRmyPDF/issues/1162
        continue_on_soft_render_error=True,
        # Only use one thread
        jobs=1,
        # Skip pages with text layer on it
        # TODO - Enable user to force OCR
        skip_text=True,
        # plugins=["insight_worker.plugin"],
        # Lossless optimization
        optimize=2,
        invalidate_digital_signatures=True,
    )


def get_pdf_pagecount(path):
    with Pdf.open(path) as pdf:
        return len(pdf.pages)


def slice_pdf(path, from_page, to_page):
    with Pdf.open(path) as pdf:
        before_range = range(0, from_page)
        after_range = range(to_page, len(pdf.pages))
        pages_to_delete = list(chain(before_range, after_range))

        # Split file from original if there's pages explicitely set
        if len(pages_to_delete):
            # Reverse the range to remove last page first as the file shrinks
            # when removing pages, leading to IndexErrors otherwise
            for p in reversed(pages_to_delete):
                del pdf.pages[p]
            pdf.save(path)


def optimize_pdf(input_path, output_path):
    # OCR & optimize new PDF
    process = Process(target=ocrmypdf_process, args=(input_path, output_path))
    process.start()
    process.join()


def extract_pdf_pages_text(path):
    texts = []
    for page_layout in extract_pages(path):
        text = ""
        for element in page_layout:
            if isinstance(element, LTTextContainer):
                text += element.get_text()

        texts.append(text)

    return texts


class IngestException(Exception):
    pass


# Generate a OCRd and optimized version of a uploaded PDF. The resulting PDF is
# optimized for "fast web view", meaning it is linearized, allowing us to load
# only parts of it
def ingest_inode(id, channel=None):
    logging.info(f"Ingesting inode {id}")

    with TemporaryDirectory() as dir:
        temp_path = Path(dir)
        original_path = temp_path / "original"

        with Session(engine) as session:
            stmt = select(Inodes).where(Inodes.id == id)
            inode = session.scalars(stmt).one()

            minio_service.download_file(
                inode.owner_id,
                inode.path,
                original_path,
            )

            try:
                try:
                    # Is file actually PDF?
                    mime = magic.from_file(original_path, mime=True)
                    if mime != "application/pdf":
                        raise IngestException("unsupported_file_type")

                    repaired_path = temp_path / "repaired"
                    optimized_path = temp_path / "optimized"

                    # User can supply to_page to slice PDF. When it's not set, slice till the end of pdf
                    if not inode.to_page:
                        inode.to_page = get_pdf_pagecount(original_path)

                    # Slice file from original file, OCR & Optimize and upload
                    repair_pdf(original_path, repaired_path)
                    slice_pdf(repaired_path, inode.from_page, inode.to_page)
                    optimize_pdf(repaired_path, optimized_path)
                    minio_service.upload_optimized_file(
                        inode.owner_id,
                        inode.path,
                        optimized_path,
                    )

                    # If this is a public inode, mark the optimized file also as a public file
                    if inode.is_public:
                        minio_service.set_public_tags(
                            inode.owner_id, inode.path, inode.is_public
                        )

                    page_texts = extract_pdf_pages_text(optimized_path)

                    stmt = insert(Pages).values(
                        [
                            {
                                # Get all contents, sorted by position on page
                                "contents": text.replace("\x00", ""),
                                # Index pages in file instead of in file
                                "index": inode.from_page + index,
                                "inode_id": inode.id,
                            }
                            for index, text in enumerate(page_texts)
                        ]
                    )

                    stmt = stmt.on_conflict_do_update(
                        constraint="pages_inode_id_index_key",
                        set_={
                            "contents": stmt.excluded.contents,
                        },
                    )

                    session.execute(stmt)
                except PdfError as e:
                    raise IngestException("corrupted_file")
            except IngestException as e:
                inode.error = str(e)
            except Exception as e:
                logging.error(f"Error occurred during ingest of {id}", e)
            finally:
                inode.is_ingested = True
                session.commit()

                if channel:
                    # After ingest, trigger index & embed
                    body = json.dumps({"after": {"id": id}})
                    channel.basic_publish(
                        exchange="insight",
                        routing_key="embed_inode",
                        body=body,
                        properties=pika.BasicProperties(
                            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
                        ),
                    )
                    channel.basic_publish(
                        exchange="insight",
                        routing_key="index_inode",
                        body=body,
                        properties=pika.BasicProperties(
                            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
                        ),
                    )

                    # Notify user when this inode had meaningful status changes
                    stmt = select(Inodes).where(Inodes.id == id)
                    inode = session.scalars(stmt).one()

                    if inode.is_ready or inode.error:
                        # Also notify user
                        channel.basic_publish(
                            exchange="user",
                            routing_key=(
                                "public"
                                if inode.is_public
                                else f"user-{inode.owner_id}"
                            ),
                            body=json.dumps({"id": id, "task": "ingest_inode"}),
                        )


# Index inode into opensearch
def index_inode(id, channel=None):
    logging.info(f"Indexing inode {id}")
    with Session(engine) as session:
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
                }
                for page in pages
            ],
        }

        try:
            opensearch_service.index_document(id, document)
            inode.is_indexed = True
            session.commit()

            if channel:
                # Notify user when this inode had meaningful status changes
                stmt = select(Inodes).where(Inodes.id == id)
                inode = session.scalars(stmt).one()

                if inode.is_ready or inode.error:
                    channel.basic_publish(
                        exchange="user",
                        routing_key="public" if inode.is_public else f"user-{owner_id}",
                        body=json.dumps({"id": id, "task": "index_inode"}),
                    )
        except Exception as e:
            logging.error(f"Error indexing document {id}: {str(e)}")
            raise


# Generate embeddings for inode pages
def embed_inode(id, channel=None):
    logging.info(f"Embedding inode {id}")
    with Session(engine) as session:
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
        embeddings = embed([page.contents for page in pages])

        for embedding, page in zip(embeddings, pages):
            page.embedding = embedding

        inode.is_embedded = True
        session.commit()

        if channel:
            # Notify user when this inode had meaningful status changes
            stmt = select(Inodes).where(Inodes.id == id)
            inode = session.scalars(stmt).one()

            if inode.is_ready or inode.error:
                channel.basic_publish(
                    exchange="user",
                    routing_key="public" if inode.is_public else f"user-{owner_id}",
                    body=json.dumps({"id": id, "task": "embed_inode"}),
                )


# Move file in object storage
def move_inode(id, channel=None):
    logging.info(f"Moving inode {id}")

    with Session(engine) as session:
        stmt = select(Inodes).where(Inodes.id == id)
        inode = session.scalars(stmt).one()
        stmt = select(func.inode_path(Inodes.id)).where(Inodes.id == inode.id)
        path = session.scalars(stmt).first()

        # If paths didn't change, we don't have to update the storage backend
        if path != inode.path:
            # Move in storage backend if this is a file
            if inode.type == "file":
                # Move file in storage
                minio_service.move_file(inode.owner_id, inode.path, path)

            # Move succesful, save new path (object paths are inferred from path)
            inode.path = path
            inode.should_move = False
            session.commit()

            # After update, re-index
            if channel:
                body = json.dumps({"after": {"id": id}})
                channel.basic_publish(
                    exchange="insight",
                    routing_key="index_inode",
                    body=body,
                    properties=pika.BasicProperties(
                        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
                    ),
                )


# Make file accessible for non-owners on inode share
def share_inode(id, channel=None):
    logging.info(f"Sharing inode {id}")

    with Session(engine) as session:
        stmt = select(Inodes).where(Inodes.id == id)
        inode = session.scalars(stmt).one()

        # For public files we use object tags to allow users access
        if inode.type == "file":
            minio_service.set_public_tags(inode.owner_id, inode.path, inode.is_public)

    if channel:
        # Re-index this inode to change share status in opensearch to
        body = json.dumps({"after": {"id": id}})
        channel.basic_publish(
            exchange="insight",
            routing_key="index_inode",
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
            ),
        )


# Remove files from object storage on inode deletion
def delete_inode(data, channel=None):
    logging.info(f"Deleting inode {data['id']}")

    # Make sure all original and optimized files are destroyed
    if data["type"] == "file":
        errors = minio_service.delete_file(data["owner_id"], data["path"])
        for error in errors:
            logging.error(f"error occurred when deleting object", error)

    # Remove indexed contents of files that descend this inode
    try:
        opensearch_service.delete_document(data["id"])
    except Exception as e:
        # Record could be not found for whatever reason
        logging.error(f"Error deleting document {data['id']}: {str(e)}")
