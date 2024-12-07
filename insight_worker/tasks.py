import re
import logging
import ocrmypdf
import fitz
import json
import magic
import pika
import subprocess
from minio import Minio
from minio.commonconfig import CopySource, Tags
from minio.deleteobjects import DeleteObject
from os import environ as env
from urllib.parse import urlparse
from multiprocessing import Process
from tempfile import TemporaryDirectory
from pathlib import Path
from pikepdf import Pdf, PdfError
from itertools import chain
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import Session
from .models import Pages, Inodes
from .opensearch import opensearch_request
from .rag import embed


logging.basicConfig(level=logging.INFO)

engine = create_engine(
    env.get("POSTGRES_URI"), connect_args={"options": "-csearch_path=private,public"}
)


# This should probably live in the models. Keeping it here allows us to
# re-generate the models with sqlacodegen.
def object_path(owner_id, path):
    return f"users/{owner_id}{path}"


def optimized_object_path(owner_id, path):
    optimized_path = re.sub(r"(.+)(/[^/.]+)(\..+)$", r"\1\2_optimized\3", path)
    return f"users/{owner_id}{optimized_path}"


def get_minio():
    url = urlparse(env.get("STORAGE_ENDPOINT"))
    return Minio(
        url.netloc,
        secure=url.scheme == "https",
        access_key=env.get("STORAGE_ACCESS_KEY"),
        secret_key=env.get("STORAGE_SECRET_KEY"),
        # Supplying random region to minio will allow us to not have to set GetBucketLocation
        region=env.get("STORAGE_REGION", "insight"),
    )


def repair_pdf(input_file, output_file):
    subprocess.check_call(
        [
            "/usr/bin/gs",
            "-q",
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


class IngestException(Exception):
    pass


# Generate a OCRd and optimized version of a uploaded PDF. The resulting PDF is
# optimized for "fast web view", meaning it is linearized, allowing us to load
# only parts of it
def ingest_inode(id, channel=None):
    logging.info(f"Ingesting inode {id}")
    minio = get_minio()

    with TemporaryDirectory() as dir:
        temp_path = Path(dir)
        original_path = temp_path / "original"

        with Session(engine) as session:
            stmt = select(Inodes).where(Inodes.id == id)
            inode = session.scalars(stmt).one()

            minio.fget_object(
                env.get("STORAGE_BUCKET"),
                object_path(inode.owner_id, inode.path),
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
                    minio.fput_object(
                        env.get("STORAGE_BUCKET"),
                        optimized_object_path(inode.owner_id, inode.path),
                        optimized_path,
                    )

                    # If this is a public inode, mark the optimized file also as a public file
                    if inode.is_public:
                        tags = Tags.new_object_tags()
                        tags["is_public"] = str(inode.is_public)
                        minio.set_object_tags(
                            env.get("STORAGE_BUCKET"),
                            optimized_object_path(inode.owner_id, inode.path),
                            tags,
                        )

                    # fitz is pyMuPDF used for extracting text layers
                    file_pdf = fitz.open(optimized_path)

                    pages = [
                        Pages(
                            # Get all contents, sorted by position on page
                            contents=page.get_text(sort=True).strip(),
                            # Index pages in file instead of in file
                            index=inode.from_page + index,
                            inode_id=inode.id,
                        )
                        for index, page in enumerate(file_pdf)
                    ]
                    session.add_all(pages)
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

                    # Also notify user
                    channel.basic_publish(
                        exchange="user",
                        routing_key=(
                            "public" if inode.is_public else f"user-{inode.owner_id}"
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
        res = opensearch_request(
            "put",
            f"/inodes/_doc/{id}",
            {
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
            },
        )
        if res.status_code not in [200, 201]:
            raise Exception(res.text)

        inode.is_indexed = True
        session.commit()

        if channel:
            channel.basic_publish(
                exchange="user",
                routing_key="public" if inode.is_public else f"user-{owner_id}",
                body=json.dumps({"id": id, "task": "index_inode"}),
            )


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
                minio = get_minio()

                paths = [
                    (
                        object_path(inode.owner_id, inode.path),
                        object_path(inode.owner_id, path),
                    ),
                    (
                        optimized_object_path(inode.owner_id, inode.path),
                        optimized_object_path(inode.owner_id, path),
                    ),
                ]

                for old_path, new_path in paths:
                    # TODO - Proper error handling of storage failing
                    minio.copy_object(
                        env.get("STORAGE_BUCKET"),
                        new_path,
                        CopySource(env.get("STORAGE_BUCKET"), old_path),
                    )

                    minio.remove_object(env.get("STORAGE_BUCKET"), old_path)

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
            minio = get_minio()

            for path in [
                object_path(inode.owner_id, inode.path),
                optimized_object_path(inode.owner_id, inode.path),
            ]:
                tags = Tags.new_object_tags()
                tags["is_public"] = str(inode.is_public)
                minio.set_object_tags(env.get("STORAGE_BUCKET"), path, tags)

            # TODO - for user/group shares we can use IAM policies

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
        minio = get_minio()
        paths = [
            object_path(data["owner_id"], data["path"]),
            optimized_object_path(data["owner_id"], data["path"]),
        ]
        delete_objects = (DeleteObject(path) for path in paths)

        errors = minio.remove_objects(env.get("STORAGE_BUCKET"), delete_objects)
        for error in errors:
            logging.error(f"error occurred when deleting object", error)

    # Remove indexed contents of files that descend this inode
    res = opensearch_request("delete", f"/inodes/_doc/{data['id']}")
    if res.status_code != 200:
        # Record could be not found for whatever reason
        logging.error(res.json())
