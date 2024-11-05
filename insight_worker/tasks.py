import logging
import ocrmypdf
import fitz
import json
import magic
from minio import Minio
from minio.commonconfig import CopySource
from minio.deleteobjects import DeleteObject
from os import environ as env
from urllib.parse import urlparse
from multiprocessing import Process
from tempfile import TemporaryDirectory
from pathlib import Path
from pikepdf import Pdf, PdfError
from itertools import chain
from sqlalchemy import create_engine, select, delete, func
from sqlalchemy.orm import Session
from .models import Pages, Inodes
from .opensearch import opensearch_request
from .rag import embed


logging.basicConfig(level=logging.INFO)

engine = create_engine(
    env.get("POSTGRES_URI"), connect_args={"options": "-csearch_path=private,public"}
)


def inode_path(owner_id, path):
    return f"users/{owner_id}{path}"


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


def ocrmypdf_process(input_file, output_file):
    ocrmypdf.ocr(
        input_file,
        output_file,
        output_type="pdf",
        language="nld",
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


def ingest_inode(id, channel=None):
    logging.info(f"Ingesting inode {id}")
    minio = get_minio()

    temp_path = Path(TemporaryDirectory().name)
    original_path = temp_path / "original"
    optimized_path = temp_path / "optimized"

    with Session(engine) as session:
        stmt = select(Inodes).where(Inodes.id == id)
        inode = session.scalars(stmt).one()
        owner_id = inode.owner_id
        path = inode_path(owner_id, inode.path)

        minio.fget_object(env.get("STORAGE_BUCKET"), f"{path}/original", original_path)

        try:
            try:
                # Is file actually PDF?
                mime = magic.from_file(original_path, mime=True)
                if mime != "application/pdf":
                    raise IngestException("unsupported_file_type")

                # User can supply to_page to slice PDF. When it's not set, slice till the end of pdf
                if not inode.to_page:
                    inode.to_page = get_pdf_pagecount(original_path)

                slice_pdf(original_path, inode.from_page, inode.to_page)

                # Trigger OCR & optimizations on sliced file
                optimize_pdf(original_path, optimized_path)

                minio.fput_object(
                    env.get("STORAGE_BUCKET"), f"{path}/optimized", optimized_path
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
                channel.basic_publish(
                    exchange="",
                    routing_key=f"user-{owner_id}",
                    body=json.dumps({"id": str(id), "task": "ingest_inode"}),
                )


def index_inode(data, channel=None):
    logging.info(f"Indexing inode {data['id']}")
    with Session(engine) as session:
        stmt = (
            select(Pages)
            .where(func.length(Pages.contents) > 0)
            .where(Pages.inode_id == data["id"])
        )
        pages = session.scalars(stmt).all()

        # Index file with pages and folder
        res = opensearch_request(
            "put",
            f"/inodes/_doc/{data['id']}",
            {
                "path": f"{data['path']}",
                "type": data["type"],
                "folder": str(Path(data["path"]).parent),
                "filename": data["name"],
                "owner_id": data["owner_id"],
                "is_public": data["is_public"],
                "readable_by": [data["owner_id"]],
                "pages": [
                    {
                        "file_id": data["id"],
                        "index": page.index - data["from_page"],
                        "contents": page.contents,
                    }
                    for page in pages
                ],
            },
        )
        if res.status_code not in [200, 201]:
            raise Exception(res.text)

        stmt = select(Inodes).where(Inodes.id == data["id"])
        inode = session.scalars(stmt).one()
        inode.is_indexed = True
        session.commit()

        if channel:
            channel.basic_publish(
                exchange="",
                routing_key=f"user-{data['owner_id']}",
                body=json.dumps({"id": data["id"], "task": "index_inode"}),
            )


def embed_inode(id, channel=None):
    logging.info(f"Embedding file {id}")
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
                exchange="",
                routing_key=f"user-{owner_id}",
                body=json.dumps({"id": str(id), "task": "embed_inode"}),
            )


def delete_inode(data, channel=None):
    logging.info(f"Deleting inode {data['id']}")
    minio = get_minio()

    # Make sure all original and optimized files are destroyed
    path = inode_path(data["owner_id"], data["path"])
    paths = [f"{path}/original", f"{path}/optimized"]
    delete_objects = (DeleteObject(path) for tuple in paths for path in tuple)

    errors = minio.remove_objects(env.get("STORAGE_BUCKET"), delete_objects)
    for error in errors:
        logging.error("error occurred when deleting object", error)

    # Remove indexed contents of files that descend this inode
    res = opensearch_request("delete", f"/inodes/_doc/{data['id']}")
    if res.status_code != 200:
        # Record could be not found for whatever reason
        logging.error(res.json())


def move_inode(id, channel=None):
    logging.info(f"Updating inode {id}")
    minio = get_minio()

    with Session(engine) as session:
        stmt = select(Inodes).where(Inodes.id == id)
        inode = session.scalars(stmt).one()
        stmt = select(func.inode_path(Inodes.id)).where(Inodes.id == inode.id)
        path = session.scalars(stmt).one()

        # If paths didn't change, we don't have to update the storage backend
        if path != inode.path:
            # Move in storage backend if this is a file
            if inode.type == "file":
                # Move both files
                for file in ["original", "optimized"]:
                    new_path = inode_path(inode.owner_id, path) + f"/{file}"
                    old_path = inode_path(inode.owner_id, inode.path) + f"/{file}"

                    # TODO - Proper error handling of storage failing
                    minio.copy_object(
                        env.get("STORAGE_BUCKET"),
                        new_path,
                        CopySource(env.get("STORAGE_BUCKET"), old_path),
                    )

                    minio.remove_object(env.get("STORAGE_BUCKET"), old_path)

            # Move succesful, save new path
            inode.path = path
            session.commit()
