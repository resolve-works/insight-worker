import logging
import ocrmypdf
import fitz
import json
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
from sqlalchemy.exc import NoResultFound
from .models import Files, Pages, Inodes
from .opensearch import opensearch_request
from .rag import embed


logging.basicConfig(level=logging.INFO)

engine = create_engine(env.get("POSTGRES_URI"))


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


def ingest_file(id):
    logging.info(f"Ingesting file {id}")
    minio = get_minio()

    temp_path = Path(TemporaryDirectory().name)
    original_path = temp_path / "original"
    optimized_path = temp_path / "optimized"

    with Session(engine) as session:
        stmt = select(Inodes).join(Inodes.files).where(Inodes.id == id)
        inode = session.scalars(stmt).one()
        owner_id = inode.owner_id
        path = inode_path(owner_id, inode.path)

        minio.fget_object(env.get("STORAGE_BUCKET"), f"{path}/original", original_path)

        try:
            # User can supply to_page to slice PDF. When it's not set, slice till the end of pdf
            if not inode.files.to_page:
                inode.files.to_page = get_pdf_pagecount(original_path)

            slice_pdf(original_path, inode.files.from_page, inode.files.to_page)

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
                    index=inode.files.from_page + index,
                    inode_id=inode.id,
                )
                for index, page in enumerate(file_pdf)
            ]
            session.add_all(pages)
            inode.files.is_ingested = True
        except PdfError as e:
            # TODO - Set file error state
            raise e
        finally:
            session.commit()

        return owner_id


def index_inode(id):
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
            f"/inodes/_doc/{str(inode.id)}",
            {
                "path": f"{inode.path}",
                "type": inode.type,
                "folder": str(Path(inode.path).parent),
                "filename": inode.name,
                "owner_id": str(owner_id),
                "readable_by": [str(owner_id)],
                "pages": [
                    {
                        "file_id": str(inode.id),
                        "index": page.index - inode.files.from_page,
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
        return owner_id


def embed_file(id):
    logging.info(f"Embedding file {id}")
    with Session(engine) as session:
        stmt = select(Inodes).join(Inodes.files).where(Inodes.id == id)
        inode = session.scalars(stmt).one()
        owner_id = inode.owner_id
        stmt = (
            select(Pages)
            .where(Pages.index >= inode.files.from_page)
            .where(Pages.index < inode.files.to_page)
            .where(Pages.embedding == None)
            .where(func.length(Pages.contents) > 0)
            .where(Pages.inode_id == inode.id)
        )
        pages = session.scalars(stmt).all()
        embeddings = embed([page.contents for page in pages])

        for embedding, page in zip(embeddings, pages):
            page.embedding = embedding

        inode.files.is_embedded = True
        session.commit()
        return owner_id


def delete_inode(id):
    logging.info(f"Deleting inode {id}")
    minio = get_minio()

    with Session(engine) as session:
        # Select the paths for the given inode
        stmt = select(Inodes).where(Inodes.id == id)
        inode = session.scalars(stmt).one()

        # Select the paths of all this inodes descendants recursively
        hierarchy = (
            select(Inodes.id, Inodes.parent_id)
            .where(Inodes.parent_id == inode.id)
            .cte(name="hierarchy", recursive=True)
        )
        hierarchy = hierarchy.union_all(
            select(Inodes.id, Inodes.parent_id).join(
                hierarchy, Inodes.parent_id == hierarchy.c.id
            )
        )
        stmt = (
            # Only get inodes with an associated file to delete
            select(Inodes)
            .join(hierarchy, Inodes.id == hierarchy.c.id)
            .join(Files, Inodes.id == Files.inode_id)
        )
        descendants = session.scalars(stmt).all()

        # Make sure all original and optimized files are destroyed
        paths = [inode_path(inode.owner_id, inode.path) for inode in descendants] + [
            inode_path(inode.owner_id, inode.path)
        ]
        paths = [(f"{path}/original", f"{path}/optimized") for path in paths]
        delete_objects = (DeleteObject(path) for tuple in paths for path in tuple)

        errors = minio.remove_objects(env.get("STORAGE_BUCKET"), delete_objects)
        for error in errors:
            logging.error("error occurred when deleting object", error)

        # Remove indexed contents of files that descend this inode
        res = opensearch_request(
            "post",
            "/inodes/_delete_by_query",
            {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "wildcard": {
                                    "path.keyword": {
                                        "value": f"{inode.path}*",
                                    }
                                },
                            },
                            {
                                "term": {
                                    "owner_id.keyword": str(inode.owner_id),
                                },
                            },
                        ]
                    }
                }
            },
        )
        if res.status_code != 200:
            # Record could be not found for whatever reason
            data = res.json()
            logging.error(data["error"]["reason"])

        stmt = delete(Inodes).where(Inodes.id == inode.id)
        session.execute(stmt)
        session.commit()


def move_inode(id, channel):
    logging.info(f"Moving inode {id}")
    minio = get_minio()

    with Session(engine) as session:
        stmt = select(Inodes).where(Inodes.id == id)
        inode = session.scalars(stmt).one()
        stmt = select(func.inode_path(Inodes.id)).where(Inodes.id == inode.id)
        path = session.scalars(stmt).one()

        # Move in storage backend if this is a file
        try:
            # This will error when no file is found
            stmt = select(Files).where(Files.inode_id == inode.id)
            session.scalars(stmt).one()

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
        except NoResultFound:
            pass

        # Move succesful, save new path
        inode.path = path
        session.commit()

        # reindex
        channel.basic_publish(
            exchange="insight",
            routing_key="index_inode",
            body=json.dumps({"id": inode.id}),
        )

        # Trigger move of children
        stmt = select(Inodes).where(Inodes.parent_id == inode.id)
        children = session.scalars(stmt).all()
        for inode in children:
            channel.basic_publish(
                exchange="insight",
                routing_key="move_inode",
                body=json.dumps({"id": inode.id}),
            )
