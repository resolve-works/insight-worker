import logging
import ocrmypdf
import fitz
import json
from minio import Minio
from minio.deleteobjects import DeleteObject
from os import environ as env
from urllib.parse import urlparse
from multiprocessing import Process
from tempfile import TemporaryDirectory
from pathlib import Path
from pikepdf import Pdf
from itertools import chain
from sqlalchemy import create_engine, select, text, delete, func
from sqlalchemy.orm import Session
from .models import Files, Prompts, Sources, Pages, Inodes
from .opensearch import opensearch_request
from .rag import embed, complete


logging.basicConfig(level=logging.INFO)

engine = create_engine(env.get("POSTGRES_URI"))


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
    )


def ingest_file(id, channel):
    logging.info(f"Ingesting file {id}")
    minio = get_minio()

    temp_path = Path(TemporaryDirectory().name)
    original_path = temp_path / "original"
    optimized_path = temp_path / "optimized"

    with Session(engine) as session:
        stmt = (
        select(Inodes, func.storage_path(Inodes)) .join(Inodes.file) .where(Inodes.id == id)
                )
        inode = session.scalars(stmt).one()
        path = f"users/{inode.owner_id}/{inode.storage_path}"

        minio.fget_object(env.get("STORAGE_BUCKET"), f"{path}/original", original_path)

        # Analyze file, store meta
        with Pdf.open(original_path) as file_pdf:
            if not inode.file.to_page:
                inode.file.to_page = len(file_pdf.pages)

            before_range = range(0, inode.file.from_page)
            after_range = range(inode.file.to_page, len(file_pdf.pages))
            pages_to_delete = list(chain(before_range, after_range))

            # Split file from original if there's pages explicitely set
            if(len(pages_to_delete)):
                # Reverse the range to remove last page first as the file shrinks
                # when removing pages, leading to IndexErrors otherwise
                for p in reversed(pages_to_delete):
                    del file_pdf.pages[p]
                file_pdf.save(original_path)

        # OCR & optimize new PDF
        process = Process(target=ocrmypdf_process, args=(original_path, optimized_path))
        process.start()
        process.join()

        minio.fput_object(env.get("STORAGE_BUCKET"), f"{path}/optimized", optimized_path)

        # fitz is pyMuPDF used for extracting text layers
        file_pdf = fitz.open(optimized_path)

        pages = [
            Pages(
                # Get all contents, sorted by position on page
                contents=page.get_text(sort=True).strip(),
                # Index pages in file instead of in file
                index=inode.file.from_page + index,
                file_id=inode.file_id,
            )
            for index, page in enumerate(file_pdf)
        ]
        session.add_all(pages)
        inode.file.is_ingested = True
        session.commit()

        # Trigger next tasks
        for routing_key in ["index_file", "embed_file"]:
            channel.basic_publish(
                exchange="insight",
                routing_key=routing_key,
                body=json.dumps({"id": str(inode.id)}),
            )
        # Let user know
        channel.basic_publish(
            exchange="",
            routing_key=f"user-{inode.owner_id}",
            body=json.dumps({"id": str(inode.id), "task": "ingest_file"}),
        )


def index_file(id, channel):
    logging.info(f"Indexing file {id}")

    with Session(engine) as session:
        stmt = (
                select(Inodes, func.storage_path(Inodes))
                .join(Inodes.file)
                .where(Inodes.id == id)
                )
        inode = session.scalars(stmt).one()
        stmt = (
            select(Pages)
            .where(func.length(Pages.contents) > 0)
            .where(Pages.file_id == inode.file.file_id)
        )
        pages = session.scalars(stmt).all()

        # Index file with pages
        data = {
            "path": inode.storage_path,
            "filename": inode.file.name,
            "owner_id": str(inode.owner_id),
            "pages": [
                {
                    "file_id": str(inode.id),
                    "index": page.index - inode.file.from_page,
                    "contents": page.contents,
                }
                for page in pages
            ],
        }

        res = opensearch_request("put", f"/inodes/_doc/{str(inode.id)}", data)
        if res.status_code not in [200, 201]:
            raise Exception(res.text)

        inode.file.is_indexed = True
        session.commit()

        channel.basic_publish(
            exchange="",
            routing_key=f"user-{inode.owner_id}",
            body=json.dumps({"id": str(inode.id), "task": "index_file"}),
        )


def embed_file(id, channel):
    logging.info(f"Embedding file {id}")

    with Session(engine) as session:
        stmt = select(Inodes).join(Inodes.file).where(Inodes.id == id)
        inode = session.scalars(stmt).one()
        stmt = (
            select(Pages)
            .where(Pages.index >= inode.file.from_page)
            .where(Pages.index < inode.file.to_page)
            .where(Pages.embedding == None)
            .where(func.length(Pages.contents) > 0)
            .where(Pages.file_id == inode.file_id)
        )
        pages = session.scalars(stmt).all()
        embeddings = embed([page.contents for page in pages])

        for embedding, page in zip(embeddings, pages):
            page.embedding = embedding

        inode.file.is_embedded = True
        session.commit()

        channel.basic_publish(
            exchange="",
            routing_key=f"user-{inode.owner_id}",
            body=json.dumps({"id": str(inode.id), "task": "embed_file"}),
        )


def delete_inode(id, channel):
    logging.info(f"Deleting inode {id}")
    minio = get_minio()

    with Session(engine) as session:
        # Select the paths for the given inode
        stmt = (
            select(func.storage_path(Inodes))
                .where(Inodes.id == id)
        )
        path = session.scalars(stmt).one()
        # Select tho paths of all this inodes descendants
        stmt = (
            select(func.storage_path(Inodes))
                .where(Inodes.id.in_(select(func.descendants(Inodes)).where(Inodes.id == id)))
        )
        paths = session.scalars(stmt).all()

        # Make sure all original and optimized files are destroyed
        paths = paths + [path]
        paths = [(f"{path}/original", f"{path}/optimized") for path in paths]
        paths = [path for tuple in paths for path in tuple]

        delete_objects = map(lambda path: DeleteObject(path), paths)
        errors = minio.remove_objects(env.get("STORAGE_BUCKET"), delete_objects)
        for error in errors:
            logging.error("error occurred when deleting object", error)

        # Remove indexed contents of files that descend this inode
        data = {
            "query": {
                "match": {
                    "path": f"{path}*"
                }
            }
        }
        res = opensearch_request("post", "/files/_delete_by_query", data)
        if res.status_code != 200:
            raise Exception(res.text)

        stmt = delete(Inodes).where(Inodes.id == id)
        session.execute(stmt)
        session.commit()


def answer_prompt(id, channel):
    logging.info(f"Answering prompt {id}")

    # Get pages that have an embedding that is close to the query
    with Session(engine) as session:
        stmt = select(Prompts).where(Prompts.id == id)
        prompt = session.scalars(stmt).one()

        # Embed query
        embedding = next(embed([prompt.query]))

        stmt = (
            select(
                Pages.id,
                Pages.embedding.cosine_distance(embedding).label("distance"),
                Pages.contents,
            )
            .order_by(text("distance asc"))
            .join(Pages.file)
            .where(Files.owner_id == prompt.owner_id)
            .where(Pages.embedding != None)
            .limit(prompt.similarity_top_k)
        )

        pages = session.execute(stmt).all()

        # Create response by sending pages to chat completion
        response = complete(prompt.query, [contents for (_, _, contents) in pages])
        prompt.response = response

        for page_id, distance, _ in pages:
            source = Sources(
                page_id=page_id,
                similarity=(1 - distance) if distance is not None else 0,
            )
            prompt.sources.append(source)
        session.commit()

        channel.basic_publish(
            exchange="",
            routing_key=f"user-{prompt.owner_id}",
            body=json.dumps({"id": str(prompt.id), "task": "answer_prompt"}),
        )
