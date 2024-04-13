from typing import Any, List, Optional

from pgvector.sqlalchemy import Vector
from sqlalchemy import BigInteger, Boolean, DateTime, Double, ForeignKeyConstraint, Identity, Integer, PrimaryKeyConstraint, Text, Uuid, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
import datetime
import uuid

class Base(DeclarativeBase):
    pass


class Files(Base):
    __tablename__ = 'files'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='files_pkey'),
        {'schema': 'private'}
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, server_default=text('gen_random_uuid()'))
    owner_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    path: Mapped[str] = mapped_column(Text)
    name: Mapped[str] = mapped_column(Text)
    is_uploaded: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))
    is_deleted: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))
    number_of_pages: Mapped[Optional[int]] = mapped_column(Integer)
    created_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('CURRENT_TIMESTAMP'))
    updated_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('CURRENT_TIMESTAMP'))

    documents: Mapped[List['Documents']] = relationship('Documents', back_populates='file')
    pages: Mapped[List['Pages']] = relationship('Pages', back_populates='file')


class Prompts(Base):
    __tablename__ = 'prompts'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='prompts_pkey'),
        {'schema': 'private'}
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    owner_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    query: Mapped[str] = mapped_column(Text)
    similarity_top_k: Mapped[int] = mapped_column(Integer, server_default=text('3'))
    response: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('CURRENT_TIMESTAMP'))
    updated_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('CURRENT_TIMESTAMP'))

    sources: Mapped[List['Sources']] = relationship('Sources', back_populates='prompt')


class SchemaMigrations(Base):
    __tablename__ = 'schema_migrations'
    __table_args__ = (
        PrimaryKeyConstraint('version', name='schema_migrations_pkey'),
        {'schema': 'private'}
    )

    version: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    dirty: Mapped[bool] = mapped_column(Boolean)


class Documents(Base):
    __tablename__ = 'documents'
    __table_args__ = (
        ForeignKeyConstraint(['file_id'], ['private.files.id'], ondelete='CASCADE', name='documents_file_id_fkey'),
        PrimaryKeyConstraint('id', name='documents_pkey'),
        {'schema': 'private'}
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, server_default=text('gen_random_uuid()'))
    file_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    path: Mapped[str] = mapped_column(Text)
    from_page: Mapped[int] = mapped_column(Integer)
    to_page: Mapped[int] = mapped_column(Integer)
    is_deleted: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))
    is_ingested: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))
    is_indexed: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))
    is_embedded: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))
    name: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('CURRENT_TIMESTAMP'))
    updated_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('CURRENT_TIMESTAMP'))

    file: Mapped['Files'] = relationship('Files', back_populates='documents')


class Pages(Base):
    __tablename__ = 'pages'
    __table_args__ = (
        ForeignKeyConstraint(['file_id'], ['private.files.id'], ondelete='CASCADE', name='pages_file_id_fkey'),
        PrimaryKeyConstraint('id', name='pages_pkey'),
        {'schema': 'private'}
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    file_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    index: Mapped[int] = mapped_column(Integer)
    contents: Mapped[str] = mapped_column(Text)
    embedding: Mapped[Optional[Any]] = mapped_column(Vector(1536))

    file: Mapped['Files'] = relationship('Files', back_populates='pages')
    sources: Mapped[List['Sources']] = relationship('Sources', back_populates='page')


class Sources(Base):
    __tablename__ = 'sources'
    __table_args__ = (
        ForeignKeyConstraint(['page_id'], ['private.pages.id'], ondelete='CASCADE', name='sources_page_id_fkey'),
        ForeignKeyConstraint(['prompt_id'], ['private.prompts.id'], ondelete='CASCADE', name='sources_prompt_id_fkey'),
        PrimaryKeyConstraint('id', name='sources_pkey'),
        {'schema': 'private'}
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    prompt_id: Mapped[int] = mapped_column(BigInteger)
    page_id: Mapped[int] = mapped_column(BigInteger)
    similarity: Mapped[float] = mapped_column(Double(53))

    page: Mapped['Pages'] = relationship('Pages', back_populates='sources')
    prompt: Mapped['Prompts'] = relationship('Prompts', back_populates='sources')
