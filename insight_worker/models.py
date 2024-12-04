from typing import Any, List, Optional

from pgvector.sqlalchemy import Vector
from sqlalchemy import BigInteger, Boolean, CheckConstraint, Column, Computed, DateTime, Double, Enum, ForeignKeyConstraint, Identity, Index, Integer, PrimaryKeyConstraint, Table, Text, UniqueConstraint, Uuid, text
from sqlalchemy.dialects.postgresql import CITEXT
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
import datetime
import uuid

class Base(DeclarativeBase):
    pass


class SchemaMigrations(Base):
    __tablename__ = 'schema_migrations'
    __table_args__ = (
        PrimaryKeyConstraint('version', name='schema_migrations_pkey'),
        {'schema': 'private'}
    )

    version: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    dirty: Mapped[bool] = mapped_column(Boolean)


class Users(Base):
    __tablename__ = 'users'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='users_pkey'),
        {'schema': 'private'}
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    name: Mapped[Optional[str]] = mapped_column(Text)
    email: Mapped[Optional[str]] = mapped_column(Text)
    obfuscated_email: Mapped[Optional[str]] = mapped_column(Text, Computed("((regexp_replace(split_part(email, '@'::text, 1), '.'::text, '*'::text, 'g'::text) || '@'::text) || split_part(email, '@'::text, 2))", persisted=True))

    conversations: Mapped[List['Conversations']] = relationship('Conversations', back_populates='owner')
    inodes: Mapped[List['Inodes']] = relationship('Inodes', back_populates='owner')


class Conversations(Base):
    __tablename__ = 'conversations'
    __table_args__ = (
        ForeignKeyConstraint(['owner_id'], ['private.users.id'], name='conversations_owner_id_fkey'),
        PrimaryKeyConstraint('id', name='conversations_pkey'),
        Index('conversations_created_at_idx', 'created_at'),
        {'schema': 'private'}
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    owner_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    created_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('CURRENT_TIMESTAMP'))
    updated_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('CURRENT_TIMESTAMP'))
    error: Mapped[Optional[str]] = mapped_column(Enum('completion_context_exceeded', name='conversation_error'))

    owner: Mapped['Users'] = relationship('Users', back_populates='conversations')
    inode: Mapped[List['Inodes']] = relationship('Inodes', secondary='private.conversations_inodes', back_populates='conversation')
    prompts: Mapped[List['Prompts']] = relationship('Prompts', back_populates='conversation')


class Inodes(Base):
    __tablename__ = 'inodes'
    __table_args__ = (
        CheckConstraint("TRIM(BOTH FROM name) <> ''::text", name='inodes_name_check'),
        ForeignKeyConstraint(['owner_id'], ['private.users.id'], name='inodes_owner_id_fkey'),
        ForeignKeyConstraint(['parent_id'], ['private.inodes.id'], ondelete='CASCADE', name='inodes_parent_id_fkey'),
        PrimaryKeyConstraint('id', name='inodes_pkey'),
        UniqueConstraint('owner_id', 'path', name='inodes_owner_id_path_key'),
        Index('inodes_created_at_idx', 'created_at'),
        Index('inodes_type_idx', 'type'),
        {'schema': 'private'}
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    owner_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    type: Mapped[str] = mapped_column(Enum('folder', 'file', name='inode_type'), server_default=text("'folder'::inode_type"))
    name: Mapped[str] = mapped_column(Text)
    path: Mapped[str] = mapped_column(CITEXT)
    is_indexed: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))
    is_public: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))
    is_uploaded: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))
    is_ingested: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))
    is_embedded: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))
    from_page: Mapped[int] = mapped_column(Integer, server_default=text('0'))
    should_move: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))
    parent_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    created_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('CURRENT_TIMESTAMP'))
    updated_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('CURRENT_TIMESTAMP'))
    is_ready: Mapped[Optional[bool]] = mapped_column(Boolean, Computed('(is_indexed AND is_uploaded AND is_ingested AND is_embedded)', persisted=True))
    to_page: Mapped[Optional[int]] = mapped_column(Integer)
    error: Mapped[Optional[str]] = mapped_column(Enum('unsupported_file_type', 'corrupted_file', name='file_error'))

    conversation: Mapped[List['Conversations']] = relationship('Conversations', secondary='private.conversations_inodes', back_populates='inode')
    owner: Mapped['Users'] = relationship('Users', back_populates='inodes')
    parent: Mapped['Inodes'] = relationship('Inodes', remote_side=[id], back_populates='parent_reverse')
    parent_reverse: Mapped[List['Inodes']] = relationship('Inodes', remote_side=[parent_id], back_populates='parent')
    pages: Mapped[List['Pages']] = relationship('Pages', back_populates='inode')


t_conversations_inodes = Table(
    'conversations_inodes', Base.metadata,
    Column('conversation_id', BigInteger, primary_key=True, nullable=False),
    Column('inode_id', BigInteger, primary_key=True, nullable=False),
    ForeignKeyConstraint(['conversation_id'], ['private.conversations.id'], ondelete='CASCADE', name='conversations_inodes_conversation_id_fkey'),
    ForeignKeyConstraint(['inode_id'], ['private.inodes.id'], ondelete='CASCADE', name='conversations_inodes_inode_id_fkey'),
    PrimaryKeyConstraint('conversation_id', 'inode_id', name='conversations_inodes_pkey'),
    schema='private'
)


class Pages(Base):
    __tablename__ = 'pages'
    __table_args__ = (
        ForeignKeyConstraint(['inode_id'], ['private.inodes.id'], ondelete='CASCADE', name='pages_inode_id_fkey'),
        PrimaryKeyConstraint('id', name='pages_pkey'),
        UniqueConstraint('inode_id', 'index', name='pages_inode_id_index_key'),
        {'schema': 'private'}
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    inode_id: Mapped[int] = mapped_column(BigInteger)
    index: Mapped[int] = mapped_column(Integer)
    contents: Mapped[str] = mapped_column(Text)
    embedding: Mapped[Optional[Any]] = mapped_column(Vector(1536))

    inode: Mapped['Inodes'] = relationship('Inodes', back_populates='pages')
    sources: Mapped[List['Sources']] = relationship('Sources', back_populates='page')


class Prompts(Base):
    __tablename__ = 'prompts'
    __table_args__ = (
        ForeignKeyConstraint(['conversation_id'], ['private.conversations.id'], ondelete='CASCADE', name='prompts_conversation_id_fkey'),
        PrimaryKeyConstraint('id', name='prompts_pkey'),
        Index('prompts_created_at_idx', 'created_at'),
        {'schema': 'private'}
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    query: Mapped[str] = mapped_column(Text)
    conversation_id: Mapped[int] = mapped_column(BigInteger)
    response: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('CURRENT_TIMESTAMP'))
    updated_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('CURRENT_TIMESTAMP'))
    embedding: Mapped[Optional[Any]] = mapped_column(Vector(1536))
    error: Mapped[Optional[str]] = mapped_column(Enum('embed_context_exceeded', name='prompt_error'))

    conversation: Mapped['Conversations'] = relationship('Conversations', back_populates='prompts')
    sources: Mapped[List['Sources']] = relationship('Sources', back_populates='prompt')


class Sources(Base):
    __tablename__ = 'sources'
    __table_args__ = (
        ForeignKeyConstraint(['page_id'], ['private.pages.id'], ondelete='CASCADE', name='sources_page_id_fkey'),
        ForeignKeyConstraint(['prompt_id'], ['private.prompts.id'], ondelete='CASCADE', name='sources_prompt_id_fkey'),
        PrimaryKeyConstraint('prompt_id', 'page_id', name='sources_pkey'),
        {'schema': 'private'}
    )

    prompt_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    page_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    similarity: Mapped[float] = mapped_column(Double(53))

    page: Mapped['Pages'] = relationship('Pages', back_populates='sources')
    prompt: Mapped['Prompts'] = relationship('Prompts', back_populates='sources')
