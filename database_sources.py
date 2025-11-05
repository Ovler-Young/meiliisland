"""
Database source abstractions for db2meili.

Provides a unified interface for fetching documents from different database types
(MySQL, MongoDB) with support for ID-diff incremental sync and optional update tracking.
"""

from typing import Protocol, AsyncIterator, Any
import aiomysql
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId

from config_loader import SourceConfig


class DatabaseSource(Protocol):
    """
    Abstract interface for database sources.
    Implementations: MySQLSource, MongoDBSource

    Uses streaming batch-check strategy for incremental sync:
    - Stream documents in batches from database
    - No need to count total or load all IDs into memory
    - Memory-efficient for large datasets
    """

    async def connect(self) -> None:
        """Establish database connection"""
        ...

    async def close(self) -> None:
        """Close database connection"""
        ...

    def fetch_documents_batched(
        self,
        collection: str,
        primary_key: str,
        batch_size: int
    ) -> AsyncIterator[list[dict]]:
        """
        Stream all documents from collection/table in batches.

        Args:
            collection: Collection/table name
            primary_key: Primary key field name
            batch_size: Number of documents per batch

        Yields:
            Lists of document dictionaries
        """
        ...

    def fetch_updated_documents(
        self,
        collection: str,
        updated_field: str,
        since: str,
        chunk_size: int
    ) -> AsyncIterator[list[dict]]:
        """
        Fetch documents updated after a specific timestamp.
        Optional method for update detection.

        Args:
            collection: Collection/table name
            updated_field: Field name containing update timestamp
            since: ISO timestamp string (e.g., "2025-01-05T10:30:00Z")
            chunk_size: Number of documents per chunk

        Yields:
            Lists of document dictionaries updated after 'since'
        """
        ...


class MySQLSource:
    """MySQL database source implementation"""

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.pool: aiomysql.Pool | None = None

    async def connect(self) -> None:
        """Create MySQL connection pool"""
        self.pool = await aiomysql.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.database,
            minsize=1,
            maxsize=10
        )

    async def close(self) -> None:
        """Close connection pool"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

    async def fetch_documents_batched(
        self,
        collection: str,
        primary_key: str,
        batch_size: int
    ) -> AsyncIterator[list[dict]]:
        """
        Stream all documents from MySQL table in batches.

        Yields batches of documents ordered by primary key.
        """
        assert self.pool is not None, "Database not connected"

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                query = f"SELECT * FROM `{collection}` ORDER BY `{primary_key}` LIMIT %s OFFSET %s"
                offset = 0

                while True:
                    await cursor.execute(query, (batch_size, offset))
                    rows = await cursor.fetchall()

                    if not rows:
                        break

                    documents = [dict(row) for row in rows]
                    yield documents

                    offset += len(documents)

                    # If we got fewer than batch_size, we're done
                    if len(documents) < batch_size:
                        break

    async def fetch_updated_documents(
        self,
        collection: str,
        updated_field: str,
        since: str,
        chunk_size: int
    ) -> AsyncIterator[list[dict]]:
        """
        Fetch documents updated after a timestamp.

        Yields chunks of updated documents.
        """
        assert self.pool is not None, "Database not connected"

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                query = f"SELECT * FROM `{collection}` WHERE `{updated_field}` > %s ORDER BY `{updated_field}` LIMIT %s OFFSET %s"
                offset = 0

                while True:
                    await cursor.execute(query, (since, chunk_size, offset))
                    rows = await cursor.fetchall()

                    if not rows:
                        break

                    documents = [dict(row) for row in rows]
                    yield documents

                    offset += len(documents)

                    # If we got fewer than chunk_size, we're done
                    if len(documents) < chunk_size:
                        break


class MongoDBSource:
    """MongoDB database source implementation"""

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database_name = database
        self.client: AsyncIOMotorClient | None = None
        self.db: Any = None

    async def connect(self) -> None:
        """Connect to MongoDB"""
        self.client = AsyncIOMotorClient(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password
        )
        self.db = self.client[self.database_name]

    async def close(self) -> None:
        """Close MongoDB connection"""
        if self.client:
            self.client.close()

    async def fetch_documents_batched(
        self,
        collection: str,
        primary_key: str,
        batch_size: int
    ) -> AsyncIterator[list[dict]]:
        """
        Stream all documents from MongoDB collection in batches.

        Yields batches of documents ordered by primary key.
        """
        assert self.db is not None, "Database not connected"
        coll = self.db[collection]

        # Use cursor with batching - no skip, use last ID for continuation
        last_id = None

        while True:
            # Build query filter
            if last_id is not None:
                if primary_key == "_id":
                    query = {"_id": {"$gt": last_id}}
                else:
                    query = {primary_key: {"$gt": last_id}}
            else:
                query = {}

            # Fetch batch sorted by primary key
            cursor = coll.find(query).sort(primary_key, 1).limit(batch_size)
            documents = await cursor.to_list(length=batch_size)

            if not documents:
                break

            # Convert ObjectId to string for MeiliSearch compatibility
            processed_docs = []
            for doc in documents:
                if '_id' in doc and isinstance(doc['_id'], ObjectId):
                    doc['_id'] = str(doc['_id'])
                processed_docs.append(doc)

            yield processed_docs

            # Update last_id for next iteration
            last_doc = documents[-1]
            last_id = last_doc.get(primary_key)

            # If we got fewer than batch_size, we're done
            if len(documents) < batch_size:
                break

    async def fetch_updated_documents(
        self,
        collection: str,
        updated_field: str,
        since: str,
        chunk_size: int
    ) -> AsyncIterator[list[dict]]:
        """
        Fetch documents updated after a timestamp.

        Yields chunks of updated documents.
        """
        assert self.db is not None, "Database not connected"
        coll = self.db[collection]

        # Build query for documents updated after 'since'
        query = {updated_field: {"$gt": since}}

        skip = 0
        while True:
            cursor = coll.find(query).skip(skip).limit(chunk_size)
            documents = await cursor.to_list(length=chunk_size)

            if not documents:
                break

            # Convert ObjectId to string
            processed_docs = []
            for doc in documents:
                if '_id' in doc and isinstance(doc['_id'], ObjectId):
                    doc['_id'] = str(doc['_id'])
                processed_docs.append(doc)

            yield processed_docs

            skip += len(documents)

            # If we got fewer than chunk_size, we're done
            if len(documents) < chunk_size:
                break


def create_source(config: SourceConfig) -> DatabaseSource:
    """
    Factory function to create appropriate database source.

    Args:
        config: Source configuration from TOML

    Returns:
        DatabaseSource instance (MySQL or MongoDB)

    Raises:
        ValueError: If source type is unknown
    """
    if config.type == "mysql":
        return MySQLSource(
            host=config.host,
            port=config.port,
            user=config.username,
            password=config.password,
            database=config.database
        )
    elif config.type == "mongodb":
        return MongoDBSource(
            host=config.host,
            port=config.port,
            username=config.username,
            password=config.password,
            database=config.database
        )
    else:
        raise ValueError(f"Unknown source type: {config.type}")
