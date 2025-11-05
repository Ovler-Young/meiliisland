"""
Sync task handler for db2meili.

Handles syncing a single collection/table to MeiliSearch index using
ID-diff strategy with optional update tracking.
"""

import asyncio
import time
import sys
from typing import Any
from pathlib import Path
from datetime import datetime, timezone

from meilisearch_python_sdk import AsyncClient
from tqdm import tqdm

from database_sources import DatabaseSource
from config_loader import SyncConfig, translate_attributes

# Use tomllib for Python 3.11+, tomli for older versions
if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

import tomli_w


class SyncTask:
    """
    Handles syncing a single collection/table to MeiliSearch index.

    Uses hybrid ID-diff + optional update tracking strategy:
    - Fetches all IDs from both MeiliSearch and database
    - Syncs only missing documents
    - Optionally re-syncs updated documents if configured
    """

    def __init__(
        self,
        sync_config: SyncConfig,
        db_source: DatabaseSource,
        meili_client: AsyncClient,
        workers: int = 12,
        chunk_size: int = 50,
        source_key: str = ""
    ):
        """
        Initialize sync task.

        Args:
            sync_config: Configuration for this sync task
            db_source: Database source instance
            meili_client: MeiliSearch async client
            workers: Number of worker coroutines
            chunk_size: Documents per chunk
            source_key: Unique identifier for source database (e.g., "mysql-localhost-3306-mydb")
        """
        self.config = sync_config
        self.db_source = db_source
        self.meili_client = meili_client
        self.workers = workers
        self.chunk_size = chunk_size
        self.source_key = source_key

        # Stats
        self.documents_synced = 0
        self.errors = 0

        # Shutdown flag
        self.should_stop = False

    async def configure_index(self) -> None:
        """
        Configure MeiliSearch index with attributes from config.

        Creates index if it doesn't exist and updates attribute settings.
        """
        try:
            # Get or create index
            index = self.meili_client.index(self.config.index)

            # Translate attributes to MeiliSearch format
            attrs = translate_attributes(self.config.attributes)

            # Update index settings
            await index.update_filterable_attributes(
                attrs.get("filterable_attributes", [])
            )
            await index.update_sortable_attributes(
                attrs.get("sortable_attributes", [])
            )
            await index.update_searchable_attributes(
                attrs.get("searchable_attributes", [])
            )

            print(f"✓ Configured index '{self.config.index}'")
            print(f"  - Filterable: {attrs.get('filterable_attributes', [])}")
            print(f"  - Sortable: {attrs.get('sortable_attributes', [])}")
            print(f"  - Searchable: {attrs.get('searchable_attributes', [])}")

        except Exception as e:
            print(f"✗ Error configuring index '{self.config.index}': {e}")
            raise

    def process_document(self, doc: dict) -> dict:
        """
        Process a single document before uploading to MeiliSearch.

        Can include field transformations and content length calculation.

        Args:
            doc: Raw document from database

        Returns:
            Processed document ready for MeiliSearch
        """
        # Calculate content_length if 'content' field exists
        if 'content' in doc and isinstance(doc['content'], str):
            doc['content_length'] = len(doc['content'].strip())

        return doc

    async def worker(
        self,
        queue: asyncio.Queue,
        progress: tqdm | None
    ) -> None:
        """
        Worker coroutine: process documents from queue and upload to MeiliSearch.

        Args:
            queue: Queue containing document batches
            progress: Progress bar to update
        """
        index = self.meili_client.index(self.config.index)

        while True:
            # Get batch from queue
            try:
                batch = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                # Check if we should stop
                if self.should_stop or queue.empty():
                    break
                continue

            if batch is None:  # Sentinel value to stop
                queue.task_done()
                break

            try:
                # Process documents
                processed_batch = [self.process_document(doc) for doc in batch]

                # Upload to MeiliSearch
                await index.add_documents(
                    processed_batch,
                    primary_key=self.config.primary_key
                )

                # Update stats
                self.documents_synced += len(processed_batch)
                if progress:
                    progress.update(len(processed_batch))

            except Exception as e:
                print(f"\n✗ Error processing batch: {e}")
                self.errors += 1

            finally:
                queue.task_done()

    async def check_ids_exist_in_meili(self, ids: list[Any]) -> set[Any]:
        """
        Check which IDs from the list already exist in MeiliSearch.

        Args:
            ids: List of primary key values to check

        Returns:
            Set of IDs that exist in MeiliSearch
        """
        index = self.meili_client.index(self.config.index)
        existing_ids = set()

        try:
            # Fetch documents by IDs (only primary key field)
            # MeiliSearch doesn't have a bulk "check existence" API, so we fetch with filters
            # For efficiency, we use the get_documents endpoint with offset/limit
            # But this is a simplified approach - for large batches, consider using search with filters

            # Build filter for IDs
            if len(ids) == 0:
                return existing_ids

            # Use search with filter to check existence
            # Note: This requires the primary_key to be filterable
            id_filter = f"{self.config.primary_key} IN [{','.join(repr(str(id)) for id in ids)}]"

            try:
                result = await index.search(
                    query="",
                    filter=id_filter,
                    limit=len(ids),
                    attributes_to_retrieve=[self.config.primary_key]
                )

                # Extract existing IDs
                for hit in result.hits:
                    existing_ids.add(hit[self.config.primary_key])

            except Exception:
                # If filter search fails, fall back to fetching all and checking
                # This is slower but more compatible
                for id_val in ids:
                    try:
                        doc = await index.get_document(id_val)
                        if doc:
                            existing_ids.add(id_val)
                    except Exception:
                        # Document doesn't exist
                        pass

        except Exception as e:
            print(f"⚠️  Warning: Could not check IDs in MeiliSearch: {e}")
            # Return empty set = assume nothing exists, sync everything

        return existing_ids

    def load_last_sync_time(self) -> str | None:
        """
        Load last sync timestamp from state file.

        Uses namespaced key format: "{source_key}.{index_name}"
        Example: "mysql-localhost-3306-mydb.posts_index"

        Returns:
            ISO timestamp string or None if not found
        """
        state_file = Path("sync_state.toml")
        if not state_file.exists():
            return None

        try:
            with open(state_file, "rb") as f:
                state = tomllib.load(f)

            # Use namespaced key to prevent conflicts between different sources
            state_key = f"{self.source_key}.{self.config.index}" if self.source_key else self.config.index
            return state.get(state_key, {}).get("last_sync")
        except Exception as e:
            print(f"⚠️  Warning: Could not read state file: {e}")
            return None

    def save_last_sync_time(self, timestamp: str) -> None:
        """
        Save sync timestamp to state file.

        Uses namespaced key format: "{source_key}.{index_name}"
        Example: "mysql-localhost-3306-mydb.posts_index"

        Args:
            timestamp: ISO timestamp string (e.g., "2025-01-05T10:30:00Z")
        """
        state_file = Path("sync_state.toml")

        # Load existing state
        state = {}
        if state_file.exists():
            try:
                with open(state_file, "rb") as f:
                    state = tomllib.load(f)
            except Exception:
                pass

        # Use namespaced key to prevent conflicts between different sources
        state_key = f"{self.source_key}.{self.config.index}" if self.source_key else self.config.index

        # Update timestamp for this sync task
        if state_key not in state:
            state[state_key] = {}
        state[state_key]["last_sync"] = timestamp

        # Save state file
        try:
            with open(state_file, "w") as f:
                tomli_w.dump(state, f)
        except Exception as e:
            print(f"⚠️  Warning: Could not save state file: {e}")

    async def run(self, init: bool = False) -> dict[str, Any]:
        """
        Run the sync task using streaming batch-check strategy.

        Args:
            init: If True, perform full sync (skip existence checks)

        Returns:
            Statistics dictionary with sync results
        """
        start_time = time.time()

        print(f"\n{'='*60}")
        print(f"Starting sync: {self.config.collection} → {self.config.index}")
        print(f"{'='*60}")

        # Step 1: Configure MeiliSearch index
        await self.configure_index()

        # Step 2: Create worker queue and start workers
        queue: asyncio.Queue[list[dict] | None] = asyncio.Queue(maxsize=self.workers * 2)

        progress = tqdm(desc=f"Syncing documents", unit=" docs")
        workers = [
            asyncio.create_task(self.worker(queue, progress))
            for _ in range(self.workers)
        ]

        try:
            # Step 3: Stream documents in batches and check existence
            if init:
                print(f"→ Full sync mode (skipping existence checks)")
            else:
                print(f"→ Incremental sync mode (streaming batch-check)")

            batch_size = 10000  # Fetch 10k documents at a time from database

            async for batch in self.db_source.fetch_documents_batched(
                self.config.collection,
                self.config.primary_key,
                batch_size
            ):
                if init:
                    # Full sync: upload everything
                    await queue.put(batch)
                else:
                    # Incremental sync: check which IDs already exist
                    batch_ids = [doc[self.config.primary_key] for doc in batch]
                    existing_ids = await self.check_ids_exist_in_meili(batch_ids)

                    # Filter out documents that already exist
                    missing_docs = [doc for doc in batch if doc[self.config.primary_key] not in existing_ids]

                    if missing_docs:
                        await queue.put(missing_docs)

            # Step 4: Handle updates (if configured)
            if not init and self.config.updated_at_field:
                print(f"\n→ Checking for updated documents...")
                last_sync = self.load_last_sync_time()

                if last_sync:
                    print(f"  Last sync: {last_sync}")

                    async for batch in self.db_source.fetch_updated_documents(
                        self.config.collection,
                        self.config.updated_at_field,
                        last_sync,
                        self.chunk_size
                    ):
                        await queue.put(batch)

                # Save current timestamp
                current_time = datetime.now(timezone.utc).isoformat()
                self.save_last_sync_time(current_time)

            # Step 5: Send sentinel values to stop workers
            for _ in range(self.workers):
                await queue.put(None)

            # Wait for all workers to finish
            await asyncio.gather(*workers)

        except Exception as e:
            print(f"\n✗ Error during sync: {e}")
            self.should_stop = True
            raise
        finally:
            # Close progress bar
            progress.close()

        # Step 6: Calculate duration and return results
        duration = time.time() - start_time

        results = {
            "index": self.config.index,
            "documents_synced": self.documents_synced,
            "errors": self.errors,
            "duration_seconds": round(duration, 2)
        }

        print(f"\n{'='*60}")
        print(f"✓ Sync complete: {self.config.index}")
        print(f"  - Documents: {self.documents_synced}")
        print(f"  - Errors: {self.errors}")
        print(f"  - Duration: {duration:.2f}s")
        print(f"{'='*60}\n")

        return results
