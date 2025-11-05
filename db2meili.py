#!/usr/bin/env python3
"""
db2meili: Sync databases (MySQL/MongoDB) to MeiliSearch

A flexible, configuration-driven sync tool supporting:
- MySQL and MongoDB sources
- Multiple sync tasks
- Sequential or parallel execution
- Hybrid incremental sync (ID-diff + optional update tracking)
"""

import asyncio
import argparse
import sys

from config_loader import load_config
from database_sources import create_source
from sync_task import SyncTask
from meilisearch_python_sdk import AsyncClient


async def main():
    """Main entry point"""

    # Parse CLI arguments
    parser = argparse.ArgumentParser(
        description="Sync databases to MeiliSearch"
    )
    parser.add_argument(
        "--config",
        default="config.toml",
        help="Path to config file (default: config.toml)"
    )
    parser.add_argument(
        "--init",
        action="store_true",
        help="Full sync (ignore existing MeiliSearch IDs and state file)"
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete indexes before syncing"
    )
    parser.add_argument(
        "--task",
        help="Sync only specific task (by index name)"
    )
    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Run all tasks in parallel (default: sequential)"
    )

    args = parser.parse_args()

    # Load configuration
    print(f"Loading config from: {args.config}")
    try:
        config = load_config(args.config)
    except Exception as e:
        print(f"✗ Error loading config: {e}")
        sys.exit(1)

    print(f"✓ Config loaded")
    print(f"  - Source: {config.source.type} @ {config.source.host}:{config.source.port}")
    print(f"  - MeiliSearch: {config.meilisearch.api_url}")
    print(f"  - Sync tasks: {len(config.sync)}")

    # Create database source
    print(f"\nConnecting to {config.source.type}...")
    db_source = create_source(config.source)

    try:
        await db_source.connect()
        print(f"✓ Connected to {config.source.type}")
    except Exception as e:
        print(f"✗ Failed to connect to database: {e}")
        sys.exit(1)

    # Generate unique source key for state file namespacing
    # Format: {type}-{host}-{port}-{database}
    # Example: "mysql-localhost-3306-mydb" or "mongodb-localhost-27017-mydb"
    source_key = f"{config.source.type}-{config.source.host}-{config.source.port}-{config.source.database}"

    # Create MeiliSearch client
    meili_client = AsyncClient(
        url=config.meilisearch.api_url,
        api_key=config.meilisearch.api_key
    )

    try:
        # Test connection
        health = await meili_client.health()
        print(f"✓ Connected to MeiliSearch (status: {health.status})")
    except Exception as e:
        print(f"✗ Failed to connect to MeiliSearch: {e}")
        await db_source.close()
        sys.exit(1)

    # Filter sync tasks if --task specified
    sync_configs = config.sync
    if args.task:
        sync_configs = [s for s in config.sync if s.index == args.task]
        if not sync_configs:
            print(f"✗ Task '{args.task}' not found in config")
            await db_source.close()
            sys.exit(1)
        print(f"→ Running single task: {args.task}")

    # Delete indexes if requested
    if args.delete:
        print("\n→ Deleting indexes...")
        for sync_config in sync_configs:
            try:
                await meili_client.delete_index(sync_config.index)
                print(f"  ✓ Deleted index: {sync_config.index}")
            except Exception as e:
                print(f"  ✗ Failed to delete {sync_config.index}: {e}")

    # Run sync tasks
    results = []

    if not args.parallel:
        # Sequential execution (one task at a time)
        print("\n→ Running tasks sequentially...")

        for sync_config in sync_configs:
            task = SyncTask(
                sync_config=sync_config,
                db_source=db_source,
                meili_client=meili_client,
                workers=config.workers.per_task,
                chunk_size=config.workers.chunk_size,
                source_key=source_key
            )

            try:
                result = await task.run(init=args.init)
                results.append(result)
            except Exception as e:
                print(f"✗ Task failed: {sync_config.index} - {e}")
                results.append({
                    "index": sync_config.index,
                    "error": str(e)
                })
                # Continue with next task even if this one failed

    else:
        # Parallel execution (all tasks at once)
        print("\n→ Running tasks in parallel...")
        print("⚠️  Warning: Parallel mode uses more database and MeiliSearch resources")

        tasks = []
        for sync_config in sync_configs:
            task = SyncTask(
                sync_config=sync_config,
                db_source=db_source,
                meili_client=meili_client,
                workers=config.workers.per_task,
                chunk_size=config.workers.chunk_size,
                source_key=source_key
            )
            tasks.append(task.run(init=args.init))

        # Run all tasks concurrently
        task_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        for i, result in enumerate(task_results):
            if isinstance(result, Exception):
                results.append({
                    "index": sync_configs[i].index,
                    "error": str(result)
                })
            else:
                results.append(result)

    # Print summary
    print("\n" + "="*60)
    print("SYNC SUMMARY")
    print("="*60)

    total_docs = 0
    total_errors = 0
    failed_tasks = 0

    for result in results:
        if "error" in result:
            print(f"✗ {result['index']}: FAILED - {result['error']}")
            failed_tasks += 1
        else:
            print(f"✓ {result['index']}: {result['documents_synced']} docs in {result['duration_seconds']}s")
            total_docs += result['documents_synced']
            total_errors += result.get('errors', 0)

    print("="*60)
    print(f"Total documents synced: {total_docs}")
    print(f"Total errors: {total_errors}")
    print(f"Failed tasks: {failed_tasks}/{len(results)}")
    print("="*60)

    # Cleanup
    await db_source.close()
    await meili_client.aclose()

    # Exit with error code if any tasks failed
    if failed_tasks > 0:
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        sys.exit(1)
