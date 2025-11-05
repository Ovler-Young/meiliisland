"""
Configuration loader for db2meili.

Parses and validates TOML configuration files for database-to-MeiliSearch sync tasks.
"""

from dataclasses import dataclass
from typing import Literal
import sys

# Use tomllib for Python 3.11+, tomli for older versions
if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


@dataclass
class SourceConfig:
    """Database source configuration"""
    type: Literal["mysql", "mongodb"]
    host: str
    port: int
    username: str
    password: str
    database: str


@dataclass
class MeiliConfig:
    """MeiliSearch configuration"""
    api_url: str
    api_key: str


@dataclass
class WorkersConfig:
    """Worker pool configuration"""
    per_task: int = 12
    chunk_size: int = 50


@dataclass
class SyncConfig:
    """Single sync task configuration"""
    collection: str  # or "table" for MySQL
    index: str
    primary_key: str
    attributes: dict[str, list[str]]  # {"id": ["filterable", "sortable"]}


@dataclass
class Config:
    """Complete configuration"""
    source: SourceConfig
    meilisearch: MeiliConfig
    workers: WorkersConfig
    sync: list[SyncConfig]


def load_config(path: str) -> Config:
    """
    Load and parse TOML configuration file.

    Args:
        path: Path to config.toml file

    Returns:
        Parsed Config object

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config is invalid
    """
    with open(path, "rb") as f:
        data = tomllib.load(f)

    # Validate required sections
    required_sections = ["source", "meilisearch", "sync"]
    for section in required_sections:
        if section not in data:
            raise ValueError(f"Missing required section: [{section}]")

    # Parse source config
    source = SourceConfig(**data["source"])
    if source.type not in ["mysql", "mongodb"]:
        raise ValueError(f"Invalid source type: {source.type}. Must be 'mysql' or 'mongodb'")

    # Parse meilisearch config
    meilisearch = MeiliConfig(**data["meilisearch"])

    # Parse workers config (with defaults)
    workers_data = data.get("workers", {})
    workers = WorkersConfig(**workers_data)

    # Parse sync configs
    sync_configs = []
    for sync_data in data["sync"]:
        # Handle both "collection" and "table" keys
        collection = sync_data.get("collection") or sync_data.get("table")
        if not collection:
            raise ValueError("Each [[sync]] must have 'collection' or 'table' key")

        sync_config = SyncConfig(
            collection=collection,
            index=sync_data["index"],
            primary_key=sync_data["primary_key"],
            attributes=sync_data["attributes"]
        )
        sync_configs.append(sync_config)

    if not sync_configs:
        raise ValueError("At least one [[sync]] configuration required")

    return Config(
        source=source,
        meilisearch=meilisearch,
        workers=workers,
        sync=sync_configs
    )


def translate_attributes(attrs: dict[str, list[str]]) -> dict[str, list[str]]:
    """
    Translate attribute configuration to MeiliSearch format.

    Input:  {"id": ["filterable", "sortable"], "title": ["searchable"]}
    Output: {"filterable_attributes": ["id"],
             "sortable_attributes": ["id"],
             "searchable_attributes": ["title"]}

    Args:
        attrs: Attribute dictionary from config

    Returns:
        MeiliSearch-compatible attribute configuration
    """
    result = {
        "filterable_attributes": [],
        "sortable_attributes": [],
        "searchable_attributes": []
    }

    for field_name, capabilities in attrs.items():
        for capability in capabilities:
            if capability == "filterable":
                result["filterable_attributes"].append(field_name)
            elif capability == "sortable":
                result["sortable_attributes"].append(field_name)
            elif capability == "searchable":
                result["searchable_attributes"].append(field_name)
            else:
                raise ValueError(f"Unknown attribute capability: {capability}")

    return result


# Example usage
if __name__ == "__main__":
    config = load_config("config.toml")
    print(f"Source: {config.source.type}")
    print(f"Sync tasks: {len(config.sync)}")

    for sync in config.sync:
        attrs = translate_attributes(sync.attributes)
        print(f"\nIndex: {sync.index}")
        print(f"Searchable: {attrs['searchable_attributes']}")
