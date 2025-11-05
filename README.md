# MeiliIsland

Database to MeiliSearch sync tools.

## Tools

### 1. freshrss_db2meili.py (Legacy)

Original MySQL-based sync tool for FreshRSS databases.

**Installation:**
```bash
pip install -r requirements.txt
cp config.py.example config.py
# Edit config.py with your settings
```

**Usage:**
```bash
python freshrss_db2meili.py --init   # Create index in MeiliSearch
python freshrss_db2meili.py          # Incremental sync
python freshrss_db2meili.py --delete # Delete index
```

---

### 2. db2meili.py (New - Recommended)

Flexible, configuration-driven sync tool supporting both MySQL and MongoDB.

#### Features

- ✅ **Multiple database sources**: MySQL and MongoDB
- ✅ **TOML configuration**: All settings in `config.toml`
- ✅ **Multi-task sync**: Sync multiple collections/tables to different indexes
- ✅ **Parallel execution**: Run sync tasks sequentially or concurrently
- ✅ **Hybrid incremental sync**: ID-diff (stateless) + optional update tracking
- ✅ **Works with any primary key**: UUID, ObjectId, sequential integers
- ✅ **Flexible attributes**: Easy-to-configure searchable/filterable/sortable fields
- ✅ **State file namespacing**: Prevents conflicts when syncing from multiple databases

#### Installation

```bash
# Install dependencies
uv pip install -r requirements.txt

# Create configuration
cp config.toml.example config.toml
# Edit config.toml with your settings
```

#### Quick Start

```bash
# Full sync (all tasks)
python db2meili.py

# Incremental sync with custom config
python db2meili.py --config myconfig.toml

# Full sync from beginning (ignore existing data)
python db2meili.py --init

# Delete indexes before syncing
python db2meili.py --delete --init
```

#### Configuration

See [config.toml.example](config.toml.example) for a complete configuration template.

**Key sections:**

```toml
[source]          # Database connection (MySQL or MongoDB)
[meilisearch]     # MeiliSearch connection
[workers]         # Performance tuning
[[sync]]          # Sync tasks (one or more)
```

**Attribute format** (user-friendly):
```toml
[sync.attributes]
id = ["filterable", "sortable"]
title = ["searchable", "filterable"]
content = ["searchable"]
author = ["filterable"]
created_at = ["sortable", "filterable"]
```

**Optional update tracking:**
```toml
[[sync]]
collection = "posts"
index = "posts_index"
primary_key = "id"
updated_at_field = "updated_at"  # Enable update detection

[sync.attributes]
# ... attributes configuration
```

#### Usage Examples

```bash
# Sync all tasks sequentially (default)
python db2meili.py

# Sync only specific task
python db2meili.py --task posts_index

# Run all tasks in parallel (faster for multiple tasks)
python db2meili.py --parallel

# Full sync with index deletion
python db2meili.py --delete --init

# Combine flags
python db2meili.py --init --task users_index
```

#### How It Works

**Default Mode (ID-Diff Strategy):**
1. Fetch all document IDs from MeiliSearch index
2. Fetch all document IDs from database
3. Calculate difference (set subtraction)
4. Sync only missing documents
5. Stateless (no local files required except for update tracking)

**Hybrid Mode (with Update Tracking):**
1. Perform ID-diff sync (above)
2. Check `sync_state.toml` for last sync timestamp
3. Fetch documents updated since last sync
4. Re-sync updated documents (overwrites in MeiliSearch)
5. Save new timestamp to state file

**State File Format:**

The `sync_state.toml` file uses namespaced keys to prevent conflicts:

```toml
["mongodb-localhost-27017-mydb.posts_index"]
last_sync = "2025-01-05T10:30:00Z"

["mysql-localhost-3306-freshdb.articles_index"]
last_sync = "2025-01-05T09:15:00Z"
```

This allows you to:
- Run db2meili with different config files without conflicts
- Sync from multiple databases to the same MeiliSearch instance
- Track sync state independently per source database

#### CLI Options

| Flag | Description |
|------|-------------|
| `--config PATH` | Path to config file (default: `config.toml`) |
| `--init` | Full sync from beginning (ignore existing data) |
| `--delete` | Delete indexes before syncing |
| `--task NAME` | Sync only specific task by index name |
| `--parallel` | Run all tasks in parallel (default: sequential) |

#### Performance Tuning

Adjust in `config.toml`:

```toml
[workers]
per_task = 12      # More workers = faster sync, more resources
chunk_size = 50    # Larger batches = fewer API calls, more memory
```

**Tips:**
- Increase `per_task` for faster sync (if resources allow)
- Increase `chunk_size` for large documents
- Use `--parallel` for multiple tasks (uses more DB/Meili resources)
- Sequential mode is safer for resource-constrained environments

---

## Development

### Project Structure

```
.
├── freshrss_db2meili.py    # Legacy FreshRSS sync tool
├── db2meili.py             # New flexible sync tool
├── config_loader.py        # TOML configuration parser
├── database_sources.py     # MySQL & MongoDB adapters
├── sync_task.py            # Sync task handler
├── config.toml             # Active config (gitignored)
├── config.toml.example     # Config template
├── sync_state.toml         # Sync timestamps (auto-created, gitignored)
├── config.py.example       # Legacy config template
└── requirements.txt        # Python dependencies
```

### Adding New Database Sources

To add support for other databases (PostgreSQL, etc.):

1. Implement `DatabaseSource` protocol in `database_sources.py`
2. Add required methods: `connect()`, `close()`, `get_all_ids()`, `fetch_documents_by_ids()`, `fetch_updated_documents()`
3. Add source type to `create_source()` factory function
4. Update `config_loader.py` to accept new source type in validation

### Testing

See Claude.md for comprehensive testing checklist covering:
- MySQL and MongoDB sources
- Full and incremental sync modes
- Multiple indexes and parallel execution
- Error handling and edge cases
