# SFMC Inventory Tool (sfmc-inv2)

A modern Python application for extracting and cataloging Salesforce Marketing Cloud objects with a Terminal UI (TUI), JSON-first output, and relationship tracking.

## Project Goals

Greenfield rebuild of an SFMC inventory tool with:
- **Textual TUI** for interactive object selection (replacing curses)
- **JSON/NDJSON output** as primary format (with CSV conversion)
- **Relationship/dependency tracking** between SFMC objects
- **Modern Python patterns** - async, Pydantic, type hints

## Design Influences

Architecture patterns drawn from prior SFMC tooling:
- **REST/SOAP client patterns** - retry with backoff, token refresh, pagination
- **Cache management** - lazy-loading folder hierarchies, breadcrumb building
- **Extractor pattern** - fetch → enrich → transform pipeline
- **Adaptive rate limiting** - backoff on failures, recovery on success

---

## Architecture Overview

```
sfmc-inv2/
├── sfmc_inv2/
│   ├── __init__.py              # Package init, version
│   ├── __main__.py              # python -m sfmc_inv2 entry
│   ├── cli.py                   # Typer CLI entry point
│   ├── core/                    # Core infrastructure
│   ├── clients/                 # API transport layer
│   ├── cache/                   # Folder/definition caching
│   ├── extractors/              # Domain extractors
│   ├── orchestration/           # Execution management
│   ├── output/                  # Output handling
│   ├── tui/                     # Terminal UI
│   └── types/                   # Pydantic models
├── tests/
├── pyproject.toml
├── requirements.txt
├── .env.example
└── .gitignore
```

---

## Module Details

### `core/` - Configuration

| File | Purpose | Key Classes/Functions |
|------|---------|----------------------|
| `config.py` | Load .env config | `SFMCConfig` dataclass, `get_config()` |

**Design Notes:**
- Uses `python-dotenv` to load from `.env` file
- `SFMCConfig` dataclass with property methods for URL construction
- `validate()` method returns list of error messages

---

### `clients/` - API Transport

| File | Purpose |
|------|---------|
| `auth.py` | OAuth2 token management with thread-safe caching |
| `rest_client.py` | REST API client with retry/backoff |
| `soap_client.py` | SOAP API client with pagination support |

**`auth.py` - TokenManager:**
- Thread-safe token caching with `threading.Lock` and `Condition`
- Double-checked locking pattern
- Single-flight coordination to prevent thundering herd on 401
- 60-second expiry buffer before token refresh
- `get_token()` and `force_refresh()` methods

**`rest_client.py` - RESTClient:**
- Both sync (`request()`) and async (`request_async()`) methods
- Retry with exponential backoff (3 attempts, 2x multiplier)
- Retryable status codes: 429, 500, 502, 503, 504
- Honors `Retry-After` header for 429 responses
- Auto token refresh on 401
- Returns dict: `{"ok": bool, "status_code": int, "data": ...}`

**`soap_client.py` - SOAPClient:**
- XML envelope construction with `xml.etree.ElementTree`
- OAuth token in SOAP header (`fueloauth` element)
- `retrieve()` and `retrieve_all_pages()` for pagination
- `ContinueRequest` pattern for SFMC SOAP pagination
- Response parsing to dict with `_element_to_dict()`
- Max 100 pages safety limit

---

### `cache/` - Data Caching

| File | Purpose |
|------|---------|
| `cache_manager.py` | Unified lazy-loading cache for folders and definitions |
| `breadcrumb_builder.py` | Recursive folder path construction with memoization |

**`cache_manager.py` - CacheManager:**
- Lazy loading - caches load on first access via `_ensure_loaded()`
- Thread-safe with `threading.RLock`
- Supports 17 cache types via `CacheType` enum:
  - SOAP folders: automation, email, template, triggered_send, list, journey
  - REST folders: DE, query, script, import, dataextract, filetransfer, filter
  - Content categories (REST)
  - Definitions: queries, scripts, emails, triggered_sends
- `get_breadcrumb()` builds folder paths with memoization
- `warm()` method for pre-loading caches
- Stats tracking: load times, cache sizes, missing folders

**`breadcrumb_builder.py` - BreadcrumbBuilder:**
- Recursive path construction from folder hierarchy
- Memoization via internal `_cache` dict
- Tracks missing folder IDs for audit
- Configurable separator (default: ` > `)

---

### `extractors/` - Domain Extractors

| File | Purpose | Object Type |
|------|---------|-------------|
| `base_extractor.py` | Template pattern | Abstract base |
| `automation.py` | Automations | REST + cache enrichment |
| `data_extension.py` | Data Extensions | REST with parallel field fetch |
| `query.py` | SQL Queries | REST + SQL parsing |
| `journey.py` | Journeys | REST + activity analysis |

**`base_extractor.py` - BaseExtractor:**
- Generic template pattern with `TypeVar`
- Pipeline: `fetch_data()` → `enrich_data()` → `transform_data()`
- Async `extract()` method with error collection
- `ExtractorOptions` dataclass for configuration
- `ExtractorResult` dataclass with items, errors, relationships
- Auto cache warming via `required_caches` class attribute
- Progress callback support

**`automation.py` - AutomationExtractor:**
- REST pagination for automation list
- Detail fetch per automation for steps/activities
- Activity type resolution via `ACTIVITY_TYPE_MAP` (20+ types)
- Status resolution via `AUTOMATION_STATUS_MAP`
- Enriches with query/script names from cache
- Extracts relationships: automation→query, automation→script, etc.

**`data_extension.py` - DataExtensionExtractor:**
- Parallel field retrieval with `asyncio.Semaphore`
- Breadcrumb enrichment from DE_FOLDERS cache
- Field transformation with type/length/PK info

**`query.py` - QueryExtractor:**
- SQL parsing with regex for FROM/JOIN clauses
- Extracts referenced DE names
- System table filtering (_, sys, dual, etc.)
- Creates QUERY_READS_DE and QUERY_WRITES_DE relationships

**`journey.py` - JourneyExtractor:**
- Journey list + detail fetch
- Trigger, activity, and goal extraction
- Relationship detection for: DEs, emails, filters, automations
- Activity type analysis (EMAILV2, UPDATECONTACTDATA, FIREAUTOMATION, etc.)

---

### `orchestration/` - Execution Management

| File | Purpose |
|------|---------|
| `extractor_runner.py` | Parallel extractor execution with progress reporting |
| `rate_limiter.py` | Adaptive rate limiting with backoff/recovery |

**`rate_limiter.py` - AdaptiveRateLimiter:**
- Per-extractor failure/success tracking
- Progressive backoff: `delay * 2` on failures
- Gradual recovery: `delay / 2` after 3 consecutive successes
- Global stress multiplier for API-wide issues
- Both sync (`acquire/release`) and async versions
- Context managers: `RateLimitContext`, `AsyncRateLimitContext`

**`extractor_runner.py` - ExtractorRunner:**
- `run()` - Async parallel execution with semaphore
- `run_sequential()` - One extractor at a time
- Merges relationships into single `RelationshipGraph`
- `RunnerResult` with statistics generation
- Presets: quick, full, content, journey

---

### `output/` - Output Handling

| File | Purpose |
|------|---------|
| `snapshot_writer.py` | NDJSON file output |
| `relationship_builder.py` | Relationship graph analysis |
| `csv_exporter.py` | CSV conversion |

**`snapshot_writer.py` - SnapshotWriter:**
- Creates timestamped directory: `inventory_YYYYMMDD_HHMMSS/`
- Uses `orjson` for fast JSON serialization
- NDJSON format (one object per line) for large collections
- Output structure:
  ```
  manifest.json
  statistics.json
  objects/*.ndjson
  relationships/graph.json
  relationships/orphans.json
  ```

**`relationship_builder.py` - RelationshipBuilder:**
- Object indexing for lookup
- Reference counting for orphan detection
- SQL dependency analysis
- Graph traversal: `get_dependencies_for()`, `get_dependents_for()`

**`csv_exporter.py` - CSVExporter:**
- Column configurations per object type
- Flattening of nested structures
- Boolean formatting (Yes/No)
- List joining with commas

---

### `tui/` - Terminal UI

| File | Purpose | Framework |
|------|---------|-----------|
| `app.py` | Main Textual app | Textual |
| `selection_screen.py` | Object type selection | Textual widgets |
| `progress_screen.py` | Extraction progress | Textual + asyncio |
| `config_store.py` | Persistent preferences | platformdirs + JSON |

**`app.py` - InventoryApp:**
- Textual `App` subclass
- Screen management: selection → progress
- Dark mode toggle binding

**`selection_screen.py` - SelectionScreen:**
- `SelectionList` widget for object types
- Preset buttons (quick, full, content, journey)
- Options: include_details, include_content
- Keybindings: q=quit, enter=confirm, a=all, n=none

**`progress_screen.py` - ProgressScreen:**
- `DataTable` for per-extractor status
- `ProgressBar` for overall progress
- Async extraction via `asyncio.create_task()`
- Summary display on completion
- Open output directory button

**`config_store.py` - ConfigStore:**
- Uses `platformdirs` for `~/.config/sfmc-inv2/config.json`
- Dot notation for nested keys
- Convenience methods: `get_last_selection()`, `set_output_dir()`, etc.

---

### `types/` - Pydantic Models

| File | Purpose |
|------|---------|
| `inventory.py` | Inventory/metadata models |
| `objects.py` | SFMC object models |
| `relationships.py` | Relationship graph models |

**`inventory.py`:**
- `InventoryManifest` - Entry point structure
- `InventoryMetadata` - Extraction metadata
- `InventoryStatistics` - Summary stats
- `ExtractionError` - Error tracking

**`objects.py`:**
- `SFMCObject` - Base with id, name, customerKey, folderPath
- `Automation`, `AutomationStep`, `AutomationActivity`
- `DataExtension`, `DataExtensionField`
- `Query`
- `Journey`, `JourneyActivity`, `JourneyGoal`
- `Asset`, `AssetType`
- `Folder`

**`relationships.py`:**
- `RelationshipType` enum (17 relationship types)
- `RelationshipEdge` - Source/target with type
- `RelationshipGraph` - Edges + orphans + stats
- `OrphanedObject` - Unused object tracking

---

### `cli.py` - CLI Entry Point

**Commands:**
- `run` - Main extraction (TUI or CLI mode)
- `types` / `list_types` - List available extractors
- `presets` / `list_presets_cmd` - List presets
- `check` / `check_config` - Validate config and test auth

**Key Options:**
- `--extract` / `-e` - Object types to extract
- `--preset` / `-p` - Use preset
- `--output-dir` / `-o` - Output directory
- `--format` / `-f` - json, csv, or both
- `--no-tui` - Skip TUI, CLI only
- `--details/--no-details` - Include object details
- `--content/--no-content` - Include SQL/scripts

---

## Key Design Patterns

1. **Template Method** - `BaseExtractor` with fetch/enrich/transform pipeline
2. **Lazy Loading** - Caches load on first access
3. **Double-Checked Locking** - Thread-safe token management
4. **Single-Flight** - Prevent multiple simultaneous token refreshes
5. **Adaptive Backoff** - Rate limiting with recovery
6. **Registry Pattern** - `EXTRACTORS` dict for extractor lookup
7. **Context Managers** - Rate limit acquire/release

---

## Relationship Types

```python
class RelationshipType(str, Enum):
    AUTOMATION_CONTAINS_QUERY = "automation_contains_query"
    AUTOMATION_CONTAINS_SCRIPT = "automation_contains_script"
    AUTOMATION_CONTAINS_IMPORT = "automation_contains_import"
    AUTOMATION_CONTAINS_EXTRACT = "automation_contains_extract"
    AUTOMATION_CONTAINS_TRANSFER = "automation_contains_transfer"
    AUTOMATION_CONTAINS_EMAIL = "automation_contains_email"
    AUTOMATION_CONTAINS_FILTER = "automation_contains_filter"
    QUERY_READS_DE = "query_reads_de"
    QUERY_WRITES_DE = "query_writes_de"
    JOURNEY_USES_DE = "journey_uses_de"
    JOURNEY_USES_EMAIL = "journey_uses_email"
    JOURNEY_USES_FILTER = "journey_uses_filter"
    JOURNEY_USES_AUTOMATION = "journey_uses_automation"
    # ... more
```

---

## Code Review Focus Areas

### 1. Thread Safety
- `auth.py` - Token manager locking logic
- `cache_manager.py` - RLock usage for cache loading
- `rate_limiter.py` - Semaphore and lock coordination

### 2. Async/Await Correctness
- `base_extractor.py` - Pipeline execution
- `data_extension.py` - Parallel field fetching
- `progress_screen.py` - Background task management

### 3. Error Handling
- API client retry logic
- Extractor error collection without abort
- SOAP response parsing edge cases

### 4. Type Safety
- Pydantic model definitions
- Generic typing in `BaseExtractor`
- Optional field handling

### 5. SFMC API Patterns
- REST pagination correctness
- SOAP ContinueRequest pagination
- Activity type mappings completeness

### 6. Memory Efficiency
- NDJSON streaming for large datasets
- Cache size management
- Relationship graph memory usage

### 7. TUI Responsiveness
- Async extraction in background
- Progress callback thread safety
- Screen transition handling

---

## Dependencies

```toml
dependencies = [
    "textual>=0.52.0",        # TUI framework
    "rich>=13.7.0",           # Rich text/progress
    "typer>=0.12.0",          # CLI framework
    "pydantic>=2.6.0",        # Data validation
    "httpx>=0.27.0",          # Async HTTP
    "python-dotenv>=1.0.0",   # Env config
    "platformdirs>=4.2.0",    # Config directories
    "orjson>=3.10.0",         # Fast JSON
]
```

---

## Usage

```bash
# Install
pip install -e .

# Check configuration
sfmc-inv2 check

# Interactive TUI
sfmc-inv2

# CLI extraction
sfmc-inv2 run --preset quick
sfmc-inv2 run --extract automations,queries --format both --no-tui
```

---

## File Count Summary

| Directory | Python Files | Purpose |
|-----------|--------------|---------|
| core/ | 2 | Configuration |
| clients/ | 4 | API transport |
| cache/ | 3 | Data caching |
| extractors/ | 6 | Domain extractors |
| orchestration/ | 3 | Execution management |
| output/ | 4 | Output handlers |
| tui/ | 5 | Terminal UI |
| types/ | 4 | Pydantic models |
| root | 3 | CLI, __init__, __main__ |
| **Total** | **34 .py files** | **~6400 lines** |
