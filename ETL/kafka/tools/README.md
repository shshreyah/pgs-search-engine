# Full-document ETL handoff

The supported ingestion contract is `crawled-documents`, keyed by
`normalized_url`, with the scraper Document JSON as the value.
Airflow and the local command both use `document_pipeline.py`.
The old file-signal publisher is a legacy diagnostic tool, not this DAG's input.

## Lightweight local test (PowerShell, repository root)

```powershell
.\.venv-etl\Scripts\python.exe .\ETL\kafka\tools\publish_crawler_document.py
.\.venv-etl\Scripts\python.exe .\ETL\kafka\document_pipeline.py --no-embeddings
```

This validates and transforms text and metadata, detects duplicate content,
and saves raw receipts and transformed records in SQLite. It atomically exports
a JSONL snapshot before committing Kafka. The smoke test uses a separate group,
`smoke.sqlite3`, and `smoke_documents.jsonl` in the processed folder. Production
uses `pipeline.sqlite3` and `crawled_documents.jsonl`.
`--no-embeddings` is a smoke test only; it does not prove embedding generation
or OpenSearch indexing. Use separate database/output paths and a new Kafka
consumer group for a later test with real embeddings.

## Airflow

The DAG runs once per minute, in batches of up to ten messages, with embeddings
required. Install/rebuild the Airflow image first. The first model download can
use substantial disk space and memory. The shared Kafka library is mounted
into Airflow by Compose. SQLite and the output are in the mounted processed
folder. Raw-file scanning remains in `transform_file`; the full-document path
uses text already extracted by the scraper and does not claim to scan the
original downloaded files.

The optional OpenSearch indexer reads `crawled_documents.jsonl`. Run that
service explicitly after transformed output exists; loading a Kafka receipt
alone is not proof of indexing.

## Durability and retries

Transformation and receipt insertion are one SQLite transaction. Transformation
failures roll back and do not commit Kafka offsets. Malformed documents are
durably rejected. Reprocessing the same topic/partition/offset returns the
existing result without transforming twice. A repeated payload at a new offset
is a new receipt, with content duplication marked by the transform helpers.
The JSONL snapshot is recoverable from SQLite after an export/acknowledgement
failure. SQLite/JSONL use local single-writer coordination; this is not a
multi-host distributed storage implementation.

`consumer.py` remains a receipt-only diagnostic command and retains its own
consumer group. It does not run transformation or claim complete ETL success.

## Tests

```powershell
.\.venv-etl\Scripts\python.exe -m unittest discover -s .\ETL\kafka -p "test_*.py" -v
```

The pipeline tests cover a real Document-shaped payload, durable output, offset
replay, content duplicates, invalid keys, transient transformation failure,
failed output export and failed Kafka acknowledgement. The scanner and real
embedding/OpenSearch integration still require their separate integration runs.
