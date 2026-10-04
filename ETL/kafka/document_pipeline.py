"""Shared Airflow/local pipeline: full Documents -> durable output -> Kafka commit."""

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

from consumer import ETLStore, validate_document

sys.path.insert(0, os.getenv("SPARK_LIB", str(Path(__file__).resolve().parents[1] / "spark")))
from transform import add_embeddings, mark_duplicates, transform_document


class PipelineStore(ETLStore):
    def __init__(self, path):
        super().__init__(path)
        with self.db:
            self.db.execute("CREATE TABLE IF NOT EXISTS transformed ("
                            "topic TEXT, partition_id INTEGER, offset_id INTEGER, body TEXT NOT NULL, "
                            "PRIMARY KEY (topic, partition_id, offset_id))")

    def process(self, message, *, with_embedding=True):
        # Reject malformed input only. Transformation failures must remain retryable.
        try:
            document = validate_document(json.loads(message.value))
            if message.key != document["normalized_url"].encode("utf-8"):
                raise ValueError("Kafka key must equal normalized_url")
        except (AttributeError, TypeError, UnicodeDecodeError, ValueError) as exc:
            self.reject(message, str(exc))
            return {"status": "rejected", "reason": str(exc), "offset": message.offset}
        coordinates = (message.topic, message.partition, message.offset)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            previous = self.db.execute("SELECT body FROM transformed WHERE "
                                      "topic=? AND partition_id=? AND offset_id=?", coordinates).fetchone()
            if previous:
                record = json.loads(previous[0])
                status = "duplicate"
            else:
                record = transform_document(source_url=document["normalized_url"],
                                            text=document["text"], title=document["title"],
                                            target_domain=document.get("host") or "", with_embedding=False)
                existing = [json.loads(row[0]) for row in self.db.execute(
                    "SELECT body FROM transformed ORDER BY rowid")]
                record = mark_duplicates([*existing, record])[-1]
                if with_embedding:
                    canonical = next((item for item in existing if not item["duplicate"]
                                      and item["document_id"] == record["duplicate_of"]), None)
                    if canonical and canonical.get("embedding"):
                        for field in ("embedding", "embedding_model", "embedding_dim"):
                            record[field] = canonical[field]
                    else:
                        add_embeddings([record])
                record["crawler_metadata"] = {k: v for k, v in document.items() if k != "text"}
                record["kafka_receipt"] = dict(topic=message.topic, partition=message.partition, offset=message.offset)
                self.db.execute("INSERT OR IGNORE INTO documents "
                                "(topic, partition_id, offset_id, normalized_url, content_hash, body) "
                                "VALUES (?, ?, ?, ?, ?, ?)",
                                (*coordinates, document["normalized_url"], document["content_hash"],
                                 json.dumps(document, ensure_ascii=False, sort_keys=True)))
                self.db.execute("INSERT INTO transformed VALUES (?, ?, ?, ?)",
                                (*coordinates, json.dumps(record, ensure_ascii=False, sort_keys=True)))
                status = "loaded"
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return dict(status=status, normalized_url=document["normalized_url"],
                    partition=message.partition, offset=message.offset,
                    document_id=record["document_id"], duplicate=record["duplicate"])

    def export_snapshot(self, output):
        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = None
        self.db.execute("BEGIN IMMEDIATE")
        try:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=output.parent,
                                             delete=False) as handle:
                temporary = Path(handle.name)
                for (body,) in self.db.execute("SELECT body FROM transformed ORDER BY rowid"):
                    handle.write(body + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, output)
            if os.name != "nt":
                fd = os.open(output.parent, os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)


def process_and_acknowledge(store, message, output, acknowledge, *, with_embedding=True):
    result = store.process(message, with_embedding=with_embedding)
    store.export_snapshot(output)
    acknowledge(message)
    return result


def run_pipeline(*, brokers, topic, group, database, output, max_messages=1,
                 idle_timeout_ms=8000, with_embedding=True):
    from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition
    consumer = KafkaConsumer(topic, bootstrap_servers=brokers.split(","), group_id=group,
                             enable_auto_commit=False, auto_offset_reset="earliest",
                             consumer_timeout_ms=idle_timeout_ms, max_poll_records=1,
                             max_poll_interval_ms=1800000)
    results, store = [], None
    try:
        store = PipelineStore(database)
        def acknowledge(message):
            consumer.commit({TopicPartition(message.topic, message.partition):
                             OffsetAndMetadata(message.offset + 1, "", getattr(message, "leader_epoch", -1))})
        for message in consumer:
            result = process_and_acknowledge(store, message, output, acknowledge,
                                            with_embedding=with_embedding)
            results.append(result)
            print(json.dumps(result, ensure_ascii=False), flush=True)
            if len(results) >= max_messages:
                break
    finally:
        consumer.close(autocommit=False)
        if store is not None:
            store.close()
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--brokers", default="localhost:9092")
    parser.add_argument("--topic", default="crawled-documents")
    parser.add_argument("--group")
    parser.add_argument("--database")
    parser.add_argument("--output")
    parser.add_argument("--max-messages", type=int, default=1)
    parser.add_argument("--no-embeddings", action="store_true", help="Lightweight smoke test only")
    args = parser.parse_args()
    if args.max_messages < 1:
        parser.error("max-messages must be positive")
    prefix = "smoke" if args.no_embeddings else "pipeline"
    group = args.group or ("etl-document-smoke-v1" if args.no_embeddings else "etl-document-pipeline-v1")
    database = args.database or f"ETL/airflow/data/processed/{prefix}.sqlite3"
    output = args.output or ("ETL/airflow/data/processed/smoke_documents.jsonl" if args.no_embeddings
                             else "ETL/airflow/data/processed/crawled_documents.jsonl")
    results = run_pipeline(brokers=args.brokers, topic=args.topic, group=group,
                          database=database, output=output, max_messages=args.max_messages,
                          with_embedding=not args.no_embeddings)
    if not results:
        print("No new crawler documents.")


if __name__ == "__main__":
    main()
