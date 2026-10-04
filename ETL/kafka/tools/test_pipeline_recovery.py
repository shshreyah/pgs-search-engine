"""Real-Kafka ETL recovery check, with a simulated acknowledgement failure.

Uses a temporary topic and temporary SQLite/JSONL output. Does not run the
Go scraper, stop Kafka, download embeddings, or test the scraper writer retry.
"""

import hashlib
import json
import sys
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from kafka import KafkaConsumer, KafkaProducer, OffsetAndMetadata, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from document_pipeline import PipelineStore, process_and_acknowledge


def main():
    suffix = uuid.uuid4().hex[:12]
    topic = f"etl-recovery-test-{suffix}"
    group = f"etl-recovery-test-{suffix}"
    admin = KafkaAdminClient(bootstrap_servers="localhost:9092")
    producer = None
    consumer = None
    store = None
    created = False
    temporary = tempfile.TemporaryDirectory(prefix="pgs-etl-recovery-")
    try:
        admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
        created = True
        text = "Kathmandu notice used for the ETL failure recovery test."
        document = {
            "url": "https://example.gov.np/recovery-test",
            "normalized_url": "https://example.gov.np/recovery-test",
            "host": "example.gov.np", "title": "Recovery test", "text": text,
            "headings": [], "links": [], "anchor_texts": [], "country": "NP",
            "depth": 0, "status_code": 200, "content_type": "text/html",
            "content_hash": hashlib.sha256(text.encode()).hexdigest(),
            "fetched_at": datetime.now(timezone.utc).isoformat(), "fetch_duration_ms": 1,
        }
        producer = KafkaProducer(bootstrap_servers="localhost:9092")
        body = json.dumps(document).encode()
        producer.send(topic, partition=0, key=document["normalized_url"].encode(), value=body).get(timeout=30)
        producer.send(topic, partition=0, key=b"wrong-key", value=body).get(timeout=30)
        producer.flush()
        producer.close()
        producer = None

        def open_consumer():
            return KafkaConsumer(topic, bootstrap_servers="localhost:9092", group_id=group,
                                 enable_auto_commit=False, auto_offset_reset="earliest",
                                 consumer_timeout_ms=30000, max_poll_records=1)

        directory = temporary.name
        database = Path(directory) / "receipts.sqlite3"
        output = Path(directory) / "output.jsonl"
        store = PipelineStore(database)
        consumer = open_consumer()
        first = next(consumer)
        partition = TopicPartition(topic, first.partition)

        def fail_acknowledgement(message):
            raise ConnectionError("Simulated transient Kafka acknowledgement failure")

        try:
            process_and_acknowledge(store, first, output, fail_acknowledgement,
                                    with_embedding=False)
        except ConnectionError:
            pass
        else:
            raise AssertionError("Expected the simulated acknowledgement failure")
        assert consumer.committed(partition) in (None, first.offset), "Offset committed too early"
        assert len(output.read_text(encoding="utf-8").splitlines()) == 1
        print("PASS: saved output survives failed acknowledgement; offset not committed")
        consumer.close(autocommit=False)
        consumer = None
        store.close()
        store = PipelineStore(database)
        consumer = open_consumer()
        replay = next(consumer)
        assert replay.offset == first.offset, "Kafka did not replay the uncommitted message"

        def acknowledge(message):
            consumer.commit({TopicPartition(message.topic, message.partition):
                             OffsetAndMetadata(message.offset + 1, "",
                                               getattr(message, "leader_epoch", -1))})

        with patch("document_pipeline.transform_document", side_effect=AssertionError("Transformed twice")):
            result = process_and_acknowledge(store, replay, output, acknowledge,
                                             with_embedding=False)
        assert result["status"] == "duplicate"
        assert consumer.committed(partition) == replay.offset + 1
        assert len(output.read_text(encoding="utf-8").splitlines()) == 1
        print("PASS: Kafka replay acknowledged; exactly one transformed record")
        rejected = process_and_acknowledge(store, next(consumer), output, acknowledge,
                                           with_embedding=False)
        assert rejected["status"] == "rejected"
        assert store.db.execute("SELECT COUNT(*) FROM rejected").fetchone()[0] == 1
        assert store.db.execute("SELECT COUNT(*) FROM transformed").fetchone()[0] == 1
        print("PASS: wrong Kafka key durably rejected")
        consumer.close(autocommit=False)
        consumer = None
        store.close()
        store = None
        print("ETL Kafka recovery test PASSED (embeddings and Go scraper excluded)")
    finally:
        if consumer is not None:
            consumer.close(autocommit=False)
        if producer is not None:
            producer.close()
        if store is not None:
            store.close()
        temporary.cleanup()
        try:
            if created:
                admin.delete_topics([topic])
        finally:
            admin.close()


if __name__ == "__main__":
    main()
