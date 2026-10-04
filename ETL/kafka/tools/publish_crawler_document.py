"""Publish one representative crawler Document to Kafka."""

import hashlib
import json
import os
from datetime import datetime, timezone

from kafka import KafkaProducer

text = (
    "Kathmandu Metropolitan City This is a representative notice used to validate "
    "text extraction in Airflow. Contact: info@example.gov.np"
)

document = {
    "url": "https://example.gov.np/notices/1",
    "normalized_url": "https://example.gov.np/notices/1",
    "host": "example.gov.np",
    "title": "Sample Municipality Notice",
    "text": text,
    "headings": [{"level": 1, "text": "Kathmandu Metropolitan City"}],
    "links": [],
    "anchor_texts": [],
    "country": "NP",
    "depth": 0,
    "status_code": 200,
    "content_type": "text/html; charset=utf-8",
    "content_hash": hashlib.sha256(text.encode("utf-8")).hexdigest(),
    "fetched_at": datetime.now(timezone.utc).isoformat(),
    "fetch_duration_ms": 120,
}

producer = KafkaProducer(
    bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")],
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
)

producer.send("crawled-documents", key=document["normalized_url"], value=document).get(timeout=30)
producer.flush()
print("Sent 1 crawler document to topic 'crawled-documents'")
