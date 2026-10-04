"""Create the index mapping for the output of spark/transform.py."""

import os

from opensearchpy import OpenSearch


INDEX_NAME = os.getenv("OPENSEARCH_INDEX", "pgs_documents")

INDEX_BODY = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "index.knn": True,
        "analysis": {
            "analyzer": {
                "document_text": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase"],
                }
            }
        },
    },
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "document_id": {"type": "keyword"},
            "source_url": {"type": "keyword", "ignore_above": 4096},
            "object_key": {"type": "keyword", "ignore_above": 1024},
            "target_domain": {"type": "keyword"},
            "title": {
                "type": "text",
                "analyzer": "document_text",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 1024}},
            },
            "language_detected": {"type": "keyword"},
            "searchable_text": {"type": "text", "analyzer": "document_text"},
            "word_count": {"type": "integer"},
            "char_count": {"type": "integer"},
            "content_sha256": {"type": "keyword"},
            "simhash": {"type": "keyword"},
            "geo_location": {
                "type": "object",
                "dynamic": "strict",
                "properties": {
                    "province": {"type": "keyword"},
                    "district": {"type": "keyword"},
                    "municipality": {"type": "keyword"},
                    "source": {"type": "keyword"},
                },
            },
            "duplicate": {"type": "boolean"},
            "duplicate_type": {"type": "keyword"},
            "duplicate_of": {"type": "keyword"},
            "embedding": {
                "type": "knn_vector",
                "dimension": 768,
                "method": {
                    "name": "hnsw",
                    "space_type": "cosinesimil",
                    "engine": "nmslib",
                },
            },
            "embedding_model": {"type": "keyword"},
            "embedding_dim": {"type": "integer"},
            "security_scan": {"type": "object", "dynamic": True},
            "crawler_metadata": {"type": "object", "enabled": False},
            "kafka_receipt": {
                "type": "object",
                "dynamic": "strict",
                "properties": {
                    "topic": {"type": "keyword"},
                    "partition": {"type": "integer"},
                    "offset": {"type": "long"},
                },
            },
        },
    },
}


def ensure_index(client: OpenSearch, index_name: str = INDEX_NAME) -> bool:
    """Create the index when absent; return True only when created."""
    if client.indices.exists(index=index_name):
        return False
    client.indices.create(index=index_name, body=INDEX_BODY)
    return True


def main() -> None:
    from .index_documents import make_client

    client = make_client()
    created = ensure_index(client)
    print(f"{'Created' if created else 'Index already exists'}: {INDEX_NAME}")


if __name__ == "__main__":
    main()
