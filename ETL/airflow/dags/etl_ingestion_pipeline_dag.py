"""Consume full scraper Documents; save output before committing Kafka offsets."""

import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException


@dag(
    dag_id="etl_ingestion_pipeline",
    schedule="* * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["etl", "ingestion", "kafka", "spark"],
)
def etl_ingestion_pipeline():
    @task
    def process_crawler_documents():
        kafka_lib = "/opt/airflow/kafka_lib"
        if kafka_lib not in sys.path:
            sys.path.insert(0, kafka_lib)
        from document_pipeline import run_pipeline

        results = run_pipeline(
            brokers=os.getenv("KAFKA_BROKERS", "kafka:29092"),
            topic="crawled-documents",
            group="etl-document-pipeline-v1",
            database="/opt/airflow/data/processed/pipeline.sqlite3",
            output="/opt/airflow/data/processed/crawled_documents.jsonl",
            max_messages=10,
            with_embedding=True,
        )
        if not results:
            raise AirflowSkipException("No new crawler documents on Kafka")
        return results

    process_crawler_documents()


etl_ingestion_pipeline()
