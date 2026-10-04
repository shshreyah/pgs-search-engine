import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from document_pipeline import PipelineStore, process_and_acknowledge
from test_consumer import message


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "pipeline.sqlite3"
        self.output = Path(self.directory.name) / "output.jsonl"
        self.store = PipelineStore(self.database)
        self.addCleanup(self.store.close)

    def run_message(self, value=None, acknowledge=None):
        return process_and_acknowledge(self.store, value or message(), self.output,
                                       acknowledge or Mock(), with_embedding=False)

    def rows(self):
        return [json.loads(line) for line in self.output.read_text(encoding="utf-8").splitlines()]

    def test_output_is_durable_before_acknowledgement(self):
        def acknowledge(value):
            self.assertEqual(self.rows()[0]["source_url"], "https://example.com/news?id=1")
            self.assertEqual(self.rows()[0]["searchable_text"], "A small document for ETL testing.")
            another = PipelineStore(self.database)
            try:
                self.assertEqual(another.db.execute("SELECT COUNT(*) FROM transformed").fetchone()[0], 1)
            finally:
                another.close()
        self.assertEqual(self.run_message(acknowledge=acknowledge)["status"], "loaded")

    def test_replay_after_reopening_does_not_transform_again(self):
        self.run_message()
        another = PipelineStore(self.database)
        try:
            with patch("document_pipeline.transform_document", side_effect=AssertionError("retransformed")):
                result = process_and_acknowledge(another, message(), self.output, Mock(), with_embedding=False)
            self.assertEqual(result["status"], "duplicate")
            self.assertEqual(len(self.rows()), 1)
        finally:
            another.close()

    def test_transient_transform_error_is_retryable_and_not_rejected(self):
        acknowledge = Mock()
        with patch("document_pipeline.transform_document", side_effect=ValueError("temporary failure")):
            with self.assertRaisesRegex(ValueError, "temporary failure"):
                self.run_message(acknowledge=acknowledge)
        acknowledge.assert_not_called()
        for table in ("documents", "transformed", "rejected"):
            self.assertEqual(self.store.db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0)
        self.assertEqual(self.run_message()["status"], "loaded")
        self.assertEqual(self.run_message()["status"], "duplicate")
        self.assertEqual(len(self.rows()), 1)

    def test_failed_snapshot_then_retry_has_one_output(self):
        acknowledge = Mock()
        with patch.object(self.store, "export_snapshot", side_effect=OSError("disk failure")):
            with self.assertRaises(OSError):
                self.run_message(acknowledge=acknowledge)
        acknowledge.assert_not_called()
        self.assertEqual(self.run_message()["status"], "duplicate")
        self.assertEqual(len(self.rows()), 1)

    def test_failed_kafka_commit_then_retry_has_one_output(self):
        with self.assertRaises(ConnectionError):
            self.run_message(acknowledge=Mock(side_effect=ConnectionError("Kafka unavailable")))
        self.assertEqual(self.run_message()["status"], "duplicate")
        self.assertEqual(len(self.rows()), 1)

    def test_bad_key_is_durably_rejected_before_acknowledgement(self):
        def acknowledge(value):
            self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM rejected").fetchone()[0], 1)
        result = self.run_message(message(key=b"wrong"), acknowledge)
        self.assertEqual(result["status"], "rejected")
        self.assertEqual(self.rows(), [])

    def test_same_content_at_new_offset_is_marked_duplicate(self):
        self.run_message()
        self.run_message(message(offset=8))
        rows = self.rows()
        self.assertEqual(len(rows), 2)
        self.assertFalse(rows[0]["duplicate"])
        self.assertTrue(rows[1]["duplicate"])
        self.assertEqual(rows[1]["duplicate_type"], "exact_sha256")

    def test_embedding_failure_is_not_acknowledged(self):
        acknowledge = Mock()
        with patch("document_pipeline.add_embeddings", side_effect=RuntimeError("model unavailable")):
            with self.assertRaises(RuntimeError):
                process_and_acknowledge(self.store, message(), self.output, acknowledge)
        acknowledge.assert_not_called()
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM transformed").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
