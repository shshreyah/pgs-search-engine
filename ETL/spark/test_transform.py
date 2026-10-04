import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

from transform import (
    detect_language,
    extract_html_text,
    hamming_distance,
    mark_duplicates,
    simhash_text,
    transform_document,
    transform_file,
)


class TransformTests(unittest.TestCase):
    def test_extracts_html_text_without_script_content(self):
        html = """
        <html><head><script>ignoreMe()</script></head>
        <body><h1>Kathmandu Metropolitan City</h1><p>Public notice</p></body></html>
        """
        self.assertEqual(
            extract_html_text(html),
            "Kathmandu Metropolitan City Public notice",
        )

    def test_detects_english_nepali_and_mixed_text(self):
        self.assertEqual(detect_language("Kathmandu notice"), "en")
        self.assertEqual(detect_language("\u0915\u093e\u0920\u092e\u093e\u0921\u094c\u0902"), "ne")
        self.assertEqual(detect_language("Kathmandu \u0915\u093e\u0920\u092e\u093e\u0921\u094c\u0902"), "mixed")

    def test_transform_document_adds_hashes_language_and_sample_geo(self):
        result = transform_document(
            source_url="https://example.gov.np/notice/1",
            target_domain="kathmandu.gov.np",
            text="Kathmandu Metropolitan City notice",
        )
        self.assertEqual(result["language_detected"], "en")
        self.assertEqual(result["geo_location"]["district"], "Kathmandu")
        self.assertEqual(len(result["content_sha256"]), 64)
        self.assertEqual(len(result["simhash"]), 16)

    def test_marks_exact_and_fuzzy_duplicates(self):
        original = transform_document(
            source_url="a",
            text="Pokhara city notice for tax payment payment payment",
        )
        exact = transform_document(
            source_url="b",
            text="Pokhara city notice for tax payment payment payment",
        )
        near = transform_document(
            source_url="c",
            text="Pokhara city notice for tax payment payment payment update",
        )

        marked = mark_duplicates([original, exact, near])

        self.assertFalse(marked[0]["duplicate"])
        self.assertEqual(marked[1]["duplicate_type"], "exact_sha256")
        self.assertEqual(marked[2]["duplicate_type"], "simhash")

    @patch("security_scanner.scan_with_clamav",
           return_value={"status": "CLEAN", "signature": None})
    def test_transform_file_uses_local_dfs_signal(self, mock_scan):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "sample.html").write_text(
                "<h1>Pokhara Metropolitan City</h1><p>Local notice</p>",
                encoding="utf-8",
            )
            result = transform_file(
                {
                    "object_key": "sample.html",
                    "content_type": "text/html",
                    "source_url": "https://pokhara.example/notice",
                    "target_domain": "pokharamun.gov.np",
                },
                root,
                with_embedding=False,
            )
            self.assertEqual(result["geo_location"]["district"], "Kaski")
            self.assertIn("Local notice", result["searchable_text"])
            self.assertTrue(result["security_scan"]["accepted"])

    def test_simhash_distance_is_stable(self):
        left = simhash_text("same same same")
        right = simhash_text("same same same")
        self.assertEqual(hamming_distance(left, right), 0)


if __name__ == "__main__":
    unittest.main()
