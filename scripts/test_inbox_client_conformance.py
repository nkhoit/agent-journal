#!/usr/bin/env python3
import tempfile
import unittest
from pathlib import Path

import yaml

from inbox_client_conformance import CASES, matched_success, validate_manifest


class InboxClientConformance(unittest.TestCase):
    def test_manifest_rejects_missing_duplicate_and_unknown_cases(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cases.yaml"
            cases = list(CASES)
            for changed in [cases[:-1], cases + [cases[0]], cases[:-1] + ["unknown"]]:
                path.write_text(yaml.safe_dump({"fixture_version": 1, "cases": changed}), encoding="utf-8")
                with self.assertRaises(ValueError):
                    validate_manifest(path)
            path.write_text(yaml.safe_dump({"fixture_version": 1, "cases": cases}), encoding="utf-8")
            self.assertEqual(validate_manifest(path), cases)

    def test_empty_ignored_failed_or_different_test_is_not_acceptance(self):
        for output in [
            "test result: ok. 0 passed",
            "test required ... ignored",
            "test required ... FAILED",
            "test unrelated ... ok",
        ]:
            self.assertFalse(matched_success(output, "required"))
        self.assertTrue(matched_success("test required ... ok\n", "required"))


if __name__ == "__main__":
    unittest.main()
