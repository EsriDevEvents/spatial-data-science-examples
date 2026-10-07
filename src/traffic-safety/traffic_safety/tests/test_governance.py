import os
import tempfile
import unittest

from traffic_safety.governance import (
    AuditRecord,
    ReviewDecision,
    append_audit_record,
    load_audit_trail,
    request_human_review,
)


class TestGovernance(unittest.TestCase):

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.audit_log_path = os.path.join(self._tmpdir.name, "nested", "audit_trail.jsonl")
        self.recommendation = {
            "location_id": 42,
            "risk_score": 2.5,
            "recommendation": "Prioritize this location for review.",
        }

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_load_audit_trail_missing_file_returns_empty_list(self):
        self.assertEqual(load_audit_trail(self.audit_log_path), [])

    def test_append_and_load_audit_record(self):
        record = AuditRecord(
            recommendation=self.recommendation,
            decision=ReviewDecision.APPROVED,
            reviewer="jane.doe",
            comment="Evidence checks out.",
        )
        append_audit_record(record, self.audit_log_path)

        trail = load_audit_trail(self.audit_log_path)
        self.assertEqual(len(trail), 1)
        self.assertEqual(trail[0]["decision"], "approved")
        self.assertEqual(trail[0]["reviewer"], "jane.doe")
        self.assertEqual(trail[0]["recommendation"], self.recommendation)

    def test_append_audit_record_is_append_only(self):
        for decision in (ReviewDecision.NEEDS_REVIEW, ReviewDecision.APPROVED):
            append_audit_record(
                AuditRecord(recommendation=self.recommendation, decision=decision, reviewer="jane.doe"),
                self.audit_log_path,
            )

        trail = load_audit_trail(self.audit_log_path)
        self.assertEqual(len(trail), 2)
        self.assertEqual(trail[0]["decision"], "needs_review")
        self.assertEqual(trail[1]["decision"], "approved")

    def test_request_human_review_persists_and_returns_record(self):
        record = request_human_review(
            self.recommendation,
            decision=ReviewDecision.REJECTED,
            reviewer="john.smith",
            comment="Needs more evidence.",
            audit_log_path=self.audit_log_path,
        )

        self.assertEqual(record.decision, ReviewDecision.REJECTED)
        self.assertEqual(load_audit_trail(self.audit_log_path)[0]["reviewer"], "john.smith")

    def test_ollama_client_raises_helpful_error_when_missing(self):
        try:
            import ollama  # noqa: F401
            self.skipTest("ollama package is installed; import-error path is not exercised")
        except ImportError:
            pass

        from traffic_safety.governance import OllamaLLMClient

        with self.assertRaises(ImportError):
            OllamaLLMClient().generate("hello")


if __name__ == '__main__':
    unittest.main()
