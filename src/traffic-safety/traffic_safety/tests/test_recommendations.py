import unittest

import pandas as pd

from traffic_safety.recommendations import (
    TemplateLLMClient,
    build_evidence_package,
    generate_recommendation,
)


class TestRecommendations(unittest.TestCase):

    def setUp(self):
        self.location = pd.Series({
            "OBJECTID": 42,
            "Gi_Bin_median": 2.5,
            "Gi_Bin_years_observed": 5,
            "Gi_Bin_min": 2.0,
            "Gi_Bin_max": 3.0,
        })

    def test_build_evidence_package_without_context(self):
        evidence = build_evidence_package(self.location)

        self.assertEqual(evidence["location_id"], 42)
        self.assertEqual(evidence["risk_score"], 2.5)
        self.assertEqual(evidence["years_observed"], 5)
        self.assertEqual(evidence["risk_range"], [2.0, 3.0])
        self.assertEqual(evidence["context_feature_count"], 0)

    def test_build_evidence_package_with_context(self):
        context = pd.DataFrame({"name": ["Intersection A", "Intersection B"]})
        evidence = build_evidence_package(self.location, context=context)

        self.assertEqual(evidence["context_feature_count"], 2)

    def test_generate_recommendation_uses_template_client_by_default(self):
        evidence = build_evidence_package(self.location)
        recommendation = generate_recommendation(evidence)

        self.assertIn("recommendation", recommendation)
        self.assertIn("42", recommendation["recommendation"])
        self.assertIn("2.50", recommendation["recommendation"])
        # Every evidence field must still be traceable on the recommendation.
        for key in evidence:
            self.assertEqual(recommendation[key], evidence[key])

    def test_generate_recommendation_with_custom_llm_client(self):
        class EchoClient:
            def generate(self, prompt: str) -> str:
                return f"ECHO: {prompt}"

        evidence = build_evidence_package(self.location)
        recommendation = generate_recommendation(evidence, llm_client=EchoClient())

        self.assertTrue(recommendation["recommendation"].startswith("ECHO: "))

    def test_template_llm_client_is_identity(self):
        client = TemplateLLMClient()
        self.assertEqual(client.generate("hello"), "hello")


if __name__ == '__main__':
    unittest.main()
