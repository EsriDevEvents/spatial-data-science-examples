import os
import pandas as pd
import unittest
from traffic_safety.utils import combine_yearly_hotspots, list_feature_classes, summarize_hotspot_bins


class TestTrafficSafety(unittest.TestCase):

    def setUp(self):
        self._gdb_path = os.getenv("HOTSPOT_GDB")
        if not self._gdb_path:
            raise ValueError("HOTSPOT_GDB environment variable is not set!")

    def test_list_feature_classes(self):
        feature_classes = list_feature_classes(self._gdb_path)
        self.assertIsInstance(feature_classes, list, "Unexpected type returned when listing feature classes!")
        self.assertGreater(len(feature_classes), 0, "No feature classes found in the geodatabase!")

    def test_combine_yearly_hotspots(self):
        feature_classes = list_feature_classes(self._gdb_path)
        base_feature_class = feature_classes[0]

        combined: pd.DataFrame = combine_yearly_hotspots(self._gdb_path, base_feature_class)
        self.assertIsInstance(combined, pd.DataFrame, "Unexpected type returned from combine_yearly_hotspots!")
        self.assertIsNotNone(combined.spatial, "DataFrame is not spatially enabled!")

        for feature_class in feature_classes:
            self.assertIn(f"Gi_Bin_{feature_class}", combined.columns, f"Missing combined Gi_Bin column for {feature_class}!")

    def test_summarize_hotspot_bins(self):
        feature_classes = list_feature_classes(self._gdb_path)
        base_feature_class = feature_classes[0]

        combined: pd.DataFrame = combine_yearly_hotspots(self._gdb_path, base_feature_class)
        summarized: pd.DataFrame = summarize_hotspot_bins(combined)

        self.assertIn("Gi_Bin_median", summarized.columns, "Missing Gi_Bin_median column!")
        self.assertIn("Gi_Bin_years_observed", summarized.columns, "Missing Gi_Bin_years_observed column!")
        self.assertTrue((summarized["Gi_Bin_years_observed"] <= len(feature_classes)).all(), "years_observed should never exceed the number of feature classes!")

    def test_summarize_hotspot_bins_min_years_observed(self):
        feature_classes = list_feature_classes(self._gdb_path)
        base_feature_class = feature_classes[0]

        combined: pd.DataFrame = combine_yearly_hotspots(self._gdb_path, base_feature_class)

        # Only keep bins that were spatially matched in every year, i.e. the
        # most reliable subset for cross-year comparisons.
        fully_observed = summarize_hotspot_bins(combined, min_years_observed=len(feature_classes))
        self.assertTrue((fully_observed["Gi_Bin_years_observed"] == len(feature_classes)).all(), "Expected only fully-observed rows to remain!")

if __name__ == '__main__':
    unittest.main()
