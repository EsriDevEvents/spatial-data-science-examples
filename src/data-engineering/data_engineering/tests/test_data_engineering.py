from arcgis.gis import GIS
from data_engineering.utils import fetch_charging_stations, fetch_traffic_accidents
import os
import pandas as pd
import unittest


class TestDataEngineering(unittest.TestCase):

    def setUp(self):
        api_key = os.getenv("ARCGIS_API_KEY")
        if not api_key:
            raise ValueError("ARCGIS_API_KEY environment variable is not set!")
        
        self._gis = GIS(api_key=api_key)
        self._traffic_data_filepath = os.getenv("TRAFFIC_DATA_FILE")
        if not self._traffic_data_filepath:
            raise ValueError("TRAFFIC_DATA_FILE environment variable is not set!")
    
    def test_living_atlas(self):
        charging_stations: pd.DataFrame = fetch_charging_stations(self._gis, max_record_count=10)
        self.assertIsInstance(charging_stations, pd.DataFrame, "Unexpected type returned from the portal!")
        self.assertGreaterEqual(len(charging_stations), 1, "No charging stations returned from the portal!")
        self.assertIsNotNone(charging_stations.spatial, "DataFrame is not spatially enabled!")

        traffic_accidents: pd.DataFrame = fetch_traffic_accidents(self._gis, max_record_count=10)
        self.assertIsInstance(traffic_accidents, pd.DataFrame, "Unexpected type returned from the portal!")
        self.assertGreaterEqual(len(traffic_accidents), 1, "No traffic accidents returned from the portal!")
        self.assertIsNotNone(traffic_accidents.spatial, "DataFrame is not spatially enabled!")


if __name__ == '__main__':
    unittest.main()