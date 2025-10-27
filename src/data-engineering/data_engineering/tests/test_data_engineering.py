from arcgis.gis import GIS
from data_engineering.utils import fetch_charging_stations, fetch_traffic_accidents
import pandas as pd
import unittest


class TestDataEngineering(unittest.TestCase):

    def setUp(self):
        self._gis = GIS()
    
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