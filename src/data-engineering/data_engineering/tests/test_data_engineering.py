from arcgis.gis import GIS
from data_engineering.utils import fetch_charging_stations, fetch_traffic_accidents
from urban_traffic.utils import fetch_traffic_data, read_bike_trail, read_traffic_features
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

        self._traffic_features_filepath = os.getenv("TRAFFIC_FEATURES")
        if not self._traffic_features_filepath:
            raise ValueError("TRAFFIC_FEATURES environment variable is not set!")
        
        self._bike_trail_filepath = os.getenv("BIKE_TRAIL_FILE")
        if not self._bike_trail_filepath:
            raise ValueError("BIKE_TRAIL_FILE environment variable is not set!")

        self._sample_size = 10
    
    def test_living_atlas(self):
        charging_stations: pd.DataFrame = fetch_charging_stations(self._gis, max_record_count=self._sample_size)
        self.assertIsInstance(charging_stations, pd.DataFrame, "Unexpected type returned from the portal!")
        self.assertEquals(len(charging_stations), self._sample_size, "Unexpected number of charging stations returned from the portal!")
        self.assertIsNotNone(charging_stations.spatial, "DataFrame is not spatially enabled!")

        traffic_accidents: pd.DataFrame = fetch_traffic_accidents(self._gis, max_record_count=self._sample_size)
        self.assertIsInstance(traffic_accidents, pd.DataFrame, "Unexpected type returned from the portal!")
        self.assertEquals(len(traffic_accidents), self._sample_size, "Unexpected number of traffic accidents returned from the portal!")
        self.assertIsNotNone(traffic_accidents.spatial, "DataFrame is not spatially enabled!")

    def test_traffic_data(self):
        traffic_data: pd.DataFrame = fetch_traffic_data(self._traffic_data_filepath, max_record_count=self._sample_size)
        self.assertIsInstance(traffic_data, pd.DataFrame, "Unexpected type returned from the database!")
        self.assertEquals(len(traffic_data), self._sample_size, "Unexpected number of traffic records returned from the database!")
        self.assertIsNotNone(traffic_data.spatial, "DataFrame is not spatially enabled!")

    def test_traffic_features(self):
        lon, lat = 8.62376, 50.11862  # Coordinates for Frankfurt am Main
        distance_in_meters = 5
        traffic_features: pd.DataFrame = read_traffic_features(self._traffic_features_filepath, lon=lon, lat=lat, meters=distance_in_meters)
        self.assertIsInstance(traffic_features, pd.DataFrame, "Unexpected type returned from the database!")
        self.assertGreater(len(traffic_features), 0, "No traffic features returned from the database!")
        self.assertIsNotNone(traffic_features.spatial, "DataFrame is not spatially enabled!")

    def test_bike_trail_features(self):
        bike_trail_features: pd.DataFrame = read_bike_trail(self._bike_trail_filepath)
        self.assertIsInstance(bike_trail_features, pd.DataFrame, "Unexpected type returned from the database!")
        self.assertGreater(len(bike_trail_features), 0, "No bike trail features returned from the database!")
        self.assertIsNotNone(bike_trail_features.spatial, "DataFrame is not spatially enabled!")


if __name__ == '__main__':
    unittest.main()