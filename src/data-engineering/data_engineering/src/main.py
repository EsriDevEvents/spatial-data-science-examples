from arcgis.gis import GIS
from data_engineering.utils import fetch_charging_stations, fetch_traffic_accidents
from urban_traffic.utils import fetch_traffic_data
import os


api_key = os.getenv("ARCGIS_API_KEY")
if not api_key:
    raise ValueError("ARCGIS_API_KEY environment variable is not set!")

gis = GIS(api_key=api_key)

traffic_data_filepath = os.getenv("traffic_data_file")
if not traffic_data_filepath:
    raise ValueError("traffic_data_file environment variable is not set!")

def explore_data():
    charging_stations = fetch_charging_stations(gis, max_record_count=10)
    print(charging_stations.info())
    print(charging_stations[["Anzahl_Ladepunkte", "Inbetriebnahmedatum"]].describe())
    print(charging_stations.to_json(orient="records"))

    traffic_accidents = fetch_traffic_accidents(gis, max_record_count=10)
    print(traffic_accidents.info())
    print(traffic_accidents[["UART", "UTYP1"]].describe())
    print(traffic_accidents.to_json(orient="records"))

def explore_traffic():
    traffic_data = fetch_traffic_data(traffic_data_filepath, max_record_count=10)
    print(traffic_data.info())
    print(traffic_data[["vehicle_type", "trip_time"]].describe())
    print(traffic_data.to_json(orient="records"))

def main():
    explore_data()
    explore_traffic()

if __name__ == "__main__":
    main()