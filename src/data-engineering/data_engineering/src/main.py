from arcgis.gis import GIS
from data_engineering.utils import fetch_charging_stations, fetch_traffic_accidents
from urban_traffic.utils import fetch_traffic_data
import os


gis = GIS()

def explore_data():
    charging_stations = fetch_charging_stations(gis)
    print(charging_stations.info())
    print(charging_stations[["Anzahl_Ladepunkte", "Inbetriebnahmedatum"]].describe())

    traffic_accidents = fetch_traffic_accidents(gis)
    print(traffic_accidents.info())
    print(traffic_accidents[["UART", "UTYP1"]].describe())

def explore_traffic():
    traffic_data_filepath = os.getenv("traffic_data_file")
    traffic_data = fetch_traffic_data(traffic_data_filepath)
    print(traffic_data.info())
    print(traffic_data[["speed", "congestion"]].describe())

def main():
    explore_data()


if __name__ == "__main__":
    main()