from arcgis.gis import GIS
from data_engineering.utils import fetch_charging_stations, fetch_traffic_accidents

def main():
    gis = GIS()
    charging_stations = fetch_charging_stations(gis)
    print(charging_stations.info())
    print(charging_stations[["Anzahl_Ladepunkte", "Inbetriebnahmedatum"]].describe())

    traffic_accidents = fetch_traffic_accidents(gis)
    print(traffic_accidents.info())
    print(traffic_accidents[["UART", "UTYP1"]].describe())


if __name__ == "__main__":
    main()
