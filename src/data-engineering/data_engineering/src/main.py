from arcgis.gis import GIS
from data_engineering.utils import fetch_charging_stations

def main():
    gis = GIS()
    charging_stations = fetch_charging_stations(gis)
    print(charging_stations.info())
    print(charging_stations[["Anzahl_Ladepunkte", "Inbetriebnahmedatum"]].describe())


if __name__ == "__main__":
    main()
