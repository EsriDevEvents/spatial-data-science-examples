from arcgis.gis import GIS, Item
from arcgis.features import FeatureLayer, FeatureSet
from arcgis.geometry import buffer, Geometry, LengthUnits, SpatialReference
from arcgis.geometry.filters import intersects
from arcgis.features import GeoAccessor
from arcgis.map.renderers import SimpleRenderer
from arcgis.map.symbols import SimpleMarkerSymbolEsriSMS, SimpleMarkerSymbolStyle, SimpleLineSymbolEsriSLS, SimpleLineSymbolStyle
from data_engineering.utils import get_hotcold_layer, get_live_traffic_item
import json
import pandas as pd
from sqlite3 import connect


def create_map(gis: GIS, location: str = "Ludwig-Erhard-Anlage 1, 60327 Frankfurt am Main, Germany"):
    """Creates a map centered on the provided location.
    Args:
        gis (GIS): An authenticated GIS object.
        location (str): The location to center the map on.

    Returns:
        A map centered on the provided location.
    """
    map_view = gis.map(location)
    map_view.basemap.basemap = "osm"
    map_view.zoom = 17
    return map_view

def create_traffic_map(gis: GIS):
    """Creates a map centered on Frankfurt am Main.

    Args:
        gis (GIS): An authenticated GIS object.

    Returns:
        A map centered on Frankfurt am Main.
    """ 
    traffic_map = create_map(gis, location="Ludwig-Erhard-Anlage 1, 60327 Frankfurt am Main, Germany")
    hotcold_layer: FeatureLayer = get_hotcold_layer(gis)
    live_traffic_item: Item = get_live_traffic_item(gis)
    traffic_map.content.add(live_traffic_item, options={"opacity": 0.7})
    traffic_map.content.add(hotcold_layer, options={"opacity": 0.7})
    #traffic_map.zoom_to_layer(hotcold_layer)
    traffic_map.zoom = 12
    return traffic_map

def prepare_traffic(traffic_df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepares the traffic DataFrame for analysis by converting data types.

    Args:
        traffic_df (pd.DataFrame): The traffic DataFrame to prepare.

    Returns:
        pd.DataFrame: The prepared traffic DataFrame.
    """
    # Extract hours, minutes, and seconds
    traffic_df["trip_time"] = pd.to_datetime(traffic_df["trip_time"], format="%Y-%m-%dT%H:%M:%S")
    traffic_df["hour"] = traffic_df["trip_time"].dt.strftime("%H").astype(int)
    traffic_df["minute"] = traffic_df["trip_time"].dt.strftime("%M").astype(int)
    traffic_df["second"] = traffic_df["trip_time"].dt.strftime("%S").astype(int)
    traffic_df = traffic_df.drop(columns=["trip_time"], axis=1)

    # Fill empty values for vehicle_type
    traffic_df = traffic_df.fillna({"vehicle_type": "pedestrian"})

    # Convert all vehicle types to lower case
    traffic_df["vehicle_type"] = traffic_df["vehicle_type"].str.lower()

    # Unique vehicle types
    vehicle_types = traffic_df["vehicle_type"].unique()

    # Create binary columns
    for vehicle_type in vehicle_types:
        traffic_df[vehicle_type] = traffic_df["vehicle_type"].apply(lambda vt: 1 if vt == vehicle_type else 0).astype(int)
    traffic_df = traffic_df.drop(columns=["vehicle_type"], axis=1)
    
    return traffic_df

def read_traffic_sql(filepath: str, limit: int) -> pd.DataFrame:
    with connect(filepath) as connection:
        if limit < 1:
            return pd.read_sql('SELECT * FROM agent_pos;', connection)
        else:
            return pd.read_sql(f'SELECT * FROM agent_pos LIMIT {limit};', connection)
        
def read_traffic_features(filepath: str, lon: float, lat: float, meters: float) -> pd.DataFrame:
    wgs84 = SpatialReference(4326)
    buffer_result = buffer([
        Geometry({"x": lon, "y": lat, "spatialReference": wgs84})
        ], 
        in_sr=wgs84, 
        distances=[meters], 
        unit=LengthUnits.METER, 
        geodesic=True, 
        out_sr=wgs84
    )
    if "error" in buffer_result:
        raise ValueError(f"Error creating buffer: {buffer_result['error']}")
    if len(buffer_result) != 1:
        raise ValueError("Buffer result must only contain one geometry!")

    buffered_geometry = buffer_result[0]
    try:
        # See: https://pro.arcgis.com/de/pro-app/latest/arcpy/data-access/searchcursor-class.htm
        from arcpy.da import SearchCursor

        new_records = []
        chunk_size = 1000
        data_frame = None
        with SearchCursor(
            filepath, 
            field_names=["OID@", "SHAPE@JSON", "trip", "person", "vehicle_type", "trip_time"], 
            spatial_reference=wgs84.as_arcpy, 
            spatial_filter=buffered_geometry.as_arcpy,
            spatial_relationship="INTERSECTS") as search_cursor:
            for feature in search_cursor:
                new_record = {field: value for field, value in zip(search_cursor.fields, feature)}
                new_records.append(new_record)
                if 0 == len(new_records) % chunk_size:
                    if data_frame is None:
                        data_frame = pd.DataFrame.from_records(new_records)
                    else:
                        data_frame = pd.concat([data_frame, pd.DataFrame.from_records(new_records)], ignore_index=True)
                    new_records = []

        if new_records:
            if data_frame is None:
                data_frame = pd.DataFrame.from_records(new_records)
            else:
                data_frame = pd.concat([data_frame, pd.DataFrame.from_records(new_records)], ignore_index=True)
        
        data_frame.rename(columns={"OID@": "OBJECTID", "SHAPE@JSON": "SHAPE"}, inplace=True)
        return GeoAccessor.from_df(data_frame, geometry_column="SHAPE", sr=4326)

    except ImportError:
        spatial_filter = intersects(buffered_geometry, sr=wgs84)
        return GeoAccessor.from_featureclass(filepath, spatial_filter=spatial_filter)

def fetch_traffic_data(filepath: str, max_record_count: int = 1000) -> pd.DataFrame:
    traffic_df = read_traffic_sql(filepath, max_record_count)
    return GeoAccessor.from_xy(traffic_df, x_column='longitude', y_column='latitude', sr=4326)

def read_bike_trail(filepath: str) -> pd.DataFrame:
    with open(filepath, 'r', encoding='utf-8') as file_in:
        bike_trail_data = json.load(file_in)
        return FeatureSet.from_geojson(bike_trail_data).sdf
    
def explode_bike_trail(filepath: str):
    with open(filepath, 'r', encoding='utf-8') as file_in:
        bike_trail_data = json.load(file_in)
        feature_set = FeatureSet.from_geojson(bike_trail_data)
        for feature in feature_set.features:
            geometry = feature.geometry
            for x, y in geometry.coordinates:
                yield {
                    **feature.attributes,
                    "geometry": Geometry({"x": x, "y": y, "spatialReference": feature_set.spatial_reference})
                }

def filter_commute_cars(traffic_df: pd.DataFrame) -> pd.DataFrame:
    return traffic_df.query("car == 1 and 7 < hour and hour < 10")

def generate_car_renderer():
    return SimpleRenderer(
        symbol=SimpleMarkerSymbolEsriSMS(
            style=SimpleMarkerSymbolStyle.esri_sms_circle.value,
            size=7,
            outline=SimpleLineSymbolEsriSLS(
                color=[155, 155, 155, 55],
                width=0.7,
                style=SimpleLineSymbolStyle.esri_sls_solid.value,
            ),
            color=[255, 0, 0, 55]
        )
    )