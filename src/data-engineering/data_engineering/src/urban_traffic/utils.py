from arcgis.geometry import buffer, Geometry, LengthUnits, SpatialReference
from arcgis.geometry.filters import intersects
from arcgis.features import GeoAccessor
import pandas as pd
from sqlite3 import connect


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
