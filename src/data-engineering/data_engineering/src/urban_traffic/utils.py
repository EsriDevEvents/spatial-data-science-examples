from arcgis.features import GeoAccessor
import pandas as pd
from sqlite3 import connect


def read_traffic_sql(filepath: str, limit: int) -> pd.DataFrame:
    with connect(filepath) as connection:
        if limit < 1:
            return pd.read_sql('SELECT * FROM agent_pos;', connection)
        else:
            return pd.read_sql(f'SELECT * FROM agent_pos LIMIT {limit};', connection)

def fetch_traffic_data(filepath: str, max_record_count: int = 1000) -> GeoAccessor:
    traffic_df = read_traffic_sql(filepath, max_record_count)
    return GeoAccessor.from_xy(traffic_df, x_column='longitude', y_column='latitude', sr=4326)
