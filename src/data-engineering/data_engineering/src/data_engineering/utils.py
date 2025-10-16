from arcgis.gis import GIS
from arcgis.gis import Item
from arcgis.features import FeatureLayer, FeatureSet, GeoAccessor


def fetch_charging_stations(gis: GIS, max_record_count: int = 1000) -> GeoAccessor:
    """Fetches the charging stations from the ArcGIS Online feature service.

    Args:
        gis (GIS): An authenticated GIS object.

    Returns:
        A spatially enabled DataFrame containing all charging stations.
    """
    portal_item: Item = gis.content.get("bc3c97f73d6b4be4921be8560fbc325a")
    feature_layer: FeatureLayer = portal_item.layers[0]
    feature_sdf = feature_layer.query(where="1=1", out_fields="*", return_all_records=False, result_record_count=max_record_count, as_df=True)
    return feature_sdf

def fetch_traffic_accidents(gis: GIS, max_record_count: int = 1000) -> GeoAccessor:
    """Fetches the traffic accidents from the ArcGIS Online feature service.

    Args:
        gis (GIS): An authenticated GIS object.
    """
    portal_item: Item = gis.content.get("027fd014ed184fd78a37b54a68afe892")
    feature_layer: FeatureLayer = portal_item.layers[0]
    feature_sdf = feature_layer.query(where="1=1", out_fields="*", return_all_records=False, result_record_count=max_record_count, as_df=True)
    return feature_sdf