from arcgis.gis import GIS, Item, Layer
from arcgis.features import FeatureLayer
from arcgis.geometry import Envelope, SpatialReference
from arcgis.geometry.filters import intersects
import pandas as pd


def get_charging_stations_layer(gis: GIS) -> FeatureLayer:
    """Gets the charging stations feature layer from ArcGIS Online.

    Args:
        gis (GIS): An authenticated GIS object.

    Returns:
        The charging stations feature layer.
    """
    portal_item: Item = gis.content.get("bc3c97f73d6b4be4921be8560fbc325a")
    return portal_item.layers[0]

def fetch_charging_stations(gis: GIS, max_record_count: int = 1000, extent: Envelope = None) -> pd.DataFrame:
    """Fetches the charging stations from the ArcGIS Online feature service.

    Args:
        gis (GIS): An authenticated GIS object.
        max_record_count (int, optional): The maximum number of records to fetch. Defaults to 1000.
        extent (Envelope, optional): An optional spatial extent to filter the charging stations.

    Returns:
        A spatially enabled DataFrame containing all charging stations.
    """
    feature_layer: FeatureLayer = get_charging_stations_layer(gis)
    if extent:
        spatial_filter = intersects(extent, sr=extent.spatial_reference)

        # Query the intersecting features
        return feature_layer.query(where="1=1", out_fields="*", return_all_records=False, result_record_count=max_record_count, geometry_filter=spatial_filter, as_df=True)
    else:
        return feature_layer.query(where="1=1", out_fields="*", return_all_records=False, result_record_count=max_record_count, as_df=True)

def get_live_traffic_item(gis: GIS) -> Item:
    """Gets the live traffic portal item from ArcGIS Online.

    Args:
        gis (GIS): An authenticated GIS object.

    Returns:
        The live traffic portal item.
    """
    return gis.content.get("ff11eb5b930b4fabba15c47feb130de4")

def get_traffic_accidents_layer(gis: GIS) -> FeatureLayer:
    """Gets the traffic accidents feature layer from ArcGIS Online.

    Args:
        gis (GIS): An authenticated GIS object.

    Returns:
        The traffic accidents feature layer.
    """
    portal_item: Item = gis.content.get("027fd014ed184fd78a37b54a68afe892")
    return portal_item.layers[0]

def fetch_traffic_accidents(gis: GIS, max_record_count: int = 1000, extent: Envelope = None) -> pd.DataFrame:
    """Fetches the traffic accidents from the ArcGIS Online feature service.

    Args:
        gis (GIS): An authenticated GIS object.
        max_record_count (int, optional): The maximum number of records to fetch. Defaults to 1000.
        extent (Envelope, optional): An optional spatial extent to filter the traffic incidents.
    """
    feature_layer: FeatureLayer = get_traffic_accidents_layer(gis)
    if extent:
        spatial_filter = intersects(extent, sr=extent.spatial_reference)

        return feature_layer.query(where="1=1", out_fields="*", return_all_records=False, result_record_count=max_record_count, geometry_filter=spatial_filter, as_df=True)
    else:
        return feature_layer.query(where="1=1", out_fields="*", return_all_records=False, result_record_count=max_record_count, as_df=True)

def get_hotcold_layer(gis: GIS) -> FeatureLayer:
    """Gets the hotcold feature layer from the portal.

    Args:
        gis (GIS): An authenticated GIS object.

    Returns:
        The hotcold feature layer.
    """
    portal_item: Item = gis.content.get("6ee6272938624808956debfc17fcc958")
    return portal_item.layers[0]

def fetch_hotcold_features(gis: GIS, spatial_features: pd.DataFrame):
    """
    Fetches the hotcold features from a portal feature service
    that intersect with the provided spatial features.

    Args:
        gis (GIS): An authenticated GIS object.
        spatial_features (pd.DataFrame): A spatially enabled DataFrame
            containing the features to use for spatial filtering.

    Returns:
        A tuple containing: a FeatureSet of the intersecting hotcold features,
        and a drawing info of the layer's renderer.
    """
    feature_layer: FeatureLayer = get_hotcold_layer(gis)
    
    # Using a geometry filter
    wgs84 = SpatialReference(4326)
    xmin, ymin, xmax, ymax = spatial_features.spatial.full_extent
    extent = Envelope({
        "xmin": xmin,
        "ymin": ymin,
        "xmax": xmax,
        "ymax": ymax,
        "spatialReference": wgs84
    })
    spatial_filter = intersects(extent, sr=wgs84)

    # Query the intersecting features
    feature_set = feature_layer.query(where="1=1", out_fields="*", geometry_filter=spatial_filter)
    return feature_set, {"renderer": convert_internal_dict(feature_layer.renderer)}

def fetch_hottest_features_by_extent(gis: GIS, extent: Envelope):
    """
    Fetches the hot features from a portal feature service
    that intersect with the provided extent.

    Args:
        gis (GIS): An authenticated GIS object.
        extent (Envelope, optional): A spatial extent to filter the features.

    Returns:
        A tuple containing: a FeatureSet of the intersecting hotcold features,
        and a drawing info of the layer's renderer.
    """
    feature_layer: FeatureLayer = get_hotcold_layer(gis)
    
    # Using a geometry filter
    spatial_filter = intersects(extent, sr=extent.spatial_reference)

    # Query the intersecting features
    feature_set = feature_layer.query(where="Gi_Bin>=3", out_fields="*", geometry_filter=spatial_filter)
    return feature_set, {"renderer": convert_internal_dict(feature_layer.renderer)}

def convert_internal_dict(obj):
    """
    Recursively convert custom InsensitiveDict instances to plain Python dicts.
    Handles:
    - Nested dicts
    - Lists containing dicts or custom dict instances
    """
    # If it's a custom InsensitiveDict type or a regular dict, recurse into its values
    if str(type(obj)) == "<class 'arcgis._impl.common._isd.InsensitiveDict'>" or isinstance(obj, dict):
        return {key: convert_internal_dict(value) for key, value in obj.items()}

    # If it's a list, recurse into each element
    elif isinstance(obj, list):
        return [convert_internal_dict(item) for item in obj]

    # Otherwise, return the value as-is
    else:        
        return obj
    
def deep_compare(val1, val2):
    """
    Recursively compare two values which can be:
    - dict
    - list
    - primitive types
    Returns:
        - differences as a dict or tuple
        - None if values are equal
    """
    if isinstance(val1, dict) and isinstance(val2, dict):
        added, removed, modified, same = dict_compare(val1, val2)
        if added or removed or modified:
            return {"added": added, "removed": removed, "modified": modified}
        return None

    elif isinstance(val1, list) and isinstance(val2, list):
        # Compare lists element-wise
        diffs = []
        for i in range(max(len(val1), len(val2))):
            v1 = val1[i] if i < len(val1) else None
            v2 = val2[i] if i < len(val2) else None
            if v1 != v2:
                diff = deep_compare(v1, v2)
                if diff is not None:
                    diffs.append({"index": i, "diff": diff})
        return diffs if diffs else None

    else:
        return (val1, val2) if val1 != val2 else None

def dict_compare(d1, d2):
    d1_keys = set(d1.keys())
    d2_keys = set(d2.keys())
    shared_keys = d1_keys.intersection(d2_keys)

    added = d1_keys - d2_keys
    removed = d2_keys - d1_keys
    modified = {}
    same = set()

    for key in shared_keys:
        diff = deep_compare(d1[key], d2[key])
        if diff is not None:
            modified[key] = diff
        else:
            same.add(key)
    
    return added, removed, modified, same