from arcgis.gis import GIS
from arcgis.gis import Item
from arcgis.features import FeatureLayer
from arcgis.geometry import Envelope, SpatialReference
from arcgis.geometry.filters import intersects
import pandas as pd


def fetch_charging_stations(gis: GIS, max_record_count: int = 1000) -> pd.DataFrame:
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

def fetch_traffic_accidents(gis: GIS, max_record_count: int = 1000) -> pd.DataFrame:
    """Fetches the traffic accidents from the ArcGIS Online feature service.

    Args:
        gis (GIS): An authenticated GIS object.
    """
    portal_item: Item = gis.content.get("027fd014ed184fd78a37b54a68afe892")
    feature_layer: FeatureLayer = portal_item.layers[0]
    feature_sdf = feature_layer.query(where="1=1", out_fields="*", return_all_records=False, result_record_count=max_record_count, as_df=True)
    return feature_sdf

def fetch_hotcold_features(gis: GIS, spatial_features: pd.DataFrame):
    # Access the portal item's feature layer
    item_id = "6ee6272938624808956debfc17fcc958"
    portal_item = gis.content.get(item_id)
    feature_layer: FeatureLayer = portal_item.layers[0]
    
    # Using a geometry filter
    wgs84 = SpatialReference(4326)
    """
    spatial_fset = spatial_features.spatial.to_featureset()
    points = [list(Point(feature.geometry).coordinates()) for feature in spatial_fset.features]
    multi_point = MultiPoint({"points": points, "spatialReference": wgs84})
    """
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