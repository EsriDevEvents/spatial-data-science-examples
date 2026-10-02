"""Utilities for combining yearly Hot Spot Analysis (Getis-Ord Gi*) results.

Each year's hot/cold spot analysis produces its own feature class of bins
(e.g. a fishnet or hexagon grid) with a ``Gi_Bin`` field. The bin geometries
are not guaranteed to be identical between years (different extents, grid
snapping, etc.), so bins cannot be joined on a shared key. Instead, this
module aligns bins spatially: it takes the centroid of every bin polygon in
a chosen base feature class and determines, for every other year, which
polygon (if any) contains that centroid. The result is a single wide
DataFrame with one row per base-year bin and one ``Gi_Bin_<year>`` column
per year.
"""
import os
import pandas as pd
from arcgis.features import GeoAccessor


def list_feature_classes(gdb_path: str) -> list[str]:
    """Lists all feature classes in a local file geodatabase.

    Prefers ``arcpy`` (``arcpy.ListFeatureClasses``) when available. Falls
    back to ``fiona.listlayers`` when arcpy is not installed.

    Args:
        gdb_path (str): The filepath to the local file geodatabase.

    Returns:
        list[str]: The names of all feature classes in the geodatabase.
    """
    try:
        import arcpy

        original_workspace = arcpy.env.workspace
        try:
            arcpy.env.workspace = gdb_path
            return arcpy.ListFeatureClasses() or []
        finally:
            arcpy.env.workspace = original_workspace

    except ImportError:
        try:
            import fiona

            return list(fiona.listlayers(gdb_path))
        except ImportError as exc:
            raise RuntimeError(
                "Listing feature classes requires either arcpy or fiona to be installed."
            ) from exc


def read_feature_classes(gdb_path: str, feature_classes: list[str] = None, fields: list[str] = None) -> dict[str, pd.DataFrame]:
    """Reads one or more feature classes from a local file geodatabase.

    Args:
        gdb_path (str): The filepath to the local file geodatabase.
        feature_classes (list[str], optional): The names of the feature
            classes to read. Defaults to all feature classes in the
            geodatabase (see `list_feature_classes`).
        fields (list[str], optional): The fields to read from each feature
            class. Defaults to all fields.

    Returns:
        dict[str, pd.DataFrame]: A mapping of feature class name to its
        spatially enabled DataFrame.
    """
    if feature_classes is None:
        feature_classes = list_feature_classes(gdb_path)

    return {
        feature_class: GeoAccessor.from_featureclass(os.path.join(gdb_path, feature_class), fields=fields)
        for feature_class in feature_classes
    }


def _polygon_rings(geometry) -> list:
    """Extracts the rings from an Esri JSON polygon geometry.

    Args:
        geometry: An `arcgis.geometry.Geometry` (or Esri JSON dict) polygon.

    Returns:
        list: The list of rings (each a list of [x, y] coordinate pairs).
    """
    return geometry.get("rings", []) if geometry else []


def _point_in_rings(x: float, y: float, rings: list) -> bool:
    """Tests whether a point lies inside a set of polygon rings.

    Uses the standard crossing-number (ray casting) algorithm applied across
    all rings together, so interior rings (holes) are handled correctly
    regardless of ring winding order: a point inside a hole crosses an even
    number of rings in total and is correctly excluded.

    Args:
        x (float): The point's x-coordinate.
        y (float): The point's y-coordinate.
        rings (list): The polygon rings to test against.

    Returns:
        bool: True if the point is inside the rings, False otherwise.
    """
    inside = False
    for ring in rings:
        n = len(ring)
        j = n - 1
        for i in range(n):
            xi, yi = ring[i][0], ring[i][1]
            xj, yj = ring[j][0], ring[j][1]
            if (yi > y) != (yj > y):
                x_intersect = (xj - xi) * (y - yi) / (yj - yi) + xi
                if x < x_intersect:
                    inside = not inside
            j = i
    return inside


def _ring_centroid(ring: list) -> tuple[float, float, float]:
    """Computes the signed area and area-weighted centroid sums of a ring.

    Args:
        ring (list): A single polygon ring (list of [x, y] coordinate pairs).

    Returns:
        tuple[float, float, float]: (signed_area, cx_sum, cy_sum) using the
        shoelace formula, to be accumulated across rings by the caller.
    """
    signed_area = 0.0
    cx_sum = 0.0
    cy_sum = 0.0
    n = len(ring)
    for i in range(n - 1):
        x0, y0 = ring[i][0], ring[i][1]
        x1, y1 = ring[i + 1][0], ring[i + 1][1]
        cross = x0 * y1 - x1 * y0
        signed_area += cross
        cx_sum += (x0 + x1) * cross
        cy_sum += (y0 + y1) * cross
    return signed_area, cx_sum, cy_sum


def polygon_centroid(geometry) -> tuple[float, float]:
    """Computes the centroid of a (possibly multi-ring) polygon geometry.

    Rings are combined using the shoelace formula. This does not special
    case interior rings (holes) when computing the centroid; for the
    fishnet/hexagon hot spot bins this module targets, holes are not
    expected in practice.

    Args:
        geometry: An `arcgis.geometry.Geometry` (or Esri JSON dict) polygon.

    Returns:
        tuple[float, float]: The (x, y) centroid of the polygon.
    """
    rings = _polygon_rings(geometry)

    total_area = 0.0
    total_cx = 0.0
    total_cy = 0.0
    for ring in rings:
        signed_area, cx_sum, cy_sum = _ring_centroid(ring)
        total_area += signed_area
        total_cx += cx_sum
        total_cy += cy_sum

    if total_area == 0:
        # Degenerate polygon (e.g. a single point or line); fall back to the
        # average of all vertices across all rings.
        points = [point for ring in rings for point in ring]
        if not points:
            return None
        return (sum(p[0] for p in points) / len(points), sum(p[1] for p in points) / len(points))

    factor = 1.0 / (3.0 * total_area)
    return (total_cx * factor, total_cy * factor)


def assign_bin_by_containment(centroids: pd.Series, target_df: pd.DataFrame, bin_field: str, geometry_column: str = "SHAPE") -> pd.Series:
    """Assigns `bin_field` values from `target_df` to a series of centroids.

    For each centroid, finds the first polygon in `target_df` that contains
    it and returns that polygon's `bin_field` value (NaN when no polygon
    contains the centroid).

    Args:
        centroids (pd.Series): A Series of (x, y) tuples.
        target_df (pd.DataFrame): A spatially enabled DataFrame of polygons
            for the year being matched against.
        bin_field (str): The name of the Gi_Bin field to read from `target_df`.
        geometry_column (str, optional): The geometry column in `target_df`.
            Defaults to "SHAPE".

    Returns:
        pd.Series: The matched `bin_field` values, aligned to `centroids`.
    """
    polygons = [(row[geometry_column], row[bin_field]) for _, row in target_df.iterrows()]

    def _match(centroid):
        if centroid is None:
            return None
        x, y = centroid
        for geometry, bin_value in polygons:
            if _point_in_rings(x, y, _polygon_rings(geometry)):
                return bin_value
        return None

    return centroids.apply(_match)


def _combine_via_python(base_df: pd.DataFrame, year_frames: dict[str, pd.DataFrame], bin_field: str, geometry_column: str, base_key: str) -> pd.DataFrame:
    """Combines Gi_Bin values using the pure-Python centroid/ray-casting approach.

    O(bins_base x bins_year) per year with no spatial index, so this is only
    suitable for small feature classes or as a dependency-free fallback. See
    `combine_hotspot_bins` for the faster arcpy/sedf alternatives.
    """
    combined = base_df.copy()
    centroids = combined[geometry_column].apply(polygon_centroid)

    if base_key is not None:
        combined[f"{bin_field}_{base_key}"] = combined[bin_field]

    for year_key, year_df in year_frames.items():
        combined[f"{bin_field}_{year_key}"] = assign_bin_by_containment(centroids, year_df, bin_field, geometry_column)

    return combined


def _combine_via_spatial_dataframe(base_df: pd.DataFrame, year_frames: dict[str, pd.DataFrame], bin_field: str, geometry_column: str, base_key: str) -> pd.DataFrame:
    """Combines Gi_Bin values using the Spatially Enabled DataFrame's built-in spatial join.

    Builds a quadtree spatial index (`df.spatial.sindex("quadtree", ...)`)
    over each year's polygons and joins base centroids against them with
    `df.spatial.join(...)`. Needs only `arcgis` (no arcpy, no geopandas), and
    is index-accelerated instead of the nested-loop containment test used by
    `_combine_via_python`.
    """
    combined = base_df.copy()

    centroids = combined[geometry_column].apply(polygon_centroid)
    sr = combined[geometry_column].iloc[0]["spatialReference"]
    points_df = pd.DataFrame({
        "_base_idx": combined.index,
        "_x": centroids.apply(lambda centroid: centroid[0] if centroid else None),
        "_y": centroids.apply(lambda centroid: centroid[1] if centroid else None),
    })
    points_sdf = GeoAccessor.from_xy(points_df, x_column="_x", y_column="_y", sr=sr)
    points_sdf.spatial.sindex("quadtree", reset=False)

    if base_key is not None:
        combined[f"{bin_field}_{base_key}"] = combined[bin_field]

    for year_key, year_df in year_frames.items():
        year_df = year_df[[bin_field, geometry_column]].copy()
        year_df.spatial.set_geometry(geometry_column)
        year_df.spatial.sindex("quadtree", reset=False)

        joined = points_sdf.spatial.join(year_df, how="left", op="within")
        joined = joined.drop_duplicates(subset="_base_idx")
        bin_by_idx = dict(zip(joined["_base_idx"], joined[bin_field]))
        combined[f"{bin_field}_{year_key}"] = combined.index.map(bin_by_idx.get)

    return combined


def _combine_via_arcpy(base_df: pd.DataFrame, year_frames: dict[str, pd.DataFrame], bin_field: str, geometry_column: str, base_key: str) -> pd.DataFrame:
    """Combines Gi_Bin values using arcpy's `SpatialJoin` analysis tool.

    Writes base centroids and each year's polygons to in-memory feature
    classes and runs `arcpy.analysis.SpatialJoin` (match_option="WITHIN"),
    which is spatially indexed and generally the fastest/most robust option
    when arcpy is licensed and available.
    """
    import arcpy
    import uuid

    combined = base_df.copy()

    sr_json = combined[geometry_column].iloc[0]["spatialReference"]
    spatial_reference = arcpy.SpatialReference(sr_json.get("wkid") or sr_json.get("latestWkid"))

    base_points_fc = f"memory\\base_centroids_{uuid.uuid4().hex}"
    out_path, out_name = base_points_fc.rsplit("\\", 1)
    arcpy.management.CreateFeatureclass(out_path, out_name, "POINT", spatial_reference=spatial_reference)
    arcpy.management.AddField(base_points_fc, "_base_idx", "LONG")
    with arcpy.da.InsertCursor(base_points_fc, ["SHAPE@", "_base_idx"]) as cursor:
        for row_idx, geometry in zip(combined.index, combined[geometry_column]):
            cursor.insertRow([geometry.as_arcpy.centroid, row_idx])

    if base_key is not None:
        combined[f"{bin_field}_{base_key}"] = combined[bin_field]

    try:
        for year_key, year_df in year_frames.items():
            year_fc = f"memory\\year_{uuid.uuid4().hex}"
            year_df.spatial.set_geometry(geometry_column)
            year_df.spatial.to_featureclass(location=year_fc, sanitize_columns=False)

            joined_fc = f"memory\\joined_{uuid.uuid4().hex}"
            arcpy.analysis.SpatialJoin(base_points_fc, year_fc, joined_fc, join_operation="JOIN_ONE_TO_ONE", match_option="WITHIN")

            bin_by_idx = {}
            with arcpy.da.SearchCursor(joined_fc, ["_base_idx", bin_field]) as cursor:
                for row_idx, bin_value in cursor:
                    bin_by_idx[row_idx] = bin_value

            combined[f"{bin_field}_{year_key}"] = combined.index.map(bin_by_idx.get)

            arcpy.management.Delete(year_fc)
            arcpy.management.Delete(joined_fc)
    finally:
        arcpy.management.Delete(base_points_fc)

    return combined


def combine_hotspot_bins(base_df: pd.DataFrame, year_frames: dict[str, pd.DataFrame], bin_field: str = "Gi_Bin", geometry_column: str = "SHAPE", base_key: str = None, method: str = "auto") -> pd.DataFrame:
    """Combines Gi_Bin values from multiple years' hotspot feature classes.

    Since yearly hot spot bin geometries are not guaranteed to line up
    exactly, this computes the centroid of every bin in `base_df` and spatially
    matches it against each year's polygons in `year_frames`, adding one
    `Gi_Bin_<year>` column per year to the base DataFrame.

    Three interchangeable implementations are available via `method`:

    - "arcpy": Uses `arcpy.analysis.SpatialJoin` (spatially indexed). Fastest
      and most robust; requires an arcpy license.
    - "sedf": Uses the Spatially Enabled DataFrame's own quadtree spatial
      index and `df.spatial.join(...)`. Index-accelerated and needs only
      `arcgis` (no arcpy, no geopandas) — the preferred non-arcpy option.
    - "python": A dependency-free nested-loop centroid/ray-casting test.
      Correct but O(bins_base x bins_year) per year, so only suitable for
      small feature classes.

    Args:
        base_df (pd.DataFrame): The spatially enabled DataFrame whose bin
            geometries and centroids will be used as the reference grid.
        year_frames (dict[str, pd.DataFrame]): A mapping of year (or feature
            class name) to that year's spatially enabled hotspot DataFrame.
        bin_field (str, optional): The name of the Gi_Bin field. Defaults to
            "Gi_Bin".
        geometry_column (str, optional): The geometry column name shared by
            `base_df` and all `year_frames`. Defaults to "SHAPE".
        base_key (str, optional): If provided, also copies `base_df`'s own
            `bin_field` into a `Gi_Bin_<base_key>` column for consistent
            naming alongside the other years.
        method (str, optional): One of "auto", "arcpy", "sedf", or "python".
            "auto" (the default) prefers arcpy, then the SeDF spatial join,
            then falls back to the pure-Python implementation. Defaults to
            "auto".

    Returns:
        pd.DataFrame: `base_df` augmented with one `Gi_Bin_<year>` column
        per entry in `year_frames`.
    """
    implementations = {
        "arcpy": _combine_via_arcpy,
        "sedf": _combine_via_spatial_dataframe,
        "python": _combine_via_python,
    }

    if method == "auto":
        for candidate in ("arcpy", "sedf", "python"):
            try:
                return implementations[candidate](base_df, year_frames, bin_field, geometry_column, base_key)
            except ImportError:
                continue
        raise RuntimeError("No supported spatial join backend is available (tried arcpy, sedf, python).")

    if method not in implementations:
        raise ValueError(f"Unknown method '{method}'; expected one of: auto, {', '.join(implementations)}.")

    return implementations[method](base_df, year_frames, bin_field, geometry_column, base_key)


def combine_yearly_hotspots(gdb_path: str, base_feature_class: str, bin_field: str = "Gi_Bin", feature_classes: list[str] = None, method: str = "auto") -> pd.DataFrame:
    """Reads every yearly hotspot feature class from a geodatabase and combines them.

    Args:
        gdb_path (str): The filepath to the local file geodatabase containing
            one hotspot feature class per year.
        base_feature_class (str): The name of the feature class whose bin
            geometries are used as the reference grid for spatial matching.
        bin_field (str, optional): The name of the Gi_Bin field. Defaults to
            "Gi_Bin".
        feature_classes (list[str], optional): The feature classes to
            combine. Defaults to every feature class in the geodatabase.
        method (str, optional): One of "auto", "arcpy", "sedf", or
            "python" (see `combine_hotspot_bins`). Defaults to "auto".

    Returns:
        pd.DataFrame: The base feature class's DataFrame augmented with one
        `Gi_Bin_<feature_class>` column per other feature class.
    """
    year_dataframes = read_feature_classes(gdb_path, feature_classes=feature_classes)

    base_df = year_dataframes.pop(base_feature_class)
    return combine_hotspot_bins(base_df, year_dataframes, bin_field=bin_field, base_key=base_feature_class, method=method)


def summarize_hotspot_bins(combined_df: pd.DataFrame, bin_field: str = "Gi_Bin", min_years_observed: int = None) -> pd.DataFrame:
    """Summarizes the per-year Gi_Bin columns produced by `combine_hotspot_bins`.

    A base-year bin's centroid can fail to fall inside any polygon for a
    given year (e.g. that year's study extent didn't cover the location),
    leaving `NaN` in that year's column. Pandas' row-wise reducers
    (`median`, `min`, `max`, ...) accept `skipna=True` (the default), so those
    `NaN`s are simply excluded from the calculation rather than propagating
    to `NaN` or raising an error — a row is only entirely `NaN` if *every*
    year is missing for that location, and `.count()` reports how many years
    actually contributed so you can tell a solid median from one based on a
    single year.

    Args:
        combined_df (pd.DataFrame): A DataFrame produced by
            `combine_hotspot_bins`/`combine_yearly_hotspots`, containing one
            `<bin_field>_<year>` column per year.
        bin_field (str, optional): The Gi_Bin field prefix used to identify
            the per-year columns. Defaults to "Gi_Bin".
        min_years_observed (int, optional): If provided, rows whose
            `<bin_field>_years_observed` is below this threshold are dropped
            from the result. Pass the number of year columns (e.g.
            `len(year_frames) + 1`) to keep only bins matched in every year.
            Defaults to None (keep all rows).

    Returns:
        pd.DataFrame: `combined_df` augmented with `<bin_field>_median`,
        `<bin_field>_years_observed` (count of non-NaN years), and
        `<bin_field>_min`/`<bin_field>_max` columns, optionally filtered by
        `min_years_observed`.
    """
    year_columns = [column for column in combined_df.columns if column.startswith(f"{bin_field}_")]
    summarized = combined_df.copy()

    summarized[f"{bin_field}_median"] = summarized[year_columns].median(axis=1, skipna=True)
    summarized[f"{bin_field}_years_observed"] = summarized[year_columns].count(axis=1)
    summarized[f"{bin_field}_min"] = summarized[year_columns].min(axis=1, skipna=True)
    summarized[f"{bin_field}_max"] = summarized[year_columns].max(axis=1, skipna=True)

    if min_years_observed is not None:
        summarized = summarized[summarized[f"{bin_field}_years_observed"] >= min_years_observed]

    return summarized


def filter_hotspot_bins(summarized_df: pd.DataFrame, bin_field: str = "Gi_Bin", summary_column: str = "median", min_value: float = None, max_value: float = None) -> pd.DataFrame:
    """Filters a summarized hotspot DataFrame by its summary Gi_Bin value.

    Returns an explicit copy (not a view) of the filtered rows, so the
    result can be safely mutated further (e.g. by `.spatial.to_featureset()`
    or `.spatial.to_featurelayer()`) without triggering a pandas
    `SettingWithCopyWarning`.

    Args:
        summarized_df (pd.DataFrame): A DataFrame produced by
            `summarize_hotspot_bins`.
        bin_field (str, optional): The Gi_Bin field prefix used by
            `summarize_hotspot_bins`. Defaults to "Gi_Bin".
        summary_column (str, optional): Which summary column to filter on:
            one of "median", "min", or "max" (matching the columns added by
            `summarize_hotspot_bins`). Defaults to "median".
        min_value (float, optional): The minimum (inclusive) value to keep.
            Defaults to None (no lower bound).
        max_value (float, optional): The maximum (inclusive) value to keep.
            Defaults to None (no upper bound).

    Returns:
        pd.DataFrame: An independent copy of the matching rows.
    """
    column = f"{bin_field}_{summary_column}"
    values = summarized_df[column]

    if min_value is not None and max_value is not None:
        mask = values.between(min_value, max_value)
    elif min_value is not None:
        mask = values >= min_value
    elif max_value is not None:
        mask = values <= max_value
    else:
        mask = pd.Series(True, index=summarized_df.index)

    return summarized_df[mask].copy()


def publish_hotspot_layer(gis, df: pd.DataFrame, title: str, field_name: str = "Gi_Bin_median", tags: list[str] = None, folder: str = None):
    """Publishes a spatially enabled DataFrame as a hosted feature layer with a hotspot renderer.

    Args:
        gis (GIS): An authenticated GIS object to publish to.
        df (pd.DataFrame): The spatially enabled DataFrame to publish (e.g.
            the result of `filter_hotspot_bins`).
        title (str): The title of the hosted feature layer item.
        field_name (str, optional): The field the hotspot renderer (see
            `generate_hotspots_renderer`) should be based on. Defaults to
            "Gi_Bin_median".
        tags (list[str], optional): Tags to apply to the published item.
        folder (str, optional): The portal content folder to publish into.

    Returns:
        Item: The published hosted feature layer item, with its renderer
        set via `generate_hotspots_renderer(field_name)`.
    """
    item = df.spatial.to_featurelayer(title=title, gis=gis, tags=tags, folder=folder)

    layer = item.layers[0]
    layer.manager.update_definition({"drawingInfo": {"renderer": generate_hotspots_renderer(field_name=field_name)}})

    return item

def generate_hotspots_renderer(field_name: str = "GI_Bin"):
        """Create an ArcGIS class-breaks renderer for Gi* hotspot analysis.

        The renderer uses the conventional ``GI_Bin`` values from -3 to 3:
        negative values represent cold spots, positive values represent hot
        spots, and zero represents a statistically insignificant result.

        Args:
                field_name (str): Feature attribute containing the Gi* bin values.
                        Defaults to ``"GI_Bin"``.

        Returns:
                dict: ArcGIS renderer configuration with manually defined class
                breaks, polygon symbols, confidence labels, and an ascending legend.
        """
        stroke = {
                "type": "CIMSolidStroke",
                "enable": True,
                "colorLocked": True,
                "capStyle": "Butt",
                "joinStyle": "Round",
                "lineStyle3D": "Strip",
                "miterLimit": 10,
                "width": 0.4,
                "height3D": 1,
                "anchor3D": "Center",
                "color": [110, 110, 110, 50],
        }
        classes = [
                (-3, "Cold Spot with 99% Confidence", [69, 117, 181, 255]),
                (-2, "Cold Spot with 95% Confidence", [132, 158, 186, 255]),
                (-1, "Cold Spot with 90% Confidence", [192, 204, 190, 255]),
                (0, "Not Significant", [255, 255, 191, 255]),
                (1, "Hot Spot with 90% Confidence", [250, 185, 132, 255]),
                (2, "Hot Spot with 95% Confidence", [237, 117, 81, 255]),
                (3, "Hot Spot with 99% Confidence", [214, 47, 39, 255]),
        ]

        class_break_infos = []
        for class_max_value, label, fill_color in classes:
                symbol = {
                        "type": "CIMSymbolReference",
                        "symbol": {
                                "type": "CIMPolygonSymbol",
                                "symbolLayers": [
                                        stroke,
                                        {"type": "CIMSolidFill", "enable": True, "color": fill_color},
                                ],
                                "angleAlignment": "Map",
                        },
                }
                class_break_infos.append(
                        {
                                "symbol": symbol,
                                "classMaxValue": class_max_value,
                                "label": label,
                        }
                )

        return {
                "type": "classBreaks",
                "authoringInfo": {
                        "type": "classedColor",
                        "classificationMethod": "esriClassifyManual",
                },
                "field": field_name,
                "classificationMethod": "esriClassifyManual",
                "minValue": -3,
                "classBreakInfos": class_break_infos,
                "legendOptions": {"order": "ascendingValues"},
        }
