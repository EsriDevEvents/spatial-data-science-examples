"""Spatially grounded recommendation generation for the Frankfurt Traffic Safety showcase.

This module implements the "Agentic AI and Spatial Grounding" session: it
takes the persistent risk layer published by `traffic_safety.utils`
(`publish_hotspot_layer`) and turns the highest-confidence locations into
ranked, explainable recommendations.

The generative step is deliberately decoupled from any specific model
provider behind the `LLMClient` protocol, so the exact same evidence
package and prompt can be sent to a hosted model, a local/sovereign model
(see `traffic_safety.governance.OllamaLLMClient`), or - for fully offline
demos and tests - the deterministic `TemplateLLMClient` defined here.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Protocol

import pandas as pd
from arcgis.features import FeatureLayer
from arcgis.gis import GIS


class LLMClient(Protocol):
    """Protocol implemented by anything that can turn a prompt into text.

    Any object with a matching `generate(prompt: str) -> str` method
    satisfies this protocol, whether it calls a hosted API, a local Ollama
    model (`traffic_safety.governance.OllamaLLMClient`), or no model at all
    (`TemplateLLMClient`).
    """

    def generate(self, prompt: str) -> str:
        ...


@dataclass
class TemplateLLMClient:
    """A dependency-free `LLMClient` that renders evidence as plain text.

    Used as the default recommendation generator so the showcase runs
    end-to-end without any external AI service configured, and so unit
    tests are deterministic. Swap in a hosted or local model client to
    generate richer, free-form recommendations from the same evidence.
    """

    def generate(self, prompt: str) -> str:
        return prompt


def get_risk_layer(gis: GIS, item_id: str) -> FeatureLayer:
    """Retrieves the published traffic safety risk layer by item id.

    Args:
        gis (GIS): An authenticated GIS object.
        item_id (str): The item id of the hosted feature layer published by
            `traffic_safety.utils.publish_hotspot_layer`.

    Returns:
        FeatureLayer: The first layer of the risk layer item.
    """
    item = gis.content.get(item_id)
    return item.layers[0]


def get_top_risk_locations(layer: FeatureLayer, field_name: str = "Gi_Bin_median", top_n: int = 5, where: str = "1=1") -> pd.DataFrame:
    """Queries the risk layer for its highest-confidence locations.

    Args:
        layer (FeatureLayer): The published traffic safety risk layer (see
            `get_risk_layer`).
        field_name (str, optional): The confidence/risk field to rank by.
            Defaults to "Gi_Bin_median".
        top_n (int, optional): The number of top locations to return.
            Defaults to 5.
        where (str, optional): An additional SQL where clause to restrict
            the candidate locations before ranking. Defaults to "1=1".

    Returns:
        pd.DataFrame: A spatially enabled DataFrame with the `top_n` rows
        sorted by `field_name` in descending order.
    """
    feature_set = layer.query(where=where, out_fields="*", order_by_fields=f"{field_name} DESC", result_record_count=top_n)
    return feature_set.sdf


def get_spatial_context(context_layer: FeatureLayer, geometry: dict, out_fields: str = "*") -> pd.DataFrame:
    """Retrieves features near a risk location to use as supporting context.

    Follows the repository convention of filtering with
    `arcgis.geometry.filters.intersects` rather than hand-built queries, so
    any additional context layer (points of interest, road network
    attributes, land use, ...) can be plugged in without changing this
    function.

    Args:
        context_layer (FeatureLayer): A feature layer providing geographic
            context (e.g. intersections, schools, prior interventions).
        geometry (dict): An Esri JSON geometry (typically a buffer polygon
            around a risk location) to intersect against.
        out_fields (str, optional): The fields to return. Defaults to "*".

    Returns:
        pd.DataFrame: The context features intersecting `geometry`.
    """
    from arcgis.geometry.filters import intersects

    spatial_reference = geometry.get("spatialReference", {"wkid": 4326})
    geometry_filter = intersects(geometry, sr=spatial_reference)
    feature_set = context_layer.query(geometry_filter=geometry_filter, out_fields=out_fields)
    return feature_set.sdf


def get_buffer_context(gis: GIS, context_sdf: pd.DataFrame, geometry: dict, distance_meters: float = 200) -> tuple[pd.DataFrame, dict]:
    """Finds context features within a fixed-radius buffer of a risk location.

    This is the simpler of the two spatial grounding strategies shown side
    by side in the demonstration: it answers "what is within N meters of
    this hotspot?" using a geodesic buffer computed by the ArcGIS geometry
    service (`arcgis.geometry.functions.buffer(..., geodesic=True)`), which
    correctly measures the distance in real-world meters regardless of the
    input geometry's spatial reference (notably WGS84, whose coordinates
    are in degrees and cannot be buffered by a naive planar distance).

    Args:
        gis (GIS): An authenticated GIS used to call the geometry service.
        context_sdf (pd.DataFrame): A spatially enabled DataFrame of context
            features (e.g. schools), such as `FeatureSet.sdf`. Does not need
            to be published - the buffer and containment test both run
            locally/via the geometry service.
        geometry (dict): An Esri JSON geometry (typically a risk location's
            polygon) to buffer and search around.
        distance_meters (float, optional): The buffer radius in meters.
            Defaults to 200 (roughly a walking-distance proximity check).

    Returns:
        tuple[pd.DataFrame, dict]: The context features within the buffer,
        and a detail dict (`{"buffer_meters": ..., "buffer_geometry": ...}`).
        `buffer_geometry` is the Esri JSON of the buffered search area itself,
        so it can be handed over as one entry of `context_geometries` to
        `rank_recommendations`, which will re-query the (live, published)
        context layer with it - keeping a single source of truth for how
        context is retrieved for a recommendation.
    """
    from arcgis.geometry import Geometry
    from arcgis.geometry.functions import buffer

    spatial_reference = geometry.get("spatialReference", {"wkid": 4326})
    buffered = buffer(
        geometries=[geometry],
        in_sr=spatial_reference,
        distances=distance_meters,
        unit=9001,  # esriSRUnit_Meter
        geodesic=True,
        gis=gis,
    )[0]
    buffered_geometry = Geometry(buffered)

    mask = context_sdf["SHAPE"].apply(lambda shape: buffered_geometry.contains(Geometry(shape)))
    matches = context_sdf[mask].copy()
    detail = {"buffer_meters": distance_meters, "buffer_geometry": dict(buffered_geometry)}
    return matches, detail


def get_proximity_context(gis: GIS, risk_locations: pd.DataFrame, context_fset, measurement_type: str = "Walking Time", max_count: int = 3, search_cutoff: float = 15, search_cutoff_units: str = "Minutes") -> tuple[pd.DataFrame, dict]:
    """Finds context features reachable from risk locations via real routing.

    This is the richer of the two spatial grounding strategies shown side
    by side in the demonstration: instead of a straight-line buffer, it
    measures actual travel time or distance along the street network using
    `arcgis.features.use_proximity.find_nearest`, which calls an ArcGIS
    Location Platform routing service and consumes credits.

    Args:
        gis (GIS): An authenticated GIS with the Routing / Network Analysis
            privilege enabled (e.g. an ArcGIS Location Platform API key).
        risk_locations (pd.DataFrame): The risk locations to route from, as
            returned by `get_top_risk_locations` (used as the `near_layer`'s
            origins, i.e. `analysis_layer`).
        context_fset: The context features to route to (e.g. the schools
            `FeatureSet` from a local GeoJSON file), used as `near_layer`.
            Accepted directly without publishing - both a `FeatureSet` and a
            spatially enabled DataFrame are valid "Feature Input" for this
            tool.
        measurement_type (str, optional): The exact ArcGIS travel mode
            literal, e.g. "Walking Time", "Walking Distance", "Driving Time".
            Defaults to "Walking Time".
        max_count (int, optional): Maximum number of nearest features to
            find per risk location. Defaults to 3.
        search_cutoff (float, optional): Only consider features within this
            range. Defaults to 15.
        search_cutoff_units (str, optional): Units for `search_cutoff`, e.g.
            "Minutes" or "Meters". Defaults to "Minutes".

    Returns:
        tuple[pd.DataFrame, dict]: A DataFrame of nearest-feature results
        (one row per match, with `IN_FID`/`NEAR_FID`/`NEAR_DIST`/`NEAR_RANK`
        columns), and a detail dict summarizing the overall query
        (`measurement_type`, `search_cutoff`, `search_cutoff_units`).
    """
    from arcgis.features.use_proximity import find_nearest

    nearest_layer, _connecting_lines_layer = find_nearest(
        analysis_layer=risk_locations,
        near_layer=context_fset,
        measurement_type=measurement_type,
        max_count=max_count,
        search_cutoff=search_cutoff,
        search_cutoff_units=search_cutoff_units,
        gis=gis,
    )
    nearest_df = nearest_layer.sdf
    detail = {
        "measurement_type": measurement_type,
        "search_cutoff": search_cutoff,
        "search_cutoff_units": search_cutoff_units,
    }
    return nearest_df, detail


def _sdf_to_geojson(sdf: pd.DataFrame) -> dict:
    """Converts a spatially enabled DataFrame into a plain GeoJSON FeatureCollection dict."""
    features = []
    for _, row in sdf.iterrows():
        properties = {column: value for column, value in row.items() if column != "SHAPE"}
        features.append({
            "type": "Feature",
            "geometry": row["SHAPE"].__geo_interface__,
            "properties": properties,
        })
    return {"type": "FeatureCollection", "features": features}


def persist_context_layer(context_sdf: pd.DataFrame, name: str, persist_dir: str = "/temp") -> str:
    """Writes a spatially enabled DataFrame to disk as a single GeoJSON file.

    Both `get_buffer_context` and `get_proximity_context` return a
    spatially enabled DataFrame, so both can be persisted the same way -
    useful for comparing the two strategies side by side with the audience,
    or for re-opening the exact context used for a given recommendation.
    GeoJSON is used rather than a shapefile or geodatabase feature class so
    the file is readable with just the standard library plus `arcgis`'s
    `FeatureSet.from_geojson` (see `load_context_layer`) - no arcpy, no
    10-character/uppercase field-name limitations, and no scratch
    geodatabase to manage.

    `name` is a plain filename stem, not a per-row key: pass one combined
    `context_sdf` (e.g. every matched school across all locations, with a
    `loc_id` column added so each row still traces back to its location) to
    get a single file for the whole run, or call this once per location
    (as `build_evidence_package` does) if each recommendation's evidence
    should be independently inspectable.

    Args:
        context_sdf (pd.DataFrame): A spatially enabled DataFrame of context
            features to persist.
        name (str): The output filename stem, e.g. "context_buffer" or
            "location_42_buffer".
        persist_dir (str, optional): Output directory. Defaults to "/temp".

    Returns:
        str: The path the context layer was written to, or None if
        `context_sdf` has no geometry column to persist.
    """
    if context_sdf is None or "SHAPE" not in context_sdf.columns:
        return None

    os.makedirs(persist_dir, exist_ok=True)
    out_path = os.path.join(persist_dir, f"{name}.geojson")
    with open(out_path, "w", encoding="utf-8") as geojson_file:
        json.dump(_sdf_to_geojson(context_sdf), geojson_file, default=str)
    return out_path


def load_context_layer(path: str) -> pd.DataFrame:
    """Loads the complete context layer previously written by `persist_context_layer`.

    Args:
        path (str): The `.geojson` path returned by `persist_context_layer`.

    Returns:
        pd.DataFrame: A spatially enabled DataFrame rebuilt from the file,
        with every row/feature that was written to it (e.g. every matched
        school across every location, if a combined `name` was used).
    """
    from arcgis.features import FeatureSet

    with open(path, "r", encoding="utf-8") as geojson_file:
        geojson = json.load(geojson_file)
    return FeatureSet.from_geojson(geojson).sdf


def context_geometries_to_sdf(context_geometries: dict) -> pd.DataFrame:
    """Converts a `context_geometries` mapping into a spatially enabled DataFrame.

    `context_geometries` (e.g. `{location_id: detail["buffer_geometry"]}`
    from `get_buffer_context`, as handed over to `rank_recommendations`) is a
    plain, in-memory dict, not something `persist_context_layer` can write
    directly. This flattens it into one row per location so the exact
    search areas used for a recommendation run can be persisted and later
    restored with `context_geometries_from_sdf`.

    Note the geometry column uses `location_id` as a plain attribute
    (`loc_id`) rather than the DataFrame index, since persisting to GeoJSON
    does not carry a pandas index through to the output file.

    Args:
        context_geometries (dict): Mapping of location id -> Esri JSON
            geometry.

    Returns:
        pd.DataFrame: A spatially enabled DataFrame with a `loc_id` column
        and a `SHAPE` geometry column, one row per location.
    """
    sdf = pd.DataFrame(
        [{"loc_id": location_id, "SHAPE": geometry} for location_id, geometry in context_geometries.items()]
    )
    sdf.spatial.set_geometry("SHAPE")
    return sdf


def context_geometries_from_sdf(sdf: pd.DataFrame) -> dict:
    """Rebuilds a `context_geometries` mapping from a spatially enabled DataFrame.

    The inverse of `context_geometries_to_sdf`: reads a `loc_id` + `SHAPE`
    DataFrame (e.g. loaded via `load_context_layer` from a file written by
    `persist_context_geometries`) back into the
    `{location_id: Esri JSON geometry}` shape expected by
    `rank_recommendations`'s `context_geometries` argument.

    Args:
        sdf (pd.DataFrame): A spatially enabled DataFrame with a `loc_id`
            column and a `SHAPE` geometry column.

    Returns:
        dict: Mapping of location id -> Esri JSON geometry.
    """
    return {row["loc_id"]: dict(row["SHAPE"]) for _, row in sdf.iterrows()}


def persist_context_geometries(context_geometries: dict, name: str = "context_geometries", persist_dir: str = "/temp") -> str:
    """Persists a `context_geometries` mapping (one search area per location) to disk.

    Converts the mapping to a spatially enabled DataFrame via
    `context_geometries_to_sdf` and writes it with `persist_context_layer`
    as a single combined file (one row per location), so a recommendation
    run's exact search areas can be reopened later
    (`load_context_geometries`) without recomputing them - useful for a
    demo where the audience sees the buffer or routing step run once and
    then reuses its output.

    Args:
        context_geometries (dict): Mapping of location id -> Esri JSON
            geometry, as produced by `get_buffer_context`'s
            `detail["buffer_geometry"]`.
        name (str, optional): The output filename stem. Defaults to
            "context_geometries".
        persist_dir (str, optional): Output directory. Defaults to "/temp".

    Returns:
        str: The path the context geometries were written to.
    """
    sdf = context_geometries_to_sdf(context_geometries)
    return persist_context_layer(sdf, name, persist_dir)


def load_context_geometries(path: str) -> dict:
    """Loads a `context_geometries` mapping previously written to disk.

    Args:
        path (str): The `.geojson` path returned by
            `persist_context_geometries`.

    Returns:
        dict: Mapping of location id -> Esri JSON geometry, ready to pass
        directly as `rank_recommendations`'s `context_geometries` argument.
    """
    return context_geometries_from_sdf(load_context_layer(path))


def build_evidence_package(location: pd.Series, context: pd.DataFrame = None, bin_field: str = "Gi_Bin", context_method: str = None, context_detail: dict = None, persist_dir: str = None) -> dict:
    """Assembles a structured, explainable evidence package for one location.

    The evidence package is the contract between spatial analysis and the
    generative step: every field surfaced in a recommendation must trace
    back to a value in this dictionary, so a human reviewer (see
    `traffic_safety.governance`) can validate the recommendation against
    its source data.

    Args:
        location (pd.Series): A row from `get_top_risk_locations`, i.e. a
            summarized risk location produced by
            `traffic_safety.utils.summarize_hotspot_bins`.
        context (pd.DataFrame, optional): Nearby context features, from
            either `get_buffer_context` or `get_proximity_context` (or the
            lower-level `get_spatial_context`). Defaults to None (no
            additional context).
        bin_field (str, optional): The Gi_Bin field prefix. Defaults to
            "Gi_Bin".
        context_method (str, optional): How `context` was derived - "buffer"
            or "proximity" - surfaced in the evidence package so the two
            strategies can be shown and compared side by side for the same
            location, rather than silently mixed. Defaults to None.
        context_detail (dict, optional): Method-specific supporting figures,
            e.g. `{"buffer_meters": 200}` from `get_buffer_context` or
            `{"measurement_type": "Walking Time", ...}` from
            `get_proximity_context`. Defaults to None (empty dict).
        persist_dir (str, optional): If given and `context` is a spatially
            enabled DataFrame, writes it to this directory via
            `persist_context_layer` and records the resulting path on the
            evidence package as `context_layer_path`. Defaults to None (no
            persistence).

    Returns:
        dict: An evidence package with the risk score, confidence metrics,
        and a summary of the surrounding spatial context.
    """
    evidence = {
        "location_id": location.get("OBJECTID"),
        "risk_score": location.get(f"{bin_field}_median"),
        "years_observed": location.get(f"{bin_field}_years_observed"),
        "risk_range": [location.get(f"{bin_field}_min"), location.get(f"{bin_field}_max")],
        "context_feature_count": 0 if context is None else len(context),
        "context_method": context_method,
        "context_detail": context_detail or {},
    }

    if persist_dir is not None:
        name = f"location_{evidence['location_id']}_{context_method or 'context'}"
        evidence["context_layer_path"] = persist_context_layer(context, name, persist_dir)

    return evidence


def generate_recommendation(evidence: dict, llm_client: LLMClient = None) -> dict:
    """Generates an explainable recommendation from an evidence package.

    Args:
        evidence (dict): An evidence package from `build_evidence_package`.
        llm_client (LLMClient, optional): The client used to turn evidence
            into free-form text. Defaults to `TemplateLLMClient()` so the
            showcase runs without any external AI service configured.

    Returns:
        dict: The original evidence package augmented with a
        `recommendation` text field.
    """
    llm_client = llm_client or TemplateLLMClient()

    prompt = (
        f"Location {evidence['location_id']} shows a persistent risk score of "
        f"{evidence['risk_score']:.2f} (range {evidence['risk_range']}), observed across "
        f"{evidence['years_observed']} year(s), with {evidence['context_feature_count']} "
        "relevant context feature(s) nearby. Recommend prioritizing this location for "
        "traffic safety review."
    )

    return {**evidence, "recommendation": llm_client.generate(prompt)}


def rank_recommendations(risk_layer: FeatureLayer, context_layer: FeatureLayer = None, context_geometries: dict = None, field_name: str = "Gi_Bin_median", top_n: int = 5, llm_client: LLMClient = None) -> list[dict]:
    """Produces ranked, explainable recommendations for the top risk locations.

    This is the end-to-end entry point demonstrated in
    `notebooks/SpatialGrounding.ipynb`: query the published risk layer,
    gather spatial context for each location, and generate a recommendation
    with full supporting evidence for every result.

    Args:
        risk_layer (FeatureLayer): The published traffic safety risk layer.
        context_layer (FeatureLayer, optional): An additional feature layer
            used to retrieve spatial context per location. Defaults to None.
        context_geometries (dict, optional): A mapping of `OBJECTID` to the
            Esri JSON geometry (e.g. a buffer) to use for that location's
            context query. Required if `context_layer` is provided.
        field_name (str, optional): The confidence/risk field to rank by.
            Defaults to "Gi_Bin_median".
        top_n (int, optional): The number of locations to generate
            recommendations for. Defaults to 5.
        llm_client (LLMClient, optional): The client used to turn evidence
            into free-form text. Defaults to `TemplateLLMClient()`.

    Returns:
        list[dict]: One recommendation dict (see `generate_recommendation`)
        per top risk location, ordered by descending risk.
    """
    top_locations = get_top_risk_locations(risk_layer, field_name=field_name, top_n=top_n)

    recommendations = []
    for _, location in top_locations.iterrows():
        context = None
        if context_layer is not None and context_geometries is not None:
            geometry = context_geometries.get(location.get("OBJECTID"))
            if geometry is not None:
                context = get_spatial_context(context_layer, geometry)

        evidence = build_evidence_package(location, context=context)
        recommendations.append(generate_recommendation(evidence, llm_client=llm_client))

    return recommendations
