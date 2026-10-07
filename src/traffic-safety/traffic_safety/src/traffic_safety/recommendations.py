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


def build_evidence_package(location: pd.Series, context: pd.DataFrame = None, bin_field: str = "Gi_Bin") -> dict:
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
        context (pd.DataFrame, optional): Nearby context features from
            `get_spatial_context`. Defaults to None (no additional context).
        bin_field (str, optional): The Gi_Bin field prefix. Defaults to
            "Gi_Bin".

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
    }
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
