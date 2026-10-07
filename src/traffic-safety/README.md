# Frankfurt Traffic Safety Showcase
A reusable end-to-end demonstration for the European Developer & Technology Summit 2026.

The showcase demonstrates:

1. Spatial Data Science — ✅ implemented (`traffic_safety/utils.py`)
   - Traffic accident analysis
   - Hot Spot Analysis
   - Cold Spot Analysis
   - Emerging patterns

2. Agentic AI & Spatial Grounding — ✅ implemented (`traffic_safety/recommendations.py`)
   - Spatially grounded recommendations
   - Tool orchestration
   - Explainable outputs

3. Trusted GeoAI — ✅ implemented (`traffic_safety/governance.py`)
   - Human-in-the-loop review
   - Governance controls
   - Responsible recommendation workflows

## Central message:
Understand → Assess → Recommend → Trust

- AI recommends.
- Humans decide.

## Risk module

`traffic_safety/` (uv-managed, mirrors `data-engineering/data_engineering`)
implements the multi-year hotspot risk workflow in
`traffic_safety/src/traffic_safety/utils.py`, exercised end-to-end in
`traffic_safety/notebooks/TrafficSafety.ipynb`:

1. **Discover & read** every yearly Hot Spot Analysis (Getis-Ord Gi*) feature
   class in a local file geodatabase
   (`list_feature_classes`, `read_feature_classes`).
2. **Combine years spatially** — since bin polygons can differ slightly
   between years, bins are aligned by geometry rather than joined by key:
   the centroid of each bin in a chosen base-year feature class is matched
   against every other year's polygons, producing one
   `Gi_Bin_<feature_class>` column per year in a single spatially enabled
   DataFrame (`combine_yearly_hotspots`/`combine_hotspot_bins`). Three
   interchangeable spatial-join backends are available via `method`:
   `"arcpy"` (`arcpy.analysis.SpatialJoin`, fastest, requires a license),
   `"sedf"` (the Spatially Enabled DataFrame's own quadtree index and
   `df.spatial.join`, no arcpy required), and a dependency-free pure-Python
   fallback.
3. **Summarize across years** — `summarize_hotspot_bins` reduces the
   per-year columns to a `Gi_Bin_median` (NaN-safe, so years without a
   spatial match are simply excluded rather than breaking the result),
   `Gi_Bin_min`/`Gi_Bin_max`, and a `Gi_Bin_years_observed` count so you can
   tell a trend backed by every year from one based on a single year
   (optionally filtered with `min_years_observed`).
4. **Filter to the areas that matter** — `filter_hotspot_bins` selects the
   highest-confidence locations (e.g. `Gi_Bin_median` between 2 and 3 for
   95-99% confidence hot spots) and returns a safe, independent copy.
5. **Publish** — `publish_hotspot_layer` publishes the filtered risk areas as
   a hosted feature layer and applies a shared hot/cold spot renderer
   (`generate_hotspots_renderer`), so the published layer renders
   consistently with the confidence-based symbology used throughout the
   analysis.

Together these turn several years of disconnected per-year hotspot feature
classes into one persistent, confidence-ranked risk layer ready for
downstream use (e.g. the planned agentic AI recommendation workflow above).

Running the tests requires a `HOTSPOT_GDB` environment variable pointing at a
local `.gdb` containing one hotspot feature class per year:

```
uv run python -m unittest tests.test_traffic_safety
```

## Recommendation module (Spatial Grounding session)

`traffic_safety/src/traffic_safety/recommendations.py` turns the published
risk layer into ranked, explainable recommendations, exercised in
`notebooks/SpatialGrounding.ipynb`:

- `get_risk_layer`/`get_top_risk_locations` query the hosted risk layer
  published in the previous session for its highest-confidence locations.
- `get_spatial_context` retrieves supporting context features (e.g.
  intersections, prior interventions) using the repository's standard
  `arcgis.geometry.filters.intersects` geometry filtering.
- `build_evidence_package` assembles a structured, fully traceable evidence
  dict (risk score, confidence range, years observed, context count) — every
  field a reviewer sees must come from here.
- `generate_recommendation`/`rank_recommendations` turn an evidence package
  into free-form, explainable text via a pluggable `LLMClient` protocol.
  The default `TemplateLLMClient` requires no external AI service, so the
  demo and its tests (`tests/test_recommendations.py`) run fully offline;
  swap in a hosted model or `governance.OllamaLLMClient` for richer output.

## Governance module (Trusted GeoAI session)

`traffic_safety/src/traffic_safety/governance.py` wraps a recommendation
with human-in-the-loop review and a durable audit trail, exercised in
`notebooks/TrustedGeoAI.ipynb`:

- `ReviewDecision`/`AuditRecord` capture a reviewer's approve / reject /
  needs-review decision alongside the full recommendation and evidence it
  was based on.
- `append_audit_record`/`load_audit_trail` persist decisions to an
  append-only JSON Lines file, so the audit trail survives interruptions and
  is simple to inspect or ship elsewhere.
- `request_human_review` is the single entry point notebooks and scripts
  call once a reviewer has made a decision.
- `OllamaLLMClient` implements the same `LLMClient` interface as
  `recommendations.TemplateLLMClient`, so swapping to local, sovereign
  inference (via a local `ollama serve`) requires no other code changes.
  Install it with the optional dependency group:
  `uv sync --extra local-inference`.

Tests for both modules (`tests/test_recommendations.py`,
`tests/test_governance.py`) are pure-logic/offline and require neither a
live GIS connection nor a running Ollama server:

```
uv run python -m unittest tests.test_recommendations tests.test_governance
```