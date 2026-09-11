# Copilot Instructions for spatial-data-science-examples

This repo contains standalone example projects demonstrating ArcGIS spatial data
science capabilities for Esri developer events. Each subfolder under `src/` is
an independent showcase — do not assume shared build/test tooling across them.

## Repository layout

- `src/data-engineering/data_engineering/` — Python project (uv-managed) with
  two packages under `src/`:
  - `data_engineering` — fetches data from ArcGIS Online / Living Atlas
    (charging stations, traffic accidents, hot/cold spot layers) via `arcgis.gis.GIS`.
  - `urban_traffic` — works with local data (SQLite traffic DB, local feature
    classes/geodatabases, bike trail GeoJSON), builds maps/renderers, and runs
    scikit-learn classification pipelines on traffic accident data.
  - `main.py` at the package root ties both together for ad-hoc exploration
    (`explore_data()`, `explore_traffic()`).
  - `notebooks/UrbanDigitalTwin_Frankfurt.ipynb` — narrative demo notebook.
- `src/traffic-safety/` — Frankfurt Traffic Safety showcase for EDTS 2026
  (currently only a README describing planned hot/cold spot analysis and
  agentic AI recommendation workflow; implementation not yet present).

## Environment setup

Samples require a `.env` file at the repo root (not committed) defining:

```
ARCGIS_API_KEY=<secret-api-key>
PYTHONPATH=./src/data-engineering/data_engineering/src
TRAFFIC_DATA_FILE="/data/Frankfurt-Main-traffic.sqlite"
TRAFFIC_FEATURES="/temp/Spatial_Data_Science.gdb/Weekday_Traffic_2025"
```

The test suite additionally expects `BIKE_TRAIL_FILE` (path to a bike trail
GeoJSON). `PYTHONPATH` must point at the package `src/` dir so that both the
`data_engineering` and `urban_traffic` top-level packages resolve — code in one
package imports directly from the other (e.g. `main.py` and `urban_traffic.utils`
import from `data_engineering.utils`).

## Build / dependency management

The `data_engineering` project uses **uv** (`uv.lock` present) with a
`pyproject.toml` declaring `arcgis>=2.4` and `arcgis-mapping>=4.31` as
dependencies (Python `>=3.11,<3.14`). From
`src/data-engineering/data_engineering/`:

```
uv sync            # install dependencies into a local venv
uv run python src/main.py
```

## Testing

Tests live in `tests/test_data_engineering.py` and use `unittest` (VS Code is
configured for `unittestEnabled`, not pytest — see `.vscode/settings.json`).
**These are integration tests, not offline unit tests**: they call the live
ArcGIS API and read real local data files, and will raise `ValueError` at
`setUp` if the required env vars (`ARCGIS_API_KEY`, `TRAFFIC_DATA_FILE`,
`TRAFFIC_FEATURES`, `BIKE_TRAIL_FILE`) aren't set. Run the full suite or a
single test with:

```
python -m unittest tests.test_data_engineering
python -m unittest tests.test_data_engineering.TestDataEngineering.test_living_atlas
```

There is no CI workflow in `.github/workflows` — tests must be run manually
with valid credentials/data.

## Conventions

- Functions that return tabular results return **spatially-enabled pandas
  DataFrames** (`pd.DataFrame` with `.spatial` accessor via
  `arcgis.features.GeoAccessor`), not plain `FeatureSet`s, e.g.
  `fetch_charging_stations`, `fetch_traffic_data`, `read_bike_trail`.
- Public functions in `utils.py` modules use Google-style docstrings
  (`Args:` / `Returns:`) — follow this style for new functions.
- Layer/portal item IDs are looked up via small `get_*_layer` /
  `get_*_item` helper functions (e.g. `get_charging_stations_layer`,
  `get_hotcold_layer`, `get_live_traffic_item`) rather than hard-coding item
  IDs inline; reuse these helpers instead of duplicating item ID lookups.
- Optional spatial filtering is done consistently via
  `arcgis.geometry.filters.intersects(extent, sr=extent.spatial_reference)`
  passed as `geometry_filter` on `FeatureLayer.query(...)`.
- `read_traffic_features` prefers `arcpy.da.SearchCursor` when `arcpy` is
  available (chunked reads in batches of 1000) and falls back to
  `GeoAccessor.from_featureclass` with a geometry filter when arcpy is not
  installed — preserve this fallback pattern if modifying that function.
- Renderers for maps are built with `arcgis.map.renderers.SimpleRenderer` plus
  `arcgis.map.symbols` classes (see `generate_car_renderer`,
  `generate_routes_renderer`) rather than raw JSON renderer dicts.
