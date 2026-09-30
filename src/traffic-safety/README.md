# Frankfurt Traffic Safety Showcase
A reusable end-to-end demonstration for the European Developer & Technology Summit 2026.

The showcase demonstrates:

1. Spatial Data Science
   - Traffic accident analysis
   - Hot Spot Analysis
   - Cold Spot Analysis
   - Emerging patterns

2. Agentic AI & Spatial Grounding
   - Spatially grounded recommendations
   - Tool orchestration
   - Explainable outputs

3. Trusted GeoAI
   - Human-in-the-loop review
   - Governance controls
   - Responsible recommendation workflows

## Central message:
Understand → Assess → Recommend → Trust

- AI recommends.
- Humans decide.

## Risk module

`traffic_safety/` (uv-managed, mirrors `data-engineering/data_engineering`) reads
yearly Hot Spot Analysis (Getis-Ord Gi*) feature classes from a local file
geodatabase and combines their `Gi_Bin` values into a single spatially enabled
DataFrame. Because bin polygons can differ slightly between years, bins are
aligned spatially rather than joined by key: the centroid of each bin in a
chosen base-year feature class is tested for containment against every other
year's polygons, producing one `Gi_Bin_<feature_class>` column per year. See
`traffic_safety/src/traffic_safety/utils.py`.

Running the tests requires a `HOTSPOT_GDB` environment variable pointing at a
local `.gdb` containing one hotspot feature class per year:

```
uv run python -m unittest tests.test_traffic_safety
```