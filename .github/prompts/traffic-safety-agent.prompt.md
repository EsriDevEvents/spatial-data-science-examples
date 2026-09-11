## Frankfurt Traffic Safety Showcase
The `src/traffic-safety/` project is the primary focus for the European Developer & Technology Summit 2026.

The showcase demonstrates a complete workflow connecting:

Spatial Data Science
→ Agentic AI
→ Trusted GeoAI

Central message:

Understand → Assess → Recommend → Trust

AI recommends.
Humans decide.

The objective is not maximum analytical sophistication.

The objective is to create a reproducible end-to-end workflow that can be demonstrated live, reused in workshops, and extended by developers.

---

## Business Scenario

A city traffic planner needs to identify locations in Frankfurt that should be prioritized for traffic safety improvements.

The showcase should help answer:

- Which locations have the highest traffic safety risk?
- Which traffic corridors should be prioritized?
- Why were these locations selected?
- Which spatial factors contribute most to risk?
- What decisions still require human approval?

---

## Existing Analytical Assets

The following data products are considered available and should be reused whenever possible:

- Frankfurt traffic accidents (2019-2024)
- Hot Spot Analysis results
- Cold Spot Analysis results
- Traffic simulation outputs
- Hosted feature layers
- UrbanDigitalTwin_Frankfurt notebook outputs

Avoid recreating existing analyses unless required.

Build on existing results first.

---

## Risk Index Requirements

The EDTS 2026 showcase introduces a traffic safety risk index.

Inputs:

- Hot Spot Analysis
- Traffic Simulation
- Historical Accidents

Output:

- Risk Score (0-100)

Risk Categories:

- Very Low
- Low
- Medium
- High
- Critical

The implementation should prioritize:

- transparency
- explainability
- reproducibility

Prefer simple weighted scoring approaches over complex machine learning models.

The goal is decision support, not research-grade predictive accuracy.

---

## Explainability Requirements

Every generated recommendation must include:

- Data sources
- Analysis steps
- Main risk factors
- Assumptions
- Limitations

Recommendations should be understandable by technical and non-technical audiences.

When possible explain:

- why a location received its score
- which indicators contributed most
- what uncertainty remains

---

## Agentic AI Requirements

Agent workflows should be spatially grounded.

The agent should be able to:

- discover maps
- identify layers
- retrieve risk information
- summarize analysis
- generate recommendations

Primary demonstration question:

Which locations in Frankfurt should be prioritized for traffic safety improvements?

Preferred workflow:

1. Find relevant layers
2. Retrieve analytical results
3. Prioritize locations
4. Explain findings
5. Generate recommendations

Avoid autonomous decision-making logic.

---

## Trusted GeoAI Constraints

The system may:

- analyze
- prioritize
- recommend

The system must never:

- make policy decisions
- allocate budgets
- approve interventions
- execute actions automatically

Human approval is always required.

---

## Human-in-the-Loop Requirements

All recommendations should support review workflows.

Typical states:

- Recommended
- Pending Review
- Approved
- Rejected

Demonstrate clear separation between:

Analysis
→ Recommendation
→ Decision

Humans retain accountability for final outcomes.

---

## Summit Architecture Preference

Preferred architecture:

ArcGIS Location Platform
→ Hosted Feature Layers
→ Risk Layer
→ Agent
→ Human Review

ArcGIS Online
→ ArcGIS Notebooks
→ Dashboard / StoryMap

Preferred technologies:

- ArcGIS API for Python
- arcpy
- ArcGIS Notebooks
- ArcGIS Location Platform
- ArcGIS Online
- GitHub Actions
- Python 3.11+

Favor notebook-based examples over one-off scripts.

---

## Documentation Expectations

New functionality should include:

- Purpose
- Inputs
- Outputs
- Assumptions
- Limitations

Documentation should support:

- summit demos
- workshops
- developer onboarding
- community reuse

---

## Developer Experience Goals

This repository is also a developer enablement asset.

Contributions should help developers:

1. Understand the workflow
2. Reproduce the analysis
3. Extend the risk model
4. Integrate AI agents
5. Implement governance patterns
6. Build production-ready solutions

Favor clarity and learning value over implementation complexity.
