Pendo Pipeline Code Review Notes

Purpose of this document:
Capture the Why, What, and How behind the Pendo pipeline code so the implementation can be explained, defended, maintained, and documented for engineering and data stakeholders.

Review lens:
For each file, answer:
- What responsibility does this file own?
- Why does it exist in this layer?
- How does it interact with the rest of the pipeline?
- What assumptions does it make?
- What risks or edge cases should be known?
- What comments/docstrings should live in the code?
- What belongs in external documentation?

Review order:
1. Entry point / orchestration
   - main.py
   - run_export.py, if still relevant

2. Configuration / constants / partitions
   - pendo/partitions.py
   - any settings/config/env helper files

3. API access layer
   - pendo/client.py
   - endpoints.py
   - endpoint_factory.py

4. Pull layer: independent pulls
   - pendo/pulls/guides.py
   - pendo/pulls/aggregations.py
   - pendo/pulls/mau.py
   - any other direct Pendo pull files

5. Pull layer: shared pull helpers
   - pendo/pulls/partitioned.py
   - any pagination/chunking/retry/date helper pull files

6. Pull layer: dependent or phase-based pulls
   - pendo/pulls/dependent.py
   - pulls/ux_lite_poll_events_phase2.py

7. Transform layer
   - transform/ux_lite.py
   - transform/transform.py
   - any other transform modules

8. Load layer
   - load.py
   - duckdb_conn.py
   - any BigQuery/load replacement files later

9. Utility layer
   - pendo/util/time.py
   - logging helpers
   - validation helpers
   - anything used across layers


# ------------------------
# File: pendo/client.py
# -------------------------

Core responsibility:
Provides the shared HTTP client and authentication boundary for Pendo API calls.

Why it exists:
The pipeline needs one centralized place to handle Pendo host URLs, integration-key authentication, request timeouts, and HTTP error formatting. Keeping this behavior in PendoClient prevents pull modules from duplicating request boilerplate or knowing authentication details.

How it works:
Each Pendo subscription is represented by a configured PendoClient. The client builds full URLs from relative API paths, applies the required integration-key header, executes GET and POST requests through requests, raises detailed HTTPError exceptions for failed responses, and returns parsed JSON for successful responses.

Design boundary:
This file intentionally does not contain endpoint-specific paths, aggregation payloads, guide logic, poll logic, UX-Lite rules, or response shaping. Those responsibilities belong to endpoint wrappers, pull modules, and transform modules.

Engineer-defense explanation:
This client creates a clean transport layer. It centralizes Pendo-specific request behavior while keeping business logic out of the HTTP layer. That makes the rest of the pipeline easier to test, explain, and modify.

Known risks / future enhancements:
- Successful responses are assumed to be JSON.
- Retry and rate-limit handling are not currently implemented.
- Timeout is configured per client, not per request.


# ------------------
# File: endpoints.py
# -------------------

Core responsibility:
Defines thin wrapper classes for individual Pendo API endpoints.

Why it exists:
The pipeline needs a small boundary between the generic HTTP client and the pull modules. Endpoint wrappers centralize endpoint paths and HTTP method selection without mixing in payload construction, query logic, filtering, or response shaping.

How it works:
Each endpoint class receives a PendoClient. The class defines the endpoint path and exposes a small method that delegates to the appropriate PendoClient method. GuideEndpoint calls client.get() for /api/v1/guide. AggregationEndpoint calls client.post() for /api/v1/aggregation.

Design boundary:
Endpoint wrappers should remain dumb. They should not build params, construct aggregation payloads, load environment configuration, apply filters, normalize responses, or contain UX-Lite business logic. Those responsibilities belong in pendo/pulls/ and transform/.

Engineer-defense explanation:
This design keeps the pipeline layered and maintainable. PendoClient owns HTTP/auth behavior, endpoint wrappers own API path/method knowledge, pull modules own request construction, and transform modules own data shaping. Keeping endpoint wrappers small prevents business logic from leaking into the API access layer.

Known risks / future enhancements:
- The main risk is future scope creep: adding payload or filtering logic to endpoint wrappers.
- Additional endpoint wrappers can be added later using the same pattern, such as PageEndpoint, FeatureEndpoint, SegmentEndpoint, or ReportEndpoint.


# --------------------------
# File: endpoint_factory.py
# --------------------------

Core responsibility:
Maps short endpoint names used by pull modules to concrete endpoint wrapper instances.

Why it exists:
Pull modules need access to endpoint wrappers, but they should not need to construct concrete endpoint classes directly. The factory centralizes endpoint construction and keeps naming aligned across the pull layer.

How it works:
The get_endpoint() function receives an EndpointName and a configured PendoClient. It returns the corresponding endpoint wrapper bound to that client. Current supported names are "guides" and "aggregations".

Design boundary:
The factory only selects and instantiates endpoint wrappers. It does not build request params, construct aggregation payloads, execute API calls itself, transform responses, or contain business rules.

Engineer-defense explanation:
This is a lightweight abstraction that keeps endpoint construction consistent without adding complexity. It gives the project one place to register endpoint wrappers and one runtime failure point for invalid endpoint names. The Literal type helps catch naming mistakes during development, while the ValueError protects runtime calls.

Known risks / future enhancements:
- EndpointName values must stay aligned with pull module calls.
- The union return type may become unwieldy if many endpoint wrappers are added.
- If endpoint count grows, a dictionary-based mapping or shared endpoint protocol may be cleaner.


# -------------------------
# File: pendo/partitions.py
# --------------------------

Core responsibility:
Defines the configured Pendo subscriptions that the pipeline pulls from. Each Partition contains the host, API key, and stable lineage label for one subscription.

Why it exists:
The pipeline needs to run the same extraction logic across multiple Pendo subscriptions without hardcoding subscription-specific details in each pull module. Centralizing subscriptions in PARTITIONS allows shared pull helpers to iterate over configured subscriptions and stamp returned rows with app_sub lineage.

How it works:
Each Partition stores a subscription name, Pendo host, and API key. partitioned.pull_all(...) loops over PARTITIONS, creates or uses a PendoClient for each partition, runs the requested pull, and stamps rows with app_sub=partition.name.

Design boundary:
This file owns subscription configuration and lineage labels. It should not contain endpoint-specific paths, pull payloads, API request logic, transform logic, or dashboard business rules.

Engineer-defense explanation:
This design keeps extraction logic generic and scalable. Pull modules define what data to request, while PARTITIONS defines where to request it from. Stamping app_sub preserves source lineage and prevents ambiguity when combining data from multiple Pendo subscriptions.

Known risks / future enhancements:
- Partition.name values become downstream data values through app_sub and should be treated as stable.
- Adding a new subscription requires config updates and downstream readiness for a new app_sub value.
- Downstream logic should not depend on PARTITIONS list order.#


# --------------------------------
# File: pendo/pulls/partitioned.py
# ---------------------------------

Core responsibility:
Runs single-subscription pull functions across all configured Pendo partitions and stamps returned data with app_sub lineage.

Why it exists:
Most pull functions should only need to know how to pull data from one PendoClient. The pipeline also needs to pull the same data from multiple Pendo subscriptions. This helper centralizes the multi-subscription loop so partition handling, client construction, and lineage stamping are not duplicated across every pull module.

How it works:
pull_all() receives a callable that accepts a PendoClient and returns raw data for one subscription. For each configured Partition, it builds a PendoClient, runs the pull function, stamps returned rows/items with app_sub=partition.name, and appends them to one combined output list.

Design boundary:
This file owns multi-subscription orchestration and lineage stamping only. It should not contain endpoint-specific paths, request payload construction, aggregation logic, transform rules, or dashboard business logic.

Engineer-defense explanation:
This helper keeps extraction code scalable and consistent. Pull modules define what to pull. PARTITIONS defines which subscriptions to pull from. partitioned.pull_all() bridges those layers by running the same pull across each subscription and preserving source lineage with app_sub.

Known risks / future enhancements:
- Function docstrings must be placed as the first statement inside each function; current placement after code means they are not actual docstrings.
- app_sub should probably be stamped after row unpacking so partition lineage is authoritative.
- Current logging should count all appended list items, not only dict rows.
- This helper currently prints progress directly; future productionization might replace print() with structured logging.


# ---------------------------
# File: pendo/pulls/guides.py
# ----------------------------

Core responsibility:
Pulls expanded Pendo guide metadata and lightly flattens selected guide fields for downstream UX-Lite registry building, debugging, and reporting.

Why it exists:
Guides are a foundational lookup layer for UX-Lite analysis. Guide metadata contains guide IDs, guide names, app IDs, state/timing fields, creator metadata, and poll-related fields needed later by the transform layer. Pulling guide metadata early allows downstream code to classify UX-Lite guides and connect guide experiences to poll questions and measurement windows.

How it works:
pull_guides() gets the Guides endpoint wrapper, requests expanded guide metadata using expand="*", optionally filters to a single appId, validates that Pendo returned a list, and returns raw guide dictionaries. guides_to_df() then extracts a curated set of guide fields into a dataframe while preserving timing and creator metadata needed for registry review and debugging.

Design boundary:
This file decides what guide metadata to request and performs light metadata flattening. It does not decide which guides count as UX-Lite, map guide modules to poll questions, score poll results, or apply reporting business rules. That logic belongs in transform/ux_lite.py and related transform modules.

Engineer-defense explanation:
The guide pull is intentionally early and broad because UX-Lite interpretation depends on guide metadata. Using expand="*" avoids under-pulling fields that are required for downstream classification and registry construction. Keeping UX-Lite classification out of this file preserves a clean separation between extraction and business transformation.

Known risks / future enhancements:
- expand="*" may pull more metadata than strictly needed, but it reduces the risk of missing fields required by UX-Lite transforms.
- The endpoint factory assertion could be replaced with an explicit TypeError because assert statements can be disabled.
- guides_to_df() assumes each item is a dictionary; this could be validated explicitly.
- Timing fields should be documented because guide state alone is not enough to interpret measurement windows.


# ---------------------------------
# File: pendo/pulls/aggregations.py
# ----------------------------------

Core responsibility:
Pulls Pendo guideSeen aggregation data for a lookback window and returns observed guide activity by guideId/appId.

Why it exists:
Guide metadata alone does not prove that a guide was actually seen during the measurement window. Teams can rename guides, reuse them, disable them, or move them back to draft. The guideSeen aggregation provides an observed activity truth set that downstream transforms can combine with guide metadata.

How it works:
pull_aggregations() builds a Pendo aggregation payload using guideEvents, expands across all accessible app IDs, filters to guideSeen events, groups by guideId and appId, and calculates firstSeenAt, lastSeenAt, and seenCount. results_to_rows() normalizes the common {"results": [...]} aggregation response shape into a list of rows.

Design boundary:
This file defines and executes the guideSeen aggregation request. It does not pull poll responses, classify UX-Lite guides, map poll questions, score responses, or apply dashboard reporting rules. Those responsibilities belong in dependent pulls and transform modules.

Engineer-defense explanation:
This module separates observed guide activity from static guide metadata. That makes the UX-Lite registry more reliable because it can use actual guideSeen events rather than relying only on current guide state or publish metadata.

Known risks / future enhancements:
- results_to_rows() returns [] for unexpected shapes; this is tolerant but could hide malformed responses.
- first_ms and days should be validated because days must be positive and the payload uses count=-days.
- The expandAppIds("*") source setting is critical for complete app-level guideSeen activity.


# ------------------------
# File: pendo/pulls/mau.py
# -------------------------

Core responsibility:
Pulls monthly active user counts from Pendo pageEvents by counting distinct visitorId values per appId within UTC month windows.

Why it exists:
UX-Lite survey results need usage-volume context. MAU provides app-level activity volume for executive rollups, interpretation of response coverage, and possible future weighting decisions. This pull intentionally returns appId-level usage only and leaves app metadata enrichment to downstream transforms/modeling.

How it works:
The module builds UTC month windows, creates a Pendo aggregation payload using pageEvents expanded across all apps, groups events by appId and visitorId, extracts aggregation rows, deduplicates app/visitor pairs, and counts distinct visitors per app/month.

Design boundary:
This file owns MAU extraction and light aggregation. It does not classify UX-Lite guides, pull poll responses, enrich app names, join domain/portfolio metadata, or make weighting decisions.

Engineer-defense explanation:
MAU is separated from UX-Lite poll/guide logic because it is usage context, not survey response data. Counting distinct visitors from pageEvents by app/month provides a consistent volume measure while preserving app metadata enrichment for downstream modeling.

Known risks / future enhancements:
- months_back includes the current month by design; this produces month-to-date MAU when months_back=1.
- The original file bypassed the endpoint wrapper by calling client.post() directly; using AggregationEndpoint keeps the API-access pattern consistent.
- _extract_rows() is tolerant of several response shapes and returns [] for unexpected shapes, which is convenient but could hide malformed responses.
- appName is intentionally None until downstream enrichment.


# ------------------------------
# File: pendo/pulls/dependent.py
# -------------------------------

Core responsibility:
Registers phase-2 pulls that depend on phase-1 raw outputs.

Why it exists:
Not all pulls can run independently. Some require earlier raw data to determine what additional data should be requested. UX-Lite poll events are a phase-2 pull because the pipeline must first use guide metadata and guideSeen activity to identify the relevant UX-Lite poll IDs.

How it works:
DEPENDENT_PULLS stores DependentPull entries. Each entry has a name and a run function that accepts the phase-1 raw output dictionary. The current ux_lite_poll_events entry reads raw["guides"] and raw["aggregations"] and passes them to pull_ux_lite_poll_events_for_registry().

Design boundary:
This file owns dependency registration and adaptation between the generic dep.run(raw) interface and specific dependent pull functions. It should not build endpoint payloads, execute unrelated pulls, transform UX-Lite results, or load data to storage.

Engineer-defense explanation:
This registry keeps phase-2 dependency logic explicit and out of main.py. It allows the orchestrator to run independent pulls first, then run dependent pulls through a consistent interface. This is especially important for UX-Lite poll events, which cannot be targeted correctly until the guide/poll registry exists.

Known risks / future enhancements:
- Dependent pull keys must stay aligned with PULL_FUNCTIONS output names.
- Missing phase-1 keys should raise a clear error.
- If dependent pulls grow, replace lambdas with named functions for easier testing and debugging.
- If the raw output dictionary grows more complex, consider typed structures or constants for shared pull names.




# ------------------------------------------------------------
# Known-good aggregation payload
# ------------------------------------------------------------

payload = {
    "response": {"mimeType": "application/json"},
    "request": {
        "pipeline": [
            {
                "source": {
                    "events": {
                        # Pull from page + track events
                        "eventClass": ["page", "track"]
                    },
                    # Limit to last 7 days
                    "timeSeries": {
                        "period": "dayRange",
                        "first": "now()",
                        "count": -7
                    }
                }
            },
            # Only pull visitor IDs (lightweight)
            {"select": {"visitorId": "visitorId"}},

            # Limit results so we don't pull huge datasets
            {"limit": 10}
        ]
    }
}

# ------------------------------------------------------------
# Run query
# ------------------------------------------------------------

Known limitation (SAE aggregation): poll and guide response sources are not available.
Errors seen: unknown aggregator 'pollEvents', unknown source 'polls'.