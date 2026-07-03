"""
UX-Lite transformation logic.

This module converts raw Pendo guide metadata, guideSeen activity, and pollEvents
rows into analytics-ready UX-Lite registry, guide session, score event, comment
event, response, and population tables.

Extraction modules preserve raw Pendo data and source lineage. This transform
module applies UX-Lite business rules: identifying eligible guides, deriving
measurement sessions, labeling poll IDs, validating score responses, and
assigning responses to reporting windows.
"""

import re
from datetime import datetime, timezone
from typing import Any

import pandas as pd


UX_LITE_CUTOFF_DATE = datetime(2026, 3, 30, tzinfo=timezone.utc)
UX_LITE_CUTOFF_MS = int(UX_LITE_CUTOFF_DATE.timestamp() * 1000)
UX_LITE_WINDOW_DAYS = 14
SESSION_GAP_DAYS = 10

# Set this to True if UX-Lite registry inclusion should require the full
# 3-question template: usability score, usefulness score, and FreeForm comment.
# The current pipeline requires the two score questions and keeps comment as an
# audit/output flag because score reporting can still be valid when comments are
# unavailable.
REQUIRE_COMMENT_FOR_REGISTRY = False

POLL_ID_COLUMNS = [
    "pollId_usability",
    "pollId_usefulness",
    "pollId_comment",
]

SCORE_EVENT_COLUMNS = [
    "ts",
    "guideId",
    "pollId",
    "metric",
    "score",
    "analyticsSessionId",
    "visitorId",
    "appId",
    "app_sub",
]

COMMENT_EVENT_COLUMNS = [
    "ts",
    "guideId",
    "pollId",
    "metric",
    "comment",
    "analyticsSessionId",
    "visitorId",
    "appId",
    "app_sub",
]

GUIDE_SESSION_COLUMNS = [
    "guideSessionId",
    "guideId",
    "guideName",
    "appId",
    "app_sub",
    "state",
    "visibilityStart",
    "reportingStart",
    "reportingEnd",
    "reportingEndExclusive",
    "expiresAfterDt",
    "windowDays",
    "sessionLabel",
    "guideFirstSeenDt",
    "guideLastSeenDt",
    "has_usability",
    "has_usefulness",
    "has_comment",
    "has_required_scores",
    "has_full_three_question_template",
    "is_template_complete",
    "responseWindowStartTs",
    "responseWindowEndTs",
    "responseCount",
    "windowDaysObserved",
    "sessionSource",
    "session_start_matches_first_seen",
    "session_has_window",
    "sessionStatus",
    "sessionMonth",
    "sessionQuarter",
    "reportingStartDate",
    "reportingEndDate",
    "isLikelyInProgress",
]

UX_LITE_RESPONSE_COLUMNS = [
    "analyticsSessionId",
    "visitorId",
    "guideId",
    "appId",
    "app_sub",
    "ease_score",
    "usefulness_score",
    "is_complete",
    "ts",
    "guideName",
    "appId_mismatch",
    "guideSessionId",
    "sessionLabel",
    "reportingEnd",
    "is_in_reporting_window",
]


def _empty_df(columns: list[str]) -> pd.DataFrame:
    """Return an empty dataframe with the requested columns."""
    return pd.DataFrame(columns=columns)


def _require_columns(df: pd.DataFrame, columns: list[str], *, context: str) -> None:
    """
    Raise a clear error when a dataframe is missing required columns.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe to validate.
    columns : list[str]
        Required column names.
    context : str
        Human-readable name for the validation context.

    Raises
    ------
    ValueError
        If any required columns are missing.
    """
    missing = [col for col in columns if col not in df.columns]
    if missing:
        raise ValueError(f"{context} missing required columns: {missing}")


def _norm_str(value: Any) -> str:
    """Normalize scalar values for joins/lookups without turning nulls into text."""
    if value is None:
        return ""

    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass

    return str(value).strip()



def _norm_app_id(value: Any) -> str:
    """
    Normalize Pendo app IDs for comparisons.

    Pendo app IDs may arrive as strings, ints, or float-looking values
    depending on the API/pandas path. Treat 627... and 627....0 as the same.
    """
    if value is None:
        return ""

    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass

    if isinstance(value, float) and value.is_integer():
        return str(int(value))

    text = str(value).strip()

    if text.endswith(".0") and text[:-2].lstrip("-").isdigit():
        return text[:-2]

    return text


def _ms_to_dt(value: Any) -> pd.Timestamp:
    """Convert a Unix millisecond value to a UTC pandas timestamp."""
    if pd.isna(value):
        return pd.NaT

    try:
        value = int(value)
        if value <= 0:
            return pd.NaT
        return pd.to_datetime(value, unit="ms", utc=True)
    except Exception:
        return pd.NaT


def _question_text(value: Any) -> str:
    """Normalize a poll question for text-pattern matching."""
    return re.sub(r"\s+", " ", _norm_str(value).lower())


def _is_usability_question(question: Any) -> bool:
    """
    Identify UX-Lite usability/ease questions from question text.

    Naming conventions are unreliable, so registry detection relies on the
    question pattern rather than guide names.
    """
    q = _question_text(question)

    return (
        "easy to use" in q
        or "ease of use" in q
        or ("easy" in q and ("use" in q or "using" in q))
    )


def _is_usefulness_question(question: Any) -> bool:
    """
    Identify UX-Lite usefulness/needs questions from question text.

    The phrasing has varied across teams, so this intentionally allows several
    common variants around usefulness, needs, requirements, and job/task fit.
    """
    q = _question_text(question)

    direct_phrases = [
        "meet my needs",
        "meets my needs",
        "meet my requirements",
        "meets my requirements",
        "does what i need it to do",
        "helps me do my job",
        "help me do my job",
        "lets me do my job",
        "let me do my job",
        "allows me to do my job",
        "enables me to do my job",
        "helps me complete my work",
        "helps me complete my task",
        "helps me accomplish my task",
    ]

    if any(phrase in q for phrase in direct_phrases):
        return True

    # Allow broad usefulness phrasing, but require a product/work noun nearby so
    # generic "useful" text does not match unrelated comments too easily.
    usefulness_terms = ["useful", "helpful"]
    context_terms = [
        "feature",
        "features",
        "capability",
        "capabilities",
        "functionality",
        "functions",
        "tool",
        "workflow",
        "task",
        "job",
        "work",
    ]

    return any(term in q for term in usefulness_terms) and any(
        term in q for term in context_terms
    )


def _extract_poll_flags(guide: dict[str, Any]) -> dict[str, Any]:
    """
    Extract UX-Lite poll IDs and template flags from one guide.

    UX-Lite registry inclusion is based on the score-question pattern rather
    than guide naming. The FreeForm comment question is retained as an audit and
    output flag; REQUIRE_COMMENT_FOR_REGISTRY controls whether it is required
    for inclusion.
    """
    row: dict[str, Any] = {
        "pollId_usability": pd.NA,
        "pollId_usefulness": pd.NA,
        "pollId_comment": pd.NA,
        "question_usability": pd.NA,
        "question_usefulness": pd.NA,
        "question_comment": pd.NA,
    }

    for poll in guide.get("polls") or []:
        if not isinstance(poll, dict):
            continue

        question = poll.get("question")
        poll_type = _norm_str((poll.get("attributes") or {}).get("type"))

        if poll_type == "FreeForm":
            row["pollId_comment"] = poll.get("id")
            row["question_comment"] = question
        elif _is_usability_question(question):
            row["pollId_usability"] = poll.get("id")
            row["question_usability"] = question
        elif _is_usefulness_question(question):
            row["pollId_usefulness"] = poll.get("id")
            row["question_usefulness"] = question

    row["has_usability"] = pd.notna(row.get("pollId_usability"))
    row["has_usefulness"] = pd.notna(row.get("pollId_usefulness"))
    row["has_comment"] = pd.notna(row.get("pollId_comment"))
    row["has_required_scores"] = row["has_usability"] and row["has_usefulness"]
    row["has_full_three_question_template"] = (
        row["has_required_scores"] and row["has_comment"]
    )
    row["is_template_complete"] = (
        row["has_full_three_question_template"]
        if REQUIRE_COMMENT_FOR_REGISTRY
        else row["has_required_scores"]
    )

    return row


def _cluster_one_guide_activity(
    df: pd.DataFrame,
    gap_days: int = SESSION_GAP_DAYS,
) -> pd.DataFrame:
    """
    Split one guide's response activity into one or more measurement windows.

    Reused guides can produce multiple clusters of response activity. A new
    session window starts when the gap between response timestamps is greater
    than gap_days.

    Expected columns
    ----------------
    guideId, ts
        Required.
    guideName, appId, app_sub
        Optional metadata. Missing values are preserved as NA rather than
        replaced with unrelated identifiers.
    """
    if gap_days <= 0:
        raise ValueError(f"gap_days must be positive, got {gap_days}")

    if df.empty:
        return pd.DataFrame()

    _require_columns(df, ["guideId", "ts"], context="_cluster_one_guide_activity input")

    work = df.copy()
    work["ts"] = pd.to_datetime(work["ts"], errors="coerce", utc=True)
    work = work[work["ts"].notna()].sort_values("ts").reset_index(drop=True)

    if work.empty:
        return pd.DataFrame()

    for col in ["guideName", "appId", "app_sub"]:
        if col not in work.columns:
            work[col] = pd.NA

    work["gap_days"] = work["ts"].diff().dt.total_seconds() / 86400
    work["new_window"] = work["gap_days"].isna() | (work["gap_days"] > gap_days)
    work["window_num"] = work["new_window"].cumsum()

    windows = (
        work.groupby("window_num", as_index=False)
        .agg(
            guideId=("guideId", "first"),
            guideName=("guideName", "first"),
            appId=("appId", "first"),
            app_sub=("app_sub", "first"),
            responseWindowStartTs=("ts", "min"),
            responseWindowEndTs=("ts", "max"),
            responseCount=("ts", "size"),
        )
    )

    # Response activity defines the start of a reused-guide measurement cluster,
    # but the official UX-Lite reporting window remains the standard playbook
    # window length.
    windows["reportingStart"] = windows["responseWindowStartTs"].dt.normalize()
    windows["reportingEndExclusive"] = (
        windows["reportingStart"] + pd.Timedelta(days=UX_LITE_WINDOW_DAYS)
    )
    windows["reportingEnd"] = (
        windows["reportingEndExclusive"] - pd.Timedelta(days=1)
    )

    # Observed response activity is still useful as an audit field, but it should
    # not define the reporting/population window.
    windows["windowDaysObserved"] = (
        (windows["responseWindowEndTs"].dt.normalize()
        - windows["responseWindowStartTs"].dt.normalize()).dt.days + 1
    )

    windows["guideSessionId"] = (
        windows["guideId"].astype(str)
        + "_"
        + windows["reportingStart"].dt.strftime("%Y-%m-%d")
    )

    windows["sessionLabel"] = (
        windows["guideName"].astype(str)
        + " | "
        + windows["reportingStart"].dt.strftime("%Y-%m-%d")
        + " to "
        + windows["reportingEnd"].dt.strftime("%Y-%m-%d")
    )

    return windows


def _derive_sessions_from_score_events(
    score_events: pd.DataFrame,
    registry: pd.DataFrame,
    gap_days: int = SESSION_GAP_DAYS,
) -> pd.DataFrame:
    """
    Build one or more guide sessions from response activity clusters.

    This supports both reused guides with multiple activity clusters and cloned
    guides with only one cluster.
    """
    if score_events.empty:
        return pd.DataFrame()

    _require_columns(
        score_events,
        ["guideId", "ts", "app_sub"],
        context="_derive_sessions_from_score_events score_events",
    )

    work = score_events.copy()
    work["guideId"] = work["guideId"].astype(str)
    work["appId"] = work["appId"].map(_norm_app_id)
    work["app_sub"] = work["app_sub"].map(_norm_str)

    reg_meta_cols = [
        col
        for col in [
            "guideId",
            "guideName",
            "appId",
            "app_sub",
            "state",
            "visibilityStart",
            "expiresAfterDt",
            "windowDays",
            "guideFirstSeenDt",
            "guideLastSeenDt",
            "has_usability",
            "has_usefulness",
            "has_comment",
            "has_required_scores",
            "has_full_three_question_template",
            "is_template_complete",
        ]
        if col in registry.columns
    ]

    reg_meta = registry[reg_meta_cols].copy() if reg_meta_cols else pd.DataFrame()
    if not reg_meta.empty:
        reg_meta["guideId"] = reg_meta["guideId"].astype(str)
        if "appId" in reg_meta.columns:
            reg_meta["appId"] = reg_meta["appId"].map(_norm_app_id)
        if "app_sub" in reg_meta.columns:
            reg_meta["app_sub"] = reg_meta["app_sub"].map(_norm_str)
            reg_meta = reg_meta.drop_duplicates(["guideId", "app_sub"])
        else:
            reg_meta = reg_meta.drop_duplicates(["guideId"])

    if "guideName" not in work.columns and not reg_meta.empty:
        merge_cols = ["guideId", "app_sub"] if "app_sub" in reg_meta.columns else ["guideId"]
        work = work.merge(reg_meta, on=merge_cols, how="left", suffixes=("", "_reg"))

    session_frames: list[pd.DataFrame] = []

    for _, group_df in work.groupby(["guideId", "app_sub"], dropna=False):
        windows = _cluster_one_guide_activity(group_df, gap_days=gap_days)
        if windows.empty:
            continue

        guide_id = _norm_str(group_df["guideId"].iloc[0])
        app_sub = _norm_str(group_df["app_sub"].iloc[0])

        if not reg_meta.empty:
            if "app_sub" in reg_meta.columns:
                reg_row = reg_meta[
                    (reg_meta["guideId"].astype(str) == guide_id)
                    & (reg_meta["app_sub"].map(_norm_str) == app_sub)
                ]
            else:
                reg_row = reg_meta[reg_meta["guideId"].astype(str) == guide_id]

            if not reg_row.empty:
                meta = reg_row.iloc[0].to_dict()
                for col, val in meta.items():
                    if col not in windows.columns:
                        windows[col] = val

        windows["sessionSource"] = "response_cluster"
        session_frames.append(windows)

    if not session_frames:
        return pd.DataFrame()

    sessions = pd.concat(session_frames, ignore_index=True)

    if "guideFirstSeenDt" in sessions.columns:
        sessions["session_start_matches_first_seen"] = (
            sessions["guideFirstSeenDt"].notna()
            & (sessions["reportingStart"] == sessions["guideFirstSeenDt"].dt.normalize())
        )
    else:
        sessions["session_start_matches_first_seen"] = False

    sessions["session_has_window"] = (
        sessions["reportingStart"].notna()
        & sessions["reportingEndExclusive"].notna()
    )

    sessions["sessionStatus"] = "historical"
    now_utc = pd.Timestamp.now(tz="UTC")

    sessions.loc[
        (sessions["reportingStart"] <= now_utc.normalize())
        & (sessions["reportingEndExclusive"] > now_utc),
        "sessionStatus",
    ] = "active"

    sessions.loc[
        sessions["reportingStart"] > now_utc.normalize(),
        "sessionStatus",
    ] = "scheduled"

    sessions["sessionMonth"] = sessions["reportingStart"].dt.to_period("M").astype(str)
    sessions["sessionQuarter"] = (
        "Q"
        + sessions["reportingStart"].dt.quarter.astype(str)
        + " "
        + sessions["reportingStart"].dt.year.astype(str)
    )
    sessions["reportingStartDate"] = sessions["reportingStart"].dt.date
    sessions["reportingEndDate"] = sessions["reportingEnd"].dt.date
    sessions["isLikelyInProgress"] = sessions["windowDaysObserved"].fillna(0) < 3

    sort_cols = [col for col in ["reportingStart", "guideName"] if col in sessions.columns]
    if sort_cols:
        ascending = [False if col == "reportingStart" else True for col in sort_cols]
        sessions = sessions.sort_values(
            sort_cols,
            ascending=ascending,
            na_position="last",
        )

    return sessions.reset_index(drop=True)


def build_ux_lite_registry(
    guides: list[dict[str, Any]],
    guide_seen: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build the UX-Lite guide registry from guide metadata and guideSeen activity.

    Registry inclusion is based on observed activity plus UX-Lite question
    structure. Guide-name matching is retained as an audit flag only because
    teams have used inconsistent naming conventions for UX-Lite testing runs.

    Parameters
    ----------
    guides : list[dict[str, Any]]
        Raw guide records from Pendo, expected to include app_sub lineage when
        pulled through partitioned.pull_all().
    guide_seen : pd.DataFrame
        GuideSeen aggregation rows used as observed guide activity evidence.

    Returns
    -------
    pd.DataFrame
        One row per eligible UX-Lite guide with metadata, reporting-window
        fields, audit flags, and poll IDs.
    """
    gs = guide_seen.copy() if guide_seen is not None else pd.DataFrame()

    if not gs.empty:
        _require_columns(
            gs,
            ["guideId", "firstSeenAt", "lastSeenAt"],
            context="build_ux_lite_registry guide_seen",
        )

        gs["guideId"] = gs["guideId"].astype(str)
        gs["lastSeenAt"] = (
            pd.to_numeric(gs["lastSeenAt"], errors="coerce")
            .fillna(0)
            .astype("int64")
        )
        gs["firstSeenAt"] = (
            pd.to_numeric(gs["firstSeenAt"], errors="coerce")
            .fillna(0)
            .astype("int64")
        )

        gs_recent = gs[gs["lastSeenAt"] >= UX_LITE_CUTOFF_MS]
        last_seen_by_guide = gs_recent.groupby("guideId")["lastSeenAt"].max().to_dict()
        first_seen_by_guide = gs_recent.groupby("guideId")["firstSeenAt"].min().to_dict()
    else:
        last_seen_by_guide = {}
        first_seen_by_guide = {}

    def name_of(guide: dict[str, Any]) -> str:
        return _norm_str(guide.get("name") or guide.get("title"))

    def name_matches_spec(guide: dict[str, Any]) -> bool:
        name_clean = re.sub(r"[\s\-_]+", "", name_of(guide).lower())
        return "umux" in name_clean or "uxlite" in name_clean

    def ran_since_cutoff(guide: dict[str, Any]) -> bool:
        guide_id = _norm_str(guide.get("id"))
        return guide_id in last_seen_by_guide

    rows: list[dict[str, Any]] = []

    for guide in guides:
        if not isinstance(guide, dict):
            raise TypeError(f"Expected guide dict, got {type(guide).__name__}")

        guide_id = _norm_str(guide.get("id"))
        last_seen = last_seen_by_guide.get(guide_id, 0)
        first_seen = first_seen_by_guide.get(guide_id, 0)

        published_at = guide.get("publishedAt")
        shows_after = guide.get("showsAfter")
        expires_after = guide.get("expiresAfter")

        published_dt = _ms_to_dt(published_at)
        shows_after_dt = _ms_to_dt(shows_after)
        expires_after_dt = _ms_to_dt(expires_after)
        first_seen_dt = _ms_to_dt(first_seen)
        last_seen_dt = _ms_to_dt(last_seen)

        poll_flags = _extract_poll_flags(guide)

        # Inclusion rule:
        # - do not exclude based on current guide state
        # - do not require guide-name match because naming conventions are inconsistent
        # - do require evidence the guide actually ran since the cutoff
        # - do require the UX-Lite score-question structure
        #
        # name_matches_spec is retained as an audit flag only.
        if not (ran_since_cutoff(guide) and poll_flags["is_template_complete"]):
            continue

        # XM Playbook anchor: guide visibility start.
        # Fallback order:
        # 1) showsAfter
        # 2) publishedAt
        # 3) firstSeenAt, only if metadata is missing
        visibility_start = shows_after_dt
        if pd.isna(visibility_start):
            visibility_start = published_dt
        if pd.isna(visibility_start):
            visibility_start = first_seen_dt

        reporting_start = visibility_start
        reporting_end_exclusive = (
            reporting_start + pd.Timedelta(days=UX_LITE_WINDOW_DAYS)
            if pd.notna(reporting_start)
            else pd.NaT
        )
        reporting_end = (
            reporting_end_exclusive - pd.Timedelta(days=1)
            if pd.notna(reporting_end_exclusive)
            else pd.NaT
        )

        session_date = (
            reporting_start.strftime("%Y-%m-%d")
            if pd.notna(reporting_start)
            else "missing"
        )
        session_label = (
            f"{name_of(guide)} | "
            f"{reporting_start.strftime('%Y-%m-%d') if pd.notna(reporting_start) else 'missing'}"
            f" to "
            f"{reporting_end.strftime('%Y-%m-%d') if pd.notna(reporting_end) else 'missing'}"
        )

        rows.append(
            {
                "guideId": guide_id,
                "guideName": name_of(guide),
                "appId": _norm_app_id(guide.get("appId")),
                "app_sub": _norm_str(guide.get("app_sub")),
                "state": guide.get("state"),
                "name_matches_spec": name_matches_spec(guide),
                "ran_since_cutoff": ran_since_cutoff(guide),
                "publishedAt": published_at,
                "publishedDt": published_dt,
                "showsAfter": shows_after,
                "showsAfterDt": shows_after_dt,
                "guideFirstSeenAt": first_seen,
                "guideFirstSeenDt": first_seen_dt,
                "guideLastSeenAt": last_seen,
                "guideLastSeenDt": last_seen_dt,
                "visibilityStart": visibility_start,
                "reportingStart": reporting_start,
                "reportingEnd": reporting_end,
                "reportingEndExclusive": reporting_end_exclusive,
                "expiresAfter": expires_after,
                "expiresAfterDt": expires_after_dt,
                "windowDays": UX_LITE_WINDOW_DAYS,
                "guideSessionId": f"{guide_id}_{session_date}",
                "sessionLabel": session_label,
                **poll_flags,
            }
        )

    return pd.DataFrame(rows)


def build_guide_sessions(
    registry: pd.DataFrame,
    score_events: pd.DataFrame,
    gap_days: int = SESSION_GAP_DAYS,
) -> pd.DataFrame:
    """
    Build guide-session reporting windows from registry and response activity.

    Response activity is preferred because reused guides may have multiple
    measurement clusters. If no response-derived sessions are available, this
    falls back to one registry-defined session per guide.

    Parameters
    ----------
    registry : pd.DataFrame
        UX-Lite registry produced by build_ux_lite_registry().
    score_events : pd.DataFrame
        Cleaned usability/usefulness score events.
    gap_days : int, default SESSION_GAP_DAYS
        Gap threshold used to split reused-guide response activity into
        separate sessions.

    Returns
    -------
    pd.DataFrame
        Guide-session rows with reporting windows and session metadata.
    """
    if registry.empty:
        return _empty_df(GUIDE_SESSION_COLUMNS)

    sessions = _derive_sessions_from_score_events(
        score_events=score_events,
        registry=registry,
        gap_days=gap_days,
    )

    if sessions.empty:
        cols = [
            "guideSessionId",
            "guideId",
            "guideName",
            "appId",
            "app_sub",
            "state",
            "visibilityStart",
            "reportingStart",
            "reportingEnd",
            "reportingEndExclusive",
            "expiresAfterDt",
            "windowDays",
            "sessionLabel",
            "guideFirstSeenDt",
            "guideLastSeenDt",
            "has_usability",
            "has_usefulness",
            "has_comment",
            "has_required_scores",
            "has_full_three_question_template",
            "is_template_complete",
        ]
        existing_cols = [col for col in cols if col in registry.columns]
        sessions = registry[existing_cols].copy()

        sessions["sessionSource"] = "registry_fallback"
        sessions["responseWindowStartTs"] = pd.NaT
        sessions["responseWindowEndTs"] = pd.NaT
        sessions["responseCount"] = pd.NA
        sessions["windowDaysObserved"] = sessions.get("windowDays", pd.NA)
        sessions["session_start_matches_first_seen"] = False
        sessions["session_has_window"] = (
            sessions.get("reportingStart", pd.NaT).notna()
            & sessions.get("reportingEndExclusive", pd.NaT).notna()
        )
        sessions["sessionStatus"] = "registry"
        sessions["sessionMonth"] = pd.to_datetime(
            sessions.get("reportingStart", pd.NaT),
            errors="coerce",
            utc=True,
        ).dt.to_period("M").astype(str)
        sessions["sessionQuarter"] = (
            "Q"
            + pd.to_datetime(
                sessions.get("reportingStart", pd.NaT),
                errors="coerce",
                utc=True,
            ).dt.quarter.astype(str)
            + " "
            + pd.to_datetime(
                sessions.get("reportingStart", pd.NaT),
                errors="coerce",
                utc=True,
            ).dt.year.astype(str)
        )
        sessions["reportingStartDate"] = pd.to_datetime(
            sessions.get("reportingStart", pd.NaT),
            errors="coerce",
            utc=True,
        ).dt.date
        sessions["reportingEndDate"] = pd.to_datetime(
            sessions.get("reportingEnd", pd.NaT),
            errors="coerce",
            utc=True,
        ).dt.date
        sessions["isLikelyInProgress"] = False

    for col in GUIDE_SESSION_COLUMNS:
        if col not in sessions.columns:
            sessions[col] = pd.NA

    return sessions[GUIDE_SESSION_COLUMNS].reset_index(drop=True)


def build_poll_lookup(registry: pd.DataFrame) -> dict[tuple[str, str], str]:
    """
    Build an app_sub-aware lookup from registry poll IDs to UX-Lite metrics.

    Parameters
    ----------
    registry : pd.DataFrame
        UX-Lite registry containing app_sub and poll ID columns.

    Returns
    -------
    dict[tuple[str, str], str]
        Mapping of (app_sub, pollId) to one of "usability", "usefulness", or
        "comment".
    """
    if registry.empty:
        return {}

    _require_columns(registry, ["app_sub"], context="build_poll_lookup registry")

    lookup: dict[tuple[str, str], str] = {}

    for _, row in registry.iterrows():
        app_sub = _norm_str(row.get("app_sub"))
        if not app_sub:
            continue

        for col, label in [
            ("pollId_usability", "usability"),
            ("pollId_usefulness", "usefulness"),
            ("pollId_comment", "comment"),
        ]:
            poll_id = row.get(col)
            if pd.notna(poll_id):
                lookup[(app_sub, _norm_str(poll_id))] = label

    return lookup


def _add_metric_from_poll_lookup(
    df: pd.DataFrame,
    poll_lookup: dict[tuple[str, str], str],
) -> pd.DataFrame:
    """Attach metric labels to poll event rows using (app_sub, pollId)."""
    out = df.copy()
    out["pollId"] = out["pollId"].map(_norm_str)
    out["app_sub"] = out["app_sub"].map(_norm_str)
    out["metric"] = [
        poll_lookup.get((app_sub, poll_id))
        for app_sub, poll_id in zip(out["app_sub"], out["pollId"])
    ]
    return out


def build_score_events(
    poll_events_rows: list[dict[str, Any]],
    poll_lookup: dict[tuple[str, str], str],
) -> pd.DataFrame:
    """
    Build cleaned UX-Lite score events from raw poll event rows.

    Parameters
    ----------
    poll_events_rows : list[dict[str, Any]]
        Raw pollEvents rows retained by the phase-2 pull.
    poll_lookup : dict[tuple[str, str], str]
        App-subscription-aware poll lookup from build_poll_lookup().

    Returns
    -------
    pd.DataFrame
        Clean usability/usefulness score events with valid 1-5 numeric scores.
    """
    if not poll_events_rows:
        return _empty_df(SCORE_EVENT_COLUMNS)

    df = pd.DataFrame(poll_events_rows)
    _require_columns(
        df,
        [
            "app_sub",
            "pollId",
            "browserTime",
            "pollResponse",
            "guideId",
            "analyticsSessionId",
            "visitorId",
            "appId",
        ],
        context="build_score_events poll_events_rows",
    )

    df = _add_metric_from_poll_lookup(df, poll_lookup)
    df = df[df["metric"].isin(["usability", "usefulness"])].copy()

    if df.empty:
        return _empty_df(SCORE_EVENT_COLUMNS)

    df["ts"] = pd.to_datetime(df["browserTime"], unit="ms", errors="coerce", utc=True)
    df["score"] = pd.to_numeric(df["pollResponse"], errors="coerce")
    df["appId"] = df["appId"].map(_norm_app_id)

    df = df[df["ts"].notna() & df["score"].between(1, 5)].copy()

    return (
        df[SCORE_EVENT_COLUMNS]
        .sort_values("ts")
        .reset_index(drop=True)
    )


def build_comment_events(
    poll_events_rows: list[dict[str, Any]],
    poll_lookup: dict[tuple[str, str], str],
) -> pd.DataFrame:
    """
    Build cleaned UX-Lite comment events from raw poll event rows.

    Parameters
    ----------
    poll_events_rows : list[dict[str, Any]]
        Raw pollEvents rows retained by the phase-2 pull.
    poll_lookup : dict[tuple[str, str], str]
        App-subscription-aware poll lookup from build_poll_lookup().

    Returns
    -------
    pd.DataFrame
        Clean FreeForm comment events with non-empty comment text.
    """
    if not poll_events_rows:
        return _empty_df(COMMENT_EVENT_COLUMNS)

    df = pd.DataFrame(poll_events_rows)
    _require_columns(
        df,
        [
            "app_sub",
            "pollId",
            "browserTime",
            "pollResponse",
            "guideId",
            "analyticsSessionId",
            "visitorId",
            "appId",
        ],
        context="build_comment_events poll_events_rows",
    )

    df = _add_metric_from_poll_lookup(df, poll_lookup)
    df = df[df["metric"].eq("comment")].copy()

    if df.empty:
        return _empty_df(COMMENT_EVENT_COLUMNS)

    df["ts"] = pd.to_datetime(df["browserTime"], unit="ms", errors="coerce", utc=True)
    df["appId"] = df["appId"].map(_norm_app_id)

    # FreeForm comment text lives in pollResponse. fillna("") prevents missing
    # values from becoming the literal string "nan".
    df["comment"] = df["pollResponse"].fillna("").astype(str).str.strip()
    df = df[df["ts"].notna() & df["comment"].ne("")].copy()

    return (
        df[COMMENT_EVENT_COLUMNS]
        .sort_values("ts")
        .reset_index(drop=True)
    )


def build_ux_lite_responses(
    score_events: pd.DataFrame,
    guide_sessions: pd.DataFrame,
    registry: pd.DataFrame,
) -> pd.DataFrame:
    """
    Pivot score events into response-level UX-Lite rows and assign sessions.

    Parameters
    ----------
    score_events : pd.DataFrame
        Clean score events from build_score_events().
    guide_sessions : pd.DataFrame
        Guide-session windows from build_guide_sessions().
    registry : pd.DataFrame
        UX-Lite registry used to attach guide metadata.

    Returns
    -------
    pd.DataFrame
        Response-level UX-Lite rows with ease/usefulness scores and session
        assignment. Only responses inside a reporting window are returned.
    """
    if score_events.empty or guide_sessions.empty:
        return _empty_df(UX_LITE_RESPONSE_COLUMNS)

    idx = ["analyticsSessionId", "visitorId", "guideId", "appId", "app_sub"]

    _require_columns(
        score_events,
        idx + ["metric", "score", "ts"],
        context="build_ux_lite_responses score_events",
    )
    _require_columns(
        guide_sessions,
        ["guideId", "app_sub", "reportingStart", "reportingEndExclusive"],
        context="build_ux_lite_responses guide_sessions",
    )

    work = score_events.copy()
    work["guideId"] = work["guideId"].astype(str)
    work["appId"] = work["appId"].map(_norm_app_id)
    work["app_sub"] = work["app_sub"].map(_norm_str)

    responses = (
        work.pivot_table(
            index=idx,
            columns="metric",
            values="score",
            aggfunc="first",
        )
        .reset_index()
        .rename(columns={"usability": "ease_score", "usefulness": "usefulness_score"})
    )

    for col in ["ease_score", "usefulness_score"]:
        if col not in responses.columns:
            responses[col] = pd.NA

    responses["is_complete"] = (
        responses["ease_score"].notna() & responses["usefulness_score"].notna()
    )

    ts_df = work.groupby(idx, as_index=False).agg(ts=("ts", "min"))
    responses = responses.merge(ts_df, on=idx, how="left")

    if not registry.empty:
        _require_columns(
            registry,
            ["guideId", "app_sub", "guideName", "appId"],
            context="build_ux_lite_responses registry",
        )

        guide_meta = (
            registry[["guideId", "app_sub", "guideName", "appId"]]
            .copy()
            .assign(
                guideId=lambda df: df["guideId"].astype(str),
                appId=lambda df: df["appId"].map(_norm_app_id),
                app_sub=lambda df: df["app_sub"].map(_norm_str),
            )
            .drop_duplicates(["guideId", "app_sub"])
            .rename(columns={"appId": "guide_appId"})
        )

        responses = responses.merge(guide_meta, on=["guideId", "app_sub"], how="left")
    else:
        responses["guideName"] = pd.NA
        responses["guide_appId"] = pd.NA

    responses["appId_mismatch"] = (
        responses["guide_appId"].notna()
        & responses["appId"].notna()
        & (responses["guide_appId"] != "")
        & (responses["appId"] != "")
        & (responses["guide_appId"] != responses["appId"])
    )
    responses["appId"] = responses["guide_appId"].fillna(responses["appId"])
    responses = responses.drop(columns=["guide_appId"], errors="ignore")

    session_cols = [
        col
        for col in [
            "guideSessionId",
            "guideId",
            "app_sub",
            "reportingStart",
            "reportingEnd",
            "reportingEndExclusive",
            "sessionLabel",
        ]
        if col in guide_sessions.columns
    ]

    sessions = guide_sessions[session_cols].copy()
    sessions["guideId"] = sessions["guideId"].astype(str)
    sessions["app_sub"] = sessions["app_sub"].map(_norm_str)

    matched_parts: list[pd.DataFrame] = []

    for _, session in sessions.iterrows():
        if pd.isna(session["reportingStart"]) or pd.isna(session["reportingEndExclusive"]):
            continue

        mask = (
            (responses["guideId"] == session["guideId"])
            & (responses["app_sub"] == session["app_sub"])
            & (responses["ts"] >= session["reportingStart"])
            & (responses["ts"] < session["reportingEndExclusive"])
        )

        if mask.any():
            part = responses.loc[mask].copy()
            part["guideSessionId"] = session.get("guideSessionId")
            part["sessionLabel"] = session.get("sessionLabel")
            part["reportingStart"] = session.get("reportingStart")
            part["reportingEnd"] = session.get("reportingEnd")
            part["reportingEndExclusive"] = session.get("reportingEndExclusive")
            matched_parts.append(part)

    if not matched_parts:
        return _empty_df(UX_LITE_RESPONSE_COLUMNS)

    responses = pd.concat(matched_parts, ignore_index=True)

    responses["is_in_reporting_window"] = (
        responses["reportingStart"].notna()
        & responses["reportingEndExclusive"].notna()
        & (responses["ts"] >= responses["reportingStart"])
        & (responses["ts"] < responses["reportingEndExclusive"])
    )

    responses = responses[responses["is_in_reporting_window"]].copy()
    responses = responses.drop(
        columns=["reportingStart", "reportingEndExclusive"],
        errors="ignore",
    )

    for col in UX_LITE_RESPONSE_COLUMNS:
        if col not in responses.columns:
            responses[col] = pd.NA

    return responses[UX_LITE_RESPONSE_COLUMNS].reset_index(drop=True)


def attach_population_to_sessions(
    sessions: pd.DataFrame,
    pop_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Attach unique visitor population counts to guide sessions.

    Parameters
    ----------
    sessions : pd.DataFrame
        Guide-session rows.
    pop_df : pd.DataFrame
        Population rows with guideSessionId and visitorId.

    Returns
    -------
    pd.DataFrame
        Session rows with a population column when population data is available.
    """
    sessions = sessions.copy()

    if sessions.empty:
        sessions["population"] = None
        return sessions

    required_cols = {"guideSessionId", "visitorId"}
    if pop_df.empty or not required_cols.issubset(pop_df.columns):
        sessions["population"] = None
        return sessions

    pop_counts = (
        pop_df.assign(visitorId=pop_df["visitorId"].astype(str))
        .groupby("guideSessionId")["visitorId"]
        .nunique()
        .rename("population")
        .reset_index()
    )

    sessions = sessions.merge(pop_counts, on="guideSessionId", how="left")
    return sessions
