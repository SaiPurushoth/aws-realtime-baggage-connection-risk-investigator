"""BagGuard Operations Console — local Streamlit application."""

from __future__ import annotations

import os
import time
from html import escape
from datetime import datetime
from typing import Any

import streamlit as st

from dashboard.service import (
    JOURNEY_STAGES,
    SCENARIOS,
    BagGuardService,
    DashboardServiceError,
    OperationsSnapshot,
    ScenarioRun,
    departure_seconds,
    parse_evidence,
    parse_incident_context,
    time_sensitive_reason,
)


st.set_page_config(
    page_title="BagGuard Operations Console",
    page_icon="🧳",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
<style>
    :root { --ink:#10212b; --muted:#647781; --navy:#0a2638; --cyan:#18b7c9;
            --amber:#ffb547; --red:#e45858; --panel:#f7fafb; --line:#dce7eb; }
    .stApp { background: linear-gradient(180deg,#f7fbfc 0,#ffffff 38%); color:var(--ink); }
    .block-container { max-width: 1500px; padding-top:.7rem; padding-bottom:2.5rem; }
    h1,h2,h3 { letter-spacing:-0.025em; color:var(--navy); }
    h1 { font-size:2.45rem !important; margin-bottom:.15rem !important; }
    h2 { font-size:1.12rem !important; text-transform:uppercase; letter-spacing:.11em;
         border-bottom:1px solid var(--line); padding-bottom:.55rem; margin-top:1.8rem !important; }
    [data-testid="stMetric"] { background:white; border:1px solid var(--line);
         border-radius:14px; padding:14px 16px; box-shadow:0 4px 18px rgba(10,38,56,.05); }
    [data-testid="stMetricLabel"] { color:var(--muted); font-weight:650; }
    .hero { background:linear-gradient(125deg,#082739,#0f4657); color:white;
            padding:18px 26px; border-radius:0 0 18px 18px; margin-bottom:14px;
            box-shadow:0 12px 30px rgba(8,39,57,.18); }
    .hero-title { font-size:1.95rem; font-weight:800; letter-spacing:-.03em; }
    .hero-sub { opacity:.86; font-size:.95rem; margin-top:2px; }
    .live-badge { display:inline-block; background:#123f4c; border:1px solid #2f6875;
                  border-radius:999px; padding:4px 10px; font-size:.75rem;
                  letter-spacing:.08em; margin-bottom:9px; }
    .stage { min-height:126px; background:white; border:1px solid var(--line);
             border-radius:14px; padding:14px; box-shadow:0 4px 18px rgba(10,38,56,.045); }
    .stage-done { border-top:4px solid var(--cyan); }
    .stage-pending { border-top:4px solid #c8d4d8; opacity:.74; }
    .stage-name { color:var(--navy); font-weight:750; font-size:.92rem; }
    .stage-state { margin-top:14px; font-weight:800; }
    .stage-time { color:var(--muted); font-size:.78rem; margin-top:5px; }
    .risk-card { background:#fff8ee; border:1px solid #ffd9a0; border-left:6px solid var(--amber);
                 border-radius:14px; padding:18px 20px; }
    .risk-card.clear { background:#effaf8; border-color:#bce9e1; border-left-color:#22a68b; }
    .ai-card { background:#f4f8ff; border:1px solid #cadcf5; border-radius:14px; padding:20px; }
    .label { color:var(--muted); text-transform:uppercase; letter-spacing:.08em;
             font-size:.72rem; font-weight:750; margin-bottom:5px; }
    .value { color:var(--navy); font-size:1rem; font-weight:700; }
    .priority-CRITICAL,.priority-HIGH { color:#b12f35; }
    .priority-MEDIUM { color:#9b6500; }
    .footer-note { color:var(--muted); font-size:.78rem; margin-top:24px; }
    div.stButton > button { min-height:3rem; border-radius:12px; font-weight:750; width:100%;
         background:#fff; color:var(--navy); border:1px solid #b9cbd2;
         box-shadow:0 3px 12px rgba(10,38,56,.06); }
    div.stButton > button:hover { background:var(--navy); color:#fff; border-color:var(--navy); }
    div.stButton > button:focus { color:#fff; background:#123f4c; border-color:var(--cyan); }
    .pipeline-grid { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:10px;
                     margin:10px 0 8px; }
    .pipe-card { position:relative; background:#fff; border:1px solid var(--line);
                 border-radius:13px; padding:13px 14px 12px; min-height:116px;
                 box-shadow:0 3px 14px rgba(10,38,56,.045); }
    .pipe-card.done { border-top:4px solid var(--cyan); }
    .pipe-card.clear { border-top:4px solid #22a68b; }
    .pipe-card.alert { border-top:4px solid var(--amber); }
    .pipe-card.waiting { border-top:4px solid #c8d4d8; }
    .pipe-step { color:#71848d; font-size:.68rem; font-weight:800; letter-spacing:.1em; }
    .pipe-name { color:var(--navy); font-size:.94rem; font-weight:800; margin-top:5px; }
    .pipe-place { color:#71848d; font-size:.72rem; margin-top:2px; }
    .pipe-state { display:inline-block; margin-top:10px; padding:3px 8px; border-radius:999px;
                  background:#eef5f7; color:#214857; font-size:.7rem; font-weight:800; }
    .pipeline-note { background:#f4f8fa; border-left:4px solid var(--cyan); border-radius:9px;
                     color:#405963; padding:10px 13px; font-size:.82rem; }
    @media (max-width:900px) { .pipeline-grid { grid-template-columns:repeat(2,minmax(0,1fr)); } }
</style>
""",
    unsafe_allow_html=True,
)


@st.cache_resource
def service() -> BagGuardService:
    return BagGuardService()


def _format_time(value: Any) -> str:
    if not value:
        return "—"
    text = str(value).replace("T", " ").replace("Z", " UTC")
    return text[:23] + (" UTC" if "UTC" not in text else "")


def _safe(value: Any) -> str:
    return escape(str(value), quote=True)


def _latest(timeline: list[dict[str, Any]]) -> dict[str, Any]:
    return timeline[-1] if timeline else {}


def _run_scenario(scenario_key: str) -> None:
    api = service()
    with st.status("Processing live baggage data…", expanded=True) as status:
        status.write("1 · LOCAL PRODUCER — Creating synthetic baggage lifecycle events…")
        run = api.start_scenario(scenario_key)
        status.write(
            f"2 · AMAZON KINESIS — Accepted {run.published_events} events, partitioned by bag tag."
        )
        status.write("3 · MANAGED FLINK — Correlating bag state and evaluating the connection timer…")
        snapshot = api.wait_for_timeline(run)
        status.write(
            f"4 · CLICKHOUSE — Stored {len(snapshot.timeline)} target-bag events through the adapter."
        )

        if run.expected_risk:
            deadline = time.monotonic() + 900
            incident_message_shown = False
            while time.monotonic() < deadline:
                snapshot = api.get_snapshot(run)
                if snapshot.incident and not incident_message_shown:
                    status.write(
                        "5 · RISK STREAM + DISPATCHER — Risk emitted; investigation dispatched."
                    )
                    status.write(
                        "6 · AGENTCORE — Reviewing timeline, comparing peers, and checking zone health…"
                    )
                    incident_message_shown = True
                if snapshot.investigation:
                    break
                time.sleep(3)
            else:
                status.update(label="Investigation timed out", state="error")
                raise DashboardServiceError(
                    "No investigation result was persisted before the timeout"
                )
            status.write(
                "7 · CLICKHOUSE — Investigation result persisted for the operations console."
            )
            status.update(label="Investigation complete", state="complete")
        else:
            status.write(
                "5 · MANAGED FLINK — Bag reached aircraft loading; no risk incident emitted."
            )
            status.write("6 · AGENTCORE — Not invoked because no investigation is required.")
            status.update(label="Normal connection verified", state="complete")

    st.session_state["bagguard_run"] = run
    st.session_state["bagguard_snapshot"] = snapshot


def _render_header() -> None:
    st.markdown(
        """
<div class="hero">
  <div class="live-badge">● LOCAL OPERATIONS VIEW</div>
  <div class="hero-title">BagGuard Operations Console</div>
  <div class="hero-sub">Real-Time Baggage Connection Risk Investigator</div>
</div>
""",
        unsafe_allow_html=True,
    )


def _render_scenario_controls() -> None:
    st.caption("Select an operating condition to follow one bag through the live data path.")
    columns = st.columns(4)
    for column, (key, label) in zip(columns, SCENARIOS.items(), strict=True):
        if column.button(label, key=f"scenario-{key}", use_container_width=True):
            try:
                _run_scenario(key)
            except Exception as exc:
                st.error(f"Scenario failed: {exc}")


def _pipeline_card(
    step: int,
    name: str,
    place: str,
    state: str,
    style: str,
) -> str:
    return f"""<div class="pipe-card {style}">
      <div class="pipe-step">STEP {step:02d}</div>
      <div class="pipe-name">{_safe(name)}</div>
      <div class="pipe-place">{_safe(place)}</div>
      <div class="pipe-state">{_safe(state)}</div>
    </div>"""


def _render_processing_path(snapshot: OperationsSnapshot | None) -> None:
    st.subheader("Live Data Path")
    if snapshot is None:
        cards = (
            ("Event Producer", "Local Python", "WAITING", "waiting"),
            ("Baggage Events", "Amazon Kinesis", "WAITING", "waiting"),
            ("Risk Detector", "Managed Apache Flink", "WAITING", "waiting"),
            ("Risk Incidents", "Amazon Kinesis", "WAITING", "waiting"),
            ("Dispatcher", "AWS Lambda", "WAITING", "waiting"),
            ("Investigator", "Amazon Bedrock AgentCore", "WAITING", "waiting"),
            ("Operational Store", "ClickHouse on private EC2", "WAITING", "waiting"),
            ("Operations Console", "Local Streamlit", "READY", "clear"),
        )
    else:
        incident = snapshot.incident
        investigation = snapshot.investigation
        if not snapshot.run.expected_risk:
            risk_state, risk_style = "NO RISK", "clear"
            dispatch_state, dispatch_style = "NOT REQUIRED", "clear"
            agent_state, agent_style = "NOT INVOKED", "clear"
        elif incident:
            risk_state, risk_style = "RISK EMITTED", "alert"
            dispatch_state, dispatch_style = "DISPATCHED", "done"
            agent_state = investigation.get("scope", "WORKING") if investigation else "WORKING"
            agent_style = "done" if investigation else "alert"
        else:
            risk_state, risk_style = "EVALUATING", "alert"
            dispatch_state, dispatch_style = "WAITING", "waiting"
            agent_state, agent_style = "WAITING", "waiting"
        cards = (
            ("Event Producer", "Local Python", f"{snapshot.run.published_events} SENT", "done"),
            ("Baggage Events", "Amazon Kinesis", "STREAMED", "done"),
            ("Risk Detector", "Managed Apache Flink", risk_state, risk_style),
            ("Risk Incidents", "Amazon Kinesis", risk_state, risk_style),
            ("Dispatcher", "AWS Lambda", dispatch_state, dispatch_style),
            ("Investigator", "Amazon Bedrock AgentCore", agent_state, agent_style),
            ("Operational Store", "ClickHouse on private EC2", f"{len(snapshot.timeline)} EVENTS", "done"),
            ("Operations Console", "Local Streamlit", "UPDATED", "clear"),
        )
    rendered = "".join(
        _pipeline_card(index, name, place, state, style)
        for index, (name, place, state, style) in enumerate(cards, start=1)
    )
    st.markdown(f'<div class="pipeline-grid">{rendered}</div>', unsafe_allow_html=True)
    st.markdown(
        """<div class="pipeline-note"><strong>Two coordinated paths:</strong>
        every baggage event is stored in ClickHouse through the controlled adapter;
        Flink independently maintains bag state and emits only connection-risk incidents.
        The dispatcher invokes AgentCore only when a risk exists, and the recommendation
        is written back to ClickHouse for this console.</div>""",
        unsafe_allow_html=True,
    )


def _render_flight(snapshot: OperationsSnapshot) -> None:
    latest = _latest(snapshot.timeline)
    st.subheader("Flight Connection")
    cols = st.columns(6)
    fields = (
        ("Inbound Flight", latest.get("inbound_flight_id", "—")),
        ("Outbound Flight", latest.get("outbound_flight_id", "—")),
        ("Airport", latest.get("airport_code", "—")),
        ("Gate", latest.get("current_gate", "—")),
        ("Departure", _format_time(latest.get("estimated_departure_time"))),
        ("Scenario", snapshot.run.scenario_label),
    )
    for column, (label, value) in zip(cols, fields, strict=True):
        column.metric(label, value)

    @st.fragment(run_every=1)
    def countdown() -> None:
        seconds = departure_seconds(snapshot.timeline)
        display = "—" if seconds is None else f"{seconds // 60:02d}:{seconds % 60:02d}"
        st.metric("Departure Countdown", display)

    countdown()


def _render_journey(snapshot: OperationsSnapshot) -> None:
    st.subheader("Bag Journey")
    by_type = {row.get("event_type"): row for row in snapshot.timeline}
    for stage_group in (JOURNEY_STAGES[:3], JOURNEY_STAGES[3:]):
        columns = st.columns(3)
        for column, (event_type, label) in zip(columns, stage_group, strict=True):
            event = by_type.get(event_type)
            css_class = "stage-done" if event else "stage-pending"
            state = "COMPLETE" if event else "PENDING"
            timestamp = _format_time(event.get("event_time")) if event else "Awaiting scan"
            column.markdown(
                f"""<div class="stage {css_class}">
                    <div class="stage-name">{_safe(label)}</div>
                    <div class="stage-state">{'✓' if event else '○'} {state}</div>
                    <div class="stage-time">{_safe(timestamp)}</div>
                </div>""",
                unsafe_allow_html=True,
            )


def _render_risk(snapshot: OperationsSnapshot) -> None:
    st.subheader("Connection Risk")
    context = parse_incident_context(snapshot.incident)
    latest = _latest(snapshot.timeline)
    if snapshot.incident:
        status = "AT RISK"
        seconds = context.get("seconds_to_departure", "—")
        last_scan = snapshot.incident.get("last_scan_type", "—")
        zone = snapshot.incident.get("transfer_zone", "—")
        card_class = "risk-card"
    else:
        status = "CLEAR — MONITORING"
        seconds = departure_seconds(snapshot.timeline)
        last_scan = latest.get("event_type", "—")
        zone = latest.get("transfer_zone", "—")
        card_class = "risk-card clear"
    st.markdown(
        f"""<div class="{card_class}">
        <div class="label">Risk status</div><div class="value">{_safe(status)}</div><br>
        <div class="label">Seconds to departure</div><div class="value">{_safe(seconds if seconds is not None else '—')}</div><br>
        <div class="label">Last scan</div><div class="value">{_safe(last_scan)}</div><br>
        <div class="label">Transfer zone</div><div class="value">{_safe(zone)}</div>
        </div>""",
        unsafe_allow_html=True,
    )


def _render_investigation(snapshot: OperationsSnapshot) -> None:
    st.subheader("AI Investigation")
    result = snapshot.investigation
    if not snapshot.run.expected_risk:
        st.info("No investigation required. The bag reached the connecting aircraft milestone.")
        return
    if not result:
        st.warning("Investigation is still in progress.")
        return
    evidence = parse_evidence(result)
    left, right = st.columns([1, 2])
    with left:
        st.metric("Status", "COMPLETE")
        st.metric("Classification", result.get("classification", "—"))
        st.metric("Scope", result.get("scope", "—"))
        priority = result.get("operational_priority", "—")
        st.markdown(
            f'<div class="ai-card"><div class="label">Operational Priority</div>'
            f'<div class="value priority-{_safe(priority)}">{_safe(priority)}</div></div>',
            unsafe_allow_html=True,
        )
    with right:
        st.markdown(
            f"""<div class="ai-card">
            <div class="label">Root Cause</div><div class="value">{_safe(result.get('root_cause', '—'))}</div><br>
            <div class="label">Recommended Action</div><div class="value">{_safe(result.get('recommended_action', '—'))}</div><br>
            <div class="label">Why action is time sensitive</div><div class="value">{_safe(time_sensitive_reason(snapshot))}</div>
            </div>""",
            unsafe_allow_html=True,
        )
        with st.expander(f"Evidence ({len(evidence)})", expanded=True):
            for item in evidence:
                st.markdown(f"- {item}")


def _render_context(snapshot: OperationsSnapshot) -> None:
    st.subheader("Airport Context")
    columns = st.columns(3)
    columns[0].metric("Bags at risk for this outbound flight", snapshot.flight_risk_bags)
    columns[1].metric(
        "Bags at risk in this flight / transfer zone",
        snapshot.flight_zone_risk_bags,
    )
    columns[2].metric("Recent zone-risk count", snapshot.recent_zone_risk_count)


def main() -> None:
    _render_header()
    _render_scenario_controls()
    snapshot: OperationsSnapshot | None = st.session_state.get("bagguard_snapshot")
    run: ScenarioRun | None = st.session_state.get("bagguard_run")
    _render_processing_path(snapshot)
    if not snapshot or not run:
        st.info("Choose a scenario to populate the operations console.")
        return
    if st.button("Refresh operational data", use_container_width=False):
        try:
            snapshot = service().get_snapshot(run)
            st.session_state["bagguard_snapshot"] = snapshot
        except DashboardServiceError as exc:
            st.error(str(exc))
    _render_flight(snapshot)
    _render_journey(snapshot)
    left, right = st.columns([1, 2])
    with left:
        _render_risk(snapshot)
    with right:
        _render_investigation(snapshot)
    _render_context(snapshot)
    st.markdown(
        f"""<div class="footer-note">Region: {service().region_name} · Data access: controlled adapter ·
        Updated: {datetime.now().astimezone().strftime('%H:%M:%S %Z')} ·
        Recommendations are advisory; no airline or airport system is modified.</div>""",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
