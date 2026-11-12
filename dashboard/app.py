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
    :root { --ink:#10212b; --muted:#627680; --navy:#082b3d; --cyan:#16b8c8;
            --amber:#f3a72f; --red:#dc565d; --green:#1d9b7f; --panel:#f6fafb;
            --line:#d9e6ea; --shadow:0 12px 32px rgba(8,43,61,.08); }
    [data-testid="stHeader"], [data-testid="stToolbar"], [data-testid="stDecoration"] {
        display:none !important;
    }
    #MainMenu, footer { visibility:hidden; }
    .stApp { background:
        radial-gradient(circle at 94% 3%,rgba(22,184,200,.10),transparent 25rem),
        linear-gradient(180deg,#f4fafb 0,#ffffff 34%); color:var(--ink); }
    .block-container { max-width:1480px; padding:1.5rem 2rem 3rem !important; }
    h1,h2,h3 { letter-spacing:-.025em; color:var(--navy); }
    h2 { font-size:1.18rem !important; letter-spacing:-.015em;
         border-bottom:1px solid var(--line); padding-bottom:.65rem;
         margin-top:2.15rem !important; }
    [data-testid="stMetric"] { background:white; border:1px solid var(--line);
         border-radius:16px; padding:15px 17px; box-shadow:0 6px 22px rgba(8,43,61,.055); }
    [data-testid="stMetricLabel"] { color:var(--muted); font-weight:700; }
    [data-testid="stMetricValue"] { color:var(--navy); }
    .hero { position:relative; overflow:hidden;
            background:linear-gradient(125deg,#06283a 0%,#0b4252 60%,#0e5664 100%);
            color:white; padding:28px 32px 30px; border-radius:24px; margin-bottom:22px;
            box-shadow:0 18px 44px rgba(8,43,61,.22); }
    .hero::after { content:""; position:absolute; width:290px; height:290px;
            right:-65px; top:-145px; border-radius:50%;
            background:radial-gradient(circle,rgba(91,224,226,.30),rgba(91,224,226,0)); }
    .hero-meta { position:relative; z-index:1; display:flex; align-items:center;
            flex-wrap:wrap; gap:8px; margin-bottom:14px; }
    .hero-title { position:relative; z-index:1; font-size:2.25rem; line-height:1.08;
            font-weight:820; letter-spacing:-.04em; }
    .hero-sub { position:relative; z-index:1; opacity:.88; font-size:1rem; margin-top:8px; }
    .hero-badge { display:inline-flex; align-items:center; gap:6px;
            background:rgba(255,255,255,.09); border:1px solid rgba(178,237,239,.28);
            border-radius:999px; padding:5px 11px; font-size:.7rem; font-weight:750;
            letter-spacing:.075em; }
    .hero-dot { width:7px; height:7px; border-radius:50%; background:#58e0cf;
            box-shadow:0 0 0 4px rgba(88,224,207,.12); }
    .scenario-copy { color:var(--muted); font-size:.92rem; margin:0 0 .5rem; }
    .stage { min-height:130px; background:rgba(255,255,255,.92); border:1px solid var(--line);
             border-radius:17px; padding:16px; box-shadow:0 7px 24px rgba(8,43,61,.055); }
    .stage-done { border-top:4px solid var(--cyan); }
    .stage-pending { border-top:4px solid #c6d4d8; opacity:.72; }
    .stage-name { color:var(--navy); font-weight:780; font-size:.92rem; }
    .stage-state { margin-top:15px; font-weight:820; }
    .stage-time { color:var(--muted); font-size:.78rem; margin-top:6px; }
    .risk-card { background:linear-gradient(145deg,#fff8ec,#fffdf8);
                 border:1px solid #f5d59f; border-left:6px solid var(--amber);
                 border-radius:17px; padding:20px 22px; box-shadow:var(--shadow); }
    .risk-card.clear { background:linear-gradient(145deg,#effaf7,#fbfffe);
                 border-color:#bce7dc; border-left-color:var(--green); }
    .ai-card { background:linear-gradient(145deg,#f3f8ff,#fbfdff);
                 border:1px solid #cadcf2; border-radius:17px; padding:21px;
                 box-shadow:0 8px 28px rgba(42,91,145,.07); }
    .label { color:var(--muted); text-transform:uppercase; letter-spacing:.08em;
             font-size:.72rem; font-weight:750; margin-bottom:5px; }
    .value { color:var(--navy); font-size:1rem; font-weight:700; }
    .priority-CRITICAL,.priority-HIGH { color:#b12f35; }
    .priority-MEDIUM { color:#9b6500; }
    .footer-note { color:var(--muted); font-size:.78rem; margin-top:24px; }
    div.stButton > button { min-height:3.15rem; border-radius:14px; font-weight:760; width:100%;
         background:rgba(255,255,255,.94); color:var(--navy); border:1px solid #b8cdd4;
         box-shadow:0 5px 18px rgba(8,43,61,.065); transition:all .18s ease; }
    div.stButton > button:hover { transform:translateY(-2px); background:var(--navy);
         color:#fff; border-color:var(--navy); box-shadow:0 10px 24px rgba(8,43,61,.16); }
    div.stButton > button:focus { color:#fff; background:#123f4c; border-color:var(--cyan); }
    .pipeline-grid { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:10px;
                     margin:10px 0 8px; }
    .pipe-card { position:relative; background:rgba(255,255,255,.94); border:1px solid var(--line);
                 border-radius:16px; padding:15px 16px 14px; min-height:120px;
                 box-shadow:0 7px 22px rgba(8,43,61,.05); }
    .pipe-card.done { border-top:4px solid var(--cyan); }
    .pipe-card.clear { border-top:4px solid #22a68b; }
    .pipe-card.alert { border-top:4px solid var(--amber); }
    .pipe-card.waiting { border-top:4px solid #c8d4d8; }
    .pipe-step { color:#71848d; font-size:.68rem; font-weight:800; letter-spacing:.1em; }
    .pipe-name { color:var(--navy); font-size:.94rem; font-weight:800; margin-top:5px; }
    .pipe-place { color:#71848d; font-size:.72rem; margin-top:2px; }
    .pipe-state { display:inline-block; margin-top:10px; padding:3px 8px; border-radius:999px;
                  background:#eef5f7; color:#214857; font-size:.7rem; font-weight:800; }
    .pipeline-note { background:linear-gradient(90deg,#eef8fa,#f8fcfd);
                     border:1px solid #d7e9ed; border-left:4px solid var(--cyan);
                     border-radius:11px; color:#405963; padding:12px 14px; font-size:.82rem; }
    @media (max-width:900px) {
        .block-container { padding:1rem 1rem 2.5rem !important; }
        .hero { padding:24px 22px 26px; border-radius:19px; }
        .hero-title { font-size:1.8rem; }
        .pipeline-grid { grid-template-columns:repeat(2,minmax(0,1fr)); }
    }
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
  <div class="hero-meta">
    <div class="hero-badge"><span class="hero-dot"></span> LIVE OPERATIONS VIEW</div>
    <div class="hero-badge">SYNTHETIC DATA ONLY</div>
    <div class="hero-badge">ADVISORY · HUMAN DECISION</div>
  </div>
  <div class="hero-title">BagGuard Operations Console</div>
  <div class="hero-sub">Real-Time Baggage Connection Risk Investigator</div>
</div>
""",
        unsafe_allow_html=True,
    )


def _render_scenario_controls() -> None:
    st.markdown(
        '<div class="scenario-copy">Select an operating condition to follow one bag '
        'through the live data path.</div>',
        unsafe_allow_html=True,
    )
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
    _render_journey(snapshot)
    left, right = st.columns([1, 2])
    with left:
        _render_risk(snapshot)
    with right:
        _render_investigation(snapshot)
    st.markdown(
        f"""<div class="footer-note">Region: {service().region_name} · Data access: controlled adapter ·
        Updated: {datetime.now().astimezone().strftime('%H:%M:%S %Z')} ·
        Recommendations are advisory; no airline or airport system is modified.</div>""",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
