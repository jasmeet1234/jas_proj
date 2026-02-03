import os
import time
import math
import pandas as pd
import streamlit as st
import clickhouse_connect
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px  # ADDED: Required for instance utilization heatmap

# ============================================================
# Streamlit Configuration & Custom CSS
# ============================================================
st.set_page_config(
    page_title="RedShift Pulse Analytics",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for enhanced visuals
st.markdown("""
<style>
    .project-header {
        font-size: 2.5rem;
        font-weight: 800;
        background: linear-gradient(90deg, #ff4b4b, #ff8f8f);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0;
    }
    .project-subheader {
        color: #6c757d;
        font-size: 1.1rem;
        margin-top: 0.5rem;
        margin-bottom: 2rem;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================
# Project Header (Task 10)
# ============================================================
st.markdown('<div class="project-header">🔴 RedShift Pulse Analytics</div>', unsafe_allow_html=True)
st.markdown('<div class="project-subheader">Real-time ClickHouse metric replay and bottleneck detection for Redshift clusters. Visualize pressure points, throughput trends, and system anomalies as they happened.</div>', unsafe_allow_html=True)

# ============================================================
# ClickHouse Connection
# ============================================================
CH_HOST = os.environ.get("CH_HOST", "wkixlqz135.eu-central-1.aws.clickhouse.cloud")
CH_PORT = int(os.environ.get("CH_PORT", "8443"))
CH_USER = os.environ.get("CH_USER", "default")
CH_PASSWORD = os.environ.get("CH_PASSWORD", "S1uOU_hkoUkDc")
CH_DB = os.environ.get("CH_DB", "default")

if not CH_PASSWORD:
    st.error("Missing CH_PASSWORD environment variable.")
    st.stop()

@st.cache_resource(show_spinner=False)
def get_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER,
        password=CH_PASSWORD, database=CH_DB,
        secure=True, verify=False
    )

try:
    client = get_client()
    client.command("SELECT 1")
except Exception as e:
    st.error(f"Failed to connect to ClickHouse: {e}")
    st.stop()

# ============================================================
# Helpers
# ============================================================
def safe_query_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    try:
        return client.query_df(sql, parameters=params or {})
    except Exception as e:
        st.error("❌ ClickHouse query failed.")
        st.exception(e)
        st.stop()

def safe_query_rows(sql: str, params: dict | None = None):
    try:
        return client.query(sql, parameters=params or {}).result_rows
    except Exception as e:
        st.error("❌ ClickHouse query failed.")
        st.exception(e)
        st.stop()

def get_min_max_bucket_for_run(rid: str):
    rows = safe_query_rows(
        "SELECT min(bucket_start), max(bucket_start) FROM system_metrics_5min WHERE run_id = %(run_id)s",
        {"run_id": rid}
    )
    return (rows[0][0], rows[0][1]) if rows else (None, None)

def robust_mad_z(x: pd.Series) -> pd.Series:
    x = pd.to_numeric(x, errors="coerce")
    med = x.median()
    mad = (x - med).abs().median()
    if mad == 0 or pd.isna(mad): 
        return pd.Series([0.0] * len(x), index=x.index)
    return 0.6745 * (x - med) / mad

def section_header(title, info_text):
    col1, col2 = st.columns([0.97, 0.03])
    with col1:
        st.subheader(title)
    with col2:
        # Small info icon matching other parts of the app
        st.markdown(f"<div title='{info_text}' style='cursor:help; font-size:0.9rem; color:#6c757d; margin-top:0.5rem;'>ⓘ</div>", unsafe_allow_html=True)
    return col1

# ============================================================
# Session State Initialization
# ============================================================
if "started" not in st.session_state:
    st.session_state.started = False
if "run_state" not in st.session_state:
    st.session_state.run_state = {}
# Default to provisioned only (strict separation)
if "deployment_filter" not in st.session_state:
    st.session_state.deployment_filter = ["provisioned"]

# ============================================================
# Data Availability Check
# ============================================================
try:
    runs_df = safe_query_df("""
        SELECT run_id, max(bucket_start) as last_bucket 
        FROM system_metrics_5min 
        GROUP BY run_id 
        ORDER BY last_bucket DESC 
        LIMIT 30
    """)
except Exception as e:
    st.error("Failed to fetch runs. Check connection and table 'system_metrics_5min'.")
    st.stop()

if runs_df.empty:
    st.warning("⚠️ No runs found in system_metrics_5min yet. Start ingestion first.")
    st.stop()

# ============================================================
# Sidebar Configuration
# ============================================================
with st.sidebar:
    st.header("⚙️ Control Deck")
    
    # Task 8: Deployment Type Buttons
    st.subheader("Deployment Target")
    dcol1, dcol2 = st.columns(2)
    
    current_deps = st.session_state.deployment_filter
    prov_active = "provisioned" in current_deps
    serv_active = "serverless" in current_deps
    
    if dcol1.button("🖥️ Provision", width = "stretch", 
                    type="primary" if prov_active else "secondary"):
        if prov_active and len(current_deps) > 1:
            current_deps.remove("provisioned")
            st.session_state.deployment_filter = current_deps
            st.rerun()
        elif not prov_active:
            current_deps.append("provisioned")
            st.session_state.deployment_filter = current_deps
            st.rerun()
            
    if dcol2.button("☁️ Serverless", width = "stretch",
                    type="primary" if serv_active else "secondary"):
        if serv_active and len(current_deps) > 1:
            current_deps.remove("serverless")
            st.session_state.deployment_filter = current_deps
            st.rerun()
        elif not serv_active:
            current_deps.append("serverless")
            st.session_state.deployment_filter = current_deps
            st.rerun()
    
    if not st.session_state.deployment_filter:
        st.error("Select at least one deployment type.")
        st.stop()
    
    st.markdown("---")
    
    # Task 4: Run Controls Relocation
    st.subheader("⏯ Replay Controls")
    
    run_id = st.selectbox("Select Run ID", runs_df["run_id"].tolist())
    
    if run_id not in st.session_state.run_state:
        st.session_state.run_state[run_id] = {"cursor_bucket": None, "last_tick_wall": None}
    
    state = st.session_state.run_state[run_id]
    start_b, end_b = get_min_max_bucket_for_run(run_id)
    
    if not start_b:
        st.warning("No data for this run yet.")
        st.stop()
    
    ctrl1, ctrl2, ctrl3 = st.columns([1, 1, 1])
    with ctrl1:
        if st.button("▶️ Start", width = "stretch", 
                    type="primary" if not st.session_state.started else "secondary"):
            st.session_state.started = True
            if state["cursor_bucket"] is None:
                state["cursor_bucket"] = start_b
            state["last_tick_wall"] = time.time()
            st.rerun()
    with ctrl2:
        if st.button("⏸ Stop", width = "stretch"):
            st.session_state.started = False
            st.rerun()
    with ctrl3:
        if st.button("🔄 Reset", width = "stretch"):
            st.session_state.started = False
            state["cursor_bucket"] = start_b
            state["last_tick_wall"] = None
            st.rerun()
    
    st.caption(f"Speed: 5s | Status: {'🟢 Running' if st.session_state.started else '⏸ Stopped'}")
    st.markdown("---")
    
    # Task 2: All Sliders in Sidebar - Updated with small info icon
    with st.expander("🔧 Analysis Parameters", expanded=True):
        # Small info icon for the section
        col_param, col_info = st.columns([0.95, 0.05])
        with col_param:
            st.markdown("**Configuration**")
        with col_info:
            st.markdown("<div title='Adjust analysis sensitivity and thresholds' style='cursor:help; font-size:0.9rem; color:#6c757d;'>ⓘ</div>", unsafe_allow_html=True)
        
        smooth_window = st.slider("Smoothing Window", 1, 24, 6, key="smooth_win")
        
        st.markdown("**Thresholds**")
        thq1, thq2 = st.columns(2)
        with thq1:
            TH_QUEUE_HIGH = st.slider("Queue High", 0.0, 1.0, 0.60, 0.01, key="q_high")
            TH_QUEUE_MED = st.slider("Queue Med", 0.0, 1.0, 0.30, 0.01, key="q_med")
        with thq2:
            TH_SPILL_HIGH = st.slider("Spill High", 0.0, 1.0, 0.60, 0.01, key="s_high")
            TH_SPILL_MED = st.slider("Spill Med", 0.0, 1.0, 0.30, 0.01, key="s_med")
        
        z_thresh = st.slider("Anomaly Sensitivity (|z| ≥)", 2.0, 8.0, 3.5, 0.5, key="z_thresh")

# ============================================================
# Replay Logic (Task 9)
# ============================================================
TICK_SECONDS = 5
BUCKET_STEP_MINUTES = 5

if st.session_state.started and state.get("cursor_bucket"):
    now = time.time()
    last = state.get("last_tick_wall", 0) or 0
    if now - last >= TICK_SECONDS:
        cursor = pd.Timestamp(state["cursor_bucket"])
        next_cursor = cursor + pd.Timedelta(minutes=BUCKET_STEP_MINUTES)
        
        if end_b and pd.Timestamp(next_cursor) > pd.Timestamp(end_b):
            next_cursor = end_b
        
        state["cursor_bucket"] = next_cursor.to_pydatetime() if hasattr(next_cursor, 'to_pydatetime') else next_cursor
        state["last_tick_wall"] = now
        st.rerun()

if not state.get("cursor_bucket"):
    state["cursor_bucket"] = start_b

# ============================================================
# Data Query (Task 7: Removed max rows slider)
# ============================================================
dep_filter = st.session_state.deployment_filter

schema_rows = safe_query_rows("DESCRIBE TABLE system_metrics_5min")
cols = [r[0] for r in schema_rows]
has_qp = "queue_pressure" in cols
has_sp = "spill_pressure" in cols
has_thr = "throughput_mb_s" in cols

select_parts = [
    "bucket_start", "deployment_type",
    "max(running_count) AS running_count",
    "max(queued_count) AS queued_count",
    ("max(queue_pressure) AS queue_pressure" if has_qp else "0.0 AS queue_pressure"),
    ("max(spill_pressure) AS spill_pressure" if has_sp else "0.0 AS spill_pressure"),
    ("max(throughput_mb_s) AS throughput_mb_s" if has_thr else "0.0 AS throughput_mb_s")
]

dep_clause = ""
params = {
    "run_id": run_id,
    "start_bucket": start_b,
    "cursor_bucket": state["cursor_bucket"]
}

if len(dep_filter) == 1:
    dep_clause = "AND deployment_type = %(dep)s"
    params["dep"] = dep_filter[0]
elif len(dep_filter) == 0:
    st.warning("Select at least one deployment type.")
    st.stop()

q = f"""
SELECT {', '.join(select_parts)}
FROM system_metrics_5min
WHERE run_id = %(run_id)s 
  AND bucket_start >= %(start_bucket)s 
  AND bucket_start <= %(cursor_bucket)s
{dep_clause}
GROUP BY bucket_start, deployment_type
ORDER BY bucket_start DESC
LIMIT 5000
"""

try:
    df = safe_query_df(q, params)
    if df is None or df.empty:
        st.info("⏳ No data available in current replay window yet...")
        if st.session_state.started:
            time.sleep(TICK_SECONDS)
            st.rerun()
        st.stop()
except Exception as e:
    st.error(f"Query error: {e}")
    st.stop()

df["bucket_start"] = pd.to_datetime(df["bucket_start"], utc=True)
df_plot = df.sort_values("bucket_start")

# ============================================================
# Task 1: Combined Pressure Gauges with Pop-out - SEPARATED BY DEPLOYMENT
# ============================================================
st.markdown("---")
section_header("🎚️ System Pressure Monitor", 
                "Combined view of Queue and Spill pressure. Expand below for individual metrics.")

latest_time = df_plot["bucket_start"].max()
latest = df_plot[df_plot["bucket_start"] == latest_time]

# Define colors for strict separation
deployment_colors = {
    "provisioned": {"primary": "#3498db", "secondary": "#85c1e9", "bg": "#ebf5fb"},
    "serverless": {"primary": "#e67e22", "secondary": "#f5b7b1", "bg": "#fdedec"}
}

if not latest.empty:
    gauge_cols = st.columns(len(dep_filter))
    
    for idx, dep in enumerate(dep_filter):
        with gauge_cols[idx]:
            dep_data = latest[latest["deployment_type"] == dep]
            if dep_data.empty:
                st.warning(f"No {dep} data")
                continue
            
            row = dep_data.iloc[0]
            q_val = float(row.get("queue_pressure", 0) or 0)
            s_val = float(row.get("spill_pressure", 0) or 0)
            max_pressure = (q_val+ s_val)
            
            dep_color = deployment_colors[dep]["primary"]
            dep_bg = deployment_colors[dep]["bg"]
            
            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number",
                value=max_pressure,
                number={'valueformat': ".2f", 'font': {'size': 36, 'color': dep_color}},
                title={'text': f"Total System Pressure<br><span style='font-size:0.6em;color:{dep_color}'>{dep.title()}</span>"},
                gauge={
                    'axis': {'range': [0, 1]},
                    'bar': {'color': dep_color, 'thickness': 0.8},
                    'steps': [
                        {'range': [0, 0.3], 'color': '#d5f5e3'},
                        {'range': [0.3, 0.6], 'color': '#fcf3cf'},
                        {'range': [0.6, 1], 'color': '#fadbd8'}
                    ],
                    'threshold': {'line': {'color': dep_color, 'width': 3}, 'thickness': 0.75, 'value': max_pressure}
                }
            ))
            fig_gauge.update_layout(
                height=250, 
                margin=dict(l=20, r=20, t=50, b=20),
                paper_bgcolor=dep_bg
            )
            
            st.plotly_chart(fig_gauge, width = "stretch")
            
            # Pop-out details
            with st.expander("🔍 View Detailed Components"):
                dcol1, dcol2 = st.columns(2)
                dcol1.metric("Queue Pressure", f"{q_val:.2f}", 
                            delta=f"{q_val-max_pressure:.2f}" if q_val != max_pressure else None,
                            delta_color="off")
                dcol2.metric("Spill Pressure", f"{s_val:.2f}",
                            delta=f"{s_val-max_pressure:.2f}" if s_val != max_pressure else None,
                            delta_color="off")
                
                hist = df_plot[df_plot["deployment_type"] == dep].tail(30)
                if not hist.empty:
                    fig_mini = go.Figure()
                    fig_mini.add_trace(go.Scatter(x=hist["bucket_start"], y=hist["queue_pressure"], 
                                                 name="Queue", line=dict(color='#3498db'), fill='tozeroy'))
                    fig_mini.add_trace(go.Scatter(x=hist["bucket_start"], y=hist["spill_pressure"], 
                                                 name="Spill", line=dict(color='#e67e22'), fill='tozeroy'))
                    fig_mini.update_layout(
                        height=200, 
                        margin=dict(l=0, r=0, t=20, b=0), 
                        showlegend=True,
                        legend=dict(orientation="h", yanchor="bottom", y=1.02),
                        paper_bgcolor=dep_bg
                    )
                    st.plotly_chart(fig_mini, width = "stretch")

# ============================================================
# Trends Section - STRICTLY SEPARATED
# ============================================================
st.markdown("---")
section_header("📈 Performance Trends", 
                "Smoothed area charts showing pressure evolution over time by deployment type.")

for m in ["queue_pressure", "spill_pressure", "throughput_mb_s"]:
    if m in df_plot.columns:
        df_plot[m] = pd.to_numeric(df_plot[m], errors="coerce").fillna(0)
        df_plot[f"{m}_smooth"] = df_plot.groupby("deployment_type")[m].transform(
            lambda x: x.rolling(smooth_window, min_periods=1).median()
        )

# Create separate trend sections for each deployment type
for dep in dep_filter:
    dep_data = df_plot[df_plot["deployment_type"] == dep]
    if dep_data.empty:
        continue
    
    dep_color = deployment_colors[dep]["primary"]
    dep_bg = deployment_colors[dep]["bg"]
    
    st.markdown(f"**{dep.title()} Trends**")
    
    fig_trends = make_subplots(
        rows=3, cols=1, 
        shared_xaxes=True, 
        vertical_spacing=0.05,
        subplot_titles=("Queue Pressure", "Spill Pressure", "Throughput (MB/s)")
    )

    metrics = ["queue_pressure_smooth", "spill_pressure_smooth", "throughput_mb_s_smooth"]
    for i, metric in enumerate(metrics, 1):
        if metric in dep_data.columns:
            fig_trends.add_trace(
                go.Scatter(
                    x=dep_data["bucket_start"], 
                    y=dep_data[metric], 
                    name=dep.title(),
                    fill='tozeroy',
                    line=dict(color=dep_color, width=2),
                    fillcolor=deployment_colors[dep]["secondary"],
                    showlegend=(i == 1)
                ),
                row=i, col=1
            )

    fig_trends.update_layout(
        height=600, 
        hovermode="x unified",
        margin=dict(l=60, r=40, t=80, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        paper_bgcolor=dep_bg,
        plot_bgcolor=dep_bg
    )

    st.plotly_chart(fig_trends, width = "stretch")

# ============================================================
# Bottleneck Attribution - STRICTLY SEPARATED
# ============================================================
st.markdown("---")
section_header("🧠 Bottleneck Attribution", 
                "Categorization of system states based on pressure thresholds.")

tp_p20 = float(df_plot["throughput_mb_s"].quantile(0.2)) if "throughput_mb_s" in df_plot.columns else 0.0
rc_p80 = float(df_plot["running_count"].quantile(0.8)) if "running_count" in df_plot.columns else 0.0

def categorize(r):
    qp = float(r.get("queue_pressure", 0) or 0)
    sp = float(r.get("spill_pressure", 0) or 0)
    tp = float(r.get("throughput_mb_s", 0) or 0)
    rc = float(r.get("running_count", 0) or 0)
    
    if sp >= TH_SPILL_HIGH: return "Memory Bound"
    if qp >= TH_QUEUE_HIGH and sp < TH_SPILL_MED: return "Queue Bound"
    if tp <= tp_p20 and rc >= rc_p80 and qp < TH_QUEUE_MED and sp < TH_SPILL_MED: 
        return "Scan/CPU Bound"
    if sp >= TH_SPILL_MED: return "Memory Pressure"
    if qp >= TH_QUEUE_MED: return "Queue Pressure"
    return "Healthy"

df_plot["category"] = df_plot.apply(categorize, axis=1)

# Separate bottleneck charts for each deployment
bottleneck_cols = st.columns(len(dep_filter))
for idx, dep in enumerate(dep_filter):
    with bottleneck_cols[idx]:
        dep_data = df_plot[df_plot["deployment_type"] == dep]
        dep_counts = dep_data.groupby("category").size().reset_index(name="count")
        
        if dep_counts.empty:
            st.info(f"No bottleneck data for {dep}.")
            continue
        
        dep_color = deployment_colors[dep]["primary"]
        dep_bg = deployment_colors[dep]["bg"]
        
        color_map = {
            "Memory Bound": "#c0392b",
            "Queue Bound": "#e67e22", 
            "Scan/CPU Bound": "#f39c12",
            "Memory Pressure": "#f1c40f",
            "Queue Pressure": "#3498db",
            "Healthy": "#27ae60"
        }
        
        fig_bottleneck = go.Figure()
        
        fig_bottleneck.add_trace(go.Bar(
            y=dep_counts["category"],
            x=dep_counts["count"],
            orientation='h',
            marker=dict(
                color=[color_map.get(c, "#95a5a6") for c in dep_counts["category"]],
                line=dict(color='rgba(0,0,0,0.2)', width=1)
            ),
            text=dep_counts["count"],
            textposition='outside'
        ))
        
        fig_bottleneck.update_layout(
            height=400,
            margin=dict(l=150, r=40, t=40, b=40),
            yaxis=dict(title=""),
            xaxis=dict(title="Bucket Count"),
            title=f"{dep.title()} Bottlenecks",
            paper_bgcolor=dep_bg,
            plot_bgcolor=dep_bg
        )
        
        st.plotly_chart(fig_bottleneck, width = "stretch")

# ============================================================
# Task 5: Anomaly Detection - TABLE ONLY (REMOVED HEATMAP AND TIME-SERIES)
# ============================================================
st.markdown("---")
section_header("🚨 Anomaly Detection", 
                "Table showing anomalous buckets based on deviation intensity (z-score).")

for m in ["queue_pressure", "spill_pressure", "throughput_mb_s"]:
    if m in df_plot.columns:
        df_plot[f"z_{m}"] = df_plot.groupby("deployment_type")[m].transform(robust_mad_z)

z_cols = [c for c in ["z_queue_pressure", "z_spill_pressure", "z_throughput_mb_s"] if c in df_plot.columns]
if z_cols:
    df_plot["abs_z_max"] = df_plot[z_cols].abs().max(axis=1)
    
    # Filter anomalies
    anomalies = df_plot[df_plot["abs_z_max"] >= z_thresh].copy()
    
    anom_count = len(anomalies)
    if anom_count > 0:
        st.warning(f"⚠️ {anom_count} anomalous buckets detected (threshold: ≥{z_thresh}σ)")
        
        # Display only the table, no visualizations
        display_cols = ["bucket_start", "deployment_type", "queue_pressure", "spill_pressure", 
                       "throughput_mb_s", "abs_z_max", "category"]
        available_cols = [c for c in display_cols if c in anomalies.columns]
        
        anomalies_display = anomalies[available_cols].sort_values("bucket_start", ascending=False)
        st.dataframe(anomalies_display, width = "stretch", hide_index=True)
    else:
        st.success("✅ No anomalies detected at current sensitivity")

# ============================================================
# ADDED FROM CODE 2: Instance Utilization Heatmap + Prediction - SOFT COLORS, NO WHITE
# ============================================================
st.markdown("---")
section_header("🖥️ Instance Utilization & Prediction", 
                "Heatmap of top 15 active instances with utilization predictions.")

# Soft, light color schemes (no white)
utilization_colors = {
    "provisioned": {
        "heat_scale": [[0, "#e8f4f8"], [0.5, "#74b9ff"], [1, "#0984e3"]],  # Soft blues
        "bg": "#f0f9ff",
        "line": "#74b9ff"
    },
    "serverless": {
        "heat_scale": [[0, "#fff3e0"], [0.5, "#ffb74d"], [1, "#f57c00"]],  # Soft oranges
        "bg": "#fff8e1",
        "line": "#ffb74d"
    }
}

def render_instance_section(dep: str, cursor_bucket):
    """Render instance utilization heatmap and prediction chart for a deployment type."""
    top_inst_q = """
    SELECT
      instance_id,
      util_latest
    FROM instance_top_active
    WHERE run_id = %(run_id)s
      AND deployment_type = %(dep)s
    ORDER BY util_latest DESC
    LIMIT 15
    """
    
    util_q = """
    SELECT
      bucket_start,
      deployment_type,
      instance_id,
      util,
      util_pred,
      util_residual
    FROM instance_util_pred_5min
    WHERE run_id = %(run_id)s
      AND bucket_start <= %(cursor_bucket)s
      AND deployment_type = %(dep)s
      AND instance_id IN %(ids)s
    ORDER BY bucket_start ASC
    """
    
    try:
        # Get top instances by latest utilization
        tops = safe_query_df(top_inst_q, {"run_id": run_id, "dep": dep})
        if tops.empty:
            st.info(f"No instance utilization data available for {dep}.")
            return

        instance_ids = tops["instance_id"].astype(int).tolist()
        
        # Get historical utilization data for these instances
        dfi = safe_query_df(util_q, {
            "run_id": run_id,
            "cursor_bucket": cursor_bucket,
            "dep": dep,
            "ids": tuple(instance_ids)
        })
        
        if dfi.empty:
            st.info(f"No utilization history for {dep} instances.")
            return

        # Data type conversions
        dfi["bucket_start"] = pd.to_datetime(dfi["bucket_start"], utc=True, errors="coerce")
        dfi["instance_id"] = pd.to_numeric(dfi["instance_id"], errors="coerce").astype("Int64")

        st.markdown(f"**{dep.title()} Instances**")
        
        dep_colors = utilization_colors[dep]
        
        # Create utilization heatmap (latest 120 buckets for performance)
        heat = dfi.pivot_table(
            index="bucket_start", 
            columns="instance_id", 
            values="util", 
            aggfunc="max"
        ).sort_index()
        
        heat_tail = heat.tail(120)
        
        # Use soft colors, no white
        fig_util_heat = go.Figure(data=go.Heatmap(
            z=heat_tail.T.values,
            x=[str(x) for x in heat_tail.index],
            y=list(heat_tail.columns),
            colorscale=dep_colors["heat_scale"],
            zmin=0, zmax=1,
            showscale=True,
            colorbar=dict(title="Util"),
            hoverongaps=False
        ))
        
        fig_util_heat.update_layout(
            height=380, 
            margin=dict(l=30, r=10, t=30, b=30),
            title=f"Utilization Heatmap - {dep.title()}",
            paper_bgcolor=dep_colors["bg"],
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=False)
        )
        st.plotly_chart(fig_util_heat, width = "stretch")

        # Prediction vs Actual line chart for selected instance
        selected_instance = st.selectbox(
            f"Select {dep} instance for prediction view", 
            instance_ids, 
            key=f"pick_{dep}_{run_id}"
        )
        
        instance_data = dfi[dfi["instance_id"] == selected_instance].sort_values("bucket_start")
        
        if len(instance_data) >= 5:
            fig_pred = go.Figure()
            
            fig_pred.add_trace(go.Scatter(
                x=instance_data["bucket_start"],
                y=instance_data["util"],
                mode='lines',
                name='Actual',
                line=dict(color=dep_colors["line"], width=2),
                fill='tozeroy',
                fillcolor=dep_colors["heat_scale"][0][1]  # Use light color from scale
            ))
            
            fig_pred.add_trace(go.Scatter(
                x=instance_data["bucket_start"],
                y=instance_data["util_pred"],
                mode='lines',
                name='Predicted',
                line=dict(color=dep_colors["heat_scale"][2][1], width=2, dash='dash')  # Darker color
            ))
            
            fig_pred.update_layout(
                height=220,
                margin=dict(l=0, r=0, t=20, b=0),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                paper_bgcolor=dep_colors["bg"],
                plot_bgcolor=dep_colors["bg"],
                showlegend=True
            )
            
            st.caption(f"Predicted vs Actual Utilization — Instance {selected_instance}")
            st.plotly_chart(fig_pred, width = "stretch")
        else:
            st.caption("Insufficient data points for prediction display (minimum 5 required).")
            
    except Exception as e:
        st.error(f"Error loading instance data for {dep}: {e}")

# Render instance sections for selected deployment types - STRICTLY SEPARATED
for dep in dep_filter:
    render_instance_section(dep, state["cursor_bucket"])



st.sidebar.subheader("View options")
SHOW_REPLAY_TABLE = st.sidebar.toggle("Show replay table (raw/debug)", value=False)

if SHOW_REPLAY_TABLE:
    st.subheader("🧾 Replay table (raw/debug)")
    st.dataframe(df, use_container_width=True, height=360)
# ============================================================
# Auto-refresh Logic
# ============================================================
if st.session_state.started:
    time.sleep(0.1)
    st.rerun()