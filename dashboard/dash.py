"""
IoT Health Intelligence Dashboard - Enhanced Internal Analysis Tool

Reads Gold Parquet data from S3. Full analytical dashboard with:
- Collapsible filter sections
- Rich data tables with sorting/export
- Multi-chart analysis panels
- MEWS clinical scoring
- Fall detection analytics
- Patient comparison tools

Dependencies:
    pip install streamlit pandas pyarrow boto3 plotly python-dotenv

Run:
    streamlit run scripts/dashboard.py
"""

import io
import os
from datetime import datetime, timezone

import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()

AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN     = os.getenv("AWS_SESSION_TOKEN")
AWS_REGION            = os.getenv("AWS_REGION", "us-west-2")
S3_BUCKET_NAME        = os.getenv("S3_BUCKET_NAME")

GOLD_VITALS   = "gold/vitals_summary/"
GOLD_MOVEMENT = "gold/movement_summary/"

# ---------------------------------------------------------------------------
# Page setup
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Health Intelligence Dashboard",
    page_icon="⚕",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=Syne:wght@400;600;700;800&display=swap');

/* ── Theme tokens — auto-switch via prefers-color-scheme ── */
:root {
    --bg:          #f0f4f8;
    --card:        #ffffff;
    --sidebar-bg:  #f8fafc;
    --border:      #dde3ec;
    --border-hover:#b0bcd0;
    --text-primary:#1a2740;
    --text-muted:  #5a7290;
    --text-faint:  #9ab0c8;
    --accent:      #1d6fa4;
    --accent-soft: #e8f2fa;
    --grid:        #e8edf5;
    --hover-bg:    #eef4fb;
    --chart-bg:    #ffffff;
    --chart-paper: #f0f4f8;
    --hover-card:  rgba(29,111,164,0.08);
    /* semantic — same in both modes */
    --red:   #dc2626; --red-bg:    #fef2f2; --red-text:   #991b1b;
    --amber: #d97706; --amber-bg:  #fffbeb; --amber-text: #92400e;
    --green: #16a34a; --green-bg:  #f0fdf4; --green-text: #166534;
    --blue:  #2563eb; --blue-bg:   #eff6ff; --blue-text:  #1e40af;
}
@media (prefers-color-scheme: dark) {
    :root {
        --bg:          #060c14;
        --card:        #080e1a;
        --sidebar-bg:  #080e1a;
        --border:      #0f1f35;
        --border-hover:#1d4e7a;
        --text-primary:#c8dff0;
        --text-muted:  #4a6280;
        --text-faint:  #1d3a52;
        --accent:      #1d6fa4;
        --accent-soft: #0a1829;
        --grid:        #0f1f35;
        --hover-bg:    #0a1829;
        --chart-bg:    #080e1a;
        --chart-paper: #060c14;
        --hover-card:  rgba(29,111,164,0.12);
        --red-bg:    #450a0a; --red-text:   #fca5a5;
        --amber-bg:  #431407; --amber-text: #fed7aa;
        --green-bg:  #052e16; --green-text: #4ade80;
        --blue-bg:   #0c1a3a; --blue-text:  #93c5fd;
    }
}
/* Allow manual override via data-theme attribute */
[data-theme="light"] {
    --bg:#f0f4f8; --card:#ffffff; --sidebar-bg:#f8fafc;
    --border:#dde3ec; --border-hover:#b0bcd0;
    --text-primary:#1a2740; --text-muted:#5a7290; --text-faint:#9ab0c8;
    --accent:#1d6fa4; --accent-soft:#e8f2fa; --grid:#e8edf5;
    --hover-bg:#eef4fb; --chart-bg:#ffffff; --chart-paper:#f0f4f8;
    --hover-card:rgba(29,111,164,0.08);
    --red-bg:#fef2f2; --red-text:#991b1b;
    --amber-bg:#fffbeb; --amber-text:#92400e;
    --green-bg:#f0fdf4; --green-text:#166534;
    --blue-bg:#eff6ff; --blue-text:#1e40af;
}
[data-theme="dark"] {
    --bg:#060c14; --card:#080e1a; --sidebar-bg:#080e1a;
    --border:#0f1f35; --border-hover:#1d4e7a;
    --text-primary:#c8dff0; --text-muted:#4a6280; --text-faint:#1d3a52;
    --accent:#1d6fa4; --accent-soft:#0a1829; --grid:#0f1f35;
    --hover-bg:#0a1829; --chart-bg:#080e1a; --chart-paper:#060c14;
    --hover-card:rgba(29,111,164,0.12);
    --red-bg:#450a0a; --red-text:#fca5a5;
    --amber-bg:#431407; --amber-text:#fed7aa;
    --green-bg:#052e16; --green-text:#4ade80;
    --blue-bg:#0c1a3a; --blue-text:#93c5fd;
}

*, html, body { box-sizing: border-box; }
html, body, [class*="css"] { font-family: 'DM Mono', monospace; }

.main .block-container { padding: 1.5rem 2rem; max-width: 100%; }
[data-testid="stAppViewContainer"] { background: var(--bg) !important; }
[data-testid="stSidebar"] {
    background: var(--sidebar-bg) !important;
    border-right: 1px solid var(--border);
}

[data-testid="stSidebar"] p,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] div { color: var(--text-muted) !important; font-size: 0.72rem; }
[data-testid="stSidebar"] .stMarkdown h3 {
    color: var(--accent) !important;
    font-family: 'Syne', sans-serif !important;
    font-size: 0.65rem !important; font-weight: 700 !important;
    letter-spacing: 0.18em !important; text-transform: uppercase !important;
    margin: 1.2rem 0 0.5rem 0 !important;
    padding-bottom: 0.4rem;
    border-bottom: 1px solid var(--border);
}

/* ── Theme toggle button ── */
.theme-toggle {
    display: inline-flex; align-items: center; gap: 6px;
    background: var(--card); border: 1px solid var(--border);
    border-radius: 20px; padding: 4px 12px; cursor: pointer;
    font-family: 'DM Mono', monospace; font-size: 0.62rem;
    color: var(--text-muted); transition: all 0.2s;
    margin-bottom: 0.8rem;
}
.theme-toggle:hover { border-color: var(--accent); color: var(--accent); }

/* ── Expander ── */
[data-testid="stExpander"] {
    background: var(--card) !important;
    border: 1px solid var(--border) !important;
    border-radius: 6px !important;
    margin-bottom: 0.5rem;
}
[data-testid="stExpander"] summary {
    font-family: 'DM Mono', monospace !important;
    font-size: 0.68rem !important;
    color: var(--accent) !important;
    letter-spacing: 0.1em; text-transform: uppercase;
    padding: 0.6rem 0.8rem;
}
[data-testid="stExpander"] summary:hover { color: var(--text-primary) !important; }
[data-testid="stExpander"] > div > div { padding: 0 0.8rem 0.8rem; }

/* ── KPI cards ── */
.kpi-card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 6px; padding: 1rem 1.1rem;
    position: relative; overflow: hidden; transition: border-color 0.2s;
}
.kpi-card:hover { border-color: var(--border-hover); }
.kpi-card::before {
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px;
    background: var(--kpi-accent, var(--accent));
}
.kpi-card.alert { --kpi-accent: var(--red); }
.kpi-card.warn  { --kpi-accent: var(--amber); }
.kpi-card.good  { --kpi-accent: var(--green); }
.kpi-label {
    font-size: 0.58rem; letter-spacing: 0.14em; text-transform: uppercase;
    color: var(--text-faint); margin-bottom: 0.4rem;
}
.kpi-value {
    font-family: 'Syne', sans-serif; font-size: 1.9rem; font-weight: 700;
    color: var(--text-primary); line-height: 1;
}
.kpi-value.alert { color: var(--red-text); }
.kpi-value.warn  { color: var(--amber-text); }
.kpi-value.good  { color: var(--green-text); }
.kpi-sub { font-size: 0.58rem; color: var(--text-faint); margin-top: 0.25rem; }

/* ── Alert banners ── */
.banner-critical {
    background: var(--red-bg);
    border-left: 3px solid var(--red);
    border-radius: 0 4px 4px 0; padding: 0.65rem 1rem;
    margin-bottom: 0.35rem; font-size: 0.72rem; color: var(--red-text);
    display: flex; align-items: center; gap: 0.8rem;
}
.banner-fall {
    background: var(--amber-bg);
    border-left: 3px solid var(--amber);
    border-radius: 0 4px 4px 0; padding: 0.65rem 1rem;
    margin-bottom: 0.35rem; font-size: 0.72rem; color: var(--amber-text);
    display: flex; align-items: center; gap: 0.8rem;
}
.banner-tag {
    background: rgba(128,128,128,0.12); border-radius: 3px;
    padding: 0.1rem 0.4rem; font-size: 0.62rem;
}

/* ── Section headers ── */
.section-hdr {
    font-family: 'Syne', sans-serif; font-size: 0.6rem; font-weight: 700;
    letter-spacing: 0.2em; text-transform: uppercase;
    color: var(--text-faint); border-bottom: 1px solid var(--border);
    padding-bottom: 0.4rem; margin: 1.4rem 0 0.8rem;
}

/* ── Tabs ── */
[data-testid="stTabs"] [role="tablist"] { border-bottom: 1px solid var(--border); gap: 0; }
[data-testid="stTabs"] button {
    font-family: 'DM Mono', monospace !important; font-size: 0.65rem !important;
    letter-spacing: 0.1em !important; text-transform: uppercase !important;
    color: var(--text-muted) !important; background: transparent !important;
    border: none !important; padding: 0.5rem 1.2rem !important; border-radius: 0 !important;
}
[data-testid="stTabs"] button[aria-selected="true"] {
    color: var(--accent) !important;
    border-bottom: 2px solid var(--accent) !important;
}

/* ── Dataframe ── */
[data-testid="stDataFrame"] { border: 1px solid var(--border) !important; border-radius: 6px; }
[data-testid="stDataFrame"] table { font-family: 'DM Mono', monospace; font-size: 0.7rem; }

/* ── Buttons ── */
.stButton button {
    background: var(--hover-bg) !important;
    border: 1px solid var(--border-hover) !important;
    color: var(--accent) !important;
    font-family: 'DM Mono', monospace !important; font-size: 0.65rem !important;
    letter-spacing: 0.08em; border-radius: 4px !important; padding: 0.4rem 1rem !important;
}
.stButton button:hover { background: var(--accent-soft) !important; border-color: var(--accent) !important; }
.stDownloadButton button {
    background: var(--card) !important; border: 1px solid var(--border) !important;
    color: var(--text-muted) !important; font-family: 'DM Mono', monospace !important;
    font-size: 0.62rem !important; border-radius: 3px !important; padding: 0.3rem 0.8rem !important;
}

/* ── Selects / sliders ── */
[data-testid="stSelectbox"] > div > div,
[data-testid="stMultiSelect"] > div > div {
    background: var(--card) !important; border: 1px solid var(--border) !important;
    border-radius: 4px !important; font-size: 0.7rem !important;
    color: var(--text-primary) !important;
}
[data-testid="stSlider"] [data-baseweb="slider"] [role="slider"] { background: var(--accent) !important; }

/* ── No data ── */
.no-data {
    border: 1px dashed var(--border-hover); border-radius: 6px;
    padding: 2rem; text-align: center; color: var(--text-faint);
    font-size: 0.7rem; letter-spacing: 0.1em; background: var(--card);
}

/* ── MEWS badge colors (semantic, same both modes) ── */
.mews-0 { color: #16a34a; } .mews-1 { color: #65a30d; }
.mews-2 { color: #ca8a04; } .mews-3 { color: #dc2626; }
.mews-4 { color: #991b1b; } .mews-5 { color: #7f1d1d; }

/* ── Header ── */
.dash-title {
    font-family: 'Syne', sans-serif; font-size: 1.6rem; font-weight: 800;
    color: var(--text-primary); letter-spacing: -0.01em; line-height: 1;
}
.dash-sub {
    font-family: 'DM Mono', monospace; font-size: 0.62rem;
    color: var(--text-faint); letter-spacing: 0.12em;
    text-transform: uppercase; margin-top: 0.3rem;
}
.about-body { color: var(--text-muted); font-size: 0.75rem; line-height: 1.8; max-width: 760px; }
.about-hdr  { font-family: Syne,sans-serif; font-size: 1.2rem; font-weight: 800; color: var(--text-primary); margin-bottom: 1rem; }
.about-sec  { color: var(--accent); font-size: 0.6rem; letter-spacing: 0.15em; text-transform: uppercase;
              margin: 1.2rem 0 0.5rem; border-bottom: 1px solid var(--border); padding-bottom: 0.3rem; }
.about-hl   { color: var(--text-primary); }
.about-tr   { color: var(--text-faint); }
.about-footer { margin-top:1.5rem; font-size:0.6rem; color: var(--text-faint); }
</style>

<script>
/* Detect and apply theme on load, expose toggle function */
(function(){
    function applyTheme(t){
        document.documentElement.setAttribute('data-theme', t);
        localStorage.setItem('dashTheme', t);
    }
    var saved = localStorage.getItem('dashTheme');
    if(saved){ applyTheme(saved); }
    else {
        var prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
        applyTheme(prefersDark ? 'dark' : 'light');
    }
    window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', function(e){
        if(!localStorage.getItem('dashTheme')) applyTheme(e.matches ? 'dark' : 'light');
    });
    window.toggleDashTheme = function(){
        var cur = document.documentElement.getAttribute('data-theme');
        applyTheme(cur === 'dark' ? 'light' : 'dark');
    };
})();
</script>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

@st.cache_resource
def get_s3():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
        region_name=AWS_REGION,
    )


@st.cache_data(ttl=60)
def load_gold(prefix: str, batch_date: str) -> pd.DataFrame:
    s3  = get_s3()
    pfx = f"{prefix}etl_batch_date={batch_date}/"
    pag = s3.get_paginator("list_objects_v2")
    frames = []
    for page in pag.paginate(Bucket=S3_BUCKET_NAME, Prefix=pfx):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                resp = s3.get_object(Bucket=S3_BUCKET_NAME, Key=obj["Key"])
                frames.append(pd.read_parquet(io.BytesIO(resp["Body"].read())))
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if "window_start" in df.columns:
        df["window_start"] = pd.to_datetime(df["window_start"], utc=True, errors="coerce")
    return df


@st.cache_data(ttl=300)
def available_dates(prefix: str) -> list[str]:
    s3  = get_s3()
    pag = s3.get_paginator("list_objects_v2")
    dates = set()
    for page in pag.paginate(Bucket=S3_BUCKET_NAME, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            p = cp["Prefix"]
            if "etl_batch_date=" in p:
                dates.add(p.split("etl_batch_date=")[1].rstrip("/"))
    return sorted(dates, reverse=True)


# ---------------------------------------------------------------------------
# Chart theme
# ---------------------------------------------------------------------------

# Chart colors — two palettes, toggled by is_dark()
def _is_dark():
    """Best-effort dark mode detection for Plotly chart theming."""
    return True  # default dark; Plotly charts use explicit colors anyway

BG_DARK      = "#060c14"; BG_LIGHT      = "#f0f4f8"
CARD_BG_DARK = "#080e1a"; CARD_BG_LIGHT = "#ffffff"
GRID_DARK    = "#0f1f35"; GRID_LIGHT    = "#e8edf5"
TEXT_DARK    = "#4a6280"; TEXT_LIGHT    = "#5a7290"

# Active values — charts detect theme from st session state
BG      = BG_DARK
CARD_BG = CARD_BG_DARK
GRID    = GRID_DARK
TEXT    = TEXT_DARK
MONO    = "DM Mono"

MEWS_SCALE = [
    [0.0,  "#16a34a"],
    [0.2,  "#65a30d"],
    [0.4,  "#ca8a04"],
    [0.6,  "#dc2626"],
    [0.8,  "#991b1b"],
    [1.0,  "#7f1d1d"],
]

PATIENT_PALETTE = [
    "#3b82f6","#06b6d4","#10b981","#f59e0b",
    "#8b5cf6","#ec4899","#f97316","#84cc16",
]

def hex_to_rgba(hex_color: str, alpha: float = 0.15) -> str:
    """Convert #rrggbb to rgba(r,g,b,alpha) for Plotly fillcolor."""
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"

def chart_base(title="", height=320):
    dark = st.session_state.get("theme", "dark") == "dark"
    bg      = BG_DARK      if dark else BG_LIGHT
    card_bg = CARD_BG_DARK if dark else CARD_BG_LIGHT
    grid    = GRID_DARK    if dark else GRID_LIGHT
    txt     = TEXT_DARK    if dark else TEXT_LIGHT
    hover_b = "#0a1829"    if dark else "#ffffff"
    title_c = "#2d4a66"    if dark else "#5a7290"
    return dict(
        title=dict(
            text=title,
            font=dict(family="Syne, sans-serif", size=10, color=title_c),
            x=0, y=0.97,
        ),
        height=height,
        paper_bgcolor=bg,
        plot_bgcolor=card_bg,
        font=dict(family=MONO, color=txt, size=9),
        margin=dict(l=45, r=15, t=38, b=40),
        xaxis=dict(gridcolor=grid, linecolor=grid, tickfont=dict(size=8), zeroline=False),
        yaxis=dict(gridcolor=grid, linecolor=grid, tickfont=dict(size=8), zeroline=False),
        legend=dict(
            bgcolor="rgba(0,0,0,0)", font=dict(size=8),
            orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0,
        ),
        hovermode="x unified",
        hoverlabel=dict(bgcolor=hover_b, bordercolor=grid, font=dict(family=MONO, size=9)),
    )


def no_data():
    st.markdown("<div class='no-data'>NO DATA FOR CURRENT FILTERS</div>",
                unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Sidebar — collapsible filter groups
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown(
        "<div style='font-family:Syne,sans-serif;font-size:1.1rem;"
        "font-weight:800;color:var(--text-primary);letter-spacing:-0.01em;"
        "padding:0.5rem 0 0.6rem'>⚕ Health Intel</div>",
        unsafe_allow_html=True,
    )
    # Theme toggle
    st.markdown(
        "<button class='theme-toggle' onclick='window.toggleDashTheme()'>"
        "◑ Toggle light / dark"
        "</button>",
        unsafe_allow_html=True,
    )
    if "theme" not in st.session_state:
        st.session_state["theme"] = "dark"
    col_td, col_tl = st.columns(2)
    with col_td:
        if st.button("🌙 Dark",  use_container_width=True, key="btn_dark"):
            st.session_state["theme"] = "dark"
            st.rerun()
    with col_tl:
        if st.button("☀ Light", use_container_width=True, key="btn_light"):
            st.session_state["theme"] = "light"
            st.rerun()

    # ── Date filter (always visible) ────────────────────────────────────── #
    st.markdown("### Date range")
    dates = available_dates(GOLD_VITALS)
    if not dates:
        dates = [datetime.now(timezone.utc).strftime("%Y-%m-%d")]

    selected_date = st.selectbox("Batch date", dates, index=0, label_visibility="collapsed")

    # Load raw data
    raw_vitals   = load_gold(GOLD_VITALS,   selected_date)
    raw_movement = load_gold(GOLD_MOVEMENT, selected_date)

    all_patients = sorted(set(
        list(raw_vitals["patient_id"].dropna().unique()   if not raw_vitals.empty   else []) +
        list(raw_movement["patient_id"].dropna().unique() if not raw_movement.empty else [])
    ))

    # ── Patient filter ───────────────────────────────────────────────────── #
    with st.expander("👤  Patient selection", expanded=True):
        sel_all = st.checkbox("Select all patients", value=True)
        if sel_all:
            selected_patients = all_patients
        else:
            selected_patients = st.multiselect(
                "Choose patients",
                options=all_patients,
                default=all_patients[:1] if all_patients else [],
                label_visibility="collapsed",
            )

    # ── Vitals filters ───────────────────────────────────────────────────── #
    with st.expander("💓  Vitals thresholds", expanded=False):
        hr_min, hr_max = st.slider(
            "Heart rate (bpm)", 30, 220, (30, 220), step=5
        )
        spo2_min = st.slider("Minimum SpO₂ (%)", 90, 100, 90)
        mews_min = st.selectbox(
            "MEWS alert level ≥",
            [1, 2, 3, 4, 5],
            index=1,
            format_func=lambda x: f"{x}  ({'Low' if x==1 else 'Medium' if x==2 else 'High' if x==3 else 'Critical'})",
        )

    # ── Movement filters ─────────────────────────────────────────────────── #
    with st.expander("🏃  Movement thresholds", expanded=False):
        fall_threshold = st.slider(
            "Fall SVM threshold", 10.0, 50.0, 25.0, step=0.5
        )
        show_only_falls = st.checkbox("Show only windows with falls", value=False)

    # ── Display options ──────────────────────────────────────────────────── #
    with st.expander("⚙  Display options", expanded=False):
        chart_height   = st.slider("Chart height (px)", 220, 500, 300, step=20)
        show_peak      = st.checkbox("Show peak values on charts", value=True)
        show_raw_table = st.checkbox("Show raw data tables", value=True)
        table_rows     = st.slider("Max table rows", 20, 200, 50, step=10)

    st.markdown("---")
    col_r1, col_r2 = st.columns(2)
    with col_r1:
        if st.button("↺ Refresh", width='stretch'):
            st.cache_data.clear()
            st.rerun()
    with col_r2:
        if st.button("✕ Reset", width='stretch'):
            st.rerun()

    st.markdown(
        f"<div style='font-size:0.55rem;color:var(--text-faint);margin-top:0.8rem;line-height:1.6'>"
        f"Bucket: {S3_BUCKET_NAME}<br>"
        f"Region: {AWS_REGION}<br>"
        f"Updated: {datetime.now().strftime('%H:%M:%S')}"
        f"</div>",
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Apply filters
# ---------------------------------------------------------------------------

def filter_vitals(df):
    if df.empty: return df
    df = df[df["patient_id"].isin(selected_patients)]
    if "avg_hr"   in df.columns: df = df[df["avg_hr"].between(hr_min, hr_max)]
    if "avg_spo2" in df.columns: df = df[df["avg_spo2"] >= spo2_min]
    return df.copy()


def filter_movement(df):
    if df.empty: return df
    df = df[df["patient_id"].isin(selected_patients)]
    if show_only_falls and "fall_events_detected" in df.columns:
        df = df[df["fall_events_detected"] > 0]
    return df.copy()


vdf = filter_vitals(raw_vitals)
mdf = filter_movement(raw_movement)

if not mdf.empty:
    mdf["is_fall"] = (mdf["peak_impact"] > fall_threshold).astype(int)


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

hcol1, hcol2 = st.columns([3, 1])
with hcol1:
    st.markdown(
        f"<div class='dash-title'>Patient Monitoring Intelligence</div>"
        f"<div class='dash-sub'>"
        f"Gold layer · {selected_date} · "
        f"{len(selected_patients)} patient(s) · "
        f"{len(vdf)} vitals windows · {len(mdf)} movement windows"
        f"</div>",
        unsafe_allow_html=True,
    )
with hcol2:
    # Export buttons
    if not vdf.empty:
        st.download_button(
            "⬇ Export vitals CSV",
            data=vdf.to_csv(index=False).encode(),
            file_name=f"vitals_gold_{selected_date}.csv",
            mime="text/csv",
            width='stretch',
        )
    if not mdf.empty:
        st.download_button(
            "⬇ Export movement CSV",
            data=mdf.to_csv(index=False).encode(),
            file_name=f"movement_gold_{selected_date}.csv",
            mime="text/csv",
            width='stretch',
        )


# ---------------------------------------------------------------------------
# Alert banners
# ---------------------------------------------------------------------------

if not vdf.empty and "mews_score" in vdf.columns:
    alerts = vdf[vdf["mews_score"] >= mews_min].sort_values("mews_score", ascending=False)
    for _, row in alerts.head(5).iterrows():
        ts = pd.Timestamp(row["window_start"]).strftime("%H:%M") if pd.notna(row.get("window_start")) else "—"
        st.markdown(
            f"<div class='banner-critical'>"
            f"<span>⚠</span>"
            f"<span><b>{row['patient_id']}</b></span>"
            f"<span class='banner-tag'>MEWS {int(row['mews_score'])}</span>"
            f"<span class='banner-tag'>HR {row['avg_hr']:.0f} bpm</span>"
            f"<span class='banner-tag'>SpO₂ {row['avg_spo2']:.1f}%</span>"
            f"<span class='banner-tag'>{ts}</span>"
            f"<span style='margin-left:auto;font-size:0.58rem;color:var(--red-text)'>"
            f"HR score={int(row.get('mews_hr_score',0))} · "
            f"SpO₂ score={int(row.get('mews_spo2_score',0))}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )

if not mdf.empty and "fall_events_detected" in mdf.columns:
    falls = mdf[mdf["fall_events_detected"] > 0].sort_values("fall_events_detected", ascending=False)
    for _, row in falls.head(3).iterrows():
        ts = pd.Timestamp(row["window_start"]).strftime("%H:%M") if pd.notna(row.get("window_start")) else "—"
        st.markdown(
            f"<div class='banner-fall'>"
            f"<span>🆘</span>"
            f"<span><b>{row['patient_id']}</b></span>"
            f"<span class='banner-tag'>FALL DETECTED</span>"
            f"<span class='banner-tag'>{int(row['fall_events_detected'])} event(s)</span>"
            f"<span class='banner-tag'>Peak SVM {row['peak_impact']:.1f}</span>"
            f"<span class='banner-tag'>{ts}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )


# ---------------------------------------------------------------------------
# KPI row
# ---------------------------------------------------------------------------

st.markdown("<div class='section-hdr'>Summary metrics</div>", unsafe_allow_html=True)

avg_hr_v   = f"{vdf['avg_hr'].mean():.0f}"         if not vdf.empty and "avg_hr"      in vdf.columns else "—"
avg_spo2_v = f"{vdf['avg_spo2'].mean():.1f}"       if not vdf.empty and "avg_spo2"    in vdf.columns else "—"
max_mews_v = int(vdf["mews_score"].max())           if not vdf.empty and "mews_score"  in vdf.columns else 0
total_fall = int(mdf["fall_events_detected"].sum()) if not mdf.empty and "fall_events_detected" in mdf.columns else 0
avg_act_v  = f"{mdf['avg_activity'].mean():.1f}"   if not mdf.empty and "avg_activity" in mdf.columns else "—"
peak_imp_v = f"{mdf['peak_impact'].max():.1f}"     if not mdf.empty and "peak_impact"  in mdf.columns else "—"

mews_cls  = "alert" if max_mews_v >= 3 else "warn" if max_mews_v >= 2 else "good"
falls_cls = "alert" if total_fall > 0 else "good"

kpis = [
    ("Avg Heart Rate",    avg_hr_v,           "bpm · filtered windows",   ""),
    ("Avg SpO₂",          f"{avg_spo2_v}%",   "oxygen saturation",        ""),
    ("Max MEWS",          str(max_mews_v),     "0–6 clinical scale",       mews_cls),
    ("Fall Events",       str(total_fall),     "total detected",           falls_cls),
    ("Avg Activity SVM",  avg_act_v,           "signal vector magnitude",  ""),
    ("Peak Impact SVM",   peak_imp_v,          "highest in selection",     ""),
]

cols = st.columns(6)
for col, (label, value, sub, cls) in zip(cols, kpis):
    accent = {"alert": "var(--red)", "warn": "var(--amber)", "good": "var(--green)"}.get(cls, "var(--accent)")
    col.markdown(
        f"<div class='kpi-card {cls}' style='--accent:{accent}'>"
        f"<div class='kpi-label'>{label}</div>"
        f"<div class='kpi-value {cls}'>{value}</div>"
        f"<div class='kpi-sub'>{sub}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Main tabs
# ---------------------------------------------------------------------------

tab_vitals, tab_movement, tab_compare, tab_tables, tab_about = st.tabs([
    "💓  Vitals Analysis",
    "🏃  Movement & Falls",
    "🔬  Patient Comparison",
    "📋  Data Tables",
    "ℹ  About",
])


# ============================================================================
# TAB 1 — Vitals Analysis
# ============================================================================

with tab_vitals:

    # ── Section: Time series ─────────────────────────────────────────────── #
    with st.expander("📈  Heart Rate & SpO₂ over time", expanded=True):
        if vdf.empty:
            no_data()
        else:
            tc1, tc2 = st.columns(2)

            with tc1:
                fig = go.Figure()
                for i, pid in enumerate(sorted(vdf["patient_id"].unique())):
                    p = vdf[vdf["patient_id"] == pid].sort_values("window_start")
                    c = PATIENT_PALETTE[i % len(PATIENT_PALETTE)]
                    fig.add_trace(go.Scatter(
                        x=p["window_start"], y=p["avg_hr"],
                        name=str(pid), line=dict(color=c, width=1.8),
                        mode="lines+markers", marker=dict(size=4, color=c),
                        hovertemplate=f"<b>{pid}</b><br>Avg HR: %{{y:.0f}} bpm<br>%{{x}}<extra></extra>",
                    ))
                    if show_peak and "peak_hr" in p.columns:
                        fig.add_trace(go.Scatter(
                            x=p["window_start"], y=p["peak_hr"],
                            name=f"{pid} peak", line=dict(color=c, width=0.8, dash="dot"),
                            mode="lines", opacity=0.45, showlegend=False,
                            hovertemplate=f"<b>{pid} peak</b><br>%{{y:.0f}} bpm<extra></extra>",
                        ))
                fig.add_hrect(y0=111, y1=220, fillcolor="#dc2626", opacity=0.04, line_width=0,
                              annotation_text="Tachycardia", annotation_font=dict(size=8, color="#dc2626"))
                fig.add_hrect(y0=30, y1=40, fillcolor="#dc2626", opacity=0.04, line_width=0,
                              annotation_text="Bradycardia", annotation_font=dict(size=8, color="#dc2626"),
                              annotation_position="bottom right")
                layout = chart_base("HEART RATE (bpm)", chart_height)
                layout["yaxis"]["range"] = [25, 185]
                fig.update_layout(**layout)
                st.plotly_chart(fig, width='stretch')

            with tc2:
                fig = go.Figure()
                for i, pid in enumerate(sorted(vdf["patient_id"].unique())):
                    p = vdf[vdf["patient_id"] == pid].sort_values("window_start")
                    c = PATIENT_PALETTE[i % len(PATIENT_PALETTE)]
                    fig.add_trace(go.Scatter(
                        x=p["window_start"], y=p["avg_spo2"],
                        name=str(pid), line=dict(color=c, width=1.8),
                        mode="lines+markers", marker=dict(size=4, color=c),
                        hovertemplate=f"<b>{pid}</b><br>SpO₂: %{{y:.1f}}%<extra></extra>",
                    ))
                fig.add_hrect(y0=90, y1=93, fillcolor="#d97706", opacity=0.06, line_width=0,
                              annotation_text="Low SpO₂", annotation_font=dict(size=8, color="#d97706"))
                fig.add_hrect(y0=88, y1=90, fillcolor="#dc2626", opacity=0.08, line_width=0)
                layout = chart_base("SpO₂ SATURATION (%)", chart_height)
                layout["yaxis"]["range"] = [87, 101]
                fig.update_layout(**layout)
                st.plotly_chart(fig, width='stretch')

    # ── Section: MEWS breakdown ──────────────────────────────────────────── #
    with st.expander("🏥  MEWS Score Analysis", expanded=True):
        if vdf.empty or "mews_score" not in vdf.columns:
            no_data()
        else:
            mc1, mc2 = st.columns([2, 1])

            with mc1:
                fig = go.Figure()
                mews_color_map = {
                    0: "#16a34a", 1: "#65a30d", 2: "#ca8a04",
                    3: "#dc2626", 4: "#991b1b", 5: "#7f1d1d"
                }
                for i, pid in enumerate(sorted(vdf["patient_id"].unique())):
                    p = vdf[vdf["patient_id"] == pid].sort_values("window_start")
                    bar_colors = [mews_color_map.get(min(int(s), 5), "#dc2626") for s in p["mews_score"]]
                    fig.add_trace(go.Bar(
                        x=p["window_start"], y=p["mews_score"],
                        name=str(pid), marker_color=bar_colors,
                        opacity=0.85, width=0.8 * 60000 * 10,
                        hovertemplate=(
                            f"<b>{pid}</b><br>MEWS: %{{y}}<br>"
                            "HR score: %{customdata[0]}<br>"
                            "SpO₂ score: %{customdata[1]}<extra></extra>"
                        ),
                        customdata=p[["mews_hr_score", "mews_spo2_score"]].values
                        if "mews_hr_score" in p.columns else [[0, 0]] * len(p),
                    ))
                fig.add_hline(
                    y=mews_min, line_dash="dot", line_color="#ef4444", line_width=1,
                    annotation_text=f"  Alert ≥{mews_min}",
                    annotation_font=dict(size=8, color="#ef4444"),
                )
                layout = chart_base("MEWS SCORE PER 10-MIN WINDOW", chart_height)
                layout["yaxis"].update(range=[0, 6.5], dtick=1)
                layout["barmode"] = "group"
                fig.update_layout(**layout)
                st.plotly_chart(fig, width='stretch')

            with mc2:
                # MEWS distribution donut
                mews_counts = vdf["mews_score"].value_counts().sort_index()
                labels = [f"MEWS {i}" for i in mews_counts.index]
                colors_d = [mews_color_map.get(min(int(i), 5), "#dc2626") for i in mews_counts.index]
                fig = go.Figure(go.Pie(
                    labels=labels, values=mews_counts.values,
                    hole=0.65,
                    marker=dict(colors=colors_d, line=dict(color=BG, width=2)),
                    textfont=dict(family=MONO, size=8),
                    hovertemplate="%{label}<br>Count: %{value}<br>%{percent}<extra></extra>",
                ))
                fig.add_annotation(
                    text=f"{int(vdf['mews_score'].mean()*10)/10}<br><span style='font-size:8px'>avg</span>",
                    x=0.5, y=0.5, showarrow=False,
                    font=dict(family="Syne, sans-serif", size=20, color=TEXT_DARK if st.session_state.get("theme","dark")=="dark" else TEXT_LIGHT),
                )
                layout = chart_base("MEWS DISTRIBUTION", chart_height)
                layout.pop("xaxis", None)
                layout.pop("yaxis", None)
                layout.pop("hovermode", None)
                fig.update_layout(**layout)
                st.plotly_chart(fig, width='stretch')

    # ── Section: Vital stats summary per patient ─────────────────────────── #
    with st.expander("📊  Per-patient vitals summary", expanded=False):
        if vdf.empty:
            no_data()
        else:
            summary = (
                vdf.groupby("patient_id", as_index=False).agg(
                    windows      =("avg_hr",     "count"),
                    avg_hr       =("avg_hr",     "mean"),
                    min_hr       =("avg_hr",     "min"),
                    max_hr       =("peak_hr",    "max"),
                    avg_spo2     =("avg_spo2",   "mean"),
                    min_spo2     =("avg_spo2",   "min"),
                    max_mews     =("mews_score", "max"),
                    avg_mews     =("mews_score", "mean"),
                    alert_windows=("mews_score", lambda x: (x >= mews_min).sum()),
                )
            )
            fig = px.bar(
                summary, x="patient_id",
                y=["avg_hr", "max_hr"],
                barmode="group",
                color_discrete_sequence=["#3b82f6", "#06b6d4"],
                custom_data=["avg_spo2", "max_mews", "alert_windows", "windows"],
            )
            fig.update_traces(
                hovertemplate=(
                    "<b>%{x}</b><br>Value: %{y:.0f}<br>"
                    "Avg SpO₂: %{customdata[0]:.1f}%<br>"
                    "Max MEWS: %{customdata[1]}<br>"
                    "Alert windows: %{customdata[2]}<br>"
                    "Total windows: %{customdata[3]}<extra></extra>"
                ),
            )
            layout = chart_base("HEART RATE SUMMARY PER PATIENT", chart_height)
            fig.update_layout(**layout)
            st.plotly_chart(fig, width='stretch')


# ============================================================================
# TAB 2 — Movement & Falls
# ============================================================================

with tab_movement:

    with st.expander("📈  Activity SVM over time", expanded=True):
        if mdf.empty:
            no_data()
        else:
            fig = go.Figure()
            for i, pid in enumerate(sorted(mdf["patient_id"].unique())):
                p = mdf[mdf["patient_id"] == pid].sort_values("window_start")
                c = PATIENT_PALETTE[i % len(PATIENT_PALETTE)]
                fig.add_trace(go.Scatter(
                    x=p["window_start"], y=p["avg_activity"],
                    name=f"{pid} avg", line=dict(color=c, width=1.8),
                    mode="lines+markers", marker=dict(size=4),
                    hovertemplate=f"<b>{pid}</b><br>Avg SVM: %{{y:.1f}}<extra></extra>",
                ))
                if show_peak:
                    fig.add_trace(go.Scatter(
                        x=p["window_start"], y=p["peak_impact"],
                        name=f"{pid} peak", line=dict(color=c, width=1, dash="dot"),
                        opacity=0.5, mode="lines",
                        hovertemplate=f"<b>{pid} peak</b><br>%{{y:.1f}}<extra></extra>",
                    ))
                # Mark fall windows
                falls = p[p["fall_events_detected"] > 0]
                if not falls.empty:
                    fig.add_trace(go.Scatter(
                        x=falls["window_start"], y=falls["peak_impact"],
                        mode="markers",
                        marker=dict(symbol="x", size=10, color="#ef4444", line=dict(width=2)),
                        name=f"{pid} FALL", showlegend=True,
                        hovertemplate=f"<b>FALL · {pid}</b><br>Peak: %{{y:.1f}}<extra></extra>",
                    ))
            fig.add_hline(
                y=fall_threshold, line_dash="dash", line_color="#d97706", line_width=1,
                annotation_text=f"  Fall threshold ({fall_threshold})",
                annotation_font=dict(size=8, color="#d97706"),
            )
            layout = chart_base("SIGNAL VECTOR MAGNITUDE — ACTIVITY & PEAK IMPACT", chart_height)
            fig.update_layout(**layout)
            st.plotly_chart(fig, width='stretch')

    with st.expander("🆘  Fall event analysis", expanded=True):
        if mdf.empty:
            no_data()
        else:
            fc1, fc2 = st.columns(2)

            with fc1:
                fall_summary = (
                    mdf.groupby("patient_id", as_index=False).agg(
                        total_falls  =("fall_events_detected", "sum"),
                        fall_windows =("fall_events_detected", lambda x: (x > 0).sum()),
                        avg_activity =("avg_activity",         "mean"),
                        peak_impact  =("peak_impact",          "max"),
                        windows      =("avg_activity",         "count"),
                    )
                )
                fall_summary["fall_rate"] = (
                    fall_summary["fall_windows"] / fall_summary["windows"] * 100
                ).round(1)

                fig = px.bar(
                    fall_summary, x="patient_id", y="total_falls",
                    color="peak_impact",
                    color_continuous_scale=[[0,GRID_DARK if st.session_state.get("theme","dark")=="dark" else GRID_LIGHT],[0.5,"#d97706"],[1,"#dc2626"]],
                    text="total_falls",
                    custom_data=["avg_activity", "peak_impact", "fall_rate"],
                )
                fig.update_traces(
                    textposition="outside",
                    textfont=dict(family=MONO, size=9, color=TEXT_DARK if st.session_state.get("theme","dark")=="dark" else "#1a2740"),
                    hovertemplate=(
                        "<b>%{x}</b><br>Falls: %{y}<br>"
                        "Avg activity: %{customdata[0]:.1f}<br>"
                        "Peak impact: %{customdata[1]:.1f}<br>"
                        "Fall rate: %{customdata[2]}%<extra></extra>"
                    ),
                )
                layout = chart_base("FALL EVENTS PER PATIENT", chart_height)
                layout["coloraxis"] = dict(showscale=False)
                fig.update_layout(**layout)
                st.plotly_chart(fig, width='stretch')

            with fc2:
                # Activity distribution histogram
                fig = go.Figure()
                for i, pid in enumerate(sorted(mdf["patient_id"].unique())):
                    p = mdf[mdf["patient_id"] == pid]
                    c = PATIENT_PALETTE[i % len(PATIENT_PALETTE)]
                    fig.add_trace(go.Histogram(
                        x=p["avg_activity"], name=str(pid),
                        nbinsx=20, opacity=0.7,
                        marker_color=c,
                        hovertemplate=f"<b>{pid}</b><br>SVM range: %{{x}}<br>Count: %{{y}}<extra></extra>",
                    ))
                fig.add_vline(
                    x=fall_threshold, line_dash="dash",
                    line_color="#d97706", line_width=1.5,
                    annotation_text=f"  Threshold {fall_threshold}",
                    annotation_font=dict(size=8, color="#d97706"),
                )
                layout = chart_base("ACTIVITY DISTRIBUTION", chart_height)
                layout["barmode"] = "overlay"
                layout.pop("hovermode", None)
                fig.update_layout(**layout)
                st.plotly_chart(fig, width='stretch')


# ============================================================================
# TAB 3 — Patient Comparison
# ============================================================================

with tab_compare:

    with st.expander("🔬  Multi-patient radar comparison", expanded=True):
        if vdf.empty or len(vdf["patient_id"].unique()) < 1:
            no_data()
        else:
            summary_c = (
                vdf.groupby("patient_id", as_index=False).agg(
                    avg_hr  =("avg_hr",     "mean"),
                    avg_spo2=("avg_spo2",   "mean"),
                    max_mews=("mews_score", "max"),
                    avg_mews=("mews_score", "mean"),
                    windows =("avg_hr",     "count"),
                )
            )
            if not mdf.empty:
                mov_c = mdf.groupby("patient_id", as_index=False).agg(
                    avg_act=("avg_activity",         "mean"),
                    falls  =("fall_events_detected", "sum"),
                )
                summary_c = summary_c.merge(mov_c, on="patient_id", how="left").fillna(0)

            categories = ["Avg HR (norm)", "Avg SpO₂ (norm)", "Max MEWS (inv)",
                          "Avg Activity", "Fall Risk"]

            fig = go.Figure()
            for i, row in summary_c.iterrows():
                hr_norm   = (row["avg_hr"]   - 30)  / (220 - 30)
                spo2_norm = (row["avg_spo2"] - 90)  / (100 - 90)
                mews_inv  = 1 - (row["max_mews"] / 6)
                act_norm  = min(row.get("avg_act", 0) / 50, 1)
                fall_risk = min(row.get("falls", 0) / 10, 1)

                vals = [hr_norm, spo2_norm, mews_inv, act_norm, fall_risk]
                vals += [vals[0]]  # close the polygon

                c = PATIENT_PALETTE[i % len(PATIENT_PALETTE)]
                fig.add_trace(go.Scatterpolar(
                    r=vals,
                    theta=categories + [categories[0]],
                    fill="toself",
                    name=str(row["patient_id"]),
                    line=dict(color=c, width=1.5),
                    fillcolor=hex_to_rgba(c, 0.15),
                    opacity=0.9,
                ))

            fig.update_layout(
                polar=dict(
                    bgcolor=CARD_BG_DARK if st.session_state.get("theme","dark")=="dark" else CARD_BG_LIGHT,
                    radialaxis=dict(visible=True, range=[0, 1],
                                    gridcolor=GRID_DARK if st.session_state.get("theme","dark")=="dark" else GRID_LIGHT,
                                    tickfont=dict(size=7, color=TEXT_DARK if st.session_state.get("theme","dark")=="dark" else TEXT_LIGHT),
                                    linecolor=GRID_DARK if st.session_state.get("theme","dark")=="dark" else GRID_LIGHT),
                    angularaxis=dict(
                                    gridcolor=GRID_DARK if st.session_state.get("theme","dark")=="dark" else GRID_LIGHT,
                                    linecolor=GRID_DARK if st.session_state.get("theme","dark")=="dark" else GRID_LIGHT,
                                    tickfont=dict(size=8, color=TEXT_DARK if st.session_state.get("theme","dark")=="dark" else TEXT_LIGHT)),
                ),
                paper_bgcolor=BG_DARK if st.session_state.get("theme","dark")=="dark" else BG_LIGHT,
                font=dict(family=MONO, color=TEXT_DARK if st.session_state.get("theme","dark")=="dark" else TEXT_LIGHT),
                legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=9)),
                margin=dict(l=60, r=60, t=60, b=40),
                height=chart_height + 60,
                title=dict(
                    text="PATIENT PROFILE COMPARISON (normalised)",
                    font=dict(family="Syne, sans-serif", size=10, color="#2d4a66"),
                    x=0,
                ),
            )
            st.plotly_chart(fig, width='stretch')

    with st.expander("📊  Side-by-side metrics table", expanded=True):
        if vdf.empty:
            no_data()
        else:
            s = (
                vdf.groupby("patient_id", as_index=False).agg(
                    vitals_windows=("avg_hr",     "count"),
                    avg_hr        =("avg_hr",     "mean"),
                    min_hr        =("avg_hr",     "min"),
                    max_hr        =("peak_hr",    "max"),
                    avg_spo2      =("avg_spo2",   "mean"),
                    min_spo2      =("avg_spo2",   "min"),
                    max_mews      =("mews_score", "max"),
                    avg_mews      =("mews_score", "mean"),
                    alert_windows =("mews_score", lambda x: (x >= mews_min).sum()),
                )
            )
            if not mdf.empty:
                m = mdf.groupby("patient_id", as_index=False).agg(
                    movement_windows=("avg_activity",         "count"),
                    avg_activity    =("avg_activity",         "mean"),
                    peak_impact     =("peak_impact",          "max"),
                    total_falls     =("fall_events_detected", "sum"),
                )
                s = s.merge(m, on="patient_id", how="left").fillna(0)

            # Round display values
            for col in s.select_dtypes("float").columns:
                s[col] = s[col].round(2)

            st.dataframe(
                s.set_index("patient_id"),
                width='stretch',
                height=min(400, (len(s) + 1) * 38),
            )


# ============================================================================
# TAB 4 — Data Tables
# ============================================================================

with tab_tables:
    if not show_raw_table:
        st.info("Raw tables are hidden. Enable them in Display Options (sidebar).")
    else:
        t1, t2 = st.tabs(["Vitals gold data", "Movement gold data"])

        with t1:
            with st.expander("🔍  Filter & search vitals", expanded=True):
                if vdf.empty:
                    no_data()
                else:
                    # Additional inline filter
                    fc1, fc2, fc3 = st.columns(3)
                    with fc1:
                        pid_filter = st.multiselect(
                            "Patient ID", sorted(vdf["patient_id"].unique()),
                            default=sorted(vdf["patient_id"].unique()),
                            key="tbl_vitals_pid",
                        )
                    with fc2:
                        mews_filter = st.multiselect(
                            "MEWS score", sorted(vdf["mews_score"].unique()) if "mews_score" in vdf.columns else [],
                            default=sorted(vdf["mews_score"].unique()) if "mews_score" in vdf.columns else [],
                            key="tbl_vitals_mews",
                        )
                    with fc3:
                        sort_col = st.selectbox(
                            "Sort by", vdf.columns.tolist(), index=0, key="tbl_vitals_sort"
                        )

                    tbl = vdf[vdf["patient_id"].isin(pid_filter)].copy()
                    if mews_filter and "mews_score" in tbl.columns:
                        tbl = tbl[tbl["mews_score"].isin(mews_filter)]
                    if sort_col in tbl.columns:
                        tbl = tbl.sort_values(sort_col, ascending=False)

                    if "window_start" in tbl.columns:
                        tbl["window_start"] = tbl["window_start"].dt.strftime("%Y-%m-%d %H:%M")

                    st.caption(f"{len(tbl)} rows · showing up to {table_rows}")
                    st.dataframe(tbl.head(table_rows), width='stretch', hide_index=True)

                    st.download_button(
                        "⬇ Download filtered vitals",
                        data=tbl.to_csv(index=False).encode(),
                        file_name=f"vitals_filtered_{selected_date}.csv",
                        mime="text/csv",
                    )

        with t2:
            with st.expander("🔍  Filter & search movement", expanded=True):
                if mdf.empty:
                    no_data()
                else:
                    fc1, fc2, fc3 = st.columns(3)
                    with fc1:
                        pid_filter_m = st.multiselect(
                            "Patient ID", sorted(mdf["patient_id"].unique()),
                            default=sorted(mdf["patient_id"].unique()),
                            key="tbl_mov_pid",
                        )
                    with fc2:
                        falls_only = st.checkbox("Falls only", key="tbl_falls_only")
                    with fc3:
                        sort_col_m = st.selectbox(
                            "Sort by", mdf.columns.tolist(),
                            index=mdf.columns.tolist().index("peak_impact")
                            if "peak_impact" in mdf.columns else 0,
                            key="tbl_mov_sort",
                        )

                    tbl_m = mdf[mdf["patient_id"].isin(pid_filter_m)].copy()
                    if falls_only and "fall_events_detected" in tbl_m.columns:
                        tbl_m = tbl_m[tbl_m["fall_events_detected"] > 0]
                    if sort_col_m in tbl_m.columns:
                        tbl_m = tbl_m.sort_values(sort_col_m, ascending=False)

                    if "window_start" in tbl_m.columns:
                        tbl_m["window_start"] = tbl_m["window_start"].dt.strftime("%Y-%m-%d %H:%M")

                    st.caption(f"{len(tbl_m)} rows · showing up to {table_rows}")
                    st.dataframe(tbl_m.head(table_rows), width='stretch', hide_index=True)

                    st.download_button(
                        "⬇ Download filtered movement",
                        data=tbl_m.to_csv(index=False).encode(),
                        file_name=f"movement_filtered_{selected_date}.csv",
                        mime="text/csv",
                    )


# ============================================================================
# TAB 5 — About / Internal analysis guide
# ============================================================================

with tab_about:
    st.markdown("""
<div class='about-body'>

<div class='about-hdr'>How to use this dashboard for internal analysis</div>

<div class='about-sec'>Clinical monitoring</div>

<b class='about-hl'>MEWS (Modified Early Warning Score)</b> is computed per 10-minute
window per patient from heart rate and SpO₂. A score of 0–1 is normal, 2 warrants
increased monitoring, 3+ requires immediate clinical review. Use the
<i>MEWS alert level</i> filter to control which patients trigger the red banners at the top.
The donut chart in the Vitals tab shows the distribution of scores across all windows —
a shift toward higher values over time signals deteriorating population health.

<div class='about-sec'>Fall detection & movement</div>

<b class='about-hl'>Signal Vector Magnitude (SVM)</b> = √(x²+y²+z²) from the IMU sensor.
A sudden spike above the threshold (default 25.0 g) indicates a high-impact event classified
as a potential fall. Adjust the threshold in the sidebar based on your sensor calibration.
The activity histogram in the Movement tab shows the distribution of SVM values —
use it to tune the threshold: the fall population should appear as a separate right-tail cluster.

<div class='about-sec'>Patient comparison</div>

The <b class='about-hl'>radar chart</b> in the Comparison tab normalises each patient's
metrics onto a 0–1 scale so you can compare patients with different absolute vital ranges.
Higher on HR = higher heart rate. Higher on MEWS (inv) = <i>lower</i> MEWS (better).
Patients with large, uneven polygons warrant investigation. The side-by-side table below
the radar shows exact figures for export or inclusion in clinical reports.

<div class='about-sec'>Data tables & export</div>

All tables in the <b class='about-hl'>Data Tables</b> tab support inline patient and MEWS
filtering, column sorting, and CSV export. Use the <i>Max table rows</i> slider in
Display Options to control pagination. The top-level export buttons (header row) export
the full filtered dataset for the selected date. Filter by <i>Falls only</i> to quickly
identify all windows where a fall event was detected.

<div class='about-sec'>Filter reference</div>

<table style='border-collapse:collapse;width:100%;font-size:0.68rem'>
<tr class='about-tr'>
  <td style='padding:0.3rem 0.8rem 0.3rem 0;width:35%'>Date picker</td>
  <td>Selects which ETL batch partition to load from S3. One batch = one day's runs.</td>
</tr>
<tr class='about-tr'>
  <td style='padding:0.3rem 0.8rem 0.3rem 0'>Patient selection</td>
  <td>Show all patients or isolate individuals. All charts and KPIs update live.</td>
</tr>
<tr class='about-tr'>
  <td style='padding:0.3rem 0.8rem 0.3rem 0'>HR range / min SpO₂</td>
  <td>Remove windows outside physiological plausibility. Useful for outlier exclusion.</td>
</tr>
<tr class='about-tr'>
  <td style='padding:0.3rem 0.8rem 0.3rem 0'>MEWS alert level</td>
  <td>Controls both the banner threshold and the chart annotation line.</td>
</tr>
<tr class='about-tr'>
  <td style='padding:0.3rem 0.8rem 0.3rem 0'>Fall SVM threshold</td>
  <td>Adjusts fall classification in real-time. Falls on the chart re-render instantly.</td>
</tr>
<tr class='about-tr'>
  <td style='padding:0.3rem 0.8rem 0.3rem 0'>Show only fall windows</td>
  <td>Restricts the movement table and charts to windows with ≥1 fall event.</td>
</tr>
</table>

<div class='about-footer'>
Data source: S3 Gold layer · Partition: etl_batch_date · Refresh: 60-second cache TTL
</div>
</div>
""", unsafe_allow_html=True)