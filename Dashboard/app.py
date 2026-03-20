"""
Walmart Sales Dashboard  —  Polyglot Persistence
=================================================
Run with:
    cd 05_Dashboard
    python app.py
Then open http://127.0.0.1:8050
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta, date as _date

import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, callback, html, ctx

# ── Path setup (data_loader already adds these, but be explicit) ─────────────
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

# ── Local imports ─────────────────────────────────────────────────────────────
from data_loader import (                         # noqa: E402
    connection_status,
    get_date_range,
    get_filter_options,
    get_kpi_data,
    get_sparkline_data,
    get_product_performance,
    get_category_performance,
    get_regional_performance,
    get_sentiment_data,
    get_trend_data,
)
from components.kpi_cards import build_kpi_row    # noqa: E402
from components.charts import (                   # noqa: E402
    create_product_scatter,
    create_regional_bar,
    create_sentiment_donut,
    create_trend_chart,
    create_category_bar,
)
from components.layout import create_layout       # noqa: E402


# ── Date bounds ───────────────────────────────────────────────────────────────
try:
    _mn, _mx = get_date_range()
    min_date = _mn.date() if hasattr(_mn, "date") else _mn
    max_date = _mx.date() if hasattr(_mx, "date") else _mx
except Exception:
    min_date = _date(2024, 1, 1)
    max_date = _date.today()

# Default view: last 12 months so the comparison period also has data
_default_start = max(min_date, max_date - timedelta(days=365))
_default_end   = max_date

# Filter options (regions + categories) for populating the dropdowns
try:
    _opts             = get_filter_options()
    _regions_list     = _opts.get("regions",    [])
    _categories_list  = _opts.get("categories", [])
except Exception:
    _regions_list    = []
    _categories_list = []


# ── App ───────────────────────────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    title="Walmart Sales Dashboard",
    suppress_callback_exceptions=True,
)

app.layout = create_layout(
    min_date=min_date,
    max_date=max_date,
    default_start=_default_start,
    default_end=_default_end,
    regions=_regions_list,
    categories=_categories_list,
)


# ── Callback 1 — DB status bar ────────────────────────────────────────────────
@callback(
    Output("db-status-bar", "children"),
    Input("date-range-picker", "start_date"),
)
def update_db_status(_):
    status = connection_status()

    def pill(label, ok):
        icon  = "\u2714\ufe0f " if ok else "\u274c "
        color = "#00B050" if ok else "#FF4444"
        return html.Span(
            f"{icon}{label}",
            className="db-status-pill",
            style={"background": color},
        )

    return [
        html.Span("Database connections: ", className="db-status-label"),
        pill("SQL Server", status["sql"]),
        pill("MongoDB",    status["mongo"]),
    ]


# ── Callback 2 — Quick date range presets ────────────────────────────────────
@callback(
    Output("date-range-picker", "start_date"),
    Output("date-range-picker", "end_date"),
    Output("region-filter",     "value"),
    Output("category-filter",   "value"),
    Input("quick-30d",      "n_clicks"),
    Input("quick-90d",      "n_clicks"),
    Input("quick-12m",      "n_clicks"),
    Input("quick-ytd",      "n_clicks"),
    Input("clear-filters",  "n_clicks"),
    prevent_initial_call=True,
)
def apply_quick_range(n30, n90, n12m, nytd, nclear):
    import dash
    trigger = ctx.triggered_id
    end = max_date

    if trigger == "quick-30d":
        start = end - timedelta(days=29)
    elif trigger == "quick-90d":
        start = end - timedelta(days=89)
    elif trigger == "quick-12m":
        start = end - timedelta(days=365)
    elif trigger == "quick-ytd":
        start = end.replace(month=1, day=1)
    else:                           # "clear-filters" → full range + reset filters
        return str(min_date), str(max_date), None, None

    # Quick presets only change the date; leave filter dropdowns untouched
    return str(start), str(end), dash.no_update, dash.no_update


# ── Callback 3 — Main dashboard update ───────────────────────────────────────
@callback(
    Output("kpi-row-container",    "children"),
    Output("period-info-label",    "children"),
    Output("product-matrix-chart", "figure"),
    Output("regional-chart",       "figure"),
    Output("sentiment-chart",      "figure"),
    Output("category-chart",       "figure"),
    Output("trend-chart",          "figure"),
    Input("date-range-picker",  "start_date"),
    Input("date-range-picker",  "end_date"),
    Input("region-filter",      "value"),
    Input("category-filter",    "value"),
)
def update_dashboard(start_date_str, end_date_str, sel_regions, sel_categories):
    import traceback
    import pandas as pd
    import plotly.graph_objects as go

    # ── Normalise filter values (Dash returns [] for empty multi-select) ────────
    regions    = sel_regions    if sel_regions    else None
    categories = sel_categories if sel_categories else None

    # ── Parse dates ──────────────────────────────────────────────────────────
    def _parse(s, fallback: datetime) -> datetime:
        if s is None:
            return fallback
        try:
            return datetime.fromisoformat(s[:10])
        except Exception:
            return fallback

    d_start   = datetime(_default_start.year, _default_start.month, _default_start.day)
    d_end     = datetime(_default_end.year,   _default_end.month,   _default_end.day)
    start     = _parse(start_date_str, d_start)
    end       = _parse(end_date_str,   d_end)

    period_days = max((end - start).days, 1)
    prev_start  = start - timedelta(days=period_days)
    prev_end    = start - timedelta(days=1)

    # ── Period info label ────────────────────────────────────────────────────
    fmt      = "%d %b %Y"
    start_d  = start.date()
    end_d    = end.date()
    days     = (end_d - start_d).days + 1

    range_label = None
    if end_d == max_date:
        if abs(days - 30) <= 1:
            range_label = "Last 30 days"
        elif abs(days - 90) <= 2:
            range_label = "Last 90 days"
        elif abs(days - 365) <= 2:
            range_label = "Last 12 months"
        elif start_d == end_d.replace(month=1, day=1):
            range_label = f"Year to date ({end_d.year})"

    period_label = html.Span([
        html.Span(
            (range_label + "  \u2014  ") if range_label else "",
            style={"fontWeight": "800"},
        ),
        html.Span("Showing: ",    style={"opacity": "0.7"}),
        html.Span(
            f"{start.strftime(fmt)} \u2013 {end.strftime(fmt)}  ({days:,}d)",
            style={"fontWeight": "700"},
        ),
        html.Span("   |   vs prior: ", style={"opacity": "0.7"}),
        html.Span(
            f"{prev_start.strftime(fmt)} \u2013 {prev_end.strftime(fmt)}",
            style={"fontWeight": "700"},
        ),
    ])

    # ── Empty fallback ───────────────────────────────────────────────────────
    def _empty(msg="No data available"):
        return go.Figure().update_layout(
            annotations=[{
                "text": msg, "showarrow": False,
                "xref": "paper", "yref": "paper",
                "x": 0.5, "y": 0.5,
                "font": {"size": 15, "color": "#aaa"},
            }],
            margin=dict(t=40, b=20, l=20, r=20),
            paper_bgcolor="#ffffff",
        )

    # ── KPI cards ────────────────────────────────────────────────────────────
    try:
        kpi_data     = get_kpi_data(start, end, regions, categories)
    except Exception:
        traceback.print_exc()
        kpi_data = {}

    try:
        sparkline_df = get_sparkline_data(start, end, regions, categories)
    except Exception:
        sparkline_df = pd.DataFrame()

    try:
        kpi_row = build_kpi_row(kpi_data, sparkline_df)
    except Exception:
        traceback.print_exc()
        kpi_row = html.Div("KPI data unavailable", style={"padding": "16px"})

    # ── Chart 1: Product Performance Bubble ─────────────────────────────────
    try:
        product_df  = get_product_performance(start, end, regions, categories)
        product_fig = create_product_scatter(product_df)
    except Exception:
        traceback.print_exc()
        product_df  = pd.DataFrame()
        product_fig = _empty("Product data unavailable")

    # ── Chart 2: Regional Revenue Bar ───────────────────────────────────────
    try:
        regional_fig = create_regional_bar(
            get_regional_performance(start, end, regions, categories)
        )
    except Exception:
        traceback.print_exc()
        regional_fig = _empty("Regional data unavailable")

    # ── Chart 3: Sentiment Donut (products ordered in period) ────────────────
    try:
        sentiment_fig = create_sentiment_donut(
            get_sentiment_data(start, end, regions, categories)
        )
    except Exception:
        traceback.print_exc()
        sentiment_fig = _empty("Sentiment data unavailable")

    # ── Chart 4: Category Revenue & Satisfaction Scorecard ───────────────────
    try:
        cat_df  = get_category_performance(start, end, regions, categories)
        cat_fig = create_category_bar(cat_df)
    except Exception:
        traceback.print_exc()
        cat_fig = _empty("Category data unavailable")

    # ── Chart 5: Revenue & Rating Trend ─────────────────────────────────────
    try:
        trend_fig = create_trend_chart(get_trend_data(start, end, regions, categories))
    except Exception:
        traceback.print_exc()
        trend_fig = _empty("Trend data unavailable")

    return (
        kpi_row,
        period_label,
        product_fig,
        regional_fig,
        sentiment_fig,
        cat_fig,      # → category-chart slot
        trend_fig,
    )


# ── Run ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  Walmart Sales Dashboard  —  Polyglot Persistence")
    print("  Open: http://127.0.0.1:8050")
    print("=" * 60 + "\n")
    app.run(debug=True, host="127.0.0.1", port=8050, use_reloader=False)
