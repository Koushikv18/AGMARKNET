"""
visualize.py — Plotly-based visualisation of AGMARKNET analytics results.

Reads CSVs from results/ and generates HTML chart files in output/.
Also downloads an India GeoJSON for the choropleth if not already present.

Outputs:
    output/price_trend.html         — line chart: national monthly price trend
    output/top_commodities.html     — bar chart: top 10 commodities by volume
    output/top_states.html          — bar chart: top 5 states by trade value
    output/volatility.html          — horizontal bar: price volatility
    output/market_inefficiency.html — scatter: market spread vs avg price
    output/arbitrage.html           — heatmap: state deviation by commodity
    output/choropleth.html          — India state choropleth (avg price / volatility)

Usage:
    python src/visualize.py
"""

import json
import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import requests
from plotly.subplots import make_subplots

# ── Config ────────────────────────────────────────────────────────────────────

RESULTS_DIR = Path(os.getenv("RESULTS_DIR", "results"))
OUTPUT_DIR  = Path(os.getenv("OUTPUT_DIR", "output"))
DATA_DIR    = Path(os.getenv("DATA_DIR", "data"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# India states GeoJSON — public domain from datameet/maps-of-india
GEOJSON_URL  = (
    "https://raw.githubusercontent.com/geohacker/india/master/state/india_telengana.geojson"
)
GEOJSON_PATH = DATA_DIR / "india_states.geojson"

# Plotly theme
TEMPLATE = "plotly_dark"
FONT_FAMILY = "Inter, Roboto, sans-serif"

# Colour palette
PRIMARY    = "#00D4AA"
SECONDARY  = "#FF6B6B"
ACCENT     = "#FFD93D"
BG_DARK    = "#0D1117"
CARD_BG    = "#161B22"
TEXT_COLOR = "#E6EDF3"

COLOR_SEQ  = px.colors.qualitative.Bold
COLOR_CONT = "Teal"

# ── Helpers ───────────────────────────────────────────────────────────────────

def load(name: str) -> pd.DataFrame | None:
    path = RESULTS_DIR / f"{name}.csv"
    if not path.exists():
        print(f"  [SKIP] {path} not found — run analyze.py first")
        return None
    df = pd.read_csv(path)
    print(f"  [OK]   Loaded {name}.csv  ({len(df)} rows)")
    return df


def save_fig(fig: go.Figure, name: str) -> None:
    path = OUTPUT_DIR / f"{name}.html"
    pio.write_html(fig, str(path), full_html=True, include_plotlyjs="cdn")
    print(f"       → {path}")


def apply_style(fig: go.Figure, title: str, subtitle: str = "") -> go.Figure:
    """Apply consistent dark-mode styling to any figure."""
    full_title = f"<b>{title}</b>"
    if subtitle:
        full_title += f"<br><sup style='color:#8B949E'>{subtitle}</sup>"
    fig.update_layout(
        template=TEMPLATE,
        title=dict(text=full_title, font=dict(size=20, family=FONT_FAMILY, color=TEXT_COLOR)),
        font=dict(family=FONT_FAMILY, color=TEXT_COLOR),
        paper_bgcolor=BG_DARK,
        plot_bgcolor=CARD_BG,
        margin=dict(t=80, b=60, l=60, r=40),
        legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="rgba(255,255,255,0.1)"),
    )
    return fig


# ── Chart 1: National monthly price trend ────────────────────────────────────

def chart_price_trend() -> None:
    print("\n[1] National monthly price trend")
    df = load("monthly_price_trend")
    if df is None:
        return

    fig = px.line(
        df,
        x="year_month",
        y="avg_modal_price",
        markers=True,
        color_discrete_sequence=[PRIMARY],
        labels={"year_month": "Month", "avg_modal_price": "Avg Modal Price (₹/quintal)"},
    )
    fig.update_traces(line=dict(width=2.5), marker=dict(size=7))
    fig = apply_style(
        fig,
        "National Monthly Average Modal Price",
        "All commodities · last 12 months · AGMARKNET"
    )
    fig.update_xaxes(showgrid=False, tickangle=-45)
    fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.07)")
    save_fig(fig, "price_trend")


# ── Chart 2: Top 10 commodities by volume ────────────────────────────────────

def chart_top_commodities() -> None:
    print("\n[2] Top 10 commodities by record volume")
    df = load("top_commodities")
    if df is None:
        return

    df = df.sort_values("record_count")

    fig = px.bar(
        df,
        x="record_count",
        y="commodity",
        orientation="h",
        color="avg_modal_price",
        color_continuous_scale=COLOR_CONT,
        text="record_count",
        labels={
            "record_count": "Number of Price Records",
            "commodity": "Commodity",
            "avg_modal_price": "Avg Price (₹)",
        },
    )
    fig.update_traces(texttemplate="%{text:,}", textposition="outside")
    fig = apply_style(
        fig,
        "Top 10 Commodities by Trading Volume",
        "Colour = average modal price (₹/quintal) · AGMARKNET"
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(255,255,255,0.07)")
    fig.update_yaxes(showgrid=False)
    save_fig(fig, "top_commodities")


# ── Chart 3: Top 5 states by trade value ─────────────────────────────────────

def chart_top_states() -> None:
    print("\n[3] Top 5 states by trade value")
    df = load("top_states")
    if df is None:
        return

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=["Total Price Sum (proxy for trade value)", "Markets & Commodities"],
    )

    fig.add_trace(
        go.Bar(
            x=df["state"], y=df["total_price_sum"],
            marker_color=PRIMARY,
            text=df["total_price_sum"].apply(lambda v: f"₹{v/1e9:.1f}B"),
            textposition="outside",
            name="Trade Value",
        ),
        row=1, col=1,
    )

    fig.add_trace(
        go.Bar(
            x=df["state"], y=df["unique_markets"],
            marker_color=SECONDARY,
            name="Markets",
        ),
        row=1, col=2,
    )
    fig.add_trace(
        go.Bar(
            x=df["state"], y=df["unique_commodities"],
            marker_color=ACCENT,
            name="Commodities",
        ),
        row=1, col=2,
    )

    fig = apply_style(
        fig,
        "Top 5 States by Trade Activity",
        "Trade value = sum of modal prices across all records · AGMARKNET"
    )
    fig.update_layout(barmode="group")
    save_fig(fig, "top_states")


# ── Chart 4: Price volatility ─────────────────────────────────────────────────

def chart_volatility() -> None:
    print("\n[4] Price volatility per commodity")
    df = load("price_volatility")
    if df is None:
        return

    df = df.sort_values("coeff_variation_pct", ascending=True)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["coeff_variation_pct"],
        y=df["commodity"],
        orientation="h",
        marker=dict(
            color=df["coeff_variation_pct"],
            colorscale="RdYlGn_r",
            showscale=True,
            colorbar=dict(title="CV %"),
        ),
        text=df["coeff_variation_pct"].apply(lambda v: f"{v:.1f}%"),
        textposition="outside",
    ))
    fig = apply_style(
        fig,
        "Price Volatility by Commodity",
        "Coefficient of Variation = σ / μ × 100 · higher = more volatile · AGMARKNET"
    )
    fig.update_xaxes(title="Coefficient of Variation (%)", showgrid=True,
                     gridcolor="rgba(255,255,255,0.07)")
    fig.update_layout(height=600)
    save_fig(fig, "volatility")


# ── Chart 5: Market inefficiency ──────────────────────────────────────────────

def chart_market_inefficiency() -> None:
    print("\n[5] Market inefficiency scatter")
    df = load("market_inefficiency")
    if df is None:
        return

    fig = px.scatter(
        df,
        x="avg_modal_price",
        y="avg_spread_pct",
        color="state",
        size="record_count",
        hover_name="market",
        hover_data=["avg_price_spread", "record_count"],
        color_discrete_sequence=COLOR_SEQ,
        labels={
            "avg_modal_price": "Avg Modal Price (₹/quintal)",
            "avg_spread_pct": "Avg Spread % (max−min / modal)",
            "state": "State",
        },
    )
    fig.update_traces(marker=dict(opacity=0.8, line=dict(width=0.5, color="white")))
    fig = apply_style(
        fig,
        "Market Inefficiency: Price Spread",
        "Higher spread % = larger gap between min & max price → less efficient market"
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(255,255,255,0.07)")
    fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.07)")
    save_fig(fig, "market_inefficiency")


# ── Chart 6: Arbitrage heatmap ────────────────────────────────────────────────

def chart_arbitrage() -> None:
    print("\n[6] Arbitrage signal heatmap")
    df = load("arbitrage_signal")
    if df is None:
        return

    pivot = df.pivot_table(
        index="state", columns="commodity",
        values="avg_deviation_pct", aggfunc="mean"
    ).fillna(0)

    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale="RdBu_r",
        zmid=0,
        text=[[f"{v:.1f}%" for v in row] for row in pivot.values],
        texttemplate="%{text}",
        hovertemplate="State: %{y}<br>Commodity: %{x}<br>Deviation: %{z:.1f}%<extra></extra>",
        colorbar=dict(title="Deviation %", ticksuffix="%"),
    ))
    fig = apply_style(
        fig,
        "Price Arbitrage Signal by State & Commodity",
        "% deviation of state avg from national avg on same date · Red = above national avg"
    )
    fig.update_layout(height=500)
    fig.update_xaxes(tickangle=-45, showgrid=False)
    fig.update_yaxes(showgrid=False)
    save_fig(fig, "arbitrage")


# ── Chart 7: India choropleth ─────────────────────────────────────────────────

def download_geojson() -> dict | None:
    """Download India states GeoJSON (once), return parsed dict."""
    if GEOJSON_PATH.exists():
        with open(GEOJSON_PATH) as f:
            return json.load(f)
    print(f"  Downloading India GeoJSON from {GEOJSON_URL} ...")
    try:
        resp = requests.get(GEOJSON_URL, timeout=30)
        resp.raise_for_status()
        geojson = resp.json()
        GEOJSON_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(GEOJSON_PATH, "w") as f:
            json.dump(geojson, f)
        print("  GeoJSON downloaded and cached.")
        return geojson
    except Exception as e:
        print(f"  WARNING: Could not download GeoJSON: {e}")
        print("  Choropleth chart will be skipped.")
        return None


def chart_choropleth() -> None:
    print("\n[7] India state choropleth")
    geojson = download_geojson()
    if geojson is None:
        return

    df_states = load("top_states")
    if df_states is None:
        return

    # Try to identify the feature property that holds state names
    sample_props = geojson["features"][0]["properties"] if geojson.get("features") else {}
    name_key = next(
        (k for k in sample_props if "name" in k.lower() or "state" in k.lower()),
        list(sample_props.keys())[0] if sample_props else "NAME_1"
    )

    # Build a fuller state-level dataset by merging volatility info
    df_vol = load("price_volatility")
    if df_vol is None:
        print("  Skipping choropleth — price_volatility.csv not found.")
        return

    # Use top_states for state-level avg_modal_price
    # We need a state → avg_modal_price mapping; use top_states as proxy
    df_map = df_states[["state", "avg_modal_price", "total_price_sum"]].copy()

    fig = px.choropleth(
        df_map,
        geojson=geojson,
        locations="state",
        color="avg_modal_price",
        featureidkey=f"properties.{name_key}",
        color_continuous_scale="YlOrRd",
        labels={"avg_modal_price": "Avg Modal Price (₹/quintal)"},
        scope="asia",
    )
    fig.update_geos(
        fitbounds="locations",
        visible=False,
        bgcolor=BG_DARK,
        landcolor="#1E2A3A",
        oceancolor=BG_DARK,
        showocean=True,
    )
    fig = apply_style(
        fig,
        "India State-Level Average Modal Prices",
        "Top-5 states shown · AGMARKNET full-history data"
    )
    fig.update_layout(
        paper_bgcolor=BG_DARK,
        geo=dict(bgcolor=BG_DARK),
    )
    save_fig(fig, "choropleth")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("AGMARKNET Visualizations")
    print(f"  Reading from : {RESULTS_DIR}")
    print(f"  Writing to   : {OUTPUT_DIR}")
    print("=" * 60)

    chart_price_trend()
    chart_top_commodities()
    chart_top_states()
    chart_volatility()
    chart_market_inefficiency()
    chart_arbitrage()
    chart_choropleth()

    print("\n\nAll charts generated.")
    print(f"Open output/*.html in your browser, or run: start output/price_trend.html")


if __name__ == "__main__":
    main()
