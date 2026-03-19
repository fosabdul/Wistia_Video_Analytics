import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

print("Loading processed data...")

# Load processed parquet files
dim_media = pd.read_parquet("data/processed/dim_media")
dim_visitor = pd.read_parquet("data/processed/dim_visitor")
fact = pd.read_parquet("data/processed/fact_media_engagement")

print(f"dim_media: {len(dim_media)} rows")
print(f"dim_visitor: {len(dim_visitor)} rows")
print(f"fact_media_engagement: {len(fact)} rows")

# ── Chart 1: Play count by channel ──
fig1 = px.bar(
    dim_media,
    x="channel",
    y="play_count",
    color="channel",
    title="Total Play Count by Channel",
    labels={"play_count": "Total Plays", "channel": "Channel"},
    color_discrete_map={"Facebook": "#1877F2", "YouTube": "#FF0000"}
)

# ── Chart 2: Play rate by channel ──
fig2 = px.bar(
    dim_media,
    x="channel",
    y="play_rate",
    color="channel",
    title="Play Rate by Channel",
    labels={"play_rate": "Play Rate", "channel": "Channel"},
    color_discrete_map={"Facebook": "#1877F2", "YouTube": "#FF0000"}
)

# ── Chart 3: Hours watched by channel ──
fig3 = px.pie(
    dim_media,
    names="channel",
    values="hours_watched",
    title="Hours Watched by Channel",
    color_discrete_map={"Facebook": "#1877F2", "YouTube": "#FF0000"}
)

# ── Chart 4: Top 10 countries by visitors ──
top_countries = dim_visitor["country"].value_counts().head(10).reset_index()
top_countries.columns = ["country", "visitors"]
fig4 = px.bar(
    top_countries,
    x="visitors",
    y="country",
    orientation="h",
    title="Top 10 Countries by Visitor Count",
    labels={"visitors": "Visitors", "country": "Country"},
    color="visitors",
    color_continuous_scale="Blues"
)

# ── Chart 5: Engagement over time ──
fact["date"] = pd.to_datetime(fact["date"])
daily = fact.groupby(["date", "media_id"]).size().reset_index(name="events")
daily = daily.merge(dim_media[["media_id", "channel"]], on="media_id", how="left")
fig5 = px.line(
    daily,
    x="date",
    y="events",
    color="channel",
    title="Daily Engagement Events Over Time",
    labels={"events": "Engagement Events", "date": "Date"},
    color_discrete_map={"Facebook": "#1877F2", "YouTube": "#FF0000"}
)

# ── Chart 6: Watch percentage distribution ──
fig6 = px.histogram(
    fact,
    x="watched_percent",
    color="media_id",
    title="Distribution of Watch Percentage",
    labels={"watched_percent": "Watch %", "count": "Visitors"},
    nbins=20,
    barmode="overlay",
    opacity=0.7
)

# ── Build HTML dashboard ──
html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Wistia Video Analytics Dashboard</title>
    <style>
        body {{ font-family: Arial, sans-serif; background: #f4f6f9; margin: 0; padding: 20px; }}
        h1 {{ text-align: center; color: #1F3864; margin-bottom: 5px; }}
        p {{ text-align: center; color: #5F5E5A; margin-bottom: 30px; }}
        .stats {{ display: flex; justify-content: center; gap: 20px; margin-bottom: 30px; flex-wrap: wrap; }}
        .stat {{ background: white; border-radius: 12px; padding: 20px 30px; text-align: center; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }}
        .stat-num {{ font-size: 28px; font-weight: bold; color: #1F3864; }}
        .stat-label {{ font-size: 13px; color: #5F5E5A; margin-top: 4px; }}
        .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; max-width: 1200px; margin: 0 auto; }}
        .chart {{ background: white; border-radius: 12px; padding: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }}
        .full {{ grid-column: 1 / -1; }}
    </style>
</head>
<body>
    <h1>Wistia Video Analytics Dashboard</h1>
    <p>Facebook vs YouTube · {len(fact):,} engagement events · {len(dim_visitor):,} unique visitors</p>

    <div class="stats">
        <div class="stat">
            <div class="stat-num">{int(dim_media['play_count'].sum()):,}</div>
            <div class="stat-label">Total Plays</div>
        </div>
        <div class="stat">
            <div class="stat-num">{len(dim_visitor):,}</div>
            <div class="stat-label">Unique Visitors</div>
        </div>
        <div class="stat">
            <div class="stat-num">{int(dim_media['hours_watched'].sum()):,}</div>
            <div class="stat-label">Hours Watched</div>
        </div>
        <div class="stat">
            <div class="stat-num">{len(fact):,}</div>
            <div class="stat-label">Engagement Events</div>
        </div>
        <div class="stat">
            <div class="stat-num">{dim_visitor['country'].nunique()}</div>
            <div class="stat-label">Countries Reached</div>
        </div>
    </div>

    <div class="grid">
        <div class="chart">{fig1.to_html(full_html=False, include_plotlyjs='cdn')}</div>
        <div class="chart">{fig2.to_html(full_html=False, include_plotlyjs=False)}</div>
        <div class="chart">{fig3.to_html(full_html=False, include_plotlyjs=False)}</div>
        <div class="chart">{fig4.to_html(full_html=False, include_plotlyjs=False)}</div>
        <div class="chart full">{fig5.to_html(full_html=False, include_plotlyjs=False)}</div>
        <div class="chart full">{fig6.to_html(full_html=False, include_plotlyjs=False)}</div>
    </div>
</body>
</html>
"""

# Save dashboard
os.makedirs("docs", exist_ok=True)
with open("docs/dashboard.html", "w") as f:
    f.write(html)

print("Dashboard saved to docs/dashboard.html")
print("Open it in your browser to view!")