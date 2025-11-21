import pandas as pd
from reportlab.platypus import PageBreak, Spacer
from reportlab.lib.units import inch

from src.services.report_service.base_report import BaseReport
from src.utils.db_utils import fetch_history_df
from src.utils.chart_utils import generate_bar_chart
from src.services.recommendation_service import call_llm_narrative


class RoutePerformanceReport(BaseReport):
    def __init__(self, logo_path=None):
        super().__init__(
            title="Route Performance Report",
            subtitle="7-Day Route Quality & Adulteration Analysis",
            logo_path=logo_path
        )

    def build(self):
        # Load last 7 days of data
        df = fetch_history_df(days=7)
        if df.empty:
            raise Exception("No data available for route performance report.")

        # Convert Decimal columns to float to avoid type mismatch errors
        decimal_columns = ['fat', 'snf', 'ts', 'sample_score']
        for col in decimal_columns:
            if col in df.columns:
                df[col] = df[col].astype(float)

        # Ensure required columns exist
        required_cols = {"route_id", "fat", "snf", "ts", "sample_score", "is_adulterated"}
        missing = required_cols - set(df.columns)
        if missing:
            raise Exception(f"History data missing columns: {missing}")

        story = []

        # =====================================================
        # TITLE PAGE
        # =====================================================
        self.add_title_page(story)

        # =====================================================
        # ROUTE-LEVEL AGGREGATION
        # =====================================================
        route_grp = (
            df.groupby("route_id")
              .agg(
                  samples=("entry_id", "count") if "entry_id" in df.columns else ("fat", "count"),
                  avg_fat=("fat", "mean"),
                  avg_snf=("snf", "mean"),
                  avg_ts=("ts", "mean"),
                  avg_score=("sample_score", "mean"),
                  adulteration_rate=("is_adulterated", lambda x: x.mean() * 100.0),
              )
              .reset_index()
        )

        # Sort routes by avg_score (descending)
        route_grp = route_grp.sort_values("avg_score", ascending=False)

        # Best / worst route (for narrative highlight)
        best_route = route_grp.iloc[0]["route_id"]
        worst_route = route_grp.iloc[-1]["route_id"]
        best_score = route_grp.iloc[0]["avg_score"]
        worst_score = route_grp.iloc[-1]["avg_score"]

        # =====================================================
        # PAGE 1 — KEY METRICS & SUMMARY TABLE
        # =====================================================

        # Key metrics box
        key_metrics = {
            "Total Routes": str(route_grp['route_id'].nunique()),
            "Total Samples": str(len(df)),
            "Best Route": str(best_route),
            "Worst Route": str(worst_route),
        }
        self.add_metrics_box(story, key_metrics)

        # Summary table with all routes
        table_data = [
            ["Route", "Samples", "Avg FAT", "Avg SNF", "Avg TS", "Avg Score", "Adulteration Rate (%)"]
        ]

        for _, row in route_grp.iterrows():
            table_data.append([
                str(row["route_id"]),
                str(int(row["samples"])),
                f"{row['avg_fat']:.2f}%",
                f"{row['avg_snf']:.2f}%",
                f"{row['avg_ts']:.2f}%",
                f"{row['avg_score']:.2f}",
                f"{row['adulteration_rate']:.1f}%",
            ])

        self.add_table(
            story,
            "Route Performance Summary (7 Days)",
            table_data,
            description="Comprehensive analysis of all routes with quality scores and adulteration rates."
        )
        story.append(PageBreak())

        # =====================================================
        # PAGE 2 — PERFORMANCE CHARTS
        # =====================================================

        # Chart 1: Route vs Average Score
        score_chart_path = generate_bar_chart(
            route_grp.rename(columns={"avg_score": "score"}),
            x="route_id",
            y="score",
            filename="route_avg_score.png"
        )
        self.add_chart(
            story,
            "Route-wise Average Quality Score",
            score_chart_path,
            description="Comparison of quality scores across all routes. Higher scores indicate better milk quality and handling."
        )

        # Chart 2: Route vs Adulteration Rate
        adulteration_chart_path = generate_bar_chart(
            route_grp.rename(columns={"adulteration_rate": "adulteration"}),
            x="route_id",
            y="adulteration",
            filename="route_adulteration_rate.png"
        )
        self.add_chart(
            story,
            "Route-wise Adulteration Risk (%)",
            adulteration_chart_path,
            description="Adulteration frequency by route. Routes with higher percentages require immediate attention and verification."
        )

        story.append(PageBreak())

        # =====================================================
        # PAGE 3 — AI-GENERATED INSIGHTS
        # =====================================================

        text_summary = f"""
Route Performance Summary (Last 7 Days):

Total Samples: {len(df)}
Unique Routes: {route_grp['route_id'].nunique()}

Route Performance Rankings:
- Best Route: {best_route} (Score: {best_score:.2f})
- Worst Route: {worst_route} (Score: {worst_score:.2f})

Global Composition Averages:
- FAT: {df['fat'].mean():.2f}%
- SNF: {df['snf'].mean():.2f}%
- TS: {df['ts'].mean():.2f}%

Quality Metrics:
- Global Average Score: {df['sample_score'].mean():.2f}
- Global Adulteration Rate: {df['is_adulterated'].mean() * 100:.1f}%
- Total Adulterated Samples: {int(df['is_adulterated'].sum())}

Route Details:
"""

        for _, row in route_grp.iterrows():
            text_summary += f"\nRoute {row['route_id']}: Score={row['avg_score']:.2f}, FAT={row['avg_fat']:.2f}%, SNF={row['avg_snf']:.2f}%, Adulteration={row['adulteration_rate']:.1f}%"

        prompt = f"""
You are an AI expert in milk collection route analytics and quality control.

Using the following weekly route performance summary, write a **comprehensive route performance analysis**:

{text_summary}

Focus on:
1. Which routes perform best and worst (by quality score)
2. Which routes have higher adulteration risk and why
3. Interesting composition patterns (FAT/SNF/TS) across routes
4. Routes with concerning trends or anomalies
5. Specific operational recommendations (inspections, training, rerouting, cooling chain improvements, supplier verification)
6. Priority actions for quality improvement

Write 3–4 paragraphs, professional tone, clear and actionable insights. No bullet points, no JSON.
"""

        narrative = call_llm_narrative(prompt)
        self.add_narrative(
            story,
            "AI Route Performance Intelligence",
            narrative,
            is_ai=True
        )

        story.append(PageBreak())

        # =====================================================
        # PAGE 4 — DETAILED ROUTE STATISTICS
        # =====================================================

        self.add_section_header(story, "Route Composition Statistics")

        # Create detailed stats table
        stats_data = [
            ["Route", "FAT (Avg)", "FAT (Std)", "SNF (Avg)", "SNF (Std)", "TS (Avg)", "TS (Std)"]
        ]

        for _, row in route_grp.iterrows():
            route_data = df[df["route_id"] == row["route_id"]]
            stats_data.append([
                str(row["route_id"]),
                f"{route_data['fat'].mean():.2f}",
                f"{route_data['fat'].std():.2f}",
                f"{route_data['snf'].mean():.2f}",
                f"{route_data['snf'].std():.2f}",
                f"{route_data['ts'].mean():.2f}",
                f"{route_data['ts'].std():.2f}",
            ])

        self.add_table(
            story,
            "Detailed Route Statistics",
            stats_data,
            description="Statistical analysis of composition parameters for each route including mean and standard deviation."
        )

        # =====================================================
        # EXPORT PDF
        # =====================================================
        return self.export(story)