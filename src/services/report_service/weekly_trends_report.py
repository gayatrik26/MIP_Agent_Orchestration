import pandas as pd
from reportlab.platypus import PageBreak
from src.services.report_service.base_report import BaseReport
from src.utils.db_utils import fetch_history_df
from src.utils.chart_utils import generate_line_chart
from src.services.recommendation_service import call_llm_narrative


class WeeklyCompositionReport(BaseReport):
    def __init__(self, logo_path=None):
        super().__init__(
            title="Weekly Composition Trends",
            subtitle="7-Day Dairy Quality Analysis & Insights",
            logo_path=logo_path
        )

    def build(self):
        # Load last 7 days
        df = fetch_history_df(days=7)
        if df.empty:
            raise Exception("No data available for weekly report.")
        
        # Convert Decimal columns to float to avoid type mismatch errors
        decimal_columns = ['fat', 'snf', 'ts']
        for col in decimal_columns:
            if col in df.columns:
                df[col] = df[col].astype(float)

        story = []

        # =====================================================
        # TITLE PAGE
        # =====================================================
        self.add_title_page(story)

        # =====================================================
        # PAGE 1 — KEY METRICS & SUMMARY TABLE
        # =====================================================
        
        # Key metrics box - using simple string values
        key_metrics = {
            "Total Samples": str(len(df)),
            "Unique Suppliers": str(df["supplier_id"].nunique()),
            "Unique Routes": str(df["route_id"].nunique()),
            "Adulteration Rate": f"{df['is_adulterated'].mean() * 100:.1f}%",
        }
        self.add_metrics_box(story, key_metrics)

        # Summary KPI table
        summary_rows = [
            ["Metric", "Value", "Status"],
            ["Average FAT", f"{df['fat'].mean():.2f}%", "✓"],
            ["Average SNF", f"{df['snf'].mean():.2f}%", "✓"],
            ["Average TS", f"{df['ts'].mean():.2f}%", "✓"],
            ["FAT Range (Min/Max)", f"{df['fat'].min():.2f}% / {df['fat'].max():.2f}%", "📊"],
            ["SNF Range (Min/Max)", f"{df['snf'].min():.2f}% / {df['snf'].max():.2f}%", "📊"],
            ["TS Range (Min/Max)", f"{df['ts'].min():.2f}% / {df['ts'].max():.2f}%", "📊"],
            ["Total Adulterated", f"{int(df['is_adulterated'].sum())} samples", "⚠️" if df['is_adulterated'].sum() > 0 else "✓"],
        ]

        self.add_table(
            story,
            "Weekly Summary KPIs",
            summary_rows,
            description="Comprehensive overview of milk composition metrics for the past 7 days."
        )

        # =====================================================
        # PAGE 2 — COMPOSITION TRENDS
        # =====================================================

        fat_chart = generate_line_chart(df, y="fat", x="timestamp", filename="weekly_fat_trend.png")
        self.add_chart(
            story,
            "FAT Content Trend (7 Days)",
            fat_chart,
            description="Analysis of Fat percentage fluctuations across the week. Consistent levels indicate stable supplier quality."
        )

        snf_chart = generate_line_chart(df, y="snf", x="timestamp", filename="weekly_snf_trend.png")
        self.add_chart(
            story,
            "SNF (Solids-Not-Fat) Trend (7 Days)",
            snf_chart,
            description="Solids-Not-Fat levels tracking. Variations may indicate different sources or collection times."
        )

        ts_chart = generate_line_chart(df, y="ts", x="timestamp", filename="weekly_ts_trend.png")
        self.add_chart(
            story,
            "Total Solids Trend (7 Days)",
            ts_chart,
            description="Overall solids content tracking. Higher values typically correlate with better milk quality."
        )

        story.append(PageBreak())

        # =====================================================
        # PAGE 3 — AI-GENERATED INSIGHTS
        # =====================================================

        weekly_summary_text = f"""
Weekly Dataset Summary:
Total Samples: {len(df)}

Composition Averages:
- FAT: {df['fat'].mean():.2f}%
- SNF: {df['snf'].mean():.2f}%
- TS: {df['ts'].mean():.2f}%

Composition Ranges:
- FAT Min/Max: {df['fat'].min():.2f}% / {df['fat'].max():.2f}%
- SNF Min/Max: {df['snf'].min():.2f}% / {df['snf'].max():.2f}%
- TS Min/Max: {df['ts'].min():.2f}% / {df['ts'].max():.2f}%

Quality Control:
- Total Adulterated: {df['is_adulterated'].sum()} samples
- Adulteration Rate: {df['is_adulterated'].mean() * 100:.1f}%
- Unique Suppliers: {df["supplier_id"].nunique()}
- Unique Routes: {df["route_id"].nunique()}
"""

        prompt = f"""
You are an advanced dairy analytics LLM expert.

Write a **professional weekly analytical narrative** (10–15 sentences) describing:

1. FAT, SNF, TS trends and significant deviations
2. Notable spikes or declines and their potential causes
3. Adulteration patterns and associated risk factors
4. Supplier or route-related observations (if detectable)
5. Quality drift or stability assessment across the week
6. Key risks or concerns identified
7. Specific operational recommendations for next week

Weekly Dataset Summary:
{weekly_summary_text}

Write ONLY the narrative in clear, professional language. No JSON, no lists, no markdown formatting.
"""

        narrative = call_llm_narrative(prompt)
        self.add_narrative(
            story,
            "AI Weekly Intelligence Report",
            narrative,
            is_ai=True
        )

        story.append(PageBreak())

        # =====================================================
        # PAGE 4 — DETAILED STATISTICS
        # =====================================================

        self.add_section_header(story, "Statistical Summary")

        stats_data = [
            ["Metric", "Mean", "Std Dev", "Min", "Max"],
            ["FAT (%)", f"{df['fat'].mean():.2f}", f"{df['fat'].std():.2f}", f"{df['fat'].min():.2f}", f"{df['fat'].max():.2f}"],
            ["SNF (%)", f"{df['snf'].mean():.2f}", f"{df['snf'].std():.2f}", f"{df['snf'].min():.2f}", f"{df['snf'].max():.2f}"],
            ["TS (%)", f"{df['ts'].mean():.2f}", f"{df['ts'].std():.2f}", f"{df['ts'].min():.2f}", f"{df['ts'].max():.2f}"],
        ]

        self.add_table(
            story,
            "Descriptive Statistics",
            stats_data,
            description="Detailed statistical measures including mean, standard deviation, and range values."
        )

        # EXPORT PDF
        return self.export(story)