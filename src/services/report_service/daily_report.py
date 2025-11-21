import pandas as pd
from reportlab.platypus import PageBreak, Spacer
from src.services.report_service.base_report import BaseReport
from src.utils.db_utils import fetch_history_df
from src.utils.chart_utils import (
    generate_line_chart,
    generate_fat_snf_ts_bar,
)
from src.services.recommendation_service import call_llm_narrative


class DailyQualityReport(BaseReport):
    def __init__(self, logo_path=None):
        super().__init__(
            title="Daily Milk Quality Report",
            subtitle="24-Hour Quality Analysis & Monitoring",
            logo_path=logo_path
        )

    def build(self):
        # Load last 24 hours
        df = fetch_history_df(days=1)
        if df.empty:
            raise Exception("No data available for today's report.")
        
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
        
        # Calculate key metrics
        avg_fat = df["fat"].mean()
        avg_snf = df["snf"].mean()
        avg_ts = df["ts"].mean()
        adulteration_rate = df["is_adulterated"].mean() * 100

        # Key metrics box
        key_metrics = {
            "Total Samples": str(len(df)),
            "Avg FAT": f"{avg_fat:.2f}%",
            "Avg SNF": f"{avg_snf:.2f}%",
            "Adulteration Rate": f"{adulteration_rate:.1f}%",
        }
        self.add_metrics_box(story, key_metrics)

        # Summary KPI table
        summary_rows = [
            ["Metric", "Value", "Status"],
            ["Average FAT", f"{avg_fat:.2f}%", "✓"],
            ["Average SNF", f"{avg_snf:.2f}%", "✓"],
            ["Average TS", f"{avg_ts:.2f}%", "✓"],
            ["FAT Range (Min/Max)", f"{df['fat'].min():.2f}% / {df['fat'].max():.2f}%", "📊"],
            ["SNF Range (Min/Max)", f"{df['snf'].min():.2f}% / {df['snf'].max():.2f}%", "📊"],
            ["TS Range (Min/Max)", f"{df['ts'].min():.2f}% / {df['ts'].max():.2f}%", "📊"],
            ["Adulteration Frequency", f"{adulteration_rate:.1f}%", "⚠️" if adulteration_rate > 0 else "✓"],
        ]

        self.add_table(
            story,
            "Daily Quality Summary",
            summary_rows,
            description="Comprehensive overview of milk composition metrics for the past 24 hours."
        )
        story.append(PageBreak())

        # =====================================================
        # PAGE 2 — COMPOSITION CHARTS
        # =====================================================

        fat_chart = generate_line_chart(df, y="fat", x="timestamp", filename="daily_fat_trend.png")
        self.add_chart(
            story,
            "FAT Content Trend (Last 24 Hours)",
            fat_chart,
            description="Real-time tracking of Fat percentage throughout the day. Sudden spikes or drops may indicate supply source changes."
        )

        composition_bar = generate_fat_snf_ts_bar(df, filename="daily_composition_bar.png")
        self.add_chart(
            story,
            "FAT / SNF / TS Composition Distribution",
            composition_bar,
            description="Comparative analysis of all three key milk composition parameters. Balance indicates good milk quality."
        )

        story.append(PageBreak())

        # =====================================================
        # PAGE 3 — AI-GENERATED INSIGHTS
        # =====================================================

        # Prepare data for AI narrative
        df_sample = df[["fat", "snf", "ts", "is_adulterated"]].head(30).to_dict(orient="records")

        narrative_prompt = f"""
You are an advanced dairy quality analytics LLM expert.

Write a **professional daily quality narrative** (10–15 sentences) describing:

1. Today's milk composition behavior (FAT, SNF, TS)
2. Significant variations or anomalies detected
3. Adulteration frequency and patterns
4. Quality assessment (excellent/good/fair/poor)
5. Comparison with expected ranges
6. Risk factors identified
7. Immediate recommendations for quality control

Daily Dataset Summary:
- Total Samples: {len(df)}
- Average FAT: {avg_fat:.2f}%
- Average SNF: {avg_snf:.2f}%
- Average TS: {avg_ts:.2f}%
- Adulteration Rate: {adulteration_rate:.1f}%
- FAT Range: {df['fat'].min():.2f}% - {df['fat'].max():.2f}%
- SNF Range: {df['snf'].min():.2f}% - {df['snf'].max():.2f}%
- TS Range: {df['ts'].min():.2f}% - {df['ts'].max():.2f}%
- Adulterated Samples: {int(df['is_adulterated'].sum())}

Sample Records:
{df_sample}

Write ONLY the narrative in clear, professional language. No JSON, no lists, no markdown formatting.
"""

        narrative = call_llm_narrative(narrative_prompt)
        self.add_narrative(
            story,
            "AI Daily Intelligence Report",
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
            description="Detailed statistical measures including mean, standard deviation, and range values for all composition parameters."
        )

        # =====================================================
        # EXPORT
        # =====================================================
        return self.export(story)