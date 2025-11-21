import pandas as pd
from reportlab.platypus import PageBreak, Spacer
from reportlab.lib.units import inch

from src.services.report_service.base_report import BaseReport
from src.utils.db_utils import fetch_history_df
from src.utils.chart_utils import (
    generate_supplier_score_bar,
    generate_supplier_adulteration_bar,
)
from src.services.recommendation_service import call_llm_narrative


class SupplierScorecardReport(BaseReport):
    def __init__(self, logo_path=None):
        super().__init__(
            title="Supplier Quality Scorecard",
            subtitle="Comprehensive Supplier Performance & Quality Analysis",
            logo_path=logo_path
        )

    def build(self):
        # =====================================================
        # 1. LOAD FULL HISTORY
        # =====================================================
        df = fetch_history_df(days=None)  # None → full history
        if df.empty:
            raise Exception("No data available for supplier scorecard report.")

        # Convert Decimal columns to float to avoid type mismatch errors
        decimal_columns = ['fat', 'snf', 'ts', 'sample_score', 'supplier_stability', 'supplier_persistence']
        for col in decimal_columns:
            if col in df.columns:
                df[col] = df[col].astype(float)

        # Defensive: drop rows without supplier_id
        df = df.dropna(subset=["supplier_id"])

        # =====================================================
        # 2. AGGREGATE PER SUPPLIER
        # =====================================================
        grouped = (
            df.groupby("supplier_id")
              .agg(
                  total_samples=("sample_id", "count"),
                  avg_fat=("fat", "mean"),
                  avg_snf=("snf", "mean"),
                  avg_ts=("ts", "mean"),
                  avg_score=("sample_score", "mean"),
                  adulteration_rate=("is_adulterated", lambda x: x.mean() * 100.0),
                  stability=("supplier_stability", "mean"),
                  persistence=("supplier_persistence", "mean"),
              )
              .reset_index()
        )

        # Handle if for some reason everything got dropped
        if grouped.empty:
            raise Exception("No valid supplier records found for scorecard.")

        # Sort by avg_score (descending)
        grouped = grouped.sort_values("avg_score", ascending=False)

        story = []

        # =====================================================
        # 3. TITLE PAGE
        # =====================================================
        self.add_title_page(story)

        # =====================================================
        # 4. PAGE 1 — KEY METRICS & SUMMARY TABLE
        # =====================================================

        best_supplier = grouped.iloc[0]
        worst_supplier = grouped.iloc[-1]
        highest_adult = grouped.sort_values("adulteration_rate", ascending=False).iloc[0]

        # Key metrics box
        key_metrics = {
            "Total Suppliers": str(len(grouped)),
            "Total Samples": str(int(df.shape[0])),
            "Best Supplier": str(best_supplier['supplier_id']),
            "Highest Adulteration": str(highest_adult['supplier_id']),
        }
        self.add_metrics_box(story, key_metrics)

        # Summary table
        table_data = [
            ["Supplier", "Samples", "Avg Score", "Adulteration %", "Stability", "Persistence"],
        ]

        for _, row in grouped.iterrows():
            table_data.append([
                str(row["supplier_id"]),
                str(int(row["total_samples"])),
                f"{row['avg_score']:.2f}",
                f"{row['adulteration_rate']:.1f}%",
                f"{row['stability']:.3f}",
                f"{row['persistence']:.3f}",
            ])

        self.add_table(
            story,
            "Supplier Quality Scorecard",
            table_data,
            description="Complete supplier performance metrics including quality scores, adulteration rates, and stability measures."
        )

        # =====================================================
        # 5. PAGE 2 — PERFORMANCE CHARTS
        # =====================================================

        # Chart 1: Supplier Average Scores
        score_chart_path = generate_supplier_score_bar(
            grouped[["supplier_id", "avg_score"]],
            filename="supplier_avg_scores.png",
        )
        self.add_chart(
            story,
            "Average Quality Score by Supplier",
            score_chart_path,
            description="Ranking of suppliers based on average quality scores. Higher scores indicate consistent, high-quality milk delivery."
        )

        # Chart 2: Supplier Adulteration Rates
        adulteration_chart_path = generate_supplier_adulteration_bar(
            grouped[["supplier_id", "adulteration_rate"]],
            filename="supplier_adulteration_rates.png",
        )
        self.add_chart(
            story,
            "Adulteration Risk by Supplier (%)",
            adulteration_chart_path,
            description="Adulteration frequency for each supplier. Suppliers with higher percentages require immediate verification and corrective action."
        )

        story.append(PageBreak())

        # =====================================================
        # 6. PAGE 3 — AI-GENERATED INSIGHTS
        # =====================================================

        text_summary = f"""
Supplier Scorecard Overview:

Total Suppliers: {len(grouped)}
Total Samples Analyzed: {int(df.shape[0])}

Best Performing Supplier:
- Supplier ID: {best_supplier['supplier_id']}
- Average Quality Score: {best_supplier['avg_score']:.2f}
- Adulteration Rate: {best_supplier['adulteration_rate']:.1f}%
- Stability Score: {best_supplier['stability']:.3f}
- Persistence Score: {best_supplier['persistence']:.3f}
- Total Samples: {int(best_supplier['total_samples'])}

Worst Performing Supplier:
- Supplier ID: {worst_supplier['supplier_id']}
- Average Quality Score: {worst_supplier['avg_score']:.2f}
- Adulteration Rate: {worst_supplier['adulteration_rate']:.1f}%
- Stability Score: {worst_supplier['stability']:.3f}
- Persistence Score: {worst_supplier['persistence']:.3f}
- Total Samples: {int(worst_supplier['total_samples'])}

Highest Adulteration Risk:
- Supplier ID: {highest_adult['supplier_id']}
- Adulteration Rate: {highest_adult['adulteration_rate']:.1f}%
- Average Quality Score: {highest_adult['avg_score']:.2f}

Global Composition Analysis:
- Average FAT: {df['fat'].mean():.2f}%
- Average SNF: {df['snf'].mean():.2f}%
- Average TS: {df['ts'].mean():.2f}%
- Global Adulteration Rate: {df['is_adulterated'].mean() * 100:.1f}%

All Suppliers Performance:
"""

        for _, row in grouped.iterrows():
            text_summary += f"\n- Supplier {row['supplier_id']}: Score={row['avg_score']:.2f}, Adulteration={row['adulteration_rate']:.1f}%, Samples={int(row['total_samples'])}"

        prompt = f"""
You are an AI dairy supply chain quality management expert.

Using the comprehensive supplier scorecard summary below, write a detailed, professional narrative
(3–4 paragraphs) that covers:

1. Best and worst performing suppliers by quality score
2. Suppliers with concerning adulteration patterns and risk levels
3. Analysis of supplier stability and persistence metrics
4. Composition trends and quality consistency across suppliers
5. Specific, actionable recommendations (e.g., targeted audits, corrective action plans, incentive programs, suspension criteria, training initiatives)
6. Prioritized follow-up actions for quality improvement

Scorecard Summary:
{text_summary}

Write only the narrative in clear, professional language. No bullet points, no JSON, no markdown formatting.
"""

        narrative = call_llm_narrative(prompt)
        self.add_narrative(
            story,
            "AI Supplier Quality Intelligence",
            narrative,
            is_ai=True
        )

        story.append(PageBreak())

        # =====================================================
        # 7. PAGE 4 — DETAILED SUPPLIER STATISTICS
        # =====================================================

        self.add_section_header(story, "Detailed Supplier Analysis")

        # Create detailed stats table
        stats_data = [
            ["Supplier", "FAT (Avg)", "FAT (Std)", "SNF (Avg)", "SNF (Std)", "TS (Avg)", "TS (Std)", "Samples"]
        ]

        for _, row in grouped.iterrows():
            supplier_data = df[df["supplier_id"] == row["supplier_id"]]
            stats_data.append([
                str(row["supplier_id"]),
                f"{supplier_data['fat'].mean():.2f}",
                f"{supplier_data['fat'].std():.2f}",
                f"{supplier_data['snf'].mean():.2f}",
                f"{supplier_data['snf'].std():.2f}",
                f"{supplier_data['ts'].mean():.2f}",
                f"{supplier_data['ts'].std():.2f}",
                str(int(row["total_samples"])),
            ])

        self.add_table(
            story,
            "Supplier Composition Statistics",
            stats_data,
            description="Detailed statistical analysis of composition parameters for each supplier including mean and standard deviation values."
        )

        # =====================================================
        # 8. EXPORT PDF
        # =====================================================
        return self.export(story)