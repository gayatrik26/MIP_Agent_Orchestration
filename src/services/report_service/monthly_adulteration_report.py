import pandas as pd
from reportlab.platypus import Spacer
from reportlab.lib.units import inch

from src.services.report_service.base_report import BaseReport
from src.utils.db_utils import fetch_history_df
from src.utils.chart_utils import (
    generate_adulteration_trend_chart,
    generate_adulteration_supplier_bar,
    generate_adulteration_route_bar,
)
from src.services.recommendation_service import call_llm_narrative


class MonthlyAdulterationReport(BaseReport):
    def __init__(self, logo_path=None):
        super().__init__(
            title="Monthly Adulteration Analysis Report",
            subtitle="30-Day Risk Assessment & Quality Control Insights",
            logo_path=logo_path
        )

    def build(self):
        # =====================================================
        # 1. LOAD LAST 30 DAYS
        # =====================================================
        df = fetch_history_df(days=30)
        if df.empty:
            raise Exception("No data available for monthly report.")

        # Convert Decimal columns to float
        decimal_columns = ['fat', 'snf', 'ts', 'sample_score']
        for col in decimal_columns:
            if col in df.columns:
                df[col] = df[col].astype(float)

        df = df.dropna(subset=["supplier_id", "route_id"])

        story = []

        # =====================================================
        # TITLE PAGE
        # =====================================================
        self.add_title_page(story)

        # =====================================================
        # PAGE 1 — KEY METRICS & SUMMARY TABLE
        # =====================================================

        total_samples = len(df)
        adulteration_rate = df["is_adulterated"].mean() * 100
        unique_suppliers = df["supplier_id"].nunique()
        unique_routes = df["route_id"].nunique()
        adulterated_samples = int(df["is_adulterated"].sum())

        # Key metrics box
        key_metrics = {
            "Total Samples": str(total_samples),
            "Adulterated Samples": str(adulterated_samples),
            "Adulteration Rate": f"{adulteration_rate:.1f}%",
            "Risk Level": "HIGH" if adulteration_rate > 10 else ("MEDIUM" if adulteration_rate > 5 else "LOW"),
        }
        self.add_metrics_box(story, key_metrics)

        # Summary table
        summary_data = [
            ["Metric", "Value", "Status"],
            ["Total Samples (30 days)", str(total_samples), "📊"],
            ["Unique Suppliers", str(unique_suppliers), "🏭"],
            ["Unique Routes", str(unique_routes), "🚚"],
            ["Avg Monthly Adulteration Rate", f"{adulteration_rate:.1f}%", "⚠️" if adulteration_rate > 5 else "✓"],
            ["Adulterated Samples", str(adulterated_samples), "🔴" if adulterated_samples > 0 else "✓"],
        ]

        self.add_table(
            story,
            "Monthly Adulteration Summary",
            summary_data,
            description="Comprehensive overview of adulteration incidents detected over the past 30 days."
        )

        # =====================================================
        # PAGE 2 — ADULTERATION TREND ANALYSIS
        # =====================================================

        # 1. Daily adulteration trend
        trend_path = generate_adulteration_trend_chart(df, filename="monthly_adulteration_trend.png")
        self.add_chart(
            story,
            "Daily Adulteration Trend (30 Days)",
            trend_path,
            description="Time-series analysis showing adulteration frequency across the month. Spikes may indicate supply chain disruptions."
        )

        # 2. Adulteration rate by supplier
        supplier_path = generate_adulteration_supplier_bar(df, filename="supplier_adulteration_30d.png")
        self.add_chart(
            story,
            "Adulteration Risk by Supplier (%)",
            supplier_path,
            description="Ranking of suppliers by adulteration rate. Suppliers with higher percentages require immediate investigation and remediation."
        )

        # 3. Adulteration rate by route
        route_path = generate_adulteration_route_bar(df, filename="route_adulteration_30d.png")
        self.add_chart(
            story,
            "Adulteration Risk by Route (%)",
            route_path,
            description="Route-level adulteration analysis. High-risk routes may indicate transportation/storage issues or collection point vulnerabilities."
        )

        # =====================================================
        # PAGE 3 — AI-GENERATED INSIGHTS
        # =====================================================

        # Calculate high-risk suppliers and routes
        supplier_adult = df.groupby("supplier_id")["is_adulterated"].agg(['sum', 'count']).reset_index()
        supplier_adult['rate'] = (supplier_adult['sum'] / supplier_adult['count'] * 100).round(2)
        supplier_adult = supplier_adult.sort_values('rate', ascending=False)

        route_adult = df.groupby("route_id")["is_adulterated"].agg(['sum', 'count']).reset_index()
        route_adult['rate'] = (route_adult['sum'] / route_adult['count'] * 100).round(2)
        route_adult = route_adult.sort_values('rate', ascending=False)

        top_sup = supplier_adult.iloc[0] if not supplier_adult.empty else None
        top_route = route_adult.iloc[0] if not route_adult.empty else None

        summary_text = f"""
Monthly Adulteration Risk Assessment:

Reporting Period: Last 30 Days
Total Samples Analyzed: {total_samples}
Average Monthly Adulteration Rate: {adulteration_rate:.1f}%
Total Adulterated Samples: {adulterated_samples}

Unique Suppliers: {unique_suppliers}
Unique Routes: {unique_routes}

Highest-Risk Supplier:
- Supplier ID: {top_sup['supplier_id'] if top_sup is not None else 'N/A'}
- Adulteration Rate: {top_sup['rate']:.1f}% ({int(top_sup['sum'])} of {int(top_sup['count'])} samples)

Highest-Risk Route:
- Route ID: {top_route['route_id'] if top_route is not None else 'N/A'}
- Adulteration Rate: {top_route['rate']:.1f}% ({int(top_route['sum'])} of {int(top_route['count'])} samples)

Top 3 Suspect Suppliers:
"""

        for i, (_, row) in enumerate(supplier_adult.head(3).iterrows(), 1):
            summary_text += f"\n{i}. Supplier {row['supplier_id']}: {row['rate']:.1f}% adulteration rate"

        summary_text += "\n\nTop 3 Suspect Routes:\n"

        for i, (_, row) in enumerate(route_adult.head(3).iterrows(), 1):
            summary_text += f"\n{i}. Route {row['route_id']}: {row['rate']:.1f}% adulteration rate"

        prompt = f"""
You are a senior dairy supply chain quality and compliance expert.

Based on the comprehensive monthly adulteration risk assessment below, write a detailed, professional narrative 
(3–4 paragraphs) describing:

1. Overall adulteration trends and monthly patterns
2. High-risk suppliers and routes requiring immediate intervention
3. Potential root causes (storage, transportation, handling, supplier practices)
4. Severity assessment and risk classification
5. Specific, actionable corrective measures (audits, inspections, supplier performance programs, route optimization)
6. Recommended escalation and follow-up timeline

Adulteration Assessment:
{summary_text}

Write only the narrative in clear, professional language with specific recommendations. No bullet points, no JSON, no markdown formatting.
"""

        narrative = call_llm_narrative(prompt)
        self.add_narrative(
            story,
            "AI Monthly Adulteration Intelligence",
            narrative,
            is_ai=True
        )

        # =====================================================
        # PAGE 4 — DETAILED SUPPLIER & ROUTE ANALYSIS
        # =====================================================

        self.add_section_header(story, "Supplier & Route Adulteration Details")

        # Detailed supplier analysis table
        detail_data = [
            ["Supplier", "Total Samples", "Adulterated", "Rate (%)", "Risk Level"],
        ]

        for _, row in supplier_adult.iterrows():
            risk_level = "🔴 HIGH" if row['rate'] > 20 else ("🟡 MEDIUM" if row['rate'] > 10 else "🟢 LOW")
            detail_data.append([
                str(row['supplier_id']),
                str(int(row['count'])),
                str(int(row['sum'])),
                f"{row['rate']:.1f}%",
                risk_level,
            ])

        self.add_table(
            story,
            "Supplier-Level Adulteration Analysis",
            detail_data,
            description="Detailed breakdown of adulteration incidents by supplier with risk classification."
        )

        # Detailed route analysis table
        route_detail_data = [
            ["Route", "Total Samples", "Adulterated", "Rate (%)", "Risk Level"],
        ]

        for _, row in route_adult.iterrows():
            risk_level = "🔴 HIGH" if row['rate'] > 20 else ("🟡 MEDIUM" if row['rate'] > 10 else "🟢 LOW")
            route_detail_data.append([
                str(row['route_id']),
                str(int(row['count'])),
                str(int(row['sum'])),
                f"{row['rate']:.1f}%",
                risk_level,
            ])

        self.add_table(
            story,
            "Route-Level Adulteration Analysis",
            route_detail_data,
            description="Detailed breakdown of adulteration incidents by collection route with risk classification."
        )

        # =====================================================
        # EXPORT PDF
        # =====================================================
        return self.export(story)
    