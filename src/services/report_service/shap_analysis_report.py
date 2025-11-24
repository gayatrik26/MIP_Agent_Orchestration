import pandas as pd
from reportlab.platypus import Paragraph, Spacer
from reportlab.lib.units import inch

from src.services.report_service.base_report import BaseReport
from src.utils.shap_cache import get_shap_history
from src.utils.chart_utils import generate_bar_chart
from src.services.recommendation_service import _call_llm  # reuse Azure client


class ShapAnalysisReport(BaseReport):
    def __init__(self):
        super().__init__("SHAP Analysis Summary Report")

    def _build_feature_df(self, records, key: str, is_adulteration: bool = False):
        """
        Flatten top_10 SHAP lists into a feature-level DataFrame.

        key:
          - "fat"
          - "ts"
          - "adulteration"
        """
        rows = []

        for rec in records:
            block = rec.get(key, {}) or {}
            top10 = block.get("top_10", []) or []
            for item in top10:
                if is_adulteration:
                    feature = str(item.get("feature"))
                else:
                    feature = str(item.get("wavelength"))

                abs_shap = float(item.get("abs_shap", 0.0))
                rows.append({"feature": feature, "abs_shap": abs_shap})

        if not rows:
            return pd.DataFrame(columns=["feature", "abs_shap"])

        df = pd.DataFrame(rows)
        agg = (
            df.groupby("feature")["abs_shap"]
              .agg(["count", "mean"])
              .reset_index()
              .rename(columns={"mean": "mean_abs_shap"})
        )

        # Sort by importance
        agg = agg.sort_values("mean_abs_shap", ascending=False)
        return agg

    def _ai_shap_narrative(self, fat_df, ts_df, adult_df):
        """
        Call Azure OpenAI to generate a narrative about SHAP behavior.
        """
        def _df_top(df, label):
            if df.empty:
                return f"{label}: no data\n"
            top = df.head(5).to_dict(orient="records")
            return f"{label} top features:\n{top}\n"

        summary_text = (
            _df_top(fat_df, "FAT") +
            _df_top(ts_df, "TS") +
            _df_top(adult_df, "Adulteration")
        )

        prompt = f"""
You are an AI explainability expert for dairy quality models.

We have SHAP feature importance summaries for:
- FAT prediction
- TS prediction
- Adulteration risk model

Below is a compact summary of the top SHAP features and their average absolute impact:

{summary_text}

Write a clear narrative (2–3 short paragraphs) explaining:
- Which features/wavelengths most strongly influence FAT and TS
- Which features are driving adulteration risk
- Any patterns or overlaps across models
- Practical insights for quality control teams

Write only the narrative text. No bullet lists, no JSON.
"""

        result = _call_llm(prompt)

        # If _call_llm returns dict in error case:
        if isinstance(result, dict) and "error" in result:
            return f"AI SHAP narrative generation failed: {result}"

        # Normal case: result is a string
        return str(result)

    # ----------------------------------------------------------
    # MAIN BUILD
    # ----------------------------------------------------------
    def build(self):
        # Get recent SHAP records from cache
        shap_records = get_shap_history(limit=100)
        if not shap_records:
            raise Exception("No SHAP history available. Wait for live data to flow first.")

        # Build aggregated feature frames
        fat_df = self._build_feature_df(shap_records, "fat", is_adulteration=False)
        ts_df = self._build_feature_df(shap_records, "ts", is_adulteration=False)
        adult_df = self._build_feature_df(shap_records, "adulteration", is_adulteration=True)

        story = []

        # ------------------------------------------------------
        # TITLE PAGE (already adds a page break)
        # ------------------------------------------------------
        self.add_title_page(story)

        # ------------------------------------------------------
        # SHAP SUMMARY TABLES (same page, continuous)
        # ------------------------------------------------------
        story.append(Paragraph("<b>FAT Model — Top SHAP Features</b>", self.styles["Heading2"]))
        story.append(Spacer(1, 0.15 * inch))
        fat_table_data = [["Feature", "Mean |SHAP|", "Count"]]
        for _, row in fat_df.head(8).iterrows():
            fat_table_data.append([
                row["feature"],
                f"{row['mean_abs_shap']:.2f}",
                int(row["count"])
            ])
        self.add_table(story, "", fat_table_data)

        story.append(Spacer(1, 0.3 * inch))

        story.append(Paragraph("<b>TS Model — Top SHAP Features</b>", self.styles["Heading2"]))
        story.append(Spacer(1, 0.15 * inch))
        ts_table_data = [["Feature", "Mean |SHAP|", "Count"]]
        for _, row in ts_df.head(8).iterrows():
            ts_table_data.append([
                row["feature"],
                f"{row['mean_abs_shap']:.2f}",
                int(row["count"])
            ])
        self.add_table(story, "", ts_table_data)

        story.append(Spacer(1, 0.3 * inch))

        story.append(Paragraph("<b>Adulteration Model — Top SHAP Features</b>", self.styles["Heading2"]))
        story.append(Spacer(1, 0.15 * inch))
        adult_table_data = [["Feature", "Mean |SHAP|", "Count"]]
        for _, row in adult_df.head(8).iterrows():
            adult_table_data.append([
                row["feature"],
                f"{row['mean_abs_shap']:.4f}",
                int(row["count"])
            ])
        self.add_table(story, "", adult_table_data)

        story.append(Spacer(1, 0.4 * inch))

        # ------------------------------------------------------
        # CHARTS (reuse generic bar chart util)
        # ------------------------------------------------------
        if not fat_df.empty:
            fat_chart_path = generate_bar_chart(
                fat_df.head(10).rename(columns={"feature": "x", "mean_abs_shap": "y"}),
                y="y",
                x="x",
                filename="shap_fat_top.png",
            )
            self.add_chart(story, "FAT Model — Top 10 SHAP Features", fat_chart_path)

        if not ts_df.empty:
            ts_chart_path = generate_bar_chart(
                ts_df.head(10).rename(columns={"feature": "x", "mean_abs_shap": "y"}),
                y="y",
                x="x",
                filename="shap_ts_top.png",
            )
            self.add_chart(story, "TS Model — Top 10 SHAP Features", ts_chart_path)

        if not adult_df.empty:
            adult_chart_path = generate_bar_chart(
                adult_df.head(10).rename(columns={"feature": "x", "mean_abs_shap": "y"}),
                y="y",
                x="x",
                filename="shap_adulteration_top.png",
            )
            self.add_chart(story, "Adulteration Model — Top 10 SHAP Features", adult_chart_path)

        # ------------------------------------------------------
        # AI NARRATIVE (same page, no extra PageBreak)
        # ------------------------------------------------------
        narrative_text = self._ai_shap_narrative(fat_df, ts_df, adult_df)
        narrative_text = narrative_text.replace("\n", "<br/>")

        self.add_narrative(story, "AI SHAP Analysis Narrative", narrative_text)

        # Export as BytesIO
        return self.export(story)
