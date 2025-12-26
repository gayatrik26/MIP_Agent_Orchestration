import os
import json
import datetime
from openai import AzureOpenAI

# ======================================================
# Azure OpenAI Client Setup
# ======================================================

AZURE_OPENAI_ENDPOINT = "https://kdnai-openai-partnersquad-dev-eastus2.openai.azure.com/"
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_KEY")

client = AzureOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_key=AZURE_OPENAI_API_KEY,
    api_version="2025-01-01-preview",
)

# Azure deployment name (your model name)
MODEL = "gpt-4.1-mini"


# ======================================================
# Extract analytics from payload
# ======================================================
def _extract_context(payload):
    inf = payload.get("inference", {})

    fat = inf.get("fat", 0)
    snf = inf.get("snf", 0)
    ts = inf.get("ts", 0)
    adulteration_risk = payload.get("adulteration_recomputed", {}).get("risk", 0)
    is_adulterated = payload.get("adulteration_recomputed", {}).get("is_adulterated", 0)

    milk_type = payload.get("milk_type", "unknown")

    analytics = payload.get("analytics", {})
    supplier = analytics.get("supplier", {})
    route = analytics.get("route", {})
    batch = analytics.get("batch", {})
    sample_stats = analytics.get("sample", {})

    return {
        "fat": fat,
        "snf": snf,
        "ts": ts,
        "milk_type": milk_type,
        "adulteration_risk": adulteration_risk,
        "is_adulterated": is_adulterated,
        "supplier": supplier,
        "route": route,
        "batch": batch,
        "sample": sample_stats,
    }


# ======================================================
# Prompt: Recommendations based on alerts
# ======================================================
def _build_alert_prompt(alerts, ctx):
    alerts_text = json.dumps(alerts, indent=2)

    return f"""
You are an expert dairy quality advisor.

Several quality alerts have been triggered:

{alerts_text}

Using these measured values:
- Fat: {ctx['fat']}
- SNF: {ctx['snf']}
- TS: {ctx['ts']}
- Milk Type: {ctx['milk_type']}
- Adulteration Risk: {ctx['adulteration_risk']}

Supplier Analytics:
{ctx['supplier']}

Route Analytics:
{ctx['route']}

Batch Analytics:
{ctx['batch']}

Generate a clear list of corrective recommendations (4–6 items).

Output JSON with:
{{
  "type": "alert_based",
  "recommendations": [...]
}}

Each recommendation must follow this structure:
{{
  "action": "<what action should be taken>",
  "reason": "<why it is needed>",
  "urgency": "<Low / Medium / High / Critical>",
  "responsible_party": "<supplier / route supervisor / lab technician / QA team>"
}}
"""


# ======================================================
# Prompt: General recommendations
# ======================================================
def _build_general_prompt(ctx):
    return f"""
You are an AI agent monitoring live milk quality in real time.

No alerts were triggered for this sample.

Using live metrics:

Fat={ctx['fat']}
SNF={ctx['snf']}
TS={ctx['ts']}
Milk Type={ctx['milk_type']}
Adulteration Risk={ctx['adulteration_risk']}

Supplier analytics:
{ctx['supplier']}

Route analytics:
{ctx['route']}

Batch analytics:
{ctx['batch']}

Generate 3–5 short routine recommendations.

Output JSON with:
{{
  "type": "routine",
  "recommendations": [...]
}}
"""


# ======================================================
# LLM CALL — Azure OpenAI
# ======================================================
def _call_llm(prompt):
    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": "You are an AI dairy-quality recommendations engine."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
        )

        text = response.choices[0].message.content

        try:
            return json.loads(text)
        except:
            return {"type": "parse_error", "raw_response": text}

    except Exception as e:
        return {"type": "error", "error": str(e)}


# ======================================================
# MAIN ENTRY — RUN RECOMMENDATION ENGINE
# ======================================================
def run_recommendation_engine(payload, alerts=None):
    """
    alerts = list of triggered alerts from alert engine
    If alerts = None => treat as empty list
    """

    alerts = alerts or []
    ctx = _extract_context(payload)

    if alerts:
        prompt = _build_alert_prompt(alerts, ctx)
    else:
        prompt = _build_general_prompt(ctx)

    output = _call_llm(prompt)

    output["timestamp"] = datetime.datetime.now().isoformat()
    output["sample_id"] = payload.get("sample_id") or ctx.get("sample", {}).get("sample_id")

    return output

# ======================================================
# PUBLIC: Narrative LLM caller for reports
# ======================================================
def call_llm_narrative(df):
    """
    Converts DF → text → sends clean prompt to Azure OpenAI.
    ALWAYS returns plain narrative text.
    """

    # Convert dataframe to readable metrics
    try:
        summary_text = f"""
Daily Milk Quality Summary:

Total Samples: {len(df)}

Average Composition:
- FAT: {df['fat'].mean():.2f}
- SNF: {df['snf'].mean():.2f}
- TS: {df['ts'].mean():.2f}

Adulteration Stats:
- Adulteration Count: {df['is_adulterated'].sum()}
- Adulteration Rate: {df['is_adulterated'].mean() * 100:.1f}%

Trend Notes:
- FAT Min/Max: {df['fat'].min():.2f} / {df['fat'].max():.2f}
- SNF Min/Max: {df['snf'].min():.2f} / {df['snf'].max():.2f}
- TS Min/Max: {df['ts'].min():.2f} / {df['ts'].max():.2f}
"""
    except Exception as e:
        summary_text = f"Error summarizing DF: {e}"

    prompt = f"""
You are an AI report-writing engine.

Write a clear professional narrative (6–10 lines) explaining:
- Composition trends
- Quality behavior
- Risks or abnormalities
- Overall health of milk supply

Summary Data:
{summary_text}

Rules:
- Only paragraphs
- No bullet points
- No lists
- No JSON
"""

    # --------- IMPORTANT: DIRECT CALL (NO JSON PARSING) ----------
    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": "You write clean professional narrative summaries."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.4,
        )

        # Azure returns plain text here
        narrative = response.choices[0].message.content
        return narrative

    except Exception as e:
        return f"[Narrative Generation Error] {str(e)}"
