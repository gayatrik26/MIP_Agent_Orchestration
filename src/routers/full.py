# src/routers/full.py
from fastapi import APIRouter, Request, HTTPException
import datetime

# Updated imports (includes adulteration shap)
from src.services.shap_service import (
    compute_shap_summary,
    compute_adulteration_shap
)

from src.services.risk_service import compute_traffic_cards, recompute_adulteration_risk
from src.services.price_service import calculate_price

from src.utils.history_utils import load_history_df
from src.services.analytics_service import (
    compute_sample_analytics,
    compute_supplier_analytics,
    compute_route_analytics,
    compute_batch_analytics,
    compute_global_analytics
)

router = APIRouter(prefix="/full", tags=["Unified Full API"])


@router.get("/")
def unified_full(request: Request):

    sample = request.app.mqtt_latest
    if sample is None:
        raise HTTPException(status_code=404, detail="No MQTT sample received yet")

    # -----------------------------------------------------
    # REMOVE SPECTRAL DATA + SHAP DATA
    # -----------------------------------------------------
    cleaned_sample = {
        k: v for k, v in sample.items()
        if not (
            (str(k).isdigit() and 900 <= int(k) <= 4000)  # spectral data
            or ("shap" in str(k).lower())                 # remove SHAP keys
            or ("shap_value" in str(k).lower())           # remove any SHAP artifacts
        )
    }


    # -----------------------------------------------------
    # SHAP — FAT
    # -----------------------------------------------------
    try:
        shap_fat = compute_shap_summary(sample, target="fat")
    except Exception as e:
        shap_fat = {"error": str(e)}

    # -----------------------------------------------------
    # SHAP — TS
    # -----------------------------------------------------
    try:
        shap_ts = compute_shap_summary(sample, target="ts")
    except Exception as e:
        shap_ts = {"error": str(e)}

    # -----------------------------------------------------
    # SHAP — ADULTERATION
    # -----------------------------------------------------
    try:
        shap_adult = compute_adulteration_shap(sample)
    except Exception as e:
        shap_adult = {"error": str(e)}

    # -----------------------------------------------------
    # TRAFFIC CARDS
    # -----------------------------------------------------
    try:
        risk_data = compute_traffic_cards(sample)
    except Exception as e:
        risk_data = {"error": str(e)}

    # -----------------------------------------------------
    # ADULTERATION RECOMPUTED
    # -----------------------------------------------------
    try:
        adulteration_calc = recompute_adulteration_risk(sample)
    except Exception as e:
        adulteration_calc = {"error": str(e)}

    # -----------------------------------------------------
    # PRICE
    # -----------------------------------------------------
    try:
        price_data = calculate_price(sample)
    except Exception as e:
        price_data = {"error": str(e)}

    # -----------------------------------------------------
    # ANALYTICS (History)
    # -----------------------------------------------------
    df = load_history_df()

    if df is not None and len(df) > 0:
        latest = df.iloc[-1].to_dict()
        sample_stats = compute_sample_analytics(latest)
        supplier_stats = compute_supplier_analytics(df, latest)
        route_stats = compute_route_analytics(df, latest)
        batch_stats = compute_batch_analytics(df, latest)
        global_stats = compute_global_analytics(df)
    else:
        sample_stats = supplier_stats = route_stats = batch_stats = global_stats = {}

    # -----------------------------------------------------
    # FINAL RESPONSE
    # -----------------------------------------------------
    return {
        "sample": cleaned_sample,

        "shap": {
            "fat": shap_fat,
            "ts": shap_ts,
            "adulteration": shap_adult
        },

        "quality": {
            "traffic_cards": risk_data,
            "adulteration_recomputed": adulteration_calc,
            "price": price_data
        },

        "analytics": {
            "sample": sample_stats,
            "supplier": supplier_stats,
            "route": route_stats,
            "batch": batch_stats,
            "global": global_stats
        },

        "timestamp": datetime.datetime.now().isoformat()
    }
