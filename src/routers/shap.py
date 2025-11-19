from fastapi import APIRouter, Request

from src.services.shap_service import (
    compute_shap_summary,
    compute_adulteration_shap
)

router = APIRouter(prefix="/shap", tags=["SHAP"])


# ----------------------------------------------------
# Helper: route target → correct SHAP function
# ----------------------------------------------------
def _compute_shap(sample, target):
    if target.lower() in ["fat", "ts"]:
        return compute_shap_summary(sample, target.lower())
    elif target.lower() == "adulteration":
        return compute_adulteration_shap(sample)
    else:
        return {"error": f"Unknown SHAP target '{target}' (allowed: fat, ts, adulteration)"}


# ----------------------------------------------------
# 1. GET — SHAP summary for the latest MQTT sample
# ----------------------------------------------------
@router.get("/latest/{target}")
def shap_latest(target: str, request: Request):
    sample = request.app.mqtt_latest

    if not sample:
        return {"error": "No MQTT data received"}

    try:
        return _compute_shap(sample, target)
    except Exception as e:
        return {"error": str(e)}


# ----------------------------------------------------
# 2. POST — SHAP summary for any JSON payload
# ----------------------------------------------------
@router.post("/compute/{target}")
def shap_compute(target: str, payload: dict):
    try:
        return _compute_shap(payload, target)
    except Exception as e:
        return {"error": str(e)}
