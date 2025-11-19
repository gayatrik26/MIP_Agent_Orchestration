# routers/test_loader.py
from fastapi import APIRouter, Request
import json

from src.services.shap_service import compute_shap_for_payload
from src.services.risk_service import compute_traffic_cards, recompute_adulteration_risk
from src.services.price_service import calculate_price

router = APIRouter(prefix="/test", tags=["Test Utilities"])

@router.post("/load-sample")
async def load_sample(request: Request):
    """
    Inject sample data manually (use instead of MQTT).
    Stores only the latest prediction.
    """
    payload = await request.json()

    # Store raw
    request.app.mqtt_latest = payload

    # SHAP fat
    try:
        payload.setdefault("shap", {})
        payload["shap"]["fat"] = compute_shap_for_payload(payload, target="fat")
    except:
        payload["shap"]["fat"] = None

    # SHAP ts
    try:
        payload.setdefault("shap", {})
        payload["shap"]["ts"] = compute_shap_for_payload(payload, target="ts")
    except:
        payload["shap"]["ts"] = None

    # Traffic cards
    try:
        payload["traffic_cards"] = compute_traffic_cards(payload)
    except:
        payload["traffic_cards"] = None

    # Adulteration
    try:
        payload["adulteration_recomputed"] = recompute_adulteration_risk(payload)
    except:
        payload["adulteration_recomputed"] = None

    # Pricing
    try:
        payload["price"] = calculate_price(payload)
    except:
        payload["price"] = None

    return {"status": "loaded", "keys": list(payload.keys())}
