# routers/risk.py
from fastapi import APIRouter, Request
from src.services.risk_service import compute_traffic_cards, recompute_adulteration_risk

router = APIRouter(prefix="/risk", tags=["Risk"])

@router.get("/latest")
def risk_latest(request: Request):
    """
    Return risk assessment (traffic cards + adulteration)
    for the latest streaming sample.
    """
    sample = request.app.mqtt_latest
    if not sample:
        return {"error": "No data received yet"}

    # traffic cards
    try:
        cards = compute_traffic_cards(sample)
    except Exception as e:
        cards = {"error": str(e)}

    # adulteration recomputation
    try:
        recomputed = recompute_adulteration_risk(sample)
    except Exception as e:
        recomputed = {"error": str(e)}

    return {
        "cards": cards,
        "adulteration_recomputed": recomputed
    }
