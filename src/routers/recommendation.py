from fastapi import APIRouter, Request
from src.services.recommendation_service import run_recommendation_engine

router = APIRouter(prefix="/recommendations", tags=["Recommendations"])

@router.post("/generate")
def generate_recommendations(payload: dict):
    """
    Manually generate recommendations for any input payload.
    """
    try:
        # Let users optionally supply alerts
        alerts = payload.get("alerts", [])
        return run_recommendation_engine(payload, alerts)
    except Exception as e:
        return {"error": str(e)}
