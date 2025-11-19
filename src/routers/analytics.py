from fastapi import APIRouter, HTTPException
from src.services.analytics_service import compute_full_analytics

router = APIRouter(prefix="/analytics", tags=["Analytics"])


@router.get("/full")
def full_analytics():
    result = compute_full_analytics()

    if result is None:
        raise HTTPException(status_code=404, detail="No analytics available yet")

    return result
