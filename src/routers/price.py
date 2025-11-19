# routers/price.py
from fastapi import APIRouter, Request
from src.services.price_service import calculate_price

router = APIRouter(prefix="/price", tags=["Price"])

@router.get("/latest")
def price_latest(request: Request):
    """
    Price computation for latest streamed sample.
    """
    sample = request.app.mqtt_latest
    if not sample:
        return {"error": "No data received yet"}

    try:
        return {"price": calculate_price(sample)}
    except Exception as e:
        return {"error": str(e)}
