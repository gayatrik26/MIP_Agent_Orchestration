# src/routers/milk_type.py
from fastapi import APIRouter, Request
from src.services.milk_type_service import classify_milk_from_payload

router = APIRouter(prefix="/milk-type", tags=["Milk Type"])

@router.get("/latest")
def milk_type_latest(request: Request):
    sample = request.app.mqtt_latest

    if not sample:
        return {"error": "No MQTT data received"}

    try:
        return classify_milk_from_payload(sample)
    except Exception as e:
        return {"error": str(e)}


@router.post("/classify")
def classify_milk(payload: dict):
    try:
        return classify_milk_from_payload(payload)
    except Exception as e:
        return {"error": str(e)}
