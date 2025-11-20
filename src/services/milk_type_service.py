# src/services/milk_type_service.py

def _extract_value(v):
    """Extracts numeric value from:
       - raw float/int
       - dict like {"value": 4.5}
       - string numeric
    """
    if isinstance(v, (int, float)):
        return float(v)

    if isinstance(v, str) and v.replace(".", "", 1).isdigit():
        return float(v)

    if isinstance(v, dict) and "value" in v:
        return float(v["value"])

    # fallback
    return 0.0


def classify_milk_type(fat: float, snf: float, ts: float):
    """Rule-based classifier."""

    # PLANT-BASED
    if fat < 1.5 and snf < 6.5:
        return "almond"

    if fat < 2.0 and snf < 7.5:
        return "oat"

    if 1.0 <= fat <= 2.5 and 7.0 <= snf <= 9.0:
        return "soy"

    # ANIMAL
    if fat > 6.0 and snf > 9.5:
        return "buffalo"

    if 2.0 < fat < 3.8 and 8.5 <= snf <= 9.5:
        return "camel"

    if fat < 4.0 and snf < 8.6:
        return "goat"

    if 3.2 <= fat <= 5.8 and 8 <= snf <= 9.8:
        return "cow"

    return "unknown"


def classify_milk_from_payload(payload: dict):
    """Extract values safely from traffic_cards."""
    tc = payload.get("traffic_cards", {})

    fat = _extract_value(tc.get("fat"))
    snf = _extract_value(tc.get("snf"))
    ts = _extract_value(tc.get("ts"))

    milk_type = classify_milk_type(fat, snf, ts)

    return {
        "milk_type": milk_type
    }
