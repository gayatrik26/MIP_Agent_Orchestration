# services/price_service.py

BASE_PRICE = 33.0


def get_value(payload, *keys):
    """
    Fetch value from payload or payload['inference'].
    Does NOT treat 0 or 0.0 as missing.
    Returns None only if not found anywhere.
    """

    # 1️⃣ Top-level lookup
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]

    # 2️⃣ Look inside payload['inference']
    if "inference" in payload and isinstance(payload["inference"], dict):
        inf = payload["inference"]
        for key in keys:
            if key in inf and inf[key] is not None:
                return inf[key]

    # 3️⃣ Not found
    return None

def calculate_quality_score(payload):
    """
    Weighted quality score using fat, snf, ts from REAL MQTT payload.
    Values are pulled from payload['inference'] if present.
    """

    fat = get_value(payload, "fat_predicted", "fat", "Fat_content")
    snf = get_value(payload, "snf")
    ts  = get_value(payload, "total_solids_predicted", "total_solids", "Total_Solids")

    print("Calculating quality score with fat:", fat, "snf:", snf, "ts:", ts)

    if fat is None or snf is None or ts is None:
        return 0.0

    try:
        fat = float(fat)
        snf = float(snf)
        ts  = float(ts)
    except Exception:
        return 0.0

    # Weighted average
    score = (fat * 0.4) + (snf * 0.35) + (ts * 0.25)
    return score


def calculate_price(payload):
    score = calculate_quality_score(payload)

    # Score roughly between 8 and 20
    # Convert to 0.5–1.5 multiplier
    multiplier = max(0.5, min(1.5, score / 10.0))

    final_price = round(BASE_PRICE * multiplier, 2)

    return {
        "base_price": BASE_PRICE,
        "quality_score": round(score, 3),
        "final_price": final_price
    }
