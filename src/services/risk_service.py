import numpy as np
from .model_loader import models


# ------------------------------------------------------
# GENERIC SAFE VALUE FETCHER
# ------------------------------------------------------

def get_value(payload, *keys):
    """
    Fetch value from payload or payload['inference'].
    Does NOT treat 0 or 0.0 as missing.
    Returns None only if not found anywhere.
    """

    # Top-level
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]

    # Inference block
    if "inference" in payload and isinstance(payload["inference"], dict):
        inf = payload["inference"]
        for key in keys:
            if key in inf and inf[key] is not None:
                return inf[key]

    return None


# ------------------------------------------------------
# TRAFFIC LIGHT CLASSIFIER
# ------------------------------------------------------

def classify_traffic(value, low, high):
    """
    Returns 'red', 'yellow', 'green' or 'unknown' based on thresholds.
    """

    if value is None:
        return "unknown"

    try:
        v = float(value)
    except:
        return "unknown"

    if v < low:
        return "red"
    if v >= high:
        return "green"
    return "yellow"


# ------------------------------------------------------
# DEFAULT THRESHOLDS
# ------------------------------------------------------

DEFAULT_THRESHOLDS = {
    "fat": {"low": 3.5, "high": 4.5},
    "snf": {"low": 8.0, "high": 9.0},
    "ts":  {"low": 11.5, "high": 13.5}
}


# ------------------------------------------------------
# MAIN TRAFFIC-CARD COMPUTATION
# ------------------------------------------------------

def compute_traffic_cards(payload, thresholds=None):
    """
    Compute risk classification for:
        fat_predicted
        snf
        total_solids_predicted

    Automatically pulls data from payload["inference"].
    """

    if thresholds is None:
        thresholds = DEFAULT_THRESHOLDS

    fat = get_value(payload, "fat_predicted", "fat", "Fat_content")
    snf = get_value(payload, "snf")
    ts  = get_value(payload, "total_solids_predicted", "total_solids", "Total_Solids")

    print("Computing traffic cards → FAT:", fat, "SNF:", snf, "TS:", ts)

    return {
        "fat": {
            "value": fat,
            "risk": classify_traffic(fat, **thresholds["fat"])
        },
        "snf": {
            "value": snf,
            "risk": classify_traffic(snf, **thresholds["snf"])
        },
        "ts": {
            "value": ts,
            "risk": classify_traffic(ts, **thresholds["ts"])
        }
    }


# ------------------------------------------------------
# ADULTERATION RISK RECOMPUTE (IsolationForest)
# ------------------------------------------------------

def recompute_adulteration_risk(payload):
    """
    Recomputes adulteration using IsolationForest model.
    
    FIXED for your actual payload structure:
    spectral readings are inside → payload["inference"]["metadata"]
    """

    iso_bundle = models.iso
    if iso_bundle is None:
        return {"error": "IsolationForest model not available"}

    spectral_cols  = iso_bundle.get("spectral_cols", [])
    important_cols = iso_bundle.get("important_cols", [])

    # Your real structure:
    # payload["inference"]["metadata"]  contains all wavelengths
    meta = payload.get("inference", {}).get("metadata", {})

    # Build spectral vector
    raw = []
    for wl in spectral_cols:
        val = meta.get(str(wl), 0.0)
        raw.append(float(val))

    # Preprocess
    from .preprocess import preprocess_vector

    processed_spec = preprocess_vector(
        np.array(raw),
        window=iso_bundle.get("window", 15),
        poly=iso_bundle.get("poly", 3),
        derivative=iso_bundle.get("derivative", 1)
    )

    # Important metadata features
    imp_vals = [float(get_value(payload, c)) if get_value(payload, c) is not None else 0.0
                for c in important_cols]

    X = np.hstack([processed_spec, np.array(imp_vals)]).reshape(1, -1)

    iso_model = iso_bundle["iso_model"]
    score = iso_model.decision_function(X).flatten()[0]

    # Risk transformation
    risk_pct = (1.0 - score) * 50.0 + 50.0
    risk_pct = float(np.clip(risk_pct, 0.0, 100.0))

    is_ad = int(risk_pct > 50.0)

    return {
        "adulteration_risk_recomputed": risk_pct,
        "is_adulterated_recomputed": is_ad
    }
