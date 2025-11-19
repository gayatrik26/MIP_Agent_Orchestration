import numpy as np
import shap
from .model_loader import models
from .preprocess import preprocess_vector


# ------------------------------------------------------
# SAFE VALUE FETCHER
# ------------------------------------------------------

def get_value(payload, *keys):
    """Fetch value from payload or payload['inference']."""
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]

    inf = payload.get("inference", {})
    if isinstance(inf, dict):
        for key in keys:
            if key in inf and inf[key] is not None:
                return inf[key]

    return None


# ------------------------------------------------------
# SHAP EXPLAINER CACHE
# ------------------------------------------------------

_EXPLAINERS = {
    "fat": None,
    "ts": None,
    "adulteration": None
}


def _make_background(dim, n=50):
    return np.zeros((n, dim))


def _get_linear_explainer(model, dim):
    background = _make_background(dim, n=50)
    return shap.LinearExplainer(
        model,
        background,
        feature_perturbation="interventional"
    )


# ------------------------------------------------------
# EXTRACT SPECTRAL DATA FROM REAL PAYLOAD
# ------------------------------------------------------

def extract_spectral_from_payload(payload):
    """
    Extract spectral vector from:
        payload["inference"]["metadata"]
    """

    spectral_cols = models.pls_fat["spectral_cols"]
    metadata = payload.get("inference", {}).get("metadata", {})

    vec = []
    for wl in spectral_cols:
        val = metadata.get(str(wl), 0.0)
        vec.append(float(val))

    wavelengths = [int(w) for w in spectral_cols]
    return np.array(wavelengths), np.array(vec)


# ------------------------------------------------------
# PLS SHAP (fat/ts)
# ------------------------------------------------------

def compute_shap_for_payload(payload, target="fat"):
    wavelengths, raw_vec = extract_spectral_from_payload(payload)

    # Pick model bundle
    bundle = models.pls_fat if target == "fat" else models.pls_ts

    processed = preprocess_vector(
        raw_vec,
        window=bundle["window"],
        poly=bundle["poly"],
        derivative=bundle["derivative"]
    )

    X = processed.reshape(1, -1)

    # Load or create SHAP explainer
    if _EXPLAINERS[target] is None:
        _EXPLAINERS[target] = _get_linear_explainer(
            model=bundle["pls_model"],
            dim=len(bundle["spectral_cols"])
        )

    explainer = _EXPLAINERS[target]

    shap_values = explainer.shap_values(X)
    if isinstance(shap_values, list):
        shap_values = shap_values[0]

    shap_values = np.array(shap_values).flatten()

    # Ranking
    sorted_idx = np.argsort(np.abs(shap_values))[::-1]
    sorted_wavelengths = wavelengths[sorted_idx]

    return {
        "top_10_influential_wavelengths": sorted_wavelengths[:10].tolist()
    }


def compute_shap_summary(payload, target="fat"):
    wavelengths, raw_vec = extract_spectral_from_payload(payload)

    # Pick model
    bundle = models.pls_fat if target == "fat" else models.pls_ts

    processed = preprocess_vector(
        raw_vec,
        window=bundle["window"],
        poly=bundle["poly"],
        derivative=bundle["derivative"]
    )

    X = processed.reshape(1, -1)

    # Load or create SHAP explainer
    if _EXPLAINERS[target] is None:
        _EXPLAINERS[target] = _get_linear_explainer(
            model=bundle["pls_model"],
            dim=len(bundle["spectral_cols"])
        )

    explainer = _EXPLAINERS[target]

    shap_values = explainer.shap_values(X)
    if isinstance(shap_values, list):
        shap_values = shap_values[0]

    shap_values = np.array(shap_values).flatten()

    shap_score = float(np.sum(np.abs(shap_values)))

    sorted_idx = np.argsort(np.abs(shap_values))[::-1]

    top_10 = []
    for idx in sorted_idx[:10]:
        top_10.append({
            "wavelength": int(wavelengths[idx]),
            "shap_value": float(shap_values[idx]),
            "abs_shap": float(abs(shap_values[idx]))
        })

    return {
        "shap_score": shap_score,
        "top_10": top_10,
        "mean_shap": "0",
    }


# ------------------------------------------------------
# ISOLATION FOREST SHAP FOR ADULTERATION
# ------------------------------------------------------

def compute_adulteration_shap(payload):
    """
    Compute SHAP summary for the adulteration IsolationForest model.
    Includes:
      - shap_score
      - top_10 features
      - negative_mean_shap (average of shap_value for top 10)
    """

    iso_bundle = models.iso
    if iso_bundle is None:
        return {"error": "IsolationForest model not loaded"}

    iso_model = iso_bundle["iso_model"]
    spectral_cols = iso_bundle["spectral_cols"]
    important_cols = iso_bundle["important_cols"]

    window = iso_bundle["window"]
    poly = iso_bundle["poly"]
    derivative = iso_bundle["derivative"]

    metadata = payload.get("inference", {}).get("metadata", {})

    # ----------------------------------------------------
    # 1. Extract raw spectral vector
    # ----------------------------------------------------
    vec = []
    for wl in spectral_cols:
        vec.append(float(metadata.get(str(wl), 0.0)))

    raw = np.array(vec).reshape(1, -1)

    # ----------------------------------------------------
    # 2. Apply SAME preprocessing as training
    #    (savgol -> SNV -> minmax)
    # ----------------------------------------------------
    from scipy.signal import savgol_filter

    # SG
    wl = window if window % 2 == 1 else window + 1
    sg = savgol_filter(raw, window_length=wl, polyorder=poly, deriv=derivative, axis=1)

    # SNV
    snv = (sg - sg.mean(axis=1, keepdims=True)) / (sg.std(axis=1, keepdims=True) + 1e-12)

    # MinMax
    mn = snv.min(axis=1, keepdims=True)
    mx = snv.max(axis=1, keepdims=True)
    norm = (snv - mn) / (mx - mn + 1e-12)

    # ----------------------------------------------------
    # 3. Add biochemical features
    # ----------------------------------------------------
    biochemical = []
    inf = payload.get("inference", {})
    for col in important_cols:
        biochemical.append(float(inf.get(col, 0.0)))

    biochemical = np.array(biochemical).reshape(1, -1)

    # Final feature vector
    X = np.hstack([norm, biochemical])

    # ----------------------------------------------------
    # 4. Compute SHAP
    # ----------------------------------------------------
    explainer = shap.TreeExplainer(iso_model)
    shap_values = explainer.shap_values(X)
    shap_values = np.array(shap_values).flatten()

    # Total magnitude
    shap_score = float(np.sum(np.abs(shap_values)))

    # ----------------------------------------------------
    # 5. Rank features
    # ----------------------------------------------------
    feature_names = [str(w) for w in spectral_cols] + important_cols
    idx_sorted = np.argsort(np.abs(shap_values))[::-1]

    top_10 = []
    for idx in idx_sorted[:10]:
        top_10.append({
            "feature": feature_names[idx],
            "shap_value": float(shap_values[idx]),
            "abs_shap": float(abs(shap_values[idx]))
        })

    # ----------------------------------------------------
    # 6. Negative mean SHAP (for your severity analysis)
    # ----------------------------------------------------
    negative_mean_shap = float(np.mean([f["shap_value"] for f in top_10]))

    # ----------------------------------------------------
    # Final JSON
    # ----------------------------------------------------
    return {
        "shap_score": shap_score,
        "mean_shap": negative_mean_shap,
        "top_10": top_10
    }
