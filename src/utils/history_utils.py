import pandas as pd
import os
import uuid
import datetime
import numpy as np

HISTORY_FILE = "data/history.csv"


# ------------------------------------------------------
# Create directory + recreate file if corrupted or empty
# ------------------------------------------------------
def _ensure_history_exists():
    # Ensure directory exists
    dir_name = os.path.dirname(HISTORY_FILE)
    if dir_name and not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)

    # If file missing OR empty OR unreadable → recreate
    if (not os.path.exists(HISTORY_FILE)) or os.path.getsize(HISTORY_FILE) == 0:
        _create_empty_history()
        return

    # Try reading — if fails, recreate
    try:
        pd.read_csv(HISTORY_FILE)
    except:
        _create_empty_history()


def _create_empty_history():
    """Internal function to create a clean history file."""
    df = pd.DataFrame(columns=[
        "entry_id", "timestamp", "sample_id",
        "supplier_id", "route_id", "collection_center",

        # quality
        "fat", "ts", "snf",
        "adulteration_risk", "is_adulterated",
        "price",

        # analytics
        "batch_id",
        "sample_score",

        # supplier analytics
        "supplier_avg_fat", "supplier_avg_snf", "supplier_avg_ts",
        "supplier_stability", "supplier_persistence",

        # route analytics
        "route_score",

        # batch analytics
        "batch_avg_score", "batch_adulteration_freq",

        # global analytics
        "global_quality_index"
    ])
    df.to_csv(HISTORY_FILE, index=False)


# ------------------------------------------------------
# Load DF safely
# ------------------------------------------------------
def load_history_df():
    """Load full history CSV as DataFrame. Auto-fixes file if corrupted."""
    _ensure_history_exists()
    try:
        df = pd.read_csv(HISTORY_FILE)
        return df
    except:
        # If read fails, rebuild file
        _create_empty_history()
        return pd.read_csv(HISTORY_FILE)


# ------------------------------------------------------
# Scoring logic
# ------------------------------------------------------
def compute_sample_score(fat, snf, ts, adulteration_risk):
    """Final score between 0–100."""
    if fat is None or snf is None or ts is None or adulteration_risk is None:
        return None

    score = (
        (fat / 6.0) * 0.40 +
        (snf / 10.0) * 0.30 +
        (ts / 15.0) * 0.20 +
        (1 - adulteration_risk / 100.0) * 0.10
    ) * 100

    return round(score, 2)


# ------------------------------------------------------
# Supplier metrics
# ------------------------------------------------------
def compute_supplier_metrics(df_supplier):
    if df_supplier.empty:
        return (0, 0, 0, 0, 1)

    avg_fat = df_supplier["fat"].mean()
    avg_snf = df_supplier["snf"].mean()
    avg_ts = df_supplier["ts"].mean()

    stability = df_supplier["fat"].std() if len(df_supplier) > 1 else 0

    persistence = (
        (df_supplier["fat"].diff().fillna(0) >= 0).mean()
        if len(df_supplier) > 1 else 1.0
    )

    return (
        round(avg_fat, 3),
        round(avg_snf, 3),
        round(avg_ts, 3),
        round(stability, 4),
        round(persistence, 4),
    )


# ------------------------------------------------------
# Route score
# ------------------------------------------------------
def compute_route_score(df_route):
    return round(df_route["sample_score"].mean(), 2) if not df_route.empty else 0


# ------------------------------------------------------
# Batch analytics
# ------------------------------------------------------
def compute_batch_metrics(df_batch):
    if df_batch.empty:
        return 0, 0

    avg_score = df_batch["sample_score"].mean()
    adulteration_freq = df_batch["is_adulterated"].mean() * 100

    return round(avg_score, 2), round(adulteration_freq, 2)


# ------------------------------------------------------
# Global analytics
# ------------------------------------------------------
def compute_global_quality_index(df):
    return round(df["sample_score"].mean(), 2) if not df.empty else 0


# ------------------------------------------------------
# Append sample
# ------------------------------------------------------
def append_sample(payload):

    _ensure_history_exists()
    df = load_history_df()
    if df is None:
        df = pd.DataFrame()

    # ---------------------------------------------------------
    # Extract inference block
    # ---------------------------------------------------------
    infer = payload.get("inference", {})

    # -------- Supplier Data Inside Inference --------
    sd = infer.get("supplier_data", {})

    sample_id = sd.get("sample_id", str(uuid.uuid4()))
    supplier_id = sd.get("supplier_id", "UNKNOWN_SUPPLIER")
    route_id = sd.get("route_id", "UNKNOWN_ROUTE")
    collection_center = sd.get("collection_center", "UNKNOWN_CENTER")

    # ---------------------------------------------------------
    # Core predicted fields (inside inference)
    # ---------------------------------------------------------
    fat = infer.get("fat_predicted")
    ts = infer.get("total_solids_predicted")
    snf = infer.get("snf")
    adulteration_risk = infer.get("adulteration_risk")
    is_adulterated = infer.get("is_adulterated")

    # pricing (added by FastAPI pipeline)
    final_price = payload.get("price", {}).get("final_price")

    # ---------------------------------------------------------
    # Compute sample score
    # ---------------------------------------------------------
    sample_score = compute_sample_score(fat, snf, ts, adulteration_risk)

    # ---------------------------------------------------------
    # Batch assignment
    # ---------------------------------------------------------
    batch_id = len(df) // 20 if len(df) > 0 else 0

    # ---------------------------------------------------------
    # Supplier analytics
    # ---------------------------------------------------------
    df_supplier = df[df["supplier_id"] == supplier_id] if len(df) else pd.DataFrame()
    (
        supplier_avg_fat,
        supplier_avg_snf,
        supplier_avg_ts,
        supplier_stability,
        supplier_persistence
    ) = compute_supplier_metrics(df_supplier)

    # ---------------------------------------------------------
    # Route analytics
    # ---------------------------------------------------------
    df_route = df[df["route_id"] == route_id] if len(df) else pd.DataFrame()
    route_score = compute_route_score(df_route)

    # ---------------------------------------------------------
    # Batch analytics
    # ---------------------------------------------------------
    df_batch = df[df["batch_id"] == batch_id] if len(df) else pd.DataFrame()
    batch_avg_score, batch_adulteration_freq = compute_batch_metrics(df_batch)

    # ---------------------------------------------------------
    # Global analytics
    # ---------------------------------------------------------
    global_quality_index = compute_global_quality_index(df) if len(df) else 0

    # ---------------------------------------------------------
    # Build entry row
    # ---------------------------------------------------------
    entry = {
        "entry_id": str(uuid.uuid4()),
        "timestamp": datetime.datetime.now().isoformat(),
        "sample_id": sample_id,

        "supplier_id": supplier_id,
        "route_id": route_id,
        "collection_center": collection_center,

        "fat": fat,
        "ts": ts,
        "snf": snf,

        "adulteration_risk": adulteration_risk,
        "is_adulterated": is_adulterated,
        "price": final_price,

        "batch_id": batch_id,
        "sample_score": sample_score,

        # supplier analytics
        "supplier_avg_fat": supplier_avg_fat,
        "supplier_avg_snf": supplier_avg_snf,
        "supplier_avg_ts": supplier_avg_ts,
        "supplier_stability": supplier_stability,
        "supplier_persistence": supplier_persistence,

        # route analytics
        "route_score": route_score,

        # batch analytics
        "batch_avg_score": batch_avg_score,
        "batch_adulteration_freq": batch_adulteration_freq,

        # global analytics
        "global_quality_index": global_quality_index,
    }

    # ---------------------------------------------------------
    # Save history
    # ---------------------------------------------------------
    df = pd.concat([df, pd.DataFrame([entry])], ignore_index=True)
    df.to_csv(HISTORY_FILE, index=False)

    return entry
