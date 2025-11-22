import pandas as pd
from src.utils.history_utils import HISTORY_FILE
import random
import uuid
from src.utils.history_utils import load_history_df

INDIAN_MILK_BRANDS = [
    "Amul", "Nandini", "Aavin", "Mother Dairy", "Gokul", "Warana", 
    "Milma", "Vijaya", "Heritage", "Saras", "Parag", "Sanchi",
    "Kwality", "Verka", "Sudha", "Gujarat Cooperative", "Hatsun"
]

# ------------------------------------------------------
# Load full history
# ------------------------------------------------------
def _load_history():
    """Load history CSV as pandas DataFrame."""
    try:
        df = pd.read_csv(HISTORY_FILE)
        if df.empty:
            return None
        return df
    except:
        return None


# ------------------------------------------------------
# SAMPLE ANALYTICS
# ------------------------------------------------------
def compute_sample_analytics(latest):
    return {
        "sample_id": latest["sample_id"],
        "timestamp": latest["timestamp"],
        "score": latest["sample_score"],

        "fat": latest["fat"],
        "snf": latest["snf"],
        "ts": latest["ts"],

        "adulteration_risk": latest["adulteration_risk"],
        "is_adulterated": latest["is_adulterated"],
        "price": latest["price"]
    }


# ------------------------------------------------------
# SUPPLIER ANALYTICS
# ------------------------------------------------------
def compute_supplier_analytics(df, latest):
    supplier_id = latest["supplier_id"]
    supplier_df = df[df["supplier_id"] == supplier_id]

    # Add random supplier name
    supplier_name = random.choice(INDIAN_MILK_BRANDS)

    if supplier_df.empty:
        return {
            "supplier_id": supplier_id,
            "supplier_name": supplier_name,
            "avg_fat": 0,
            "avg_snf": 0,
            "avg_ts": 0,
            "stability": 0,
            "persistence": 1.0,
        }

    return {
        "supplier_id": supplier_id,
        "supplier_name": supplier_name,
        "avg_fat": round(supplier_df["fat"].mean(), 3),
        "avg_snf": round(supplier_df["snf"].mean(), 3),
        "avg_ts": round(supplier_df["ts"].mean(), 3),

        # stability = standard deviation of fat
        "stability": round(supplier_df["fat"].std() if len(supplier_df) > 1 else 0, 4),

        # persistence = upward trend %
        "persistence": round(
            (supplier_df["fat"].diff().fillna(0) >= 0).mean()
            if len(supplier_df) > 1 else 1.0,
            4
        )
    }


# ------------------------------------------------------
# ROUTE ANALYTICS
# ------------------------------------------------------
def compute_route_analytics(df, latest):
    route_id = latest["route_id"]
    route_df = df[df["route_id"] == route_id]

    if route_df.empty:
        return {
            "route_id": route_id,
            "avg_fat": 0,
            "avg_snf": 0,
            "avg_ts": 0,
            "stability": 0,
            "persistence": 1.0,
            "route_score": 0
        }

    return {
        "route_id": route_id,
        "avg_fat": round(route_df["fat"].mean(), 3),
        "avg_snf": round(route_df["snf"].mean(), 3),
        "avg_ts": round(route_df["ts"].mean(), 3),

        # stability = std of sample score
        "stability": round(
            route_df["sample_score"].std() if len(route_df) > 1 else 0,
            4
        ),

        # persistence = trend %
        "persistence": round(
            (route_df["sample_score"].diff().fillna(0) >= 0).mean()
            if len(route_df) > 1 else 1.0,
            4
        ),

        "route_score": round(route_df["sample_score"].mean(), 2)
    }


# ------------------------------------------------------
# BATCH ANALYTICS
# ------------------------------------------------------
def compute_batch_analytics(df, latest):
    batch_id = latest["batch_id"]
    batch_df = df[df["batch_id"] == batch_id]

    if batch_df.empty:
        return {
            "batch_id": batch_id,
            "avg_fat": 0,
            "avg_snf": 0,
            "avg_ts": 0,
            "stability": 0,
            "persistence": 1.0,
            "adulteration_freq": 0
        }

    adulteration_freq = (
        batch_df["is_adulterated"].mean() * 100
        if len(batch_df) > 0 else 0
    )

    return {
        "batch_id": batch_id,
        "avg_fat": round(batch_df["fat"].mean(), 3),
        "avg_snf": round(batch_df["snf"].mean(), 3),
        "avg_ts": round(batch_df["ts"].mean(), 3),

        "stability": round(
            batch_df["sample_score"].std() if len(batch_df) > 1 else 0,
            4
        ),

        "persistence": round(
            (batch_df["sample_score"].diff().fillna(0) >= 0).mean()
            if len(batch_df) > 1 else 1.0,
            4
        ),

        "adulteration_freq": round(adulteration_freq, 2)
    }


# ------------------------------------------------------
# GLOBAL ANALYTICS
# ------------------------------------------------------
def compute_global_analytics(df):
    if df.empty:
        return {
            "global_quality_index": 0,
            "total_samples": 0
        }

    return {
        "global_quality_index": round(df["sample_score"].mean(), 2),
        "total_samples": len(df)
    }


# ------------------------------------------------------
# FULL ANALYTICS (used by /full)
# ------------------------------------------------------
def compute_full_analytics(payload=None):
    # Load from PostgreSQL history, NOT CSV
    df = load_history_df()
    history_valid = True

    required_cols = [
        "supplier_id", "route_id", "batch_id",
        "fat", "snf", "ts",
        "sample_score", "adulteration_risk", "is_adulterated"
    ]

    if df is None or df.empty:
        history_valid = False
    else:
        for col in required_cols:
            if col not in df.columns:
                history_valid = False
                break

    if history_valid:
        latest = df.iloc[-1].to_dict()

        return {
            "sample": compute_sample_analytics(latest),
            "supplier": compute_supplier_analytics(df, latest),
            "route": compute_route_analytics(df, latest),
            "batch": compute_batch_analytics(df, latest),
            "global": compute_global_analytics(df)
        }

    # 🔥 Fallback to live payload
    if payload is None:
        return {
            "sample": {},
            "supplier": {},
            "route": {},
            "batch": {},
            "global": {}
        }

    inf = payload.get("inference", {})

    return {
        "sample": {
            "sample_id": payload.get("sample_id"),
            "timestamp": payload.get("timestamp"),
            "fat": inf.get("fat_predicted"),
            "snf": inf.get("snf"),
            "ts": inf.get("total_solids_predicted"),
            "adulteration_risk": payload.get("adulteration_recomputed", {}).get("adulteration_risk_recomputed"),
            "is_adulterated": payload.get("adulteration_recomputed", {}).get("is_adulterated_recomputed"),
            "price": payload.get("price", {}).get("final_price")
        },
        "supplier": inf.get("supplier_data", {}),
        "route": inf.get("route_data", {}),
        "batch": inf.get("batch_data", {}),
        "global": {"note": "insufficient history for global analytics"}
    }
