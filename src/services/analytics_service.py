import pandas as pd
from src.utils.history_utils import HISTORY_FILE
import random

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
def compute_full_analytics():
    df = _load_history()
    if df is None:
        return None

    latest = df.iloc[-1]     # last row
    latest_dict = latest.to_dict()

    return {
        "sample": compute_sample_analytics(latest_dict),
        "supplier": compute_supplier_analytics(df, latest_dict),
        "route": compute_route_analytics(df, latest_dict),
        "batch": compute_batch_analytics(df, latest_dict),
        "global": compute_global_analytics(df)
    }
