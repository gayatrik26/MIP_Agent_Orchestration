import pandas as pd
import os
import uuid
import datetime
import numpy as np
import psycopg2

HISTORY_FILE = "data/history.csv"

# ======================================================
# PostgreSQL CONNECTION DETAILS
# ======================================================
PG_CONFIG = {
    "host": "kdnai-partnersquad-psql-dev-eastus2.postgres.database.azure.com",
    "port": 5432,
    "user": "psqladmin",
    "password": "Myserver@123",
    "database": "postgres",
    "sslmode": "require"
}


# ======================================================
# Create PG connection
# ======================================================
def _get_pg_conn():
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        return conn
    except Exception as e:
        print("❌ PostgreSQL connection error:", e)
        return None


# ======================================================
# Save history row to PostgreSQL
# ======================================================
def _save_history_to_db(entry):
    conn = _get_pg_conn()
    if not conn:
        print("⚠️ Skipping DB insert (no connection)")
        return

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quality_history (
                    entry_id, timestamp, sample_id,
                    supplier_id, route_id, collection_center,

                    fat, ts, snf,
                    adulteration_risk, is_adulterated, price,

                    batch_id, sample_score,

                    supplier_avg_fat, supplier_avg_snf, supplier_avg_ts,
                    supplier_stability, supplier_persistence,

                    route_score, batch_avg_score, batch_adulteration_freq,
                    global_quality_index
                )
                VALUES (
                    %(entry_id)s, %(timestamp)s, %(sample_id)s,
                    %(supplier_id)s, %(route_id)s, %(collection_center)s,

                    %(fat)s, %(ts)s, %(snf)s,
                    %(adulteration_risk)s, %(is_adulterated)s, %(price)s,

                    %(batch_id)s, %(sample_score)s,

                    %(supplier_avg_fat)s, %(supplier_avg_snf)s, %(supplier_avg_ts)s,
                    %(supplier_stability)s, %(supplier_persistence)s,

                    %(route_score)s, %(batch_avg_score)s, %(batch_adulteration_freq)s,
                    %(global_quality_index)s
                )
                """,
                entry
            )
            conn.commit()

    except Exception as e:
        print("❌ Failed inserting history row into DB:", e)
    finally:
        conn.close()


# ======================================================
# Ensure CSV exists
# ======================================================
def _ensure_history_exists():
    dir_name = os.path.dirname(HISTORY_FILE)
    if dir_name and not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)

    if (not os.path.exists(HISTORY_FILE)) or os.path.getsize(HISTORY_FILE) == 0:
        _create_empty_history()
        return

    try:
        pd.read_csv(HISTORY_FILE)
    except:
        _create_empty_history()


def _create_empty_history():
    df = pd.DataFrame(columns=[
        "entry_id", "timestamp", "sample_id",
        "supplier_id", "route_id", "collection_center",

        "fat", "ts", "snf",
        "adulteration_risk", "is_adulterated", "price",

        "batch_id", "sample_score",

        "supplier_avg_fat", "supplier_avg_snf", "supplier_avg_ts",
        "supplier_stability", "supplier_persistence",

        "route_score",
        "batch_avg_score", "batch_adulteration_freq",

        "global_quality_index"
    ])
    df.to_csv(HISTORY_FILE, index=False)


# ======================================================
# CSV loader
# ======================================================
def load_history_df():
    _ensure_history_exists()
    try:
        return pd.read_csv(HISTORY_FILE)
    except:
        _create_empty_history()
        return pd.read_csv(HISTORY_FILE)


# ======================================================
# Computing sample score
# ======================================================
def compute_sample_score(fat, snf, ts, adulteration_risk):
    if None in (fat, snf, ts, adulteration_risk):
        return None

    score = (
        (fat / 6.0) * 0.40 +
        (snf / 10.0) * 0.30 +
        (ts / 15.0) * 0.20 +
        (1 - adulteration_risk / 100.0) * 0.10
    ) * 100

    return round(score, 2)


# ======================================================
# Supplier metrics
# ======================================================
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


# ======================================================
# Route + Batch + Global metrics
# ======================================================
def compute_route_score(df_route):
    return round(df_route["sample_score"].mean(), 2) if not df_route.empty else 0


def compute_batch_metrics(df_batch):
    if df_batch.empty:
        return 0, 0
    return (
        round(df_batch["sample_score"].mean(), 2),
        round(df_batch["is_adulterated"].mean() * 100, 2)
    )


def compute_global_quality_index(df):
    return round(df["sample_score"].mean(), 2) if not df.empty else 0


# ======================================================
# MAIN: append_sample (writes CSV + DB)
# ======================================================
def append_sample(payload):
    _ensure_history_exists()
    df = load_history_df()

    infer = payload.get("inference", {})
    sd = infer.get("supplier_data", {})

    sample_id = sd.get("sample_id", str(uuid.uuid4()))
    supplier_id = sd.get("supplier_id", "UNKNOWN_SUPPLIER")
    route_id = sd.get("route_id", "UNKNOWN_ROUTE")
    collection_center = sd.get("collection_center", "UNKNOWN_CENTER")

    fat = infer.get("fat_predicted")
    ts = infer.get("total_solids_predicted")
    snf = infer.get("snf")
    adulteration_risk = infer.get("adulteration_risk")
    is_adulterated = infer.get("is_adulterated")

    final_price = payload.get("price", {}).get("final_price")

    sample_score = compute_sample_score(fat, snf, ts, adulteration_risk)

    batch_id = len(df) // 20 if len(df) else 0

    df_supplier = df[df["supplier_id"] == supplier_id] if len(df) else pd.DataFrame()
    supplier_avg_fat, supplier_avg_snf, supplier_avg_ts, supplier_stability, supplier_persistence = compute_supplier_metrics(df_supplier)

    df_route = df[df["route_id"] == route_id] if len(df) else pd.DataFrame()
    route_score = compute_route_score(df_route)

    df_batch = df[df["batch_id"] == batch_id] if len(df) else pd.DataFrame()
    batch_avg_score, batch_adulteration_freq = compute_batch_metrics(df_batch)

    global_quality_index = compute_global_quality_index(df) if len(df) else 0

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

        "supplier_avg_fat": supplier_avg_fat,
        "supplier_avg_snf": supplier_avg_snf,
        "supplier_avg_ts": supplier_avg_ts,
        "supplier_stability": supplier_stability,
        "supplier_persistence": supplier_persistence,

        "route_score": route_score,

        "batch_avg_score": batch_avg_score,
        "batch_adulteration_freq": batch_adulteration_freq,

        "global_quality_index": global_quality_index,
    }

    # Save to CSV
    df = pd.concat([df, pd.DataFrame([entry])], ignore_index=True)
    df.to_csv(HISTORY_FILE, index=False)

    # Save to PostgreSQL
    _save_history_to_db(entry)

    return entry
