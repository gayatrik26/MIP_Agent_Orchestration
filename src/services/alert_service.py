import datetime
import os
import csv
import json
import psycopg2


ALERTS_FILE = "data/alerts_history.csv"

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
def get_db_connection():
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        return conn
    except Exception as e:
        print("❌ PostgreSQL connection error:", e)
        return None

# ======================================================
# Ensure CSV history exists (backup only)
# ======================================================
if not os.path.exists(ALERTS_FILE):
    with open(ALERTS_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp",
            "alert_type",
            "severity",
            "sample_id",
            "supplier_id",
            "route_id",
            "message",
            "details_json"
        ])


# ======================================================
# INSERT ALERT INTO POSTGRESQL
# ======================================================
def insert_alert_to_db(alert):
    """Insert a new alert row into Azure PostgreSQL."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        query = """
        INSERT INTO alert_history (
            timestamp,
            alert_type,
            severity,
            sample_id,
            supplier_id,
            route_id,
            message,
            details
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Convert dict → valid JSON string
        details_json = json.dumps(alert.get("details", {}))

        cur.execute(query, (
            alert["timestamp"],
            alert["type"],
            alert["severity"],
            alert.get("sample_id"),
            alert.get("supplier_id"),
            alert.get("route_id"),
            alert["message"],
            details_json
        ))

        conn.commit()
        cur.close()
        conn.close()
        print("✔️ Alert inserted into DB")

    except Exception as e:
        print("❌ Failed inserting alert into PostgreSQL:", e)


# ======================================================
# BACKUP CSV IF DB INSERT FAILS
# ======================================================
def _save_alert_csv(alert):
    try:
        with open(ALERTS_FILE, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                alert["timestamp"],
                alert["type"],
                alert["severity"],
                alert.get("sample_id", ""),
                alert.get("supplier_id", ""),
                alert.get("route_id", ""),
                alert["message"],
                json.dumps(alert.get("details", {}))
            ])
    except Exception as e:
        print("⚠️ Failed saving alert to CSV:", e)


# ======================================================
# Build a standard alert object
# ======================================================
def _build_alert(alert_type, severity, payload, message, details=None):

    supplier_info = (
        payload.get("supplier_data")
        or payload.get("inference", {}).get("supplier_data")
        or payload.get("inference", {}).get("supplier")
        or {}
    )

    sample_id = supplier_info.get("sample_id") or payload.get("sample_id")
    supplier_id = supplier_info.get("supplier_id")
    route_id = supplier_info.get("route_id")

    alert = {
        "type": alert_type,
        "severity": severity,
        "message": message,
        "timestamp": datetime.datetime.now().isoformat(),
        "sample_id": sample_id,
        "supplier_id": supplier_id,
        "route_id": route_id,
        "details": details or {}
    }

    # Try DB → fallback to CSV
    try:
        insert_alert_to_db(alert)
    except Exception as e:
        print("⚠️ DB insert failed, saving to CSV:", e)
        _save_alert_csv(alert)

    return alert


# ======================================================
# RULE ENGINE
# ======================================================
def evaluate_alert_rules(payload):

    alerts = []
    inf = payload.get("inference", {})

    fat = float(inf.get("fat", 0))
    snf = float(inf.get("snf", 0))
    ts = float(inf.get("ts", 0))

    adulteration_obj = payload.get("adulteration_recomputed", {})
    adulteration_risk = float(adulteration_obj.get("risk", 0))
    is_adulterated = adulteration_obj.get("is_adulterated", 0)

    milk_type = payload.get("milk_type", "unknown")

    supplier_stats = payload.get("analytics", {}).get("supplier", {})
    route_stats = payload.get("analytics", {}).get("route", {})
    batch_stats = payload.get("analytics", {}).get("batch", {})

    stability = float(supplier_stats.get("stability", 0))
    persistence = float(supplier_stats.get("persistence", 0))
    route_score = float(route_stats.get("route_score", 0))
    batch_adult_freq = float(batch_stats.get("adulteration_freq", 0))

    # 1. Critical adulteration
    if adulteration_risk > 80 or is_adulterated == 1:
        alerts.append(_build_alert(
            "CRITICAL_ADULTERATION",
            "high",
            payload,
            f"Adulteration risk is {adulteration_risk}%",
            {"adulteration_risk": adulteration_risk}
        ))

    # 2. Low fat
    if fat < 2.5:
        alerts.append(_build_alert(
            "LOW_FAT",
            "medium",
            payload,
            f"Fat content too low: {fat}",
            {"fat": fat}
        ))

    # 3. Low SNF
    if snf < 8.0:
        alerts.append(_build_alert(
            "LOW_SNF",
            "medium",
            payload,
            f"SNF below safe limit: {snf}",
            {"snf": snf}
        ))

    # 4. Low TS
    if ts < 11.5:
        alerts.append(_build_alert(
            "LOW_TS",
            "medium",
            payload,
            f"Total solids too low: {ts}",
            {"ts": ts}
        ))

    # 5. Supplier stability
    if stability < 0.5:
        alerts.append(_build_alert(
            "SUPPLIER_STABILITY_DROP",
            "low",
            payload,
            f"Supplier stability dropped: {stability}",
            {"stability": stability}
        ))

    # 6. Supplier persistence
    if persistence < 0.4:
        alerts.append(_build_alert(
            "SUPPLIER_PERSISTENCE_LOW",
            "low",
            payload,
            f"Supplier persistence is low: {persistence}",
            {"persistence": persistence}
        ))

    # 7. Route score
    if route_score < 60:
        alerts.append(_build_alert(
            "ROUTE_QUALITY_LOW",
            "medium",
            payload,
            f"Route Score is low: {route_score}",
            {"route_score": route_score}
        ))

    # 8. Batch adulteration frequency
    if batch_adult_freq > 30:
        alerts.append(_build_alert(
            "HIGH_BATCH_ADULTERATION_RATE",
            "high",
            payload,
            f"Batch adulteration frequency high ({batch_adult_freq}%)",
            {"batch_adulteration_freq": batch_adult_freq}
        ))

    # 9. Milk type mismatch
    if milk_type not in ["cow", "buffalo", "mixed"]:
        alerts.append(_build_alert(
            "MILK_TYPE_UNKNOWN",
            "low",
            payload,
            f"Unknown milk type detected: {milk_type}",
            {"milk_type": milk_type}
        ))

    return alerts


# ======================================================
# Main entry used in MQTT
# ======================================================
def run_alert_engine(payload):
    try:
        alerts = evaluate_alert_rules(payload)
        return alerts
    except Exception as e:
        print("❌ ALERT ENGINE ERROR:", e)
        return []
