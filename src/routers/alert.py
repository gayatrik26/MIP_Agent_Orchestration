from fastapi import APIRouter, Request
import csv
import datetime
from src.services.alert_service import run_alert_engine, ALERTS_FILE

router = APIRouter(prefix="/alerts", tags=["Alerts"])



# ======================================================
# 1. READ FULL ALERT HISTORY
# ======================================================
@router.get("/")
def get_all_alerts():
    alerts = []
    try:
        with open(ALERTS_FILE, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                alerts.append(row)
    except Exception as e:
        return {"error": f"Failed to read alerts history: {e}"}

    return {"count": len(alerts), "alerts": alerts}



# ======================================================
# 2. GET MOST RECENT N ALERTS
# ======================================================
@router.get("/recent/{n}")
def get_recent_alerts(n: int):
    alerts = []
    try:
        with open(ALERTS_FILE, "r") as f:
            reader = list(csv.DictReader(f))
            alerts = reader[-n:] if n > 0 else []
    except Exception as e:
        return {"error": f"Failed to read alerts history: {e}"}

    return {"count": len(alerts), "alerts": alerts}



# ======================================================
# 3. GET ALERTS FOR SPECIFIC SUPPLIER
# ======================================================
@router.get("/supplier/{supplier_id}")
def get_supplier_alerts(supplier_id: str):
    matched = []
    try:
        with open(ALERTS_FILE, "r") as f:
            for row in csv.DictReader(f):
                if row.get("supplier_id") == supplier_id:
                    matched.append(row)
    except Exception as e:
        return {"error": str(e)}

    return {"supplier_id": supplier_id, "count": len(matched), "alerts": matched}



# ======================================================
# 4. GET ALERTS FOR SPECIFIC ROUTE
# ======================================================
@router.get("/route/{route_id}")
def get_route_alerts(route_id: str):
    matched = []
    try:
        with open(ALERTS_FILE, "r") as f:
            for row in csv.DictReader(f):
                if row.get("route_id") == route_id:
                    matched.append(row)
    except Exception as e:
        return {"error": str(e)}

    return {"route_id": route_id, "count": len(matched), "alerts": matched}



# ======================================================
# 5. TRIGGER ALERT ENGINE ON LAST MQTT PAYLOAD
# ======================================================
@router.get("/run-latest")
def run_alerts_on_latest(request: Request):
    payload = request.app.mqtt_latest
    if payload is None:
        return {"error": "No MQTT sample received yet"}

    alerts = run_alert_engine(payload)
    return {"alerts_triggered": len(alerts), "alerts": alerts}



# ======================================================
# 6. TRIGGER ALERT ENGINE ON CUSTOM PAYLOAD
# ======================================================
@router.post("/run")
def run_alerts_on_payload(payload: dict):
    alerts = run_alert_engine(payload)
    return {"alerts_triggered": len(alerts), "alerts": alerts}



# ======================================================
# 7. CREATE A MANUAL TEST ALERT
# ======================================================
@router.post("/test")
def create_test_alert():
    now = datetime.datetime.now().isoformat()

    test_alert = {
        "timestamp": now,
        "type": "TEST_ALERT",
        "severity": "low",
        "sample_id": "TEST-SAMPLE",
        "supplier_id": "TEST-SUPPLIER",
        "route_id": "TEST-ROUTE",
        "message": "This is a test alert",
        "details": {},
    }

    # Save via CSV
    try:
        with open(ALERTS_FILE, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                test_alert["timestamp"],
                test_alert["type"],
                test_alert["severity"],
                test_alert["sample_id"],
                test_alert["supplier_id"],
                test_alert["route_id"],
                test_alert["message"],
                "{}"
            ])
    except Exception as e:
        return {"error": f"Failed to write test alert: {e}"}

    return {"status": "ok", "alert": test_alert}
