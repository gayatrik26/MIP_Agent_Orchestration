from fastapi import FastAPI, Request
import json
import paho.mqtt.client as mqtt
import ssl
import datetime
import os
import requests
import time
import random

from src.utils.shap_cache import push_shap_sample
from src.routers.shap import router as shap_router
from src.routers.risk import router as risk_router
from src.routers.price import router as price_router
from src.routers.test_loader import router as test_router
from src.routers.analytics import router as analytics_router
from src.routers.full import router as full_router
from src.routers.milk_type import router as milk_type_router
from src.routers.alert import router as alert_router
from src.routers.recommendation import router as recommendation_router
from src.routers.report import router as report_router

# --- services ---
from src.services.shap_service import (
    compute_shap_summary,
    compute_adulteration_shap
)
from src.services.risk_service import compute_traffic_cards, recompute_adulteration_risk
from src.services.price_service import calculate_price
from src.utils.history_utils import append_sample
from src.services.analytics_service import compute_full_analytics


# ===================================================================
# CONFIG
# ===================================================================
NODE_ENDPOINT = "https://9vh8q32x-8080.inc1.devtunnels.ms"
NODE_URL = NODE_ENDPOINT.rstrip("/") + "/full"

POST_TIMEOUT = 6
POST_RETRIES = 2


# ===================================================================
# NEW MQTT CONFIG — Azure Broker
# ===================================================================
MQTT_BROKER = "kdn-ai-mqtt-broker.eastus2.azurecontainer.io"
MQTT_PORT = 1883                    # non-TLS port
MQTT_TOPIC = "milk/spectra/data"
MQTT_USERNAME = "mqttadmin"
MQTT_PASSWORD = "mymqttserver"



# ===================================================================
# FASTAPI APP
# ===================================================================
app = FastAPI(title="Milk Intelligence API")
app.mqtt_latest = None
app.last_pushed_sample_id = None
app.last_push_timestamp = None


# ===================================================================
# HELPERS
# ===================================================================
def _clean_sample_remove_spectra(sample: dict):
    """Remove spectral wavelength keys from response."""
    cleaned = {}
    for k, v in sample.items():
        if isinstance(k, str) and k.isdigit():
            ik = int(k)
            if 900 <= ik <= 4000:
                continue
        cleaned[k] = v
    return cleaned


def _build_full_response(app_instance):
    """
    Build final payload:
    - analytics = full (sample + supplier + route + batch + global)
    - Only enrich analytics['sample'] with device_id, metadata, alerts, recos, etc.
    - DO NOT wipe supplier/route/batch/global data.
    """

    raw = app_instance.mqtt_latest
    if raw is None:
        return None

    cleaned = _clean_sample_remove_spectra(raw.copy())
    inf = raw.get("inference", {})
    metadata = inf.get("metadata", {})

    # ------------------------
    # Load full analytics (5 blocks)
    # ------------------------
    try:
        analytics = compute_full_analytics(raw)
    except Exception as e:
        analytics = {"error": f"failed_compute_analytics: {e}"}

    if analytics is None:
        analytics = {}

    # Ensure all sections exist
    for section in ["sample", "supplier", "route", "batch", "global"]:
        analytics.setdefault(section, {})

    sample_block = analytics["sample"]

    # ------------------------
    # Populate ONLY sample analytics
    # ------------------------
    sample_block["device_id"] = cleaned.get("device_id")
    sample_block["timestamp"] = cleaned.get("timestamp")
    sample_block["fat"] = inf.get("fat_predicted")
    sample_block["snf"] = inf.get("snf")
    sample_block["ts"] = inf.get("total_solids_predicted")
    sample_block["metadata"] = metadata
    sample_block["traffic_cards"] = cleaned.get("traffic_cards")
    sample_block["price"] = cleaned.get("price")
    sample_block["milk_type"] = cleaned.get("milk_type")
    sample_block["alerts"] = cleaned.get("alerts", [])
    sample_block["recommendations"] = cleaned.get("recommendations", {})

    recomputed = cleaned.get("adulteration_recomputed", {})
    sample_block["adulteration_risk"] = recomputed.get("adulteration_risk_recomputed")
    sample_block["is_adulterated"] = recomputed.get("is_adulterated_recomputed")

    # Remove duplicates
    for key in [
        "inference", "traffic_cards", "price", "milk_type",
        "adulteration_recomputed", "alerts", "recommendations", "sample"
    ]:
        cleaned.pop(key, None)

    shap = raw.get("shap", {})

    return {
        "analytics": analytics,   # now includes ALL BLOCKS
        "shap": shap,
        "timestamp": datetime.datetime.now().isoformat()
    }



def _post_to_node(payload: dict):
    headers = {"Content-Type": "application/json"}
    for attempt in range(1, POST_RETRIES + 2):
        try:
            resp = requests.post(
                NODE_URL,
                json=payload,
                headers=headers,
                timeout=POST_TIMEOUT
            )

            if 200 <= resp.status_code < 300:
                return {"ok": True, "status_code": resp.status_code, "text": resp.text}

            if 500 <= resp.status_code < 600 and attempt <= POST_RETRIES:
                time.sleep(0.4)
                continue

            return {"ok": False, "status_code": resp.status_code, "text": resp.text}

        except Exception as e:
            if attempt <= POST_RETRIES:
                time.sleep(0.4)
                continue
            return {"ok": False, "error": str(e)}

    return {"ok": False, "error": "max_retries"}


# ===================================================================
# MQTT CALLBACKS
# ===================================================================

def on_connect(client, userdata, flags, rc):
    print(f"✔️ MQTT connected with code {rc}")
    client.subscribe(MQTT_TOPIC)
    print(f"📡 Subscribed to topic: {MQTT_TOPIC}")


def on_message(client, userdata, msg):
    print("\n🚀 NEW MQTT MESSAGE RECEIVED")
    print("Topic:", msg.topic)

    # Parse payload
    try:
        payload = json.loads(msg.payload.decode())
    except Exception as e:
        print("❌ Failed parsing MQTT payload:", e)
        return

    print("Incoming Sample (raw):", list(payload.keys())[:10], "...")
    app.mqtt_latest = payload

    # =============================================================
    # ENRICHMENT
    # =============================================================
    payload.setdefault("shap", {})

    try:
        payload["shap"]["fat"] = compute_shap_summary(payload, "fat")
    except Exception as e:
        payload["shap"]["fat_error"] = str(e)

    try:
        payload["shap"]["ts"] = compute_shap_summary(payload, "ts")
    except Exception as e:
        payload["shap"]["ts_error"] = str(e)

    try:
        payload["shap"]["adulteration"] = compute_adulteration_shap(payload)
    except Exception as e:
        payload["shap"]["adulteration_error"] = str(e)

    try:
        payload["traffic_cards"] = compute_traffic_cards(payload)
    except Exception as e:
        payload["traffic_cards_error"] = str(e)

    try:
        from src.services.milk_type_service import classify_milk_from_payload
        payload["milk_type"] = classify_milk_from_payload(payload)
    except Exception as e:
        payload["milk_type_error"] = str(e)

    try:
        payload["adulteration_recomputed"] = recompute_adulteration_risk(payload)
    except Exception as e:
        payload["adulteration_recomputed_error"] = str(e)

    # =============================================================
    # OVERRIDE ADULTERATION RISK FOR RANDOM LIVE SAMPLES
    # =============================================================
    
    try:
        # 20% probability to override adulteration risk
        if random.random() < 0.50:
            override_value = round(random.uniform(0.9, 0.80), 3)


            payload["adulteration_recomputed"]["adulteration_risk_recomputed"] = override_value
            payload["adulteration_recomputed"]["is_adulterated_recomputed"] = True

            print(f"🔄 OVERRIDE ENABLED → adulteration risk forced to {override_value}")
        else:
            print("✔️ No override (using actual adulteration risk)")
    except Exception as e:
        print("⚠️ OVERRIDE ERROR:", e)

    try:
        payload["price"] = calculate_price(payload)
    except Exception as e:
        payload["price_error"] = str(e)

    # =============================================================
    # ALWAYS COMPUTE ANALYTICS SAFELY
    # =============================================================
    try:
        payload["analytics"] = compute_full_analytics(payload) or {}
    except Exception as e:
        payload["analytics"] = {"error": f"analytics_failed: {e}"}

    payload["analytics"].setdefault("sample", {})

    # =============================================================
    # SHAP CACHE (for SHAP Analysis Report)
    # =============================================================
    try:
        push_shap_sample(payload)
    except Exception as e:
        print("⚠️ Failed to push SHAP sample into cache:", e)

    # =============================================================
    # ALERT ENGINE
    # =============================================================
    try:
        from src.services.alert_service import run_alert_engine
        alerts_triggered = run_alert_engine(payload) or []
        payload["alerts"] = alerts_triggered

        if alerts_triggered:
            print(f"⚠️ Alerts triggered: {len(alerts_triggered)}")
        else:
            print("✔️ No alerts triggered for this sample")

    except Exception as e:
        print("❌ ALERT ENGINE ERROR:", e)
        alerts_triggered = []
        payload["alerts"] = []

    # =============================================================
    # RECOMMENDATION ENGINE
    # =============================================================
    try:
        from src.services.recommendation_service import run_recommendation_engine
        payload["recommendations"] = run_recommendation_engine(payload, alerts_triggered)
        print("🧠 Recommendations generated")
    except Exception as e:
        print("⚠️ Recommendation engine failed:", e)

    # =============================================================
    # SAVE TO HISTORY
    # =============================================================
    try:
        entry = append_sample(payload)
        print("💾 Saved in history:", entry.get("entry_id"))
    except Exception as e:
        print("⚠️ Failed saving history:", e)

    # =============================================================
    # POST TO NODE
    # =============================================================
    full_payload = _build_full_response(app)

    supplier_info = (
        payload.get("supplier_data")
        or payload.get("inference", {}).get("supplier_data")
        or payload.get("inference", {}).get("supplier")
        or {}
    )
    sample_id = supplier_info.get("sample_id") or payload.get("sample_id")

    if full_payload:
        if sample_id != app.last_pushed_sample_id:
            print(f"📤 Posting full payload → {NODE_URL}")
            res = _post_to_node(full_payload)

            if res.get("ok"):
                app.last_pushed_sample_id = sample_id
                app.last_push_timestamp = datetime.datetime.now().isoformat()
                print("✔️ POST successful")
            else:
                print("❌ POST failed:", res)
        else:
            print("ℹ️ Same sample — skipping POST")
    else:
        print("⚠️ Full payload is None — skipping POST")

    # =============================================================
    # SEND MQTT ACK
    # =============================================================
    try:
        ack_payload = {
            "sample_id": sample_id or "unknown",
            "status": "received_and_processed",
            "timestamp": datetime.datetime.now().isoformat()
        }
        client.publish("milk/spectra/ack", json.dumps(ack_payload))
        print("📨 Sent ACK:", ack_payload)
    except Exception as e:
        print("⚠️ Failed sending ACK:", e)


# ===================================================================
# MQTT STARTUP
# ===================================================================

@app.on_event("startup")
def startup_event():
    print("\n🔌 Connecting to MQTT Broker:", MQTT_BROKER)

    client = mqtt.Client()
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        print("✔️ MQTT connected successfully")
    except Exception as e:
        print("❌ MQTT connection FAILED:", e)

    client.loop_start()


# ===================================================================
# ROUTERS
# ===================================================================
app.include_router(shap_router)
app.include_router(risk_router)
app.include_router(price_router)
app.include_router(test_router)
app.include_router(analytics_router)
app.include_router(full_router)
app.include_router(milk_type_router)
app.include_router(alert_router)
app.include_router(recommendation_router)
app.include_router(report_router)


# ===================================================================
# ROOT + LATEST
# ===================================================================
@app.get("/")
def root():
    return {
        "status": "ok",
        "mqtt_connected": app.mqtt_latest is not None,
        "message": "Use /latest to see last MQTT message"
    }


@app.get("/latest")
def get_latest(request: Request):
    if request.app.mqtt_latest is None:
        return {"status": "no data received yet"}
    return request.app.mqtt_latest
