from collections import deque
import threading
import datetime

# Keep last N SHAP snapshots
_SHAP_CACHE = deque(maxlen=200)
_LOCK = threading.Lock()


def push_shap_sample(payload: dict):
    """
    Store a compact snapshot of SHAP outputs for this sample.
    Expected payload structure:
      payload["shap"]["fat"] = { shap_score, top_10: [...] }
      payload["shap"]["ts"]  = { ... }
      payload["shap"]["adulteration"] = { ... }
    """

    shap_block = payload.get("shap", {})

    record = {
        "timestamp": payload.get("timestamp")
                    or payload.get("inference", {}).get("timestamp")
                    or datetime.datetime.now().isoformat(),
        "sample_id": payload.get("sample_id")
                     or payload.get("inference", {}).get("sample_id"),
        "fat": shap_block.get("fat", {}),
        "ts": shap_block.get("ts", {}),
        "adulteration": shap_block.get("adulteration", {}),
    }

    with _LOCK:
        _SHAP_CACHE.append(record)


def get_shap_history(limit: int | None = None):
    """
    Return a list of recent SHAP records.
    If limit is provided, return only the last `limit`.
    """
    with _LOCK:
        data = list(_SHAP_CACHE)

    if limit is not None and limit > 0:
        return data[-limit:]
    return data
