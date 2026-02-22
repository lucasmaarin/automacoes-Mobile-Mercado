import os
import json
from datetime import datetime
import threading

# Serializadores JSON e helpers extraidos de app.py

try:
    from google.cloud.firestore_v1 import DocumentReference, GeoPoint
except Exception:
    try:
        from google.cloud.firestore_v1 import DocumentReference
        from google.cloud.firestore_v1._helpers import GeoPoint
    except Exception:
        DocumentReference = None
        GeoPoint = None

try:
    from google.api_core.datetime_helpers import DatetimeWithNanoseconds
except Exception:
    DatetimeWithNanoseconds = None

try:
    from google.protobuf.timestamp_pb2 import Timestamp as ProtoTimestamp
except Exception:
    ProtoTimestamp = None


def to_json_safe(value):
    if DatetimeWithNanoseconds and isinstance(value, DatetimeWithNanoseconds):
        return value.isoformat()
    from datetime import datetime
    if isinstance(value, datetime):
        return value.isoformat()
    if GeoPoint and isinstance(value, GeoPoint):
        return {"_type": "GeoPoint", "latitude": float(value.latitude), "longitude": float(value.longitude)}
    if hasattr(value, "latitude") and hasattr(value, "longitude") and \
       isinstance(getattr(value, "latitude"), (int, float)) and \
       isinstance(getattr(value, "longitude"), (int, float)):
        return {"_type": "GeoPoint", "latitude": float(value.latitude), "longitude": float(value.longitude)}
    if DocumentReference and isinstance(value, DocumentReference):
        return {"_type": "DocumentReference", "path": value.path}
    if hasattr(value, "path") and hasattr(value, "parent") and hasattr(value, "id"):
        try:
            return {"_type": "DocumentReference", "path": str(value.path)}
        except Exception:
            pass
    if isinstance(value, (bytes, bytearray, memoryview)):
        try:
            return {"_type": "bytes", "base16": bytes(value).hex()}
        except Exception:
            return {"_type": "bytes", "len": len(value)}
    if isinstance(value, dict):
        return {str(k): to_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_json_safe(v) for v in list(value)]
    try:
        json.dumps(value)
        return value
    except Exception:
        return str(value)


def firestore_default(obj):
    if 'GeoPoint' in obj.__class__.__name__ if hasattr(obj, '__class__') else False:
        try:
            lat = getattr(obj, 'latitude', None) or getattr(obj, '_latitude', None)
            lng = getattr(obj, 'longitude', None) or getattr(obj, '_longitude', None)
            return {"_type": "GeoPoint", "latitude": float(lat), "longitude": float(lng)}
        except Exception:
            return {"_type": "GeoPoint", "repr": str(obj)}
    try:
        if DocumentReference and isinstance(obj, DocumentReference):
            return {"_type": "DocumentReference", "path": obj.path}
    except Exception:
        pass
    if ProtoTimestamp is not None and isinstance(obj, ProtoTimestamp):
        try:
            return obj.ToJsonString()
        except Exception:
            pass
    from datetime import datetime
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, (bytes, bytearray, memoryview)):
        try:
            return {"_type": "bytes", "base16": bytes(obj).hex()}
        except Exception:
            return {"_type": "bytes", "len": len(obj)}
    if isinstance(obj, (set, tuple)):
        return list(obj)
    try:
        return str(obj)
    except Exception:
        return "<unserializable>"


def safe_sample(value):
    try:
        return json.loads(json.dumps(value, default=firestore_default))
    except Exception:
        s = str(value)
        return s[:100] + ("..." if len(s) > 100 else "")

# Estatisticas diarias
DAILY_STATS_FILE = 'daily_stats.json'
_daily_stats_data: dict = {}


def _load_daily_stats():
    global _daily_stats_data
    try:
        if os.path.exists(DAILY_STATS_FILE):
            with open(DAILY_STATS_FILE, 'r') as f:
                _daily_stats_data = json.load(f)
    except Exception:
        _daily_stats_data = {}


def _save_daily_stats():
    try:
        with open(DAILY_STATS_FILE, 'w') as f:
            json.dump(_daily_stats_data, f)
    except Exception:
        pass


def get_today_stats() -> dict:
    today = datetime.now().strftime('%Y-%m-%d')
    d = _daily_stats_data.get(today, {})
    return {
        'date': today,
        'tokens': d.get('tokens', 0),
        'cost': d.get('cost', 0.0),
        'calls': d.get('calls', 0),
    }


def record_daily_usage(tokens: int, cost: float):
    today = datetime.now().strftime('%Y-%m-%d')
    if today not in _daily_stats_data:
        _daily_stats_data[today] = {'tokens': 0, 'cost': 0.0, 'calls': 0}
    _daily_stats_data[today]['tokens'] += tokens
    _daily_stats_data[today]['cost'] += cost
    _daily_stats_data[today]['calls'] += 1
    _save_daily_stats()
    try:
        # defer import to avoid circular import at module load
        from extensions import socketio
        socketio.emit('daily_stats_update', get_today_stats())
    except Exception:
        pass


_load_daily_stats()

# Estado globals usados pelas classes e rotas
automation_state = {
    'running': False,
    'progress': {
        'total': 0, 'processed': 0, 'updated': 0,
        'unchanged': 0, 'errors': 0,
        'tokens_used': 0, 'estimated_cost': 0.0
    },
    'current_product': None,
    'logs': []
}

explorer_state = {
    'exploring': False,
    'progress': {'total_docs': 0, 'processed_docs': 0, 'collections_found': 0},
    'current_path': None,
    'logs': [],
    'structure_cache': {}
}

categorizer_state = {
    'running': False,
    'progress': {
        'total': 0, 'processed': 0, 'updated': 0,
        'errors': 0, 'tokens_used': 0, 'estimated_cost': 0.0
    },
    'current_product': None,
    'logs': []
}

categorizer_targeted_state = {
    'running': False,
    'progress': {
        'total': 0, 'processed': 0, 'updated': 0,
        'errors': 0, 'tokens_used': 0, 'estimated_cost': 0.0
    },
    'current_product': None,
    'logs': []
}

# Undo store and lock
undo_store = {
    'renamer': [],
    'categorizer': [],
    'categorizer_targeted': [],
}
_undo_lock = threading.Lock()
