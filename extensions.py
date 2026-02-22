import os
import json
import firebase_admin
from firebase_admin import credentials, firestore
from openai import OpenAI
import openai as _openai_module
from flask_socketio import SocketIO
from flask_cors import CORS

# module-level globals set by init_extensions
socketio = None
openai_client = None
_db = None
_async_mode = None


def init_extensions(app):
    global socketio, openai_client, _db, _async_mode
    try:
        import gevent as _gevent_check  # noqa
        _async_mode = 'gevent'
    except Exception:
        _async_mode = 'threading'
    socketio = SocketIO(app, cors_allowed_origins="*", async_mode=_async_mode)
    CORS(app)
    openai_client = OpenAI()
    return {
        'socketio': socketio,
        'openai_client': openai_client,
    }


def init_firebase():
    global _db
    if not firebase_admin._apps:
        creds_json = os.getenv('FIREBASE_CREDENTIALS_JSON', '').strip()
        if creds_json:
            creds_dict = json.loads(creds_json)
            cred = credentials.Certificate(creds_dict)
            firebase_admin.initialize_app(cred)
        elif os.getenv('FIREBASE_CREDENTIALS_PATH') and os.path.exists(os.getenv('FIREBASE_CREDENTIALS_PATH')):
            cred = credentials.Certificate(os.getenv('FIREBASE_CREDENTIALS_PATH'))
            firebase_admin.initialize_app(cred)
        else:
            firebase_admin.initialize_app()
    _db = firestore.client()
    return _db


def get_db():
    return _db


def _reload_openai_client(api_key: str):
    global openai_client
    openai_client = OpenAI(api_key=api_key)


def _is_quota_error(exc: Exception) -> bool:
    """Retorna True apenas para quota esgotada (insufficient_quota), não para rate limiting temporário."""
    if not isinstance(exc, _openai_module.RateLimitError):
        return False
    # rate_limit_exceeded = throttling temporário (RPM/TPM), não é quota esgotada
    code = getattr(exc, 'code', '') or ''
    if code == 'rate_limit_exceeded':
        return False
    msg = str(exc).lower()
    return 'insufficient_quota' in msg or 'billing' in msg


def emit_quota_exceeded():
    try:
        if socketio:
            socketio.emit('openai_quota_exceeded', {
                'message': 'Os créditos da API OpenAI acabaram. Verifique o saldo em platform.openai.com.'
            })
    except Exception:
        pass
