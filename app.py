# Monkey-patch deve ser o primeiro import para modo gevent (producao)
try:
    from gevent import monkey
    monkey.patch_all()
except ImportError:
    pass

import os
import logging
import time
import json
from typing import List, Dict, Any
from datetime import datetime
from threading import Thread
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import firebase_admin
from firebase_admin import credentials, firestore
from openai import OpenAI
import openai as _openai_module
from dotenv import load_dotenv
from flask import Flask, request, jsonify, render_template, Response, session, redirect, url_for
from flask_socketio import SocketIO, emit
from flask_cors import CORS

# Tipos Firestore especiais
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

# Configuração e extensões extraídas para módulos separados
from config import logger, SECRET_KEY
from extensions import init_extensions, init_firebase, get_db, _reload_openai_client, _is_quota_error, emit_quota_exceeded
from utils import to_json_safe, firestore_default, safe_sample, get_today_stats, record_daily_usage

app = Flask(__name__)
app.config['SECRET_KEY'] = SECRET_KEY

# Inicializa socketio, CORS e OpenAI client (setados no módulo extensions)
init_extensions(app)
from extensions import socketio, openai_client  # disponíveis após init_extensions

# ============================================================
# Firebase compartilhado
# ============================================================
db = None

def init_firebase():
    global db
    if not firebase_admin._apps:
        # 1. JSON inline (preferido em producao/Render — cole o conteudo do JSON como env var)
        creds_json = os.getenv('FIREBASE_CREDENTIALS_JSON', '').strip()
        if creds_json:
            creds_dict = json.loads(creds_json)
            cred = credentials.Certificate(creds_dict)
            firebase_admin.initialize_app(cred)
        # 2. Caminho para arquivo (desenvolvimento local)
        elif os.getenv('FIREBASE_CREDENTIALS_PATH') and os.path.exists(os.getenv('FIREBASE_CREDENTIALS_PATH')):
            cred = credentials.Certificate(os.getenv('FIREBASE_CREDENTIALS_PATH'))
            firebase_admin.initialize_app(cred)
        # 3. Application Default Credentials (Google Cloud / fallback)
        else:
            firebase_admin.initialize_app()
    db = firestore.client()
    return db

# ============================================================
# Serializadores JSON
# ============================================================
def to_json_safe(value):
    """Converte recursivamente tipos do Firestore em algo serializavel."""
    if DatetimeWithNanoseconds and isinstance(value, DatetimeWithNanoseconds):
        return value.isoformat()
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
    """Serializador para json.dumps(default=...)."""
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


# ============================================================
# Contagem diária de uso de tokens/custo
# ============================================================
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
        socketio.emit('daily_stats_update', get_today_stats())
    except Exception:
        pass


_load_daily_stats()


# ============================================================
# Estado global
# ============================================================
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

# Histórico de desfazer — limpo no início de cada execução
# Cada entrada: dict com dados suficientes para reverter a operação
undo_store = {
    'renamer': [],           # [{product_id, estabelecimento_id, old_name, new_name}]
    'categorizer': [],       # [{product_id, estabelecimento_id, old_data, new_data}]
    'categorizer_targeted': [],
}
_undo_lock = threading.Lock()


from automator import FirestoreProductAutomator


# ============================================================
# Classe: FirestoreStructureExplorer (avancado)
from explorer import FirestoreStructureExplorer, FirestoreSimpleExplorer


# ============================================================
# Classe: FirestoreSimpleExplorer (rapido)
# ============================================================
class FirestoreSimpleExplorer:
    def __init__(self, db_client):
        self.db = db_client

    def parse_path(self, path: str):
        path = path.strip('/')
        return path.split('/') if path else []

    def explore_collection(self, collection_ref, max_docs=5):
        docs_out = []
        docs = collection_ref.limit(max_docs).stream()
        for doc in docs:
            raw = doc.to_dict() or {}
            docs_out.append({"id": doc.id, "fields": to_json_safe(raw)})
        return docs_out

    def explore_document(self, doc_ref):
        doc = doc_ref.get()
        if not doc.exists:
            return {}
        raw = doc.to_dict() or {}
        return {"id": doc.id, "fields": to_json_safe(raw)}

    def explore(self, path: str, max_docs=5):
        components = self.parse_path(path)
        if not components:
            return {"error": "Caminho vazio"}
        is_collection_path = len(components) % 2 == 1
        if is_collection_path:
            collection_ref = self.db.collection(components[0])
            for i in range(1, len(components), 2):
                if i + 1 < len(components):
                    collection_ref = collection_ref.document(components[i]).collection(components[i + 1])
            return {"collection": components[-1] if components else "", "documents": self.explore_collection(collection_ref, max_docs)}
        else:
            doc_ref = self.db.collection(components[0])
            for i in range(1, len(components), 2):
                doc_ref = doc_ref.document(components[i])
                if i + 1 < len(components):
                    doc_ref = doc_ref.collection(components[i + 1])
            return {"document": components[-1], "data": self.explore_document(doc_ref)}


from categorizer import ProductCategorizerAgent
class ProductCategorizerAgent:
    DEFAULT_CAT_SYSTEM_PROMPT = (
        "Voce e um especialista em categorizacao de produtos de supermercado. "
        "Responda APENAS com o ID solicitado, sem explicacoes, sem pontuacao extra."
    )

    def __init__(self, db_client):
        self.db = db_client
        self.tokens_used = 0
        self.estimated_cost = 0
        self.input_token_cost  = 0.00015 / 1000   # gpt-4o-mini: $0.15/1M input
        self.output_token_cost = 0.00060 / 1000   # gpt-4o-mini: $0.60/1M output
        self._lock = threading.Lock()
        saved = self.load_cat_prompt_from_firestore()
        self.cat_system_prompt = saved if saved else self.DEFAULT_CAT_SYSTEM_PROMPT

    def load_cat_prompt_from_firestore(self) -> str:
        try:
            doc = self.db.collection('Automacoes').document('defenir_catsub').get()
            if doc.exists:
                prompt = (doc.to_dict() or {}).get('prompt', '')
                if prompt:
                    logger.info("Prompt do categorizador carregado do Firestore (Automacoes/defenir_catsub)")
                    return prompt
        except Exception as e:
            logger.warning(f"Nao foi possivel carregar prompt do categorizador: {e}")
        return None

    def save_cat_prompt_to_firestore(self, prompt: str) -> bool:
        try:
            self.db.collection('Automacoes').document('defenir_catsub').set({
                'prompt': prompt,
                'descricao': 'Instrucoes de sistema usadas pela IA para definir categoria e subcategoria dos produtos',
                'updated_at': datetime.now().isoformat(),
            }, merge=True)
            self.cat_system_prompt = prompt
            logger.info("Prompt do categorizador salvo no Firestore (Automacoes/defenir_catsub)")
            return True
        except Exception as e:
            logger.error(f"Erro ao salvar prompt do categorizador: {e}")
            return False

    def log_message(self, message, level="info"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = {'timestamp': timestamp, 'message': message, 'level': level}
        categorizer_state['logs'].append(log_entry)
        if len(categorizer_state['logs']) > 200:
            categorizer_state['logs'] = categorizer_state['logs'][-200:]
        socketio.emit('categorizer_log_update', log_entry)
        logger.info(f"CATEGORIZER {level.upper()}: {message}")

    def update_progress(self, current_product=None):
        if current_product is not None:
            categorizer_state['current_product'] = current_product
        socketio.emit('categorizer_progress_update', {
            'progress': categorizer_state['progress'],
            'current_product': categorizer_state['current_product']
        })

    def load_categories(self, estabelecimento_id):
        try:
            col_ref = (self.db.collection('estabelecimentos')
                       .document(estabelecimento_id)
                       .collection('ProductCategories'))
            docs = col_ref.stream()
            categories = []
            for doc in docs:
                data = doc.to_dict()
                if data:
                    categories.append({
                        'id': data.get('id', doc.id),
                        'name': data.get('name', ''),
                        'isActive': data.get('isActive', True)
                    })
            self.log_message(f"Carregadas {len(categories)} categorias", "info")
            return categories
        except Exception as e:
            self.log_message(f"Erro ao carregar categorias: {e}", "error")
            return []

    def load_subcategories(self, estabelecimento_id):
        try:
            col_ref = (self.db.collection('estabelecimentos')
                       .document(estabelecimento_id)
                       .collection('ProductSubcategories'))
            docs = col_ref.stream()
            subcategories = []
            for doc in docs:
                data = doc.to_dict()
                if data:
                    subcategories.append({
                        'id': data.get('id', doc.id),
                        'name': data.get('name', ''),
                        'categoryId': data.get('categoryId', ''),
                        'isActive': data.get('isActive', True)
                    })
            self.log_message(f"Carregadas {len(subcategories)} subcategorias", "info")
            return subcategories
        except Exception as e:
            self.log_message(f"Erro ao carregar subcategorias: {e}", "error")
            return []

    def load_products(self, estabelecimento_id, only_uncategorized=False, filter_category_id=None):
        try:
            col_ref = (self.db.collection('estabelecimentos')
                       .document(estabelecimento_id)
                       .collection('Products'))
            products = []
            for doc in col_ref.stream():
                data = doc.to_dict()
                if not data or not data.get('name'):
                    continue
                cats = data.get('categoriesIds') or []
                if only_uncategorized and cats:
                    continue
                if filter_category_id and filter_category_id not in cats:
                    continue
                products.append({'id': doc.id, 'name': data['name']})
            filtro = []
            if only_uncategorized:
                filtro.append('apenas sem categoria')
            if filter_category_id:
                filtro.append(f'categoria={filter_category_id}')
            desc = f" ({', '.join(filtro)})" if filtro else ''
            self.log_message(f"Carregados {len(products)} produtos{desc}", "info")
            return products
        except Exception as e:
            self.log_message(f"Erro ao carregar produtos: {e}", "error")
            return []

    def _call_openai(self, prompt, max_tokens=60):
        max_retries = 5
        for attempt in range(max_retries):
            try:
                response = openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": self.cat_system_prompt},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=max_tokens,
                    temperature=0.1
                )
            except _openai_module.RateLimitError as e:
                if _is_quota_error(e):
                    self.log_message("ERRO: Créditos da API OpenAI esgotados. O agente foi interrompido.", "error")
                    emit_quota_exceeded()
                    raise
                # Throttling temporário (RPM/TPM) — aguarda e tenta novamente
                wait = 0.5 * (2 ** attempt)  # 0.5s, 1s, 2s, 4s, 8s
                self.log_message(f"Rate limit atingido, aguardando {wait:.1f}s (tentativa {attempt + 1}/{max_retries})...", "warning")
                time.sleep(wait)
                if attempt == max_retries - 1:
                    raise
                continue
            except Exception as e:
                if _is_quota_error(e):
                    self.log_message("ERRO: Créditos da API OpenAI esgotados. O agente foi interrompido.", "error")
                    emit_quota_exceeded()
                raise
            break
        if hasattr(response, 'usage') and response.usage:
            inp = response.usage.prompt_tokens
            out = response.usage.completion_tokens
            call_cost = (inp * self.input_token_cost) + (out * self.output_token_cost)
            with self._lock:
                self.tokens_used += inp + out
                self.estimated_cost += call_cost
            record_daily_usage(inp + out, call_cost)
        return response.choices[0].message.content.strip()

    def _best_match(self, returned_id, valid_items):
        """Retorna o item com id que melhor corresponde ao retornado pela IA."""
        valid_ids = {item['id'] for item in valid_items}
        if returned_id in valid_ids:
            return returned_id
        lower = returned_id.lower()
        for item in valid_items:
            if item['id'].lower() == lower:
                return item['id']
        for item in valid_items:
            if item['id'].lower() in lower or lower in item['id'].lower():
                return item['id']
        return valid_items[0]['id'] if valid_items else None

    def get_category_and_subcategory(self, product_name, categories, subcategories):
        # Uma única chamada retorna categoria e subcategoria no formato "cat_id|sub_id"
        cats_text = "\n".join(f"{c['id']}|{c['name']}" for c in categories)
        subs_text = "\n".join(f"{s['id']}|{s['categoryId']}|{s['name']}" for s in subcategories)
        prompt = (
            f"Produto: \"{product_name}\"\n\n"
            f"CATEGORIAS (id|nome):\n{cats_text}\n\n"
            f"SUBCATEGORIAS (id|categoria_id|nome):\n{subs_text}\n\n"
            f"Responda APENAS no formato exato: categoria_id|subcategoria_id\n"
            f"Escolha a categoria e subcategoria mais adequadas para o produto."
        )
        try:
            raw = self._call_openai(prompt, max_tokens=80)
            parts = raw.strip().split('|')
            cat_raw = parts[0].strip() if len(parts) >= 1 else ''
            sub_raw = parts[1].strip() if len(parts) >= 2 else ''
            category_id = self._best_match(cat_raw, categories) if cat_raw else None
            if not category_id:
                self.log_message(f"Nao foi possivel determinar categoria para '{product_name}'", "warning")
                return None, None
            relevant_subs = [s for s in subcategories if s['categoryId'] == category_id]
            if not relevant_subs:
                return category_id, None
            subcategory_id = self._best_match(sub_raw, relevant_subs) if sub_raw else relevant_subs[0]['id']
            return category_id, subcategory_id
        except Exception as e:
            self.log_message(f"Erro na chamada OpenAI para '{product_name}': {e}", "error")
            return None, None

    def get_categories_batch(self, product_names, categories, subcategories):
        """Categoriza N produtos em uma única chamada. Retorna lista de (cat_id, sub_id)."""
        cats_text = "\n".join(f"{c['id']}|{c['name']}" for c in categories)
        subs_text = "\n".join(f"{s['id']}|{s['categoryId']}|{s['name']}" for s in subcategories)
        numbered = "\n".join(f"{i+1}. {name}" for i, name in enumerate(product_names))
        prompt = (
            f"CATEGORIAS (id|nome):\n{cats_text}\n\n"
            f"SUBCATEGORIAS (id|categoria_id|nome):\n{subs_text}\n\n"
            f"Produtos:\n{numbered}\n\n"
            f"Responda APENAS com uma linha por produto no formato exato:\n"
            f"N. categoria_id|subcategoria_id\n"
            f"Onde N é o número do produto. Sem explicações."
        )
        results = [(None, None)] * len(product_names)
        try:
            raw = self._call_openai(prompt, max_tokens=40 * len(product_names))
        except Exception as e:
            self.log_message(f"Erro OpenAI no batch: {e}", "error")
            return results
        for line in raw.strip().split('\n'):
            line = line.strip()
            if not line:
                continue
            try:
                dot_idx = line.index('.')
                n = int(line[:dot_idx].strip()) - 1
                if n < 0 or n >= len(product_names):
                    continue
                rest = line[dot_idx + 1:].strip()
                parts = rest.split('|')
                cat_raw = parts[0].strip() if len(parts) >= 1 else ''
                sub_raw = parts[1].strip() if len(parts) >= 2 else ''
                category_id = self._best_match(cat_raw, categories) if cat_raw else None
                if not category_id:
                    continue
                relevant_subs = [s for s in subcategories if s['categoryId'] == category_id]
                if not relevant_subs:
                    results[n] = (category_id, None)
                else:
                    subcategory_id = self._best_match(sub_raw, relevant_subs) if sub_raw else relevant_subs[0]['id']
                    results[n] = (category_id, subcategory_id)
            except (ValueError, IndexError):
                continue
        return results

    def update_product_categories(self, product_id, estabelecimento_id,
                                   category_id, subcategory_id,
                                   category_name, subcategory_name, dry_run=False,
                                   history_key='categorizer'):
        shelf_id = f"{category_id}_{subcategory_id}"
        shelf_entry = {
            'id': shelf_id,
            'productCategoryId': category_id,
            'productSubcategoryId': subcategory_id,
            'categoryName': category_name,
            'subcategoryName': subcategory_name
        }
        update_data = {
            'categoriesIds': [category_id],
            'subcategoriesIds': [subcategory_id],
            'shelves': [shelf_entry],
            'shelvesIds': [shelf_id]
        }
        if dry_run:
            self.log_message(
                f"[DRY RUN] {product_id}: categoria={category_id} | sub={subcategory_id}", "warning"
            )
            return True
        try:
            doc_ref = (self.db.collection('estabelecimentos')
                       .document(estabelecimento_id)
                       .collection('Products')
                       .document(product_id))
            # Lê estado anterior para possibilitar desfazer
            old_data = {}
            try:
                snap = doc_ref.get()
                if snap.exists:
                    d = snap.to_dict() or {}
                    old_data = {
                        'categoriesIds': d.get('categoriesIds', []),
                        'subcategoriesIds': d.get('subcategoriesIds', []),
                        'shelves': d.get('shelves', []),
                        'shelvesIds': d.get('shelvesIds', []),
                    }
            except Exception:
                pass
            doc_ref.update(update_data)
            with _undo_lock:
                undo_store[history_key].append({
                    'product_id': product_id,
                    'estabelecimento_id': estabelecimento_id,
                    'old_data': old_data,
                    'new_data': update_data,
                })
            return True
        except Exception as e:
            self.log_message(f"Erro ao atualizar produto {product_id}: {e}", "error")
            return False

    # ---- Targeted (Dirigido) mode ----

    def log_message_targeted(self, message, level="info"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = {'timestamp': timestamp, 'message': message, 'level': level}
        categorizer_targeted_state['logs'].append(log_entry)
        if len(categorizer_targeted_state['logs']) > 200:
            categorizer_targeted_state['logs'] = categorizer_targeted_state['logs'][-200:]
        socketio.emit('categorizer_targeted_log_update', log_entry)
        logger.info(f"CATDIR {level.upper()}: {message}")

    def update_progress_targeted(self, current_product=None):
        if current_product is not None:
            categorizer_targeted_state['current_product'] = current_product
        socketio.emit('categorizer_targeted_progress_update', {
            'progress': categorizer_targeted_state['progress'],
            'current_product': categorizer_targeted_state['current_product']
        })

    def load_all_products_with_cats(self, estabelecimento_id):
        """Carrega todos os produtos com id, name e categoriesIds."""
        try:
            col_ref = (self.db.collection('estabelecimentos')
                       .document(estabelecimento_id)
                       .collection('Products'))
            products = []
            for doc in col_ref.stream():
                data = doc.to_dict()
                if data and data.get('name'):
                    products.append({
                        'id': doc.id,
                        'name': data['name'],
                        'categories_ids': data.get('categoriesIds', [])
                    })
            self.log_message_targeted(f"Carregados {len(products)} produtos", "info")
            return products
        except Exception as e:
            self.log_message_targeted(f"Erro ao carregar produtos: {e}", "error")
            return []

    def _should_assign_to_category(self, product_name, category_name, target_subs):
        """Pergunta a IA se o produto pertence a categoria e, se sim, qual subcategoria."""
        subs_text = "\n".join([f"id={s['id']} | nome={s['name']}" for s in target_subs])
        prompt = (
            f"Produto: \"{product_name}\"\n"
            f"Categoria em avaliacao: {category_name}\n\n"
            f"Este produto pertence a categoria '{category_name}'?\n"
            f"Se SIM, responda: SIM|id_da_subcategoria\n"
            f"Se NAO, responda apenas: NAO\n\n"
            f"Subcategorias disponiveis:\n{subs_text}"
        )
        raw = self._call_openai(prompt, max_tokens=80).strip().upper()
        if raw.startswith('SIM'):
            parts = raw.split('|', 1)
            sub_id_raw = parts[1].strip() if len(parts) > 1 else ''
            sub_id = self._best_match(sub_id_raw, target_subs)
            return True, sub_id
        return False, None

    def run_categorization_targeted(self, estabelecimento_id, target_category_id,
                                     include_others=False, delay=0.5, dry_run=False):
        try:
            self.tokens_used = 0
            self.estimated_cost = 0
            categorizer_targeted_state['running'] = True
            categorizer_targeted_state['logs'] = []
            with _undo_lock:
                undo_store['categorizer_targeted'].clear()

            self.log_message_targeted(f"Iniciando modo dirigido: {estabelecimento_id}", "info")
            self.log_message_targeted(f"Categoria alvo: {target_category_id}", "info")
            if include_others:
                self.log_message_targeted("Opcao ativa: tambem avaliar produtos de outras categorias", "info")
            if dry_run:
                self.log_message_targeted("MODO DRY RUN - Nenhuma atualizacao sera feita", "warning")

            categories = self.load_categories(estabelecimento_id)
            subcategories = self.load_subcategories(estabelecimento_id)
            cat_by_id = {c['id']: c for c in categories}
            sub_by_id = {s['id']: s for s in subcategories}

            target_cat = cat_by_id.get(target_category_id)
            if not target_cat:
                self.log_message_targeted(f"Categoria '{target_category_id}' nao encontrada", "error")
                categorizer_targeted_state['running'] = False
                return False

            target_subs = [s for s in subcategories if s['categoryId'] == target_category_id]
            if not target_subs:
                self.log_message_targeted(f"Sem subcategorias para '{target_category_id}'", "error")
                categorizer_targeted_state['running'] = False
                return False

            subs_preview = ', '.join(s['name'] for s in target_subs[:8])
            self.log_message_targeted(f"Subcategorias ({len(target_subs)}): {subs_preview}...", "info")

            all_products = self.load_all_products_with_cats(estabelecimento_id)
            if not all_products:
                self.log_message_targeted("Nenhum produto encontrado", "warning")
                categorizer_targeted_state['running'] = False
                return False

            # Fase 1: produtos que já têm a categoria alvo → só define subcategoria
            phase1 = [p for p in all_products if target_category_id in p.get('categories_ids', [])]
            # Fase 2 (opcional): demais produtos → IA decide se pertencem à categoria
            phase2 = [p for p in all_products if target_category_id not in p.get('categories_ids', [])] if include_others else []

            self.log_message_targeted(f"Fase 1 — ja em '{target_cat['name']}': {len(phase1)} produtos", "info")
            if include_others:
                self.log_message_targeted(f"Fase 2 — outras categorias a avaliar: {len(phase2)} produtos", "info")

            total = len(phase1) + len(phase2)
            if total == 0:
                self.log_message_targeted("Nenhum produto para processar", "warning")
                categorizer_targeted_state['running'] = False
                return False

            categorizer_targeted_state['progress'] = {
                'total': total, 'processed': 0, 'updated': 0,
                'skipped': 0, 'errors': 0, 'tokens_used': 0, 'estimated_cost': 0.0
            }
            self.update_progress_targeted()

            subs_text = "\n".join([f"id={s['id']} | nome={s['name']}" for s in target_subs])

            def _tick(ok):
                # Must be called inside self._lock
                if ok is True:
                    categorizer_targeted_state['progress']['updated'] += 1
                elif ok is False:
                    categorizer_targeted_state['progress']['errors'] += 1
                else:
                    categorizer_targeted_state['progress']['skipped'] += 1
                categorizer_targeted_state['progress']['processed'] += 1
                categorizer_targeted_state['progress']['tokens_used'] = self.tokens_used
                categorizer_targeted_state['progress']['estimated_cost'] = self.estimated_cost
                self.update_progress_targeted()

            # ── Fase 1: só subcategoria ──────────────────────────────────────
            if phase1:
                self.log_message_targeted(f"=== Fase 1: definindo subcategorias ({len(phase1)} produtos, 8 paralelos) ===", "info")

            def _phase1_one(args):
                idx, product = args
                if not categorizer_targeted_state['running']:
                    return
                pid, pname = product['id'], product['name']
                self.update_progress_targeted({'id': pid, 'name': pname, 'index': idx, 'total': total})
                self.log_message_targeted(f"[{idx}/{total}] {pname}", "info")

                sub_prompt = (
                    f"Produto: \"{pname}\"\n"
                    f"Categoria: {target_category_id} ({target_cat['name']})\n\n"
                    f"Subcategorias disponiveis:\n{subs_text}\n\n"
                    f"Qual e o id da subcategoria mais adequada? Responda APENAS com o id exato."
                )
                try:
                    raw_sub = self._call_openai(sub_prompt, max_tokens=100)
                    subcategory_id = self._best_match(raw_sub, target_subs)
                except Exception as e:
                    self.log_message_targeted(f"  Erro OpenAI: {e}", "error")
                    with self._lock:
                        _tick(False)
                    return

                subcategory_name = sub_by_id.get(subcategory_id, {}).get('name', subcategory_id)
                self.log_message_targeted(f"  -> {target_cat['name']} / {subcategory_name}", "success")
                ok = self.update_product_categories(pid, estabelecimento_id,
                                                    target_category_id, subcategory_id,
                                                    target_cat['name'], subcategory_name, dry_run,
                                                    history_key='categorizer_targeted')
                with self._lock:
                    _tick(ok)

            with ThreadPoolExecutor(max_workers=3) as executor:
                list(executor.map(_phase1_one, enumerate(phase1, 1)))

            # ── Fase 2: avalia se produto pertence à categoria ───────────────
            if phase2 and categorizer_targeted_state['running']:
                self.log_message_targeted(f"=== Fase 2: avaliando outros produtos ({len(phase2)}, 3 paralelos) ===", "info")

            def _phase2_one(args):
                idx, product = args
                if not categorizer_targeted_state['running']:
                    return
                pid, pname = product['id'], product['name']
                self.update_progress_targeted({'id': pid, 'name': pname, 'index': idx, 'total': total})
                self.log_message_targeted(f"[{idx}/{total}] {pname}", "info")

                try:
                    fits, subcategory_id = self._should_assign_to_category(pname, target_cat['name'], target_subs)
                except Exception as e:
                    self.log_message_targeted(f"  Erro OpenAI: {e}", "error")
                    with self._lock:
                        _tick(False)
                    return

                if not fits:
                    self.log_message_targeted(f"  -> Nao pertence a '{target_cat['name']}', ignorado", "info")
                    with self._lock:
                        _tick(None)
                    return

                subcategory_name = sub_by_id.get(subcategory_id, {}).get('name', subcategory_id) if subcategory_id else ''
                self.log_message_targeted(f"  -> {target_cat['name']} / {subcategory_name}", "success")
                ok = self.update_product_categories(pid, estabelecimento_id,
                                                    target_category_id, subcategory_id,
                                                    target_cat['name'], subcategory_name, dry_run,
                                                    history_key='categorizer_targeted')
                with self._lock:
                    _tick(ok)

            with ThreadPoolExecutor(max_workers=3) as executor:
                list(executor.map(_phase2_one, enumerate(phase2, len(phase1) + 1)))

            prog = categorizer_targeted_state['progress']
            self.log_message_targeted("=== RESULTADO ===", "info")
            self.log_message_targeted(
                f"Total: {prog['total']} | Atualizados: {prog['updated']} | "
                f"Ignorados: {prog['skipped']} | Erros: {prog['errors']}", "info"
            )
            self.log_message_targeted(f"Tokens: {self.tokens_used:,} | Custo: ${self.estimated_cost:.4f}", "info")

            categorizer_targeted_state['running'] = False
            categorizer_targeted_state['current_product'] = None
            self.update_progress_targeted()
            return True
        except Exception as e:
            self.log_message_targeted(f"Erro: {e}", "error")
            categorizer_targeted_state['running'] = False
            categorizer_targeted_state['current_product'] = None
            self.update_progress_targeted()
            return False

    def run_categorization(self, estabelecimento_id, delay_between_products=0.5, dry_run=False,
                           only_uncategorized=False, filter_category_id=None):
        try:
            self.tokens_used = 0
            self.estimated_cost = 0
            categorizer_state['running'] = True
            categorizer_state['logs'] = []
            with _undo_lock:
                undo_store['categorizer'].clear()

            self.log_message(f"Iniciando categorizacao para: {estabelecimento_id}", "info")
            if dry_run:
                self.log_message("MODO DRY RUN - Nenhuma atualizacao sera feita", "warning")

            categories = self.load_categories(estabelecimento_id)
            if not categories:
                self.log_message("Nenhuma categoria encontrada", "warning")
                categorizer_state['running'] = False
                return False

            # Exclui mercearia do modo automatico (gerenciada pelo Modo Dirigido)
            categories = [c for c in categories if c['id'].lower() != 'mercearia']
            if not categories:
                self.log_message("Nenhuma categoria disponivel apos remover mercearia", "warning")
                categorizer_state['running'] = False
                return False
            self.log_message(f"Categorias disponiveis (excluindo mercearia): {[c['id'] for c in categories]}", "info")

            subcategories = self.load_subcategories(estabelecimento_id)
            if not subcategories:
                self.log_message("Nenhuma subcategoria encontrada", "warning")
                categorizer_state['running'] = False
                return False

            products = self.load_products(estabelecimento_id, only_uncategorized, filter_category_id)
            if not products:
                self.log_message("Nenhum produto encontrado com os filtros aplicados", "warning")
                categorizer_state['running'] = False
                return False

            cat_by_id = {c['id']: c for c in categories}
            sub_by_id = {s['id']: s for s in subcategories}

            total = len(products)
            categorizer_state['progress'] = {
                'total': total, 'processed': 0, 'updated': 0,
                'errors': 0, 'tokens_used': 0, 'estimated_cost': 0.0
            }
            self.update_progress()

            BATCH_SIZE = 20
            batches = [products[s:s + BATCH_SIZE] for s in range(0, total, BATCH_SIZE)]
            self.log_message(
                f"Processando {total} produtos em {len(batches)} lotes de {BATCH_SIZE} (2 paralelos)", "info"
            )

            def _cat_batch(args):
                batch_start, batch = args
                if not categorizer_state['running']:
                    return
                names = [p['name'] for p in batch]
                self.update_progress({'id': batch[0]['id'], 'name': names[0], 'index': batch_start + 1, 'total': total})
                cat_results = self.get_categories_batch(names, categories, subcategories)
                for j, (product, (category_id, subcategory_id)) in enumerate(zip(batch, cat_results)):
                    if not categorizer_state['running']:
                        return
                    pid, pname = product['id'], product['name']
                    i = batch_start + j + 1
                    self.log_message(f"[{i}/{total}] {pname}", "info")
                    with self._lock:
                        if not category_id:
                            self.log_message(f"  Erro: nao foi possivel categorizar {pid}", "error")
                            categorizer_state['progress']['errors'] += 1
                        else:
                            category_name = cat_by_id.get(category_id, {}).get('name', category_id)
                            subcategory_name = sub_by_id.get(subcategory_id, {}).get('name', subcategory_id) if subcategory_id else ''
                            self.log_message(f"  -> {category_name} / {subcategory_name}", "success")
                            ok = self.update_product_categories(
                                pid, estabelecimento_id,
                                category_id, subcategory_id,
                                category_name, subcategory_name, dry_run
                            )
                            if ok:
                                categorizer_state['progress']['updated'] += 1
                            else:
                                categorizer_state['progress']['errors'] += 1
                        categorizer_state['progress']['processed'] += 1
                        categorizer_state['progress']['tokens_used'] = self.tokens_used
                        categorizer_state['progress']['estimated_cost'] = self.estimated_cost
                    self.update_progress()

            with ThreadPoolExecutor(max_workers=2) as executor:
                list(executor.map(_cat_batch, ((i * BATCH_SIZE, b) for i, b in enumerate(batches))))

            prog = categorizer_state['progress']
            self.log_message("=== ESTATISTICAS FINAIS ===", "info")
            self.log_message(f"Total: {prog['total']}", "info")
            self.log_message(f"Processados: {prog['processed']}", "info")
            self.log_message(f"Atualizados: {prog['updated']}", "success")
            self.log_message(f"Erros: {prog['errors']}", "error" if prog['errors'] > 0 else "info")
            self.log_message(f"Tokens: {self.tokens_used:,}", "info")
            self.log_message(f"Custo estimado: ${self.estimated_cost:.4f}", "info")

            categorizer_state['running'] = False
            categorizer_state['current_product'] = None
            self.update_progress()
            return True
        except Exception as e:
            self.log_message(f"Erro durante a categorizacao: {e}", "error")
            categorizer_state['running'] = False
            categorizer_state['current_product'] = None
            self.update_progress()
            return False


# ============================================================
# Instancias globais
# ============================================================
automator = None
advanced_explorer = None
simple_explorer = None
categorizer = None


def init_all():
    global automator, advanced_explorer, simple_explorer, categorizer
    try:
        init_firebase()
        automator = FirestoreProductAutomator(db)
        advanced_explorer = FirestoreStructureExplorer(db)
        simple_explorer = FirestoreSimpleExplorer(db)
        categorizer = ProductCategorizerAgent(db)
        # Carrega credenciais de admin no cache
        try:
            _load_admin_creds()
        except Exception as e:
            logger.warning(f"Nao foi possivel carregar credenciais do Firestore: {e}")
        # Carrega chave OpenAI salva no Firestore (se existir)
        try:
            if db:
                doc = db.collection('Automacoes').document('config').get()
                if doc.exists:
                    key = (doc.to_dict() or {}).get('openai_api_key', '').strip()
                    if key:
                        _reload_openai_client(key)
                        logger.info("Chave OpenAI carregada do Firestore")
        except Exception as e:
            logger.warning(f"Nao foi possivel carregar chave OpenAI do Firestore: {e}")
        logger.info("Todos os modulos inicializados com sucesso")
        return True
    except Exception as e:
        logger.error(f"Erro ao inicializar: {e}")
        return False


# ============================================================
# Autenticação de administrador
# ============================================================

# Cache em memória das credenciais de admin — evita chamada ao Firestore em cada login
_admin_creds_cache: dict = {'user': None, 'passwd': None}


def _load_admin_creds():
    """Carrega credenciais do Firestore para o cache em memória.
    Fallback para variáveis de ambiente ADMIN_USERNAME / ADMIN_PASSWORD."""
    global _admin_creds_cache
    try:
        if db:
            doc = db.collection('Automacoes').document('admin').get()
            if doc.exists:
                data = doc.to_dict() or {}
                user = data.get('userAdmin', '').strip()
                passwd = data.get('passAdmin', '').strip()
                if user and passwd:
                    _admin_creds_cache = {'user': user, 'passwd': passwd}
                    logger.info("Credenciais de admin carregadas do Firestore")
                    return
    except Exception as e:
        logger.warning(f"Erro ao carregar credenciais do Firestore: {e}")
    # Fallback: variáveis de ambiente (desenvolvimento local)
    env_user = os.getenv('ADMIN_USERNAME', '').strip()
    env_pass = os.getenv('ADMIN_PASSWORD', '').strip()
    if env_user and env_pass:
        _admin_creds_cache = {'user': env_user, 'passwd': env_pass}
        logger.info("Credenciais de admin carregadas do .env (fallback)")
    else:
        _admin_creds_cache = {'user': None, 'passwd': None}


def get_admin_credentials():
    """Retorna credenciais de admin do cache em memória (sem chamada ao Firestore)."""
    return _admin_creds_cache.get('user'), _admin_creds_cache.get('passwd')

PUBLIC_ROUTES = {'login', 'static'}

@app.before_request
def require_login():
    if request.endpoint in PUBLIC_ROUTES:
        return
    if not session.get('logged_in'):
        return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()
        admin_user, admin_pass = get_admin_credentials()
        if admin_user is None:
            error = 'Serviço indisponível. Tente novamente em instantes.'
        elif username == admin_user and password == admin_pass:
            session['logged_in'] = True
            return redirect(url_for('index'))
        else:
            error = 'Usuário ou senha incorretos.'
    return render_template('login.html', error=error)

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


# ============================================================
# Rotas: Configurações
# ============================================================

@app.route('/api/settings', methods=['GET'])
def get_settings():
    username, _ = get_admin_credentials()
    api_key_preview = ''
    api_key_set = False
    tema = False
    try:
        if db:
            doc = db.collection('Automacoes').document('config').get()
            if doc.exists:
                data = doc.to_dict() or {}
                key = data.get('openai_api_key', '').strip()
                if key:
                    api_key_set = True
                    visible = min(8, len(key))
                    tail = key[-4:] if len(key) > 4 else ''
                    api_key_preview = key[:visible] + '...' + tail
                tema = bool(data.get('tema', False))
    except Exception as e:
        logger.warning(f"Erro ao buscar config do Firestore: {e}")
    return jsonify({'username': username, 'api_key_set': api_key_set, 'api_key_preview': api_key_preview, 'tema': tema})


@app.route('/api/settings/credentials', methods=['POST'])
def update_credentials():
    data = request.get_json() or {}
    new_user = data.get('username', '').strip()
    new_pass = data.get('password', '').strip()
    if not new_user or not new_pass:
        return jsonify({'success': False, 'error': 'Usuário e senha são obrigatórios'}), 400
    try:
        db.collection('Automacoes').document('admin').set({
            'userAdmin': new_user,
            'passAdmin': new_pass,
            'updated_at': datetime.now().isoformat(),
        }, merge=True)
        _admin_creds_cache['user'] = new_user
        _admin_creds_cache['passwd'] = new_pass
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Erro ao salvar credenciais: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/settings/openai-key', methods=['POST'])
def update_openai_key():
    data = request.get_json() or {}
    key = data.get('api_key', '').strip()
    if not key or not key.startswith('sk-') or len(key) < 20:
        return jsonify({'success': False, 'error': 'Chave inválida. Deve começar com sk- e ter pelo menos 20 caracteres'}), 400
    try:
        db.collection('Automacoes').document('config').set({
            'openai_api_key': key,
            'updated_at': datetime.now().isoformat(),
        }, merge=True)
        _reload_openai_client(key)
        logger.info("Chave OpenAI atualizada via settings")
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Erro ao salvar chave OpenAI: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/settings/theme', methods=['GET'])
def get_theme():
    try:
        if db:
            doc = db.collection('Automacoes').document('config').get()
            if doc.exists:
                tema = (doc.to_dict() or {}).get('tema', False)
                return jsonify({'tema': bool(tema)})
    except Exception as e:
        logger.warning(f"Erro ao buscar tema: {e}")
    return jsonify({'tema': False})


@app.route('/api/settings/theme', methods=['POST'])
def save_theme():
    data = request.get_json() or {}
    tema = bool(data.get('tema', False))
    try:
        db.collection('Automacoes').document('config').set(
            {'tema': tema, 'updated_at': datetime.now().isoformat()},
            merge=True
        )
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Erro ao salvar tema: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================
# Rotas: Pagina principal
# ============================================================
@app.route('/')
def index():
    return render_template("index.html")


# ============================================================
# Rotas: Renamer
# ============================================================
@app.route('/api/renamer/categories', methods=['GET'])
def get_categories():
    global automator
    if not automator:
        return jsonify({'error': 'Automatizador nao inicializado'}), 500
    estabelecimento_id = request.args.get('estabelecimento_id', 'estabelecimento-teste')
    categories = automator.get_available_categories(estabelecimento_id)
    return jsonify({'success': True, 'categories': categories})


@app.route('/api/renamer/prompt', methods=['GET'])
def get_prompt():
    global automator
    if not automator:
        return jsonify({'error': 'Automatizador nao inicializado'}), 500
    return jsonify({
        'success': True,
        'prompt': automator.prompt_template,
        'default_prompt': automator.default_prompt_template
    })


@app.route('/api/renamer/prompt', methods=['POST'])
def save_prompt():
    global automator
    if not automator:
        return jsonify({'error': 'Automatizador nao inicializado'}), 500
    try:
        data = request.json or {}
        prompt = data.get('prompt', '').strip()
        if not prompt:
            return jsonify({'error': 'Prompt vazio'}), 400
        if automator.save_prompt_to_firestore(prompt):
            return jsonify({'success': True, 'message': 'Prompt salvo no Firestore'})
        else:
            return jsonify({'error': 'Erro ao salvar no Firestore'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/renamer/start', methods=['POST'])
def start_automation():
    global automator
    if not automator:
        return jsonify({'error': 'Automatizador nao inicializado'}), 500
    if automation_state['running']:
        return jsonify({'error': 'Automacao ja esta em execucao'}), 400
    try:
        data = request.json or {}
        estabelecimento_id = data.get('estabelecimento_id', 'estabelecimento-teste')
        delay = float(data.get('delay', 1.0))
        dry_run = data.get('dry_run', False)
        categories = data.get('categories', [])
        custom_prompt = data.get('custom_prompt', '').strip() or None

        if not categories:
            return jsonify({'error': 'Selecione ao menos uma categoria'}), 400


        def run_thread():
            automator.run_automation(estabelecimento_id, categories, delay, dry_run, custom_prompt)

        thread = Thread(target=run_thread)
        thread.daemon = True
        thread.start()
        return jsonify({'success': True, 'message': 'Automacao iniciada'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/renamer/stop', methods=['POST'])
def stop_automation():
    if not automation_state['running']:
        return jsonify({'error': 'Nenhuma automacao em execucao'}), 400
    automation_state['running'] = False
    return jsonify({'success': True, 'message': 'Automacao interrompida'})


@app.route('/api/renamer/undo-info', methods=['GET'])
def renamer_undo_info():
    with _undo_lock:
        count = len(undo_store['renamer'])
    return jsonify({'count': count, 'available': count > 0})


@app.route('/api/renamer/undo', methods=['POST'])
def renamer_undo():
    global automator
    if not automator:
        return jsonify({'error': 'Automatizador nao inicializado'}), 500
    if automation_state['running']:
        return jsonify({'error': 'Aguarde a execucao terminar antes de desfazer'}), 400
    with _undo_lock:
        changes = list(undo_store['renamer'])
    if not changes:
        return jsonify({'error': 'Nenhuma alteracao para desfazer'}), 400
    reverted, errors = 0, 0
    for entry in changes:
        try:
            doc_ref = (automator.db.collection('estabelecimentos')
                       .document(entry['estabelecimento_id'])
                       .collection('Products')
                       .document(entry['product_id']))
            doc_ref.update({'name': entry['old_name']})
            reverted += 1
        except Exception as e:
            logger.error(f"Undo renamer erro {entry['product_id']}: {e}")
            errors += 1
    with _undo_lock:
        undo_store['renamer'].clear()
    return jsonify({'success': True, 'reverted': reverted, 'errors': errors})


@app.route('/api/renamer/status', methods=['GET'])
def renamer_status():
    return jsonify({
        'running': automation_state['running'],
        'progress': automation_state['progress'],
        'current_product': automation_state['current_product']
    })


@app.route('/api/renamer/logs', methods=['GET'])
def renamer_logs():
    return jsonify({'logs': automation_state['logs']})


# ============================================================
# Rotas: Explorador Avancado
# ============================================================
@app.route('/api/explorer/explore', methods=['POST'])
def explorer_explore():
    global advanced_explorer
    if not advanced_explorer:
        return jsonify({'error': 'Explorador nao inicializado'}), 500
    if explorer_state['exploring']:
        return jsonify({'error': 'Exploração ja esta em andamento'}), 400
    try:
        data = request.json or {}
        path = data.get('path', '').strip()
        max_docs = min(int(data.get('max_docs', 10)), 50)

        if path in explorer_state['structure_cache']:
            cached_result = explorer_state['structure_cache'][path]
            cached_result['from_cache'] = True
            return Response(
                json.dumps({'success': True, 'data': cached_result}, default=firestore_default),
                mimetype="application/json"
            )

        result = advanced_explorer.explore_firestore_path(path, max_docs)
        return Response(
            json.dumps({'success': True, 'data': result}, default=firestore_default),
            mimetype="application/json"
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/explorer/suggestions', methods=['POST'])
def explorer_suggestions():
    global advanced_explorer
    if not advanced_explorer:
        return jsonify({'error': 'Explorador nao inicializado'}), 500
    try:
        data = request.json or {}
        partial_path = data.get('path', '').strip()
        suggestions = advanced_explorer.get_path_suggestions(partial_path)
        return Response(
            json.dumps({'success': True, 'suggestions': suggestions}, default=firestore_default),
            mimetype="application/json"
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/explorer/cache', methods=['GET'])
def explorer_cache_list():
    return Response(
        json.dumps({'success': True, 'cached_paths': list(explorer_state['structure_cache'].keys())}, default=firestore_default),
        mimetype="application/json"
    )


@app.route('/api/explorer/cache/<path:cached_path>', methods=['GET'])
def explorer_cache_get(cached_path):
    if cached_path in explorer_state['structure_cache']:
        result = explorer_state['structure_cache'][cached_path]
        result['from_cache'] = True
        return Response(
            json.dumps({'success': True, 'data': result}, default=firestore_default),
            mimetype="application/json"
        )
    return jsonify({'error': 'Caminho nao encontrado no cache'}), 404


@app.route('/api/explorer/export/<path:cached_path>', methods=['GET'])
def explorer_export(cached_path):
    if cached_path in explorer_state['structure_cache']:
        result = explorer_state['structure_cache'][cached_path]
        export_data = {
            "firestore_structure": result,
            "exported_at": datetime.now().isoformat(),
            "export_format": "firestore_structure_v1"
        }
        response = Response(
            json.dumps(export_data, default=firestore_default),
            mimetype="application/json"
        )
        response.headers['Content-Disposition'] = f'attachment; filename=firestore_structure_{cached_path.replace("/", "_")}.json'
        return response
    return jsonify({'error': 'Caminho nao encontrado no cache'}), 404


@app.route('/api/explorer/status', methods=['GET'])
def explorer_status():
    return Response(
        json.dumps({
            'exploring': explorer_state['exploring'],
            'progress': explorer_state['progress'],
            'current_path': explorer_state['current_path']
        }, default=firestore_default),
        mimetype="application/json"
    )


@app.route('/api/explorer/logs', methods=['GET'])
def explorer_logs():
    return Response(
        json.dumps({'logs': explorer_state['logs']}, default=firestore_default),
        mimetype="application/json"
    )


# ============================================================
# Rotas: Explorador Simples
# ============================================================
@app.route('/api/explorer-simple/explore', methods=['POST'])
def simple_explore():
    global simple_explorer
    if not simple_explorer:
        return jsonify({'error': 'Explorador simples nao inicializado'}), 500
    try:
        data = request.json or {}
        path = data.get('path', '').strip()
        max_docs = int(data.get('max_docs', 5))
        result = simple_explorer.explore(path, max_docs)
        result_safe = to_json_safe(result)
        return Response(json.dumps({'success': True, 'data': result_safe}, ensure_ascii=False), mimetype="application/json")
    except Exception as e:
        return Response(json.dumps({'error': str(e)}), status=500, mimetype="application/json")


# ============================================================
# Rotas: Categorizador
# ============================================================
@app.route('/api/categorizer/prompt', methods=['GET'])
def get_cat_prompt():
    global categorizer
    if not categorizer:
        return jsonify({'error': 'Categorizador nao inicializado'}), 500
    return jsonify({
        'success': True,
        'prompt': categorizer.cat_system_prompt,
        'default_prompt': ProductCategorizerAgent.DEFAULT_CAT_SYSTEM_PROMPT
    })


@app.route('/api/categorizer/prompt', methods=['POST'])
def save_cat_prompt():
    global categorizer
    if not categorizer:
        return jsonify({'error': 'Categorizador nao inicializado'}), 500
    try:
        data = request.json or {}
        prompt = data.get('prompt', '').strip()
        if not prompt:
            return jsonify({'error': 'Prompt vazio'}), 400
        if categorizer.save_cat_prompt_to_firestore(prompt):
            return jsonify({'success': True, 'message': 'Prompt salvo no Firestore'})
        else:
            return jsonify({'error': 'Erro ao salvar no Firestore'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/categorizer/start', methods=['POST'])
def start_categorization():
    global categorizer
    if not categorizer:
        return jsonify({'error': 'Categorizador nao inicializado'}), 500
    if categorizer_state['running']:
        return jsonify({'error': 'Categorizacao ja em execucao'}), 400
    data = request.json or {}
    estabelecimento_id = data.get('estabelecimento_id', '').strip()
    if not estabelecimento_id:
        return jsonify({'error': 'estabelecimento_id e obrigatorio'}), 400
    delay = float(data.get('delay', 0.5))
    dry_run = bool(data.get('dry_run', False))
    only_uncategorized = bool(data.get('only_uncategorized', False))
    filter_category_id = data.get('filter_category_id', '').strip() or None
    custom_prompt = data.get('custom_prompt', '').strip() or None

    # Sobrescreve o prompt apenas para esta execucao (restaura ao final)
    original_prompt = categorizer.cat_system_prompt
    if custom_prompt:
        categorizer.cat_system_prompt = custom_prompt

    def run():
        try:
            categorizer.run_categorization(
                estabelecimento_id, delay, dry_run,
                only_uncategorized=only_uncategorized,
                filter_category_id=filter_category_id
            )
        finally:
            if custom_prompt:
                categorizer.cat_system_prompt = original_prompt

    Thread(target=run, daemon=True).start()
    return jsonify({'success': True, 'message': 'Categorizacao iniciada'})


@app.route('/api/categorizer/stop', methods=['POST'])
def stop_categorization():
    if not categorizer_state['running']:
        return jsonify({'error': 'Nenhuma categorizacao em execucao'}), 400
    categorizer_state['running'] = False
    return jsonify({'success': True})


def _undo_categorizer_changes(history_key):
    """Reverte alterações de categoria/subcategoria do undo_store para a chave dada."""
    global categorizer
    if not categorizer:
        return 0, 0, 'Categorizador nao inicializado'
    with _undo_lock:
        changes = list(undo_store[history_key])
    if not changes:
        return 0, 0, 'Nenhuma alteracao para desfazer'
    reverted, errors = 0, 0
    for entry in changes:
        try:
            doc_ref = (categorizer.db.collection('estabelecimentos')
                       .document(entry['estabelecimento_id'])
                       .collection('Products')
                       .document(entry['product_id']))
            old = entry.get('old_data', {})
            doc_ref.update({
                'categoriesIds': old.get('categoriesIds', []),
                'subcategoriesIds': old.get('subcategoriesIds', []),
                'shelves': old.get('shelves', []),
                'shelvesIds': old.get('shelvesIds', []),
            })
            reverted += 1
        except Exception as e:
            logger.error(f"Undo categorizer erro {entry['product_id']}: {e}")
            errors += 1
    with _undo_lock:
        undo_store[history_key].clear()
    return reverted, errors, None


@app.route('/api/categorizer/undo-info', methods=['GET'])
def categorizer_undo_info():
    with _undo_lock:
        count = len(undo_store['categorizer'])
    return jsonify({'count': count, 'available': count > 0})


@app.route('/api/categorizer/undo', methods=['POST'])
def categorizer_undo():
    if categorizer_state['running']:
        return jsonify({'error': 'Aguarde a execucao terminar antes de desfazer'}), 400
    reverted, errors, err_msg = _undo_categorizer_changes('categorizer')
    if err_msg:
        return jsonify({'error': err_msg}), (500 if 'inicializado' in err_msg else 400)
    return jsonify({'success': True, 'reverted': reverted, 'errors': errors})


@app.route('/api/categorizer-targeted/undo-info', methods=['GET'])
def categorizer_targeted_undo_info():
    with _undo_lock:
        count = len(undo_store['categorizer_targeted'])
    return jsonify({'count': count, 'available': count > 0})


@app.route('/api/categorizer-targeted/undo', methods=['POST'])
def categorizer_targeted_undo():
    if categorizer_targeted_state['running']:
        return jsonify({'error': 'Aguarde a execucao terminar antes de desfazer'}), 400
    reverted, errors, err_msg = _undo_categorizer_changes('categorizer_targeted')
    if err_msg:
        return jsonify({'error': err_msg}), (500 if 'inicializado' in err_msg else 400)
    return jsonify({'success': True, 'reverted': reverted, 'errors': errors})


@app.route('/api/categorizer/status', methods=['GET'])
def categorizer_status():
    return jsonify({
        'running': categorizer_state['running'],
        'progress': categorizer_state['progress'],
        'current_product': categorizer_state['current_product']
    })


@app.route('/api/categorizer/logs', methods=['GET'])
def categorizer_logs():
    return jsonify({'logs': categorizer_state['logs']})


@app.route('/api/categorizer/categories', methods=['GET'])
def get_categorizer_categories():
    global categorizer
    if not categorizer:
        return jsonify({'error': 'Categorizador nao inicializado'}), 500
    estabelecimento_id = request.args.get('estabelecimento_id', '').strip()
    if not estabelecimento_id:
        return jsonify({'error': 'estabelecimento_id e obrigatorio'}), 400
    cats = categorizer.load_categories(estabelecimento_id)
    return jsonify({'success': True, 'categories': cats})


# ============================================================
# Rotas: Categorizador Dirigido
# ============================================================
@app.route('/api/categorizer-targeted/start', methods=['POST'])
def start_categorization_targeted():
    global categorizer
    if not categorizer:
        return jsonify({'error': 'Categorizador nao inicializado'}), 500
    if categorizer_targeted_state['running']:
        return jsonify({'error': 'Ja em execucao'}), 400
    data = request.json or {}
    est_id = data.get('estabelecimento_id', '').strip()
    target_cat_id = data.get('target_category_id', '').strip()
    if not est_id or not target_cat_id:
        return jsonify({'error': 'estabelecimento_id e target_category_id sao obrigatorios'}), 400
    include_others = bool(data.get('include_others', False))
    delay = float(data.get('delay', 0.5))
    dry_run = bool(data.get('dry_run', False))

    def run():
        categorizer.run_categorization_targeted(est_id, target_cat_id, include_others, delay, dry_run)

    Thread(target=run, daemon=True).start()
    return jsonify({'success': True, 'message': 'Categorizacao dirigida iniciada'})


@app.route('/api/categorizer-targeted/stop', methods=['POST'])
def stop_categorization_targeted():
    if not categorizer_targeted_state['running']:
        return jsonify({'error': 'Nenhuma execucao em andamento'}), 400
    categorizer_targeted_state['running'] = False
    return jsonify({'success': True})


@app.route('/api/categorizer-targeted/status', methods=['GET'])
def categorizer_targeted_status():
    return jsonify({
        'running': categorizer_targeted_state['running'],
        'progress': categorizer_targeted_state['progress'],
        'current_product': categorizer_targeted_state['current_product']
    })


@app.route('/api/categorizer-targeted/logs', methods=['GET'])
def categorizer_targeted_logs_route():
    return jsonify({'logs': categorizer_targeted_state['logs']})


# ============================================================
# Rotas: Estatísticas diárias
# ============================================================
@app.route('/api/stats/daily', methods=['GET'])
def daily_stats_api():
    return jsonify({'success': True, 'today': get_today_stats(), 'all': _daily_stats_data})


# ============================================================
# WebSocket
# ============================================================
@socketio.on('connect')
def handle_connect():
    emit('renamer_status_update', {
        'running': automation_state['running'],
        'progress': automation_state['progress'],
        'current_product': automation_state['current_product']
    })
    emit('explorer_status_update', {
        'exploring': explorer_state['exploring'],
        'progress': explorer_state['progress'],
        'current_path': explorer_state['current_path']
    })
    emit('categorizer_status_update', {
        'running': categorizer_state['running'],
        'progress': categorizer_state['progress'],
        'current_product': categorizer_state['current_product']
    })
    emit('categorizer_targeted_status_update', {
        'running': categorizer_targeted_state['running'],
        'progress': categorizer_targeted_state['progress'],
        'current_product': categorizer_targeted_state['current_product']
    })
    emit('renamer_logs_update', {'logs': automation_state['logs']})
    emit('explorer_logs_update', {'logs': explorer_state['logs']})
    emit('categorizer_logs_update', {'logs': categorizer_state['logs']})
    emit('categorizer_targeted_logs_update', {'logs': categorizer_targeted_state['logs']})
    emit('daily_stats_update', get_today_stats())


@socketio.on('disconnect')
def handle_disconnect():
    pass


# ============================================================
# Inicializacao (modulo carregado por gunicorn ou diretamente)
# Roda em thread separada para que gunicorn possa fazer bind
# na porta antes do Firebase responder.
# ============================================================
_init_thread = Thread(target=init_all, daemon=True)
_init_thread.start()
_init_thread.join(timeout=5)  # Permite no maximo 5 segundos para inicializar
if _init_thread.is_alive():
    logger.warning("Inicializacao ainda em andamento (timeout). Servidor funcionara sem Firestore completo.")


# ============================================================
# Main (desenvolvimento local)
# ============================================================
if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_DEBUG', '0') == '1'
    socketio.run(app, host='0.0.0.0', port=port, debug=debug, allow_unsafe_werkzeug=True)